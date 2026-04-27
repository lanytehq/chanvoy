//! Bootstrap-state file handoff for sandbox-aware daemon startup (PER-014).
//!
//! Background: `chanvoy-daemon::start` historically called `whoami()` post-detach
//! to validate identity before binding the UDS socket. Under sandbox restrictions
//! (Codex agents, macOS sandboxd, Docker without `--network`, etc.) the detached
//! child has no interactive context to satisfy a network-approval prompt, so the
//! call fails and the daemon dies before bind. PER-014 moves identity validation
//! into the CLI parent (which already calls `whoami()` in
//! `validate_and_finalize_profile`) and hands the validated identity to the
//! detached child via a per-profile bootstrap-state file plus a one-shot env
//! nonce. The daemon reads, validates, consumes-and-deletes, then binds without
//! a network call.

use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{default_runtime_dir, CoreError, Profile};

/// Env var carrying the one-shot nonce from CLI parent to detached daemon child.
/// Anti-replay defense for the bootstrap-state file: a stale file from a prior
/// invocation has a stale nonce and will not match the current spawn's env.
pub const BOOTSTRAP_NONCE_ENV: &str = "CHANVOY_BOOTSTRAP_NONCE";

/// Maximum age of a bootstrap-state file the daemon will accept. Files older
/// than this are treated as stale (e.g., abandoned by a crashed parent or a
/// prior invocation that didn't clean up) and rejected with a clear diagnostic.
/// 60s is generous — auto-setup → daemon-spawn is sub-second in practice.
pub const BOOTSTRAP_MAX_AGE_SECS: u64 = 60;

/// Per-profile bootstrap-state file written by the CLI parent on the
/// daemon-spawn path and consumed by the daemon at start. Identity-only;
/// no token material.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BootstrapState {
    pub username: String,
    pub user_id: String,
    pub profile_name: String,
    pub profile_fingerprint: String,
    /// Unix epoch seconds at which the parent wrote this file.
    pub issued_at: u64,
    /// PID of the spawning CLI parent. Recorded for diagnostics; not used
    /// as the primary anti-replay signal — that's the nonce. With detach +
    /// `setsid` (PER-008D) the daemon may already be reparented to init by
    /// the time it reads this file, so process-tree walk is cross-platform
    /// flaky. See PER-014 brief Validation Strategy section.
    pub parent_pid: u32,
    /// One-shot anti-replay nonce. Mirrored to env (`BOOTSTRAP_NONCE_ENV`)
    /// at spawn time; daemon validates env nonce matches file nonce.
    pub nonce: String,
}

#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error("bootstrap-state file missing for profile {0}")]
    Missing(String),
    #[error("bootstrap-state file stale: issued_at {issued_at} is {age}s old (max {max}s)")]
    Stale { issued_at: u64, age: u64, max: u64 },
    #[error("bootstrap-state profile_fingerprint mismatch: file={file} computed={computed}")]
    FingerprintMismatch { file: String, computed: String },
    #[error("bootstrap-state nonce mismatch: env nonce does not match file nonce")]
    NonceMismatch,
    #[error("bootstrap-state nonce env var {0} not set")]
    NonceEnvMissing(&'static str),
    #[error("bootstrap-state username mismatch: file={file} profile={profile}")]
    UsernameMismatch { file: String, profile: String },
    #[error("bootstrap-state file io error: {0}")]
    Io(#[from] io::Error),
    #[error("bootstrap-state file deserialize error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("bootstrap-state file system clock before unix epoch")]
    ClockBeforeEpoch,
}

impl From<BootstrapError> for CoreError {
    fn from(err: BootstrapError) -> Self {
        CoreError::Io(io::Error::other(err.to_string()))
    }
}

/// Path to the bootstrap-state file for the given profile.
///
/// Lives alongside `<profile>.sock` and `<profile>.pid` in the runtime dir,
/// using the established `<profile>.<ext>` naming convention.
pub fn bootstrap_path_for_profile(profile: &str) -> PathBuf {
    default_runtime_dir().join(format!("{profile}.bootstrap.json"))
}

/// SHA-256 fingerprint over the canonical non-secret profile fields the
/// daemon relies on. Returns `"sha256:<hex>"`. The daemon recomputes this
/// after reloading the profile from disk and rejects on mismatch — catches
/// profile mutation between parent validation and daemon load.
///
/// Fields are concatenated with `\x1f` (unit separator) to avoid ambiguity
/// across values that might contain `:` or `/`.
pub fn compute_profile_fingerprint(profile: &Profile) -> String {
    let credential_mode = serde_json::to_string(&profile.credential_mode)
        .unwrap_or_else(|_| "\"env_name\"".to_string());
    let mut hasher = Sha256::new();
    let parts = [
        profile.name.as_str(),
        profile.role.as_str(),
        profile.scope.as_str(),
        profile.server_url.as_str(),
        profile.team_name.as_str(),
        profile.env_name.as_str(),
        credential_mode.as_str(),
        profile.bot_username.as_str(),
    ];
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            hasher.update([0x1f]);
        }
        hasher.update(part.as_bytes());
    }
    let digest = hasher.finalize();
    let mut out = String::with_capacity(7 + digest.len() * 2);
    out.push_str("sha256:");
    for byte in digest {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

/// Generate a fresh anti-replay nonce. UUID v4 is sufficient: ~122 bits of
/// entropy, single-use, deleted with the file. Not credential material — its
/// only job is to prove the file matches the current spawn.
pub fn generate_nonce() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn now_epoch_secs() -> Result<u64, BootstrapError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|_| BootstrapError::ClockBeforeEpoch)
}

/// Atomically write `state` to the per-profile bootstrap path with mode 0600
/// inside a 0700 runtime dir. Overwrites any existing file (a stale prior
/// bootstrap from a crashed parent gets replaced).
pub fn write_bootstrap_state(state: &BootstrapState) -> Result<PathBuf, BootstrapError> {
    let path = bootstrap_path_for_profile(&state.profile_name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    }
    let tmp_path = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(state)?;
    fs::write(&tmp_path, &bytes)?;
    fs::set_permissions(&tmp_path, fs::Permissions::from_mode(0o600))?;
    fs::rename(&tmp_path, &path)?;
    Ok(path)
}

/// Read the bootstrap-state file for the given profile. Returns `Ok(None)`
/// when the file does not exist (legacy / non-auto-setup spawn path); errors
/// only on actual io / deserialize failures.
pub fn read_bootstrap_state(profile: &str) -> Result<Option<BootstrapState>, BootstrapError> {
    let path = bootstrap_path_for_profile(profile);
    match fs::read(&path) {
        Ok(bytes) => {
            let state: BootstrapState = serde_json::from_slice(&bytes)?;
            Ok(Some(state))
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(BootstrapError::Io(err)),
    }
}

/// Delete the bootstrap-state file for the given profile. Idempotent — a
/// missing file is not an error. Called by the daemon after successful
/// validation (consume-and-delete) and on validation failure (cleanup of
/// poisoned state).
pub fn consume_bootstrap_state(profile: &str) -> Result<(), BootstrapError> {
    let path = bootstrap_path_for_profile(profile);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(BootstrapError::Io(err)),
    }
}

/// Validate `state` against the loaded `profile` and the spawn-time env nonce.
/// Runs all checks: freshness, profile fingerprint match, nonce match,
/// username match. On any failure, returns the specific error so the daemon
/// can surface a clear diagnostic.
///
/// `env_nonce` is read from `BOOTSTRAP_NONCE_ENV` by the caller; a missing
/// env var maps to `BootstrapError::NonceEnvMissing`.
pub fn validate_bootstrap_state(
    state: &BootstrapState,
    profile: &Profile,
    env_nonce: Option<&str>,
) -> Result<(), BootstrapError> {
    let now = now_epoch_secs()?;
    let age = now.saturating_sub(state.issued_at);
    if age > BOOTSTRAP_MAX_AGE_SECS {
        return Err(BootstrapError::Stale {
            issued_at: state.issued_at,
            age,
            max: BOOTSTRAP_MAX_AGE_SECS,
        });
    }
    let computed = compute_profile_fingerprint(profile);
    if computed != state.profile_fingerprint {
        return Err(BootstrapError::FingerprintMismatch {
            file: state.profile_fingerprint.clone(),
            computed,
        });
    }
    let env_nonce = env_nonce.ok_or(BootstrapError::NonceEnvMissing(BOOTSTRAP_NONCE_ENV))?;
    if env_nonce != state.nonce {
        return Err(BootstrapError::NonceMismatch);
    }
    if !profile.bot_username.is_empty() && profile.bot_username != state.username {
        return Err(BootstrapError::UsernameMismatch {
            file: state.username.clone(),
            profile: profile.bot_username.clone(),
        });
    }
    Ok(())
}

/// Build a fresh bootstrap state. Convenience for the parent-side write site
/// in `ensure_daemon_running`: caller passes validated `Identity` + `Profile`
/// + a freshly generated nonce, gets back a ready-to-write `BootstrapState`.
pub fn build_bootstrap_state(
    profile: &Profile,
    user_id: &str,
    nonce: &str,
    parent_pid: u32,
) -> Result<BootstrapState, BootstrapError> {
    Ok(BootstrapState {
        username: profile.bot_username.clone(),
        user_id: user_id.to_string(),
        profile_name: profile.name.clone(),
        profile_fingerprint: compute_profile_fingerprint(profile),
        issued_at: now_epoch_secs()?,
        parent_pid,
        nonce: nonce.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CapabilityClass, CredentialMode, Provider};

    fn sample_profile() -> Profile {
        Profile {
            name: "bravo-devlead-lanytehq".to_string(),
            role: "bravo-devlead".to_string(),
            scope: "lanytehq".to_string(),
            provider: Provider::Mattermost,
            bot_username: "agent-bravo-devlead".to_string(),
            team_name: "org-lanytehq".to_string(),
            server_url: "https://mm.3leaps.dev".to_string(),
            env_name: "LANYTE_MM_TOKEN".to_string(),
            env_file: None,
            credential_mode: CredentialMode::EnvName,
            capability_class: CapabilityClass::Standard,
            monitored_channels: Vec::new(),
            ipc: None,
        }
    }

    #[test]
    fn fingerprint_is_stable_for_unchanged_profile() {
        let p = sample_profile();
        assert_eq!(
            compute_profile_fingerprint(&p),
            compute_profile_fingerprint(&p)
        );
    }

    #[test]
    fn fingerprint_changes_when_canonical_field_changes() {
        let p = sample_profile();
        let mut q = p.clone();
        q.bot_username = "agent-other".to_string();
        assert_ne!(
            compute_profile_fingerprint(&p),
            compute_profile_fingerprint(&q),
        );
    }

    #[test]
    fn fingerprint_format_is_sha256_prefixed_hex() {
        let fp = compute_profile_fingerprint(&sample_profile());
        assert!(fp.starts_with("sha256:"), "fp={fp}");
        let hex = &fp["sha256:".len()..];
        assert_eq!(hex.len(), 64, "expected 64 hex chars, got {hex}");
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn validate_rejects_stale_state() {
        let p = sample_profile();
        let nonce = generate_nonce();
        let mut state = build_bootstrap_state(&p, "uid-1", &nonce, 12345).expect("build");
        // Backdate issued_at past the freshness window.
        state.issued_at = state.issued_at.saturating_sub(BOOTSTRAP_MAX_AGE_SECS + 5);
        let err = validate_bootstrap_state(&state, &p, Some(&nonce)).unwrap_err();
        assert!(matches!(err, BootstrapError::Stale { .. }), "got {err:?}");
    }

    #[test]
    fn validate_rejects_fingerprint_mismatch() {
        let p = sample_profile();
        let nonce = generate_nonce();
        let state = build_bootstrap_state(&p, "uid-1", &nonce, 12345).expect("build");
        let mut q = p.clone();
        q.bot_username = "agent-mutated".to_string();
        let err = validate_bootstrap_state(&state, &q, Some(&nonce)).unwrap_err();
        assert!(
            matches!(err, BootstrapError::FingerprintMismatch { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_rejects_nonce_mismatch() {
        let p = sample_profile();
        let nonce = generate_nonce();
        let state = build_bootstrap_state(&p, "uid-1", &nonce, 12345).expect("build");
        let err = validate_bootstrap_state(&state, &p, Some("wrong-nonce")).unwrap_err();
        assert!(matches!(err, BootstrapError::NonceMismatch), "got {err:?}");
    }

    #[test]
    fn validate_rejects_missing_nonce_env() {
        let p = sample_profile();
        let nonce = generate_nonce();
        let state = build_bootstrap_state(&p, "uid-1", &nonce, 12345).expect("build");
        let err = validate_bootstrap_state(&state, &p, None).unwrap_err();
        assert!(
            matches!(err, BootstrapError::NonceEnvMissing(_)),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_rejects_username_mismatch() {
        let p = sample_profile();
        let nonce = generate_nonce();
        let mut state = build_bootstrap_state(&p, "uid-1", &nonce, 12345).expect("build");
        state.username = "agent-impersonator".to_string();
        let err = validate_bootstrap_state(&state, &p, Some(&nonce)).unwrap_err();
        assert!(
            matches!(err, BootstrapError::UsernameMismatch { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn validate_accepts_fresh_matched_state() {
        let p = sample_profile();
        let nonce = generate_nonce();
        let state = build_bootstrap_state(&p, "uid-1", &nonce, 12345).expect("build");
        validate_bootstrap_state(&state, &p, Some(&nonce)).expect("valid");
    }

    #[test]
    fn read_returns_none_when_bootstrap_file_missing() {
        // Use a profile name that almost certainly has no on-disk
        // bootstrap-state file. CHANVOY_RUNTIME_DIR override would isolate
        // us further, but the per-profile filename is unique enough for
        // a unit smoke test.
        let unique = format!("test-missing-{}", uuid::Uuid::new_v4());
        let result = read_bootstrap_state(&unique).expect("read missing -> Ok(None)");
        assert!(
            result.is_none(),
            "expected None for missing file, got {result:?}"
        );
    }

    #[test]
    fn write_read_consume_roundtrip_under_runtime_override() {
        // Isolate from any real runtime dir on the test machine.
        let tmp = tempfile::tempdir().expect("tempdir");
        let original = std::env::var_os("CHANVOY_RUNTIME_DIR");
        // SAFETY: tests in this crate run sequentially within this module
        // by default; we restore the env at the end.
        std::env::set_var("CHANVOY_RUNTIME_DIR", tmp.path());

        let p = sample_profile();
        let nonce = generate_nonce();
        let state = build_bootstrap_state(&p, "uid-42", &nonce, 99_999).expect("build");
        let written_path = write_bootstrap_state(&state).expect("write");
        assert!(
            written_path.exists(),
            "bootstrap file should exist after write"
        );

        let loaded = read_bootstrap_state(&p.name)
            .expect("read")
            .expect("file present");
        assert_eq!(loaded, state, "roundtripped state must match original");

        consume_bootstrap_state(&p.name).expect("consume");
        assert!(
            !written_path.exists(),
            "bootstrap file must be deleted after consume"
        );

        // consume is idempotent — second call with file already gone is OK
        consume_bootstrap_state(&p.name).expect("consume idempotent");

        // Restore env
        if let Some(prev) = original {
            std::env::set_var("CHANVOY_RUNTIME_DIR", prev);
        } else {
            std::env::remove_var("CHANVOY_RUNTIME_DIR");
        }
    }

    #[test]
    fn bootstrap_path_for_profile_uses_dot_extension_convention() {
        let path = bootstrap_path_for_profile("alpha-foo-lanytehq");
        let filename = path.file_name().and_then(|s| s.to_str()).unwrap();
        assert_eq!(
            filename, "alpha-foo-lanytehq.bootstrap.json",
            "filename should follow `<profile>.<ext>` convention",
        );
    }
}
