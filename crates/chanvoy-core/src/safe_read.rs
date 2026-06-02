//! ADR-0016 safe agent-critical file reads.
//!
//! Centralizes the chanvoy-local implementation of the ADR-0016 portable
//! baseline for reading named files whose contents carry agent-critical
//! context (credentials, identity, control-plane state). PER-036's
//! `--message-file` reader is the original reference implementation; this
//! module generalizes the same baseline so the *other* chanvoy file reads
//! (credential `--env-file`, persisted profile/state/bootstrap reads) get
//! it consistently.
//!
//! Three trust tiers, per the ADR and PER-036A:
//!
//! - **Caller-named** ([`read_caller_named_file`]): the path comes from an
//!   operator/agent argument and may live in an untrusted location (shared
//!   `/tmp`). Fail CLOSED on a symlinked final component (`symlink_metadata`,
//!   no follow), refuse non-regular files, bound the read.
//! - **Credential** ([`read_credential_file`]): caller-named *and* carries
//!   secret material. Everything above, plus a tight cap and (on Unix)
//!   refusal of group/other-accessible files — token material in a
//!   loose-permission file is a leak even when it is not symlinked.
//! - **Tool-owned** ([`read_tool_owned_file`]): the path lives under a
//!   chanvoy-created config/runtime directory. Blanket symlink refusal would
//!   break legitimate dotfile / seclusor-materialized layouts, so we FOLLOW a
//!   symlink to a regular file but still refuse non-regular targets and bound
//!   the read.
//!
//! Scope is the ADR portable floor: final-component symlink handling only.
//! Intermediate-directory symlinks and the metadata→open TOCTOU race are
//! documented out-of-scope here; `O_NOFOLLOW`+`fstat` is the named Unix
//! hardening upgrade (tracked for identity-adjacent tools).
//!
//! Drift control (ADR §Drift Control): until the shared safe-read micro-crate
//! exists, the conformance fixtures in this module's `#[cfg(test)] mod tests`
//! are the non-driftable bar — every refusal class (symlink / FIFO / socket /
//! device / directory / oversize / loose-permission / non-UTF-8) is exercised
//! there. If the micro-crate lands, chanvoy adopts it and these move with it.

use std::io::Read;
use std::path::Path;

use thiserror::Error;

/// Default bounded-read cap for agent-critical content (matches PER-036's
/// `--message-file` resource guard). A resource guard, not a content policy.
pub const DEFAULT_MAX_BYTES: u64 = 4 * 1024 * 1024;

/// Tight cap for credential files (token env-files). Credential material is
/// tiny; a multi-KiB cap is generous while bounding a pathological input.
pub const CREDENTIAL_MAX_BYTES: u64 = 64 * 1024;

/// Refusal reasons for an ADR-0016 safe read. Each names the path and the
/// remediation; credential diagnostics never include file contents.
#[derive(Debug, Error)]
pub enum SafeReadError {
    #[error("file not found: {path}")]
    NotFound { path: String },
    #[error(
        "refusing to read symlinked input {path}; symlinks are not followed for \
         agent-critical inputs (ADR-0016) — pass the real (resolved) path"
    )]
    Symlink { path: String },
    #[error(
        "refusing to read {path}: not a regular file \
         (symlink target, FIFO, socket, device, or directory)"
    )]
    NonRegular { path: String },
    #[error("refusing to read {path}: {len} bytes over the {max}-byte cap")]
    TooLarge { path: String, len: u64, max: u64 },
    #[error(
        "refusing to read credential file {path}: it is group- or world-accessible \
         (mode {mode:04o}); restrict it to owner-only (chmod 600)"
    )]
    LoosePermissions { path: String, mode: u32 },
    #[error(
        "refusing to read tool-owned file {path}: its containing directory {dir} is not \
         private ({reason}); a writable or non-owned directory lets another user plant a \
         redirect. Ensure chanvoy's config/runtime dir is owner-owned and not \
         group/world-writable (chmod 700)"
    )]
    InsecureParentDir {
        path: String,
        dir: String,
        reason: String,
    },
    #[error("{path} is not valid UTF-8 text")]
    InvalidUtf8 { path: String },
    #[error("io error reading {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

impl SafeReadError {
    /// True for the absent-file case, so callers that treat "missing" as a
    /// non-error (default state, no active profile, no daemon) can branch.
    pub fn is_not_found(&self) -> bool {
        matches!(self, SafeReadError::NotFound { .. })
    }
}

fn display(path: &Path) -> String {
    path.display().to_string()
}

/// Caller-named agent-critical input — full ADR-0016 fail-closed baseline.
/// Refuses a symlinked final component, refuses non-regular files, bounds the
/// read at `max_bytes`. Use for paths supplied by an operator/agent argument.
pub fn read_caller_named_file(path: &Path, max_bytes: u64) -> Result<String, SafeReadError> {
    let meta = stat_no_follow(path)?;
    if meta.file_type().is_symlink() {
        return Err(SafeReadError::Symlink {
            path: display(path),
        });
    }
    if !meta.is_file() {
        return Err(SafeReadError::NonRegular {
            path: display(path),
        });
    }
    enforce_size(path, meta.len(), max_bytes)?;
    bounded_utf8(path, max_bytes)
}

/// Caller-named credential file — [`read_caller_named_file`] plus a tight cap
/// and, on Unix, refusal of group/other-accessible files (a token in a
/// loose-permission file is a leak even when the path is not symlinked).
pub fn read_credential_file(path: &Path, max_bytes: u64) -> Result<String, SafeReadError> {
    let meta = stat_no_follow(path)?;
    if meta.file_type().is_symlink() {
        return Err(SafeReadError::Symlink {
            path: display(path),
        });
    }
    if !meta.is_file() {
        return Err(SafeReadError::NonRegular {
            path: display(path),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode();
        if mode & 0o077 != 0 {
            return Err(SafeReadError::LoosePermissions {
                path: display(path),
                mode: mode & 0o7777,
            });
        }
    }
    enforce_size(path, meta.len(), max_bytes)?;
    bounded_utf8(path, max_bytes)
}

/// Tool-owned config/state read — the path lives under a chanvoy-created
/// directory, so a symlink to a regular file is allowed (legitimate dotfile /
/// seclusor layouts), but non-regular targets are refused and the read is
/// bounded. Follows symlinks via `metadata` (resolving to the target's type).
pub fn read_tool_owned_file(path: &Path, max_bytes: u64) -> Result<String, SafeReadError> {
    // PER-036A / ADR-0016 (devrev PR #39 finding #2): the tool-owned tier
    // FOLLOWS a symlink to a regular file, so the safety of the read rests on
    // the containing directory being private — if another user can write the
    // directory, they can plant a redirect that this tier would follow.
    // Verify the parent is operator-owned and not group/world-writable BEFORE
    // trusting the file. (`NotFound` is checked first so absent-file callers
    // still get their normal branch.)
    let meta = match std::fs::metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(SafeReadError::NotFound {
                path: display(path),
            });
        }
        Err(err) => {
            return Err(SafeReadError::Io {
                path: display(path),
                source: err,
            });
        }
    };
    verify_private_parent(path)?;
    if !meta.is_file() {
        return Err(SafeReadError::NonRegular {
            path: display(path),
        });
    }
    enforce_size(path, meta.len(), max_bytes)?;
    bounded_utf8(path, max_bytes)
}

/// Verify the file's containing directory is private enough to trust a
/// followed symlink: on Unix, owned by the current euid and not group- or
/// world-writable. A writable-by-others or non-owned directory is where a
/// redirect gets planted. No-op on non-Unix (the portable baseline does not
/// model Windows ACLs here; symlink creation there is privilege-gated).
#[cfg(unix)]
fn verify_private_parent(path: &Path) -> Result<(), SafeReadError> {
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::PermissionsExt;

    let dir = path.parent().filter(|p| !p.as_os_str().is_empty());
    let Some(dir) = dir else {
        return Ok(());
    };
    let meta = match std::fs::metadata(dir) {
        Ok(meta) => meta,
        // If the parent can't be stat'd, fall through — the file read itself
        // will surface the real error with the file path.
        Err(_) => return Ok(()),
    };
    let mode = meta.permissions().mode();
    if mode & 0o022 != 0 {
        return Err(SafeReadError::InsecureParentDir {
            path: display(path),
            dir: display(dir),
            reason: format!("group/world-writable, mode {:04o}", mode & 0o7777),
        });
    }
    // SAFETY: geteuid is a pure libc getter with no arguments and no memory
    // effects; it cannot fail.
    let euid = unsafe { libc::geteuid() };
    if meta.uid() != euid {
        return Err(SafeReadError::InsecureParentDir {
            path: display(path),
            dir: display(dir),
            reason: format!(
                "owned by uid {}, not the current user (uid {euid})",
                meta.uid()
            ),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_private_parent(_path: &Path) -> Result<(), SafeReadError> {
    Ok(())
}

/// `symlink_metadata` (does NOT follow the final component) with NotFound
/// distinguished, for the caller-named / credential tiers.
fn stat_no_follow(path: &Path) -> Result<std::fs::Metadata, SafeReadError> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => Ok(meta),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(SafeReadError::NotFound {
            path: display(path),
        }),
        Err(err) => Err(SafeReadError::Io {
            path: display(path),
            source: err,
        }),
    }
}

fn enforce_size(path: &Path, len: u64, max_bytes: u64) -> Result<(), SafeReadError> {
    if len > max_bytes {
        return Err(SafeReadError::TooLarge {
            path: display(path),
            len,
            max: max_bytes,
        });
    }
    Ok(())
}

/// Open and read at most `max_bytes`, decoding as UTF-8. `Take(max+1)` bounds
/// the read against a file that grows after the stat (TOCTOU) or any stream
/// surprise, so we never allocate without limit.
fn bounded_utf8(path: &Path, max_bytes: u64) -> Result<String, SafeReadError> {
    let file = std::fs::File::open(path).map_err(|err| SafeReadError::Io {
        path: display(path),
        source: err,
    })?;
    let mut bytes = Vec::new();
    file.take(max_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|err| SafeReadError::Io {
            path: display(path),
            source: err,
        })?;
    if bytes.len() as u64 > max_bytes {
        return Err(SafeReadError::TooLarge {
            path: display(path),
            len: bytes.len() as u64,
            max: max_bytes,
        });
    }
    String::from_utf8(bytes).map_err(|_| SafeReadError::InvalidUtf8 {
        path: display(path),
    })
}

#[cfg(test)]
mod tests {
    //! ADR-0016 §Drift Control conformance fixtures. Until the shared
    //! safe-read micro-crate exists, this is chanvoy's non-driftable bar:
    //! every refusal class (symlink / FIFO / socket / device / directory /
    //! oversize / loose-permission / non-UTF-8) must be exercised. Unix-only
    //! special-file fixtures are `#[cfg(unix)]` gated so a future Windows
    //! test job still compiles.

    use super::*;
    use std::io::Write;

    fn write_file(dir: &std::path::Path, name: &str, contents: &[u8]) -> std::path::PathBuf {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(contents).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        path
    }

    // ---- happy paths ----

    #[test]
    fn caller_named_reads_regular_file_verbatim() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "ok.md", b"hello\nworld\n");
        assert_eq!(
            read_caller_named_file(&path, DEFAULT_MAX_BYTES).unwrap(),
            "hello\nworld\n"
        );
    }

    #[test]
    fn credential_reads_owner_only_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "tok.env", b"LANYTE_MM_TOKEN=secret\n");
        assert_eq!(
            read_credential_file(&path, CREDENTIAL_MAX_BYTES).unwrap(),
            "LANYTE_MM_TOKEN=secret\n"
        );
    }

    #[test]
    fn tool_owned_reads_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "state.json", b"{}");
        assert_eq!(
            read_tool_owned_file(&path, DEFAULT_MAX_BYTES).unwrap(),
            "{}"
        );
    }

    // ---- private-parent precondition for tool-owned reads (devrev PR #39 #2) ----

    #[cfg(unix)]
    #[test]
    fn tool_owned_accepts_private_0700_dir() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let path = write_file(dir.path(), "ok.json", b"{}");
        assert_eq!(
            read_tool_owned_file(&path, DEFAULT_MAX_BYTES).unwrap(),
            "{}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn tool_owned_refuses_group_or_world_writable_dir() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "state.json", b"{}");
        // Loosen the containing dir to world-writable — a redirect could be
        // planted here, so the followed-symlink tier must refuse.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o777)).unwrap();
        let err = read_tool_owned_file(&path, DEFAULT_MAX_BYTES).unwrap_err();
        assert!(
            matches!(err, SafeReadError::InsecureParentDir { .. }),
            "world-writable parent dir must be refused; got {err:?}"
        );
        // Restore 0700 so TempDir cleanup is unsurprising.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }

    // ---- not found ----

    #[test]
    fn not_found_is_distinguishable_across_tiers() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope");
        assert!(read_caller_named_file(&missing, DEFAULT_MAX_BYTES)
            .unwrap_err()
            .is_not_found());
        assert!(read_credential_file(&missing, CREDENTIAL_MAX_BYTES)
            .unwrap_err()
            .is_not_found());
        assert!(read_tool_owned_file(&missing, DEFAULT_MAX_BYTES)
            .unwrap_err()
            .is_not_found());
    }

    // ---- refusal class: oversize (portable) ----

    #[test]
    fn oversize_is_refused_via_metadata_for_all_tiers() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "big", &[b'x'; 100]);
        assert!(matches!(
            read_caller_named_file(&path, 10),
            Err(SafeReadError::TooLarge { .. })
        ));
        assert!(matches!(
            read_credential_file(&path, 10),
            Err(SafeReadError::TooLarge { .. })
        ));
        assert!(matches!(
            read_tool_owned_file(&path, 10),
            Err(SafeReadError::TooLarge { .. })
        ));
    }

    // ---- refusal class: non-UTF-8 (portable) ----

    #[test]
    fn non_utf8_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "bin", &[0xff, 0xfe, 0x00, 0x80]);
        assert!(matches!(
            read_caller_named_file(&path, DEFAULT_MAX_BYTES),
            Err(SafeReadError::InvalidUtf8 { .. })
        ));
    }

    // ---- refusal class: directory (portable, non-regular) ----

    #[test]
    fn directory_is_refused_as_non_regular() {
        let dir = tempfile::tempdir().unwrap();
        // caller-named has no private-parent check, so the directory itself
        // is refused as non-regular regardless of where it lives.
        assert!(matches!(
            read_caller_named_file(dir.path(), DEFAULT_MAX_BYTES),
            Err(SafeReadError::NonRegular { .. })
        ));
        // tool-owned verifies the *parent* dir first. The directory-under-test
        // must therefore sit inside a private 0700 parent — otherwise on CI
        // (whose tempdir root is world-writable) the parent check fires first
        // and we'd see InsecureParentDir instead of NonRegular. Nest the
        // subject directory inside our own 0700 tempdir to isolate the
        // non-regular assertion. (The world-writable-parent path has its own
        // fixture: `tool_owned_refuses_group_or_world_writable_dir`.)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let subdir = dir.path().join("inner");
        std::fs::create_dir(&subdir).unwrap();
        assert!(matches!(
            read_tool_owned_file(&subdir, DEFAULT_MAX_BYTES),
            Err(SafeReadError::NonRegular { .. })
        ));
    }

    // ---- refusal class: symlink (Unix) ----

    #[cfg(unix)]
    #[test]
    fn caller_named_refuses_symlink_even_to_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        let target = write_file(dir.path(), "real", b"legit\n");
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(matches!(
            read_caller_named_file(&link, DEFAULT_MAX_BYTES),
            Err(SafeReadError::Symlink { .. })
        ));
        assert!(matches!(
            read_credential_file(&link, CREDENTIAL_MAX_BYTES),
            Err(SafeReadError::Symlink { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn tool_owned_follows_symlink_to_regular_file() {
        // Tool-owned tier deliberately ALLOWS a symlink to a regular file
        // (legitimate dotfile / seclusor layouts) — it follows and reads.
        let dir = tempfile::tempdir().unwrap();
        let target = write_file(dir.path(), "real.json", b"{\"ok\":true}");
        let link = dir.path().join("link.json");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert_eq!(
            read_tool_owned_file(&link, DEFAULT_MAX_BYTES).unwrap(),
            "{\"ok\":true}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn tool_owned_refuses_symlink_to_non_regular_target() {
        // A symlink whose target is a directory resolves (metadata follows)
        // to a non-regular type → refused.
        let dir = tempfile::tempdir().unwrap();
        let target_dir = dir.path().join("subdir");
        std::fs::create_dir(&target_dir).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&target_dir, &link).unwrap();
        assert!(matches!(
            read_tool_owned_file(&link, DEFAULT_MAX_BYTES),
            Err(SafeReadError::NonRegular { .. })
        ));
    }

    // ---- refusal class: FIFO (Unix) ----

    #[cfg(unix)]
    #[test]
    fn fifo_is_refused_as_non_regular() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fifo");
        let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).unwrap();
        // SAFETY: mkfifo takes a C path + mode and creates a FIFO inode; no
        // memory is shared and we check the return code.
        let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
        assert_eq!(rc, 0, "mkfifo failed");
        // Caller-named refuses it (symlink_metadata sees a non-regular,
        // non-symlink type). Critically, this must NOT block on open.
        assert!(matches!(
            read_caller_named_file(&path, DEFAULT_MAX_BYTES),
            Err(SafeReadError::NonRegular { .. })
        ));
        assert!(matches!(
            read_tool_owned_file(&path, DEFAULT_MAX_BYTES),
            Err(SafeReadError::NonRegular { .. })
        ));
    }

    // ---- refusal class: socket (Unix) ----

    #[cfg(unix)]
    #[test]
    fn socket_is_refused_as_non_regular() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sock");
        let _listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        assert!(matches!(
            read_caller_named_file(&path, DEFAULT_MAX_BYTES),
            Err(SafeReadError::NonRegular { .. })
        ));
    }

    // ---- refusal class: character device (Unix) ----

    #[cfg(unix)]
    #[test]
    fn char_device_is_refused_as_non_regular() {
        // /dev/null is a character device — a non-regular file.
        let path = std::path::Path::new("/dev/null");
        if path.exists() {
            assert!(matches!(
                read_caller_named_file(path, DEFAULT_MAX_BYTES),
                Err(SafeReadError::NonRegular { .. })
            ));
        }
    }

    // ---- credential-specific: loose permissions (Unix) ----

    #[cfg(unix)]
    #[test]
    fn credential_refuses_group_or_world_accessible() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(dir.path(), "loose.env", b"LANYTE_MM_TOKEN=secret\n");
        // 0644 → group/other readable.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let err = read_credential_file(&path, CREDENTIAL_MAX_BYTES).unwrap_err();
        assert!(
            matches!(err, SafeReadError::LoosePermissions { .. }),
            "0644 credential file must be refused; got {err:?}"
        );
        // The caller-named (non-credential) tier does NOT impose the perm
        // check — only the credential tier does.
        assert!(read_caller_named_file(&path, DEFAULT_MAX_BYTES).is_ok());
    }
}
