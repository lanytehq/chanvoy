//! Read-visibility diagnostics for `chanvoy doctor` (CHAN-TASK-003).
//!
//! Pure classification and report types live here so skew arithmetic can be
//! unit-tested without a daemon or token. Network probes that feed these
//! types sit on [`crate::MattermostClient`].

use serde::{Deserialize, Serialize};

/// Bound (ms) inside which |mid − server| − RTT/2 is treated as healthy noise.
pub const CLOCK_HEALTHY_BOUND_MS: i64 = 5_000;

/// Minimum residual skew (ms) after subtracting RTT/2 before a suspected
/// ahead/behind verdict is emitted. Below this, evidence is too thin.
pub const CLOCK_SUSPECT_THRESHOLD_MS: i64 = 30_000;

/// Per-check outcome for a doctor probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckVerdict {
    Pass,
    Warn,
    Fail,
    Unavailable,
}

/// Clock skew classification (evidence-bearing vocabulary).
///
/// Bands (residual after RTT/2 allowance):
/// * `healthy` — residual ≤ [`CLOCK_HEALTHY_BOUND_MS`] (5s noise)
/// * `elevated_*` — residual in (5s, 30s] — soft non-healthy (FIX-2)
/// * `suspected_*` — residual > [`CLOCK_SUSPECT_THRESHOLD_MS`] (30s)
/// * `unavailable` — no trustworthy server observation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClockVerdict {
    Healthy,
    /// Residual above noise and at or under the suspicion threshold; host
    /// appears ahead. Soft finding — not pass/exit 0 (FIX-2).
    ElevatedAhead,
    /// Same mid-band, host appears behind.
    ElevatedBehind,
    SuspectedAhead,
    SuspectedBehind,
    Unavailable,
}

/// How server time was observed (or why it was not).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServerTimeSource {
    /// HTTP `Date` response header on an authenticated provider GET.
    HttpDate,
}

/// Raw observation used as input to [`classify_clock_skew`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerTimeObservation {
    pub local_before_ms: i64,
    pub local_after_ms: i64,
    pub local_mid_ms: i64,
    pub rtt_ms: i64,
    /// Parsed HTTP Date as Unix epoch milliseconds, when present and valid.
    pub server_ms: Option<i64>,
    pub source: ServerTimeSource,
    pub date_header_present: bool,
    pub date_header_parse_ok: bool,
    /// Why server_ms is None (no provider bodies).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
}

/// Clock check block in the doctor report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockCheck {
    pub verdict: ClockVerdict,
    pub check: CheckVerdict,
    /// local_mid − server (positive ⇒ host ahead of server). Present only
    /// when a server observation was available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delta_ms: Option<i64>,
    pub local_mid_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_ms: Option<i64>,
    pub rtt_ms: i64,
    pub source: ServerTimeSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Operator pointer when skew can empty short `--since` windows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guidance: Option<String>,
}

/// Parse an HTTP `Date` header value (RFC 7231 / RFC 1123) to Unix millis.
///
/// Returns `None` when the string is empty or not a valid HTTP-date.
pub fn parse_http_date_ms(value: &str) -> Option<i64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    // HTTP-date is a subset of RFC 2822; chrono accepts it.
    chrono::DateTime::parse_from_rfc2822(trimmed)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

/// Classify local mid-time vs a server observation.
///
/// * `delta = local_mid − server` — positive means the host is ahead.
/// * Residual after subtracting `RTT/2`:
///   - ≤ [`CLOCK_HEALTHY_BOUND_MS`] → `healthy`
///   - (healthy, [`CLOCK_SUSPECT_THRESHOLD_MS`]] → `elevated_*` (soft; not pass)
///   - \> suspect threshold → `suspected_*`
/// * Missing server time ⇒ `unavailable` (never a green skew verdict).
///
/// Does **not** invent clock as the explanation for a missing post that
/// sits at or after an emitted `?since=` boundary (CHAN-TASK-002).
pub fn classify_clock_skew(
    local_mid_ms: i64,
    server_ms: Option<i64>,
    rtt_ms: i64,
) -> (ClockVerdict, Option<i64>, Option<String>) {
    let Some(server_ms) = server_ms else {
        return (
            ClockVerdict::Unavailable,
            None,
            Some("no trustworthy server-time observation".into()),
        );
    };
    let delta = local_mid_ms - server_ms;
    let uncertainty = rtt_ms.saturating_abs() / 2;
    let residual = delta.abs().saturating_sub(uncertainty);

    if residual <= CLOCK_HEALTHY_BOUND_MS {
        return (ClockVerdict::Healthy, Some(delta), None);
    }
    if residual > CLOCK_SUSPECT_THRESHOLD_MS {
        if delta > 0 {
            return (
                ClockVerdict::SuspectedAhead,
                Some(delta),
                Some(format!(
                    "local clock appears ahead of server by ~{residual}ms after RTT allowance; short --since windows can return empty while post-id catch-up still works"
                )),
            );
        }
        return (
            ClockVerdict::SuspectedBehind,
            Some(delta),
            Some(format!(
                "local clock appears behind server by ~{residual}ms after RTT allowance"
            )),
        );
    }
    // Mid-band (FIX-2): residual above the healthy noise bound and at or
    // under the suspicion threshold. This is *not* healthy — a 15s residual
    // can empty a 5s/10s --since window — but it is not yet full suspicion.
    if delta > 0 {
        return (
            ClockVerdict::ElevatedAhead,
            Some(delta),
            Some(format!(
                "local clock residual ~{residual}ms ahead of server (above {CLOCK_HEALTHY_BOUND_MS}ms noise, at or under {CLOCK_SUSPECT_THRESHOLD_MS}ms suspicion); short --since windows may return empty"
            )),
        );
    }
    (
        ClockVerdict::ElevatedBehind,
        Some(delta),
        Some(format!(
            "local clock residual ~{residual}ms behind server (above {CLOCK_HEALTHY_BOUND_MS}ms noise, at or under {CLOCK_SUSPECT_THRESHOLD_MS}ms suspicion)"
        )),
    )
}

/// Build a [`ClockCheck`] from a raw observation.
pub fn clock_check_from_observation(obs: &ServerTimeObservation) -> ClockCheck {
    let (verdict, delta_ms, reason) =
        classify_clock_skew(obs.local_mid_ms, obs.server_ms, obs.rtt_ms);
    let reason = reason.or_else(|| obs.unavailable_reason.clone());
    let check = match verdict {
        ClockVerdict::Healthy => CheckVerdict::Pass,
        ClockVerdict::ElevatedAhead
        | ClockVerdict::ElevatedBehind
        | ClockVerdict::SuspectedAhead
        | ClockVerdict::SuspectedBehind => CheckVerdict::Warn,
        ClockVerdict::Unavailable => CheckVerdict::Unavailable,
    };
    let guidance = match verdict {
        ClockVerdict::ElevatedAhead | ClockVerdict::SuspectedAhead => Some(
            "For catch-up use: chanvoy check <channel> --json  then  chanvoy read <channel> --after <anchor>. Fix host time sync; chanvoy cannot compensate for a wrong clock.".into(),
        ),
        _ => None,
    };
    ClockCheck {
        verdict,
        check,
        delta_ms,
        local_mid_ms: obs.local_mid_ms,
        server_ms: obs.server_ms,
        rtt_ms: obs.rtt_ms,
        source: obs.source.clone(),
        reason,
        guidance,
    }
}

/// Build a [`ServerTimeObservation`] from timestamps and an optional Date header.
pub fn observe_server_time_from_date_header(
    local_before_ms: i64,
    local_after_ms: i64,
    date_header: Option<&str>,
) -> ServerTimeObservation {
    let rtt_ms = (local_after_ms - local_before_ms).max(0);
    let local_mid_ms = local_before_ms + rtt_ms / 2;
    let present = date_header.is_some_and(|s| !s.trim().is_empty());
    let parsed = date_header.and_then(parse_http_date_ms);
    let parse_ok = parsed.is_some();
    let unavailable_reason = if !present {
        Some("provider response omitted Date header".into())
    } else if !parse_ok {
        Some("provider Date header was not a valid HTTP-date".into())
    } else {
        None
    };
    ServerTimeObservation {
        local_before_ms,
        local_after_ms,
        local_mid_ms,
        rtt_ms,
        server_ms: parsed,
        source: ServerTimeSource::HttpDate,
        date_header_present: present,
        date_header_parse_ok: parse_ok,
        unavailable_reason,
    }
}

/// Map a provider HTTP status to a short class label for doctor output.
///
/// Never returns the provider body. Distinguishes auth/throttle from other
/// failures without guessing "permission denied" for every error.
pub fn provider_status_class(status: u16) -> &'static str {
    match status {
        401 | 403 => "credential_or_forbidden",
        404 => "not_found",
        429 => "throttled",
        _ if (500..600).contains(&status) => "server_error",
        _ => "provider_error",
    }
}

/// Overall process exit code for a doctor run.
///
/// * **0** — every check pass (or healthy)
/// * **1** — soft findings (warn / suspected skew / unavailable clock only)
/// * **2** — hard failure (identity fail, channel hard fail, daemon hard fail)
pub fn doctor_exit_code(checks: &[CheckVerdict], hard_failure: bool) -> i32 {
    if hard_failure || checks.contains(&CheckVerdict::Fail) {
        return 2;
    }
    if checks
        .iter()
        .any(|c| matches!(c, CheckVerdict::Warn | CheckVerdict::Unavailable))
    {
        return 1;
    }
    0
}

/// Socket-access classification for `chanvoy doctor` sandbox diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SocketAccessStatus {
    Reachable,
    Absent,
    Denied,
    Unreachable,
}

impl SocketAccessStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::Reachable => "reachable",
            Self::Absent => "absent",
            Self::Denied => "denied",
            Self::Unreachable => "unreachable",
        }
    }
}

/// Read-only observation of local daemon socket reachability and sandbox access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SocketAccessObservation {
    pub status: SocketAccessStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guidance: Option<String>,
}

/// Capability classification for native runtime probes in `chanvoy doctor`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeCapabilityStatus {
    Supported,
    Denied,
    IncompleteOrChanging,
    Unavailable,
}

impl NativeCapabilityStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Denied => "denied",
            Self::IncompleteOrChanging => "incomplete_or_changing",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Read-only observation of a single native runtime capability.
///
/// `qualification` is always `false`: a passing diagnostic observation never
/// certifies host deployment qualification or authorizes daemon retirement,
/// socket reaping, or restart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeCapabilityObservation {
    pub status: NativeCapabilityStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub qualification: bool,
}

/// Combined sandbox-context and native runtime capability diagnostics block
/// for `chanvoy doctor`.
///
/// Reported separately from daemon health, credential identity, and restart
/// ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeRuntimeDiagnostics {
    pub qualification: bool,
    pub socket_access: SocketAccessObservation,
    pub process_inventory: NativeCapabilityObservation,
    pub listener_table: NativeCapabilityObservation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guidance: Option<String>,
}

const SANDBOX_SOCKET_GUIDANCE: &str = "run chanvoy with sandbox escalation/non-sandbox execution, grant the sandbox read/write access to the runtime directory, or set CHANVOY_RUNTIME_DIR to a mutually reachable private directory (see docs/getting-started.md#sandboxed-agents)";
const SANDBOX_CAPABILITY_GUIDANCE: &str = "sandbox restricts native process-inventory or kernel socket-table inspection; ordinary daemon RPCs work when the socket is reachable, while native lifecycle recovery and socket reaping require an unsandboxed or escalated host shell (see docs/getting-started.md#sandboxed-agents)";

#[cfg(unix)]
fn is_io_permission_denied(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::PermissionDenied
        || matches!(err.raw_os_error(), Some(libc::EPERM | libc::EACCES))
}

#[cfg(not(unix))]
fn is_io_permission_denied(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::PermissionDenied
}

fn is_incomplete_capability_reason(reason: &str) -> bool {
    reason.contains("incomplete")
        || reason.contains("truncated")
        || reason.contains("overflow")
        || reason.contains("size/cap uncertainty")
        || reason.contains("size budget")
        || reason.contains("trailer missing")
        || reason.contains("deadline")
}

/// Map a native runtime capability probe result to a redacted [`NativeCapabilityObservation`].
#[cfg(unix)]
pub fn classify_native_capability(
    result: Result<(), &crate::runtime::RuntimeError>,
) -> NativeCapabilityObservation {
    use crate::runtime::RuntimeError;
    match result {
        Ok(()) => NativeCapabilityObservation {
            status: NativeCapabilityStatus::Supported,
            reason: None,
            qualification: false,
        },
        Err(RuntimeError::Changed(reason)) => NativeCapabilityObservation {
            status: NativeCapabilityStatus::IncompleteOrChanging,
            reason: Some((*reason).to_string()),
            qualification: false,
        },
        Err(RuntimeError::Unavailable(reason)) => {
            let status = if reason.contains("permission denied") {
                NativeCapabilityStatus::Denied
            } else if is_incomplete_capability_reason(reason) {
                NativeCapabilityStatus::IncompleteOrChanging
            } else {
                NativeCapabilityStatus::Unavailable
            };
            NativeCapabilityObservation {
                status,
                reason: Some((*reason).to_string()),
                qualification: false,
            }
        }
        Err(RuntimeError::Invalid(reason)) => NativeCapabilityObservation {
            status: NativeCapabilityStatus::Unavailable,
            reason: Some((*reason).to_string()),
            qualification: false,
        },
        Err(RuntimeError::Locked) => NativeCapabilityObservation {
            status: NativeCapabilityStatus::Unavailable,
            reason: Some("runtime namespace is already locked".into()),
            qualification: false,
        },
        Err(RuntimeError::PublicationUncertain) => NativeCapabilityObservation {
            status: NativeCapabilityStatus::Unavailable,
            reason: Some("runtime record publication completion is uncertain".into()),
            qualification: false,
        },
        Err(RuntimeError::Io(err)) => {
            if is_io_permission_denied(err) {
                NativeCapabilityObservation {
                    status: NativeCapabilityStatus::Denied,
                    reason: Some("native runtime I/O permission denied".into()),
                    qualification: false,
                }
            } else {
                NativeCapabilityObservation {
                    status: NativeCapabilityStatus::Unavailable,
                    reason: Some("native runtime I/O failed".into()),
                    qualification: false,
                }
            }
        }
    }
}

fn denied_socket_observation(reason: &str) -> SocketAccessObservation {
    SocketAccessObservation {
        status: SocketAccessStatus::Denied,
        reason: Some(reason.to_string()),
        guidance: Some(SANDBOX_SOCKET_GUIDANCE.into()),
    }
}

#[cfg(unix)]
fn nonblocking_unix_connect_probe(socket_path: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;

    let bytes = socket_path.as_os_str().as_bytes();
    if bytes.is_empty() || bytes.contains(&0) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid Unix socket path",
        ));
    }
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    if bytes.len() >= addr.sun_path.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Unix socket path exceeds sockaddr_un limit",
        ));
    }
    unsafe {
        std::ptr::copy_nonoverlapping(
            bytes.as_ptr().cast::<libc::c_char>(),
            addr.sun_path.as_mut_ptr(),
            bytes.len(),
        );
    }
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let result = (|| {
        let fd_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if fd_flags >= 0 {
            let _ = unsafe { libc::fcntl(fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC) };
        }
        let fl_flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if fl_flags < 0 {
            return Err(std::io::Error::last_os_error());
        }
        if unsafe { libc::fcntl(fd, libc::F_SETFL, fl_flags | libc::O_NONBLOCK) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let rc = unsafe {
            libc::connect(
                fd,
                (&addr as *const libc::sockaddr_un).cast::<libc::sockaddr>(),
                std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
            )
        };
        if rc == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    })();
    unsafe {
        libc::close(fd);
    }
    result
}

/// Inspect an unreachable socket path without creating or mutating any files.
pub fn inspect_unreachable_socket_path(socket_path: &std::path::Path) -> SocketAccessObservation {
    match std::fs::symlink_metadata(socket_path) {
        Err(err) if is_io_permission_denied(&err) => denied_socket_observation(
            "permission denied inspecting daemon socket path; sandbox may restrict the runtime directory",
        ),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = socket_path.parent() {
                if let Err(parent_err) = std::fs::metadata(parent) {
                    if is_io_permission_denied(&parent_err) {
                        return denied_socket_observation(
                            "permission denied inspecting daemon runtime directory; sandbox may restrict the runtime directory",
                        );
                    }
                }
            }
            SocketAccessObservation {
                status: SocketAccessStatus::Absent,
                reason: Some("no daemon socket present for this profile".into()),
                guidance: None,
            }
        }
        Err(_) => SocketAccessObservation {
            status: SocketAccessStatus::Unreachable,
            reason: Some("daemon socket path metadata could not be inspected".into()),
            guidance: None,
        },
        Ok(meta) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileTypeExt;
                if !meta.file_type().is_socket() {
                    return SocketAccessObservation {
                        status: SocketAccessStatus::Unreachable,
                        reason: Some(
                            "configured socket path exists but is not a Unix domain socket".into(),
                        ),
                        guidance: None,
                    };
                }
                match nonblocking_unix_connect_probe(socket_path) {
                    Err(err) if is_io_permission_denied(&err) => denied_socket_observation(
                        "permission denied connecting to daemon socket; sandbox or filesystem permissions blocked Unix domain socket connect",
                    ),
                    Err(err) if err.kind() == std::io::ErrorKind::ConnectionRefused => {
                        SocketAccessObservation {
                            status: SocketAccessStatus::Unreachable,
                            reason: Some(
                                "daemon socket exists on disk but connection was refused".into(),
                            ),
                            guidance: None,
                        }
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                        SocketAccessObservation {
                            status: SocketAccessStatus::Absent,
                            reason: Some("no daemon socket present for this profile".into()),
                            guidance: None,
                        }
                    }
                    Ok(()) => SocketAccessObservation {
                        status: SocketAccessStatus::Unreachable,
                        reason: Some(
                            "socket accepted a connection, but the local daemon RPC did not respond within its local budget".into(),
                        ),
                        guidance: None,
                    },
                    Err(_) => SocketAccessObservation {
                        status: SocketAccessStatus::Unreachable,
                        reason: Some(
                            "local daemon did not respond within its local budget".into(),
                        ),
                        guidance: None,
                    },
                }
            }
            #[cfg(not(unix))]
            {
                let _ = meta;
                SocketAccessObservation {
                    status: SocketAccessStatus::Unreachable,
                    reason: Some("local daemon did not respond within its local budget".into()),
                    guidance: None,
                }
            }
        }
    }
}

/// Observe local socket reachability and read-only native runtime capabilities
/// for `chanvoy doctor`.
#[cfg(unix)]
pub fn observe_native_runtime_diagnostics(
    profile_name: &str,
    daemon_reachable: bool,
    inventory_deadline: std::time::Instant,
) -> NativeRuntimeDiagnostics {
    let socket_access = if daemon_reachable {
        SocketAccessObservation {
            status: SocketAccessStatus::Reachable,
            reason: None,
            guidance: None,
        }
    } else {
        match crate::runtime::RuntimeBinding::resolve_profile(profile_name) {
            Ok(Some(binding)) => inspect_unreachable_socket_path(binding.socket_path()),
            Ok(None) => SocketAccessObservation {
                status: SocketAccessStatus::Absent,
                reason: Some("no active daemon socket; runtime record is exited".into()),
                guidance: None,
            },
            Err(ref err) => {
                let classified = classify_native_capability(Err(err));
                let fallback =
                    inspect_unreachable_socket_path(&crate::socket_path_for_profile(profile_name));
                if fallback.status == SocketAccessStatus::Denied {
                    fallback
                } else if classified.status == NativeCapabilityStatus::Denied {
                    denied_socket_observation(
                        "permission denied resolving native runtime authority or socket path; sandbox may restrict the home or runtime directory",
                    )
                } else {
                    SocketAccessObservation {
                        status: SocketAccessStatus::Unreachable,
                        reason: Some(
                            classified
                                .reason
                                .unwrap_or_else(|| "native runtime binding unavailable".into()),
                        ),
                        guidance: None,
                    }
                }
            }
        }
    };
    let process_inventory =
        match crate::runtime::NativeInventory::observe_capability(inventory_deadline) {
            Ok(()) => classify_native_capability(Ok(())),
            Err(ref err) => classify_native_capability(Err(err)),
        };
    let listener_table = {
        let probe = crate::runtime::PathBytes::from_path(std::path::Path::new(
            "/chanvoy-doctor-capability-probe",
        ));
        match probe.and_then(|p| crate::runtime::observe_listener_table(&p)) {
            Ok(_) => classify_native_capability(Ok(())),
            Err(ref err) => classify_native_capability(Err(err)),
        }
    };
    let guidance = if socket_access.status == SocketAccessStatus::Denied {
        socket_access.guidance.clone()
    } else if process_inventory.status == NativeCapabilityStatus::Denied
        || listener_table.status == NativeCapabilityStatus::Denied
    {
        Some(SANDBOX_CAPABILITY_GUIDANCE.into())
    } else {
        None
    };
    NativeRuntimeDiagnostics {
        qualification: false,
        socket_access,
        process_inventory,
        listener_table,
        guidance,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_http_date_rfc1123() {
        // Standard GMT form used by most servers. Day-of-week must match
        // the calendar date (chrono rejects Impossible weekday mismatches).
        let ms = parse_http_date_ms("Tue, 11 Aug 2026 12:00:00 GMT").expect("parse");
        assert!(ms > 0);
    }

    #[test]
    fn parse_http_date_rejects_garbage() {
        assert!(parse_http_date_ms("").is_none());
        assert!(parse_http_date_ms("not-a-date").is_none());
    }

    #[test]
    fn classify_unavailable_without_server() {
        let (v, d, r) = classify_clock_skew(1_000_000, None, 50);
        assert_eq!(v, ClockVerdict::Unavailable);
        assert!(d.is_none());
        assert!(r.is_some());
    }

    #[test]
    fn classify_healthy_within_noise() {
        let server = 1_000_000;
        let (v, d, _) = classify_clock_skew(server + 2_000, Some(server), 100);
        assert_eq!(v, ClockVerdict::Healthy);
        assert_eq!(d, Some(2_000));
    }

    #[test]
    fn classify_suspected_ahead_above_threshold() {
        let server = 1_000_000;
        // 60s ahead, tiny RTT → residual >> 30s
        let (v, d, reason) = classify_clock_skew(server + 60_000, Some(server), 200);
        assert_eq!(v, ClockVerdict::SuspectedAhead);
        assert_eq!(d, Some(60_000));
        assert!(reason.unwrap().contains("ahead"));
    }

    #[test]
    fn classify_suspected_behind_above_threshold() {
        let server = 1_000_000;
        let (v, d, reason) = classify_clock_skew(server - 90_000, Some(server), 200);
        assert_eq!(v, ClockVerdict::SuspectedBehind);
        assert_eq!(d, Some(-90_000));
        assert!(reason.unwrap().contains("behind"));
    }

    #[test]
    fn classify_rtt_eats_into_residual() {
        let server = 1_000_000;
        // 40s raw delta but 30s RTT → uncertainty 15s → residual 25s → elevated mid-band
        let (v, _, _) = classify_clock_skew(server + 40_000, Some(server), 30_000);
        assert_eq!(v, ClockVerdict::ElevatedAhead);
    }

    #[test]
    fn classify_mid_band_ahead_is_elevated_not_healthy() {
        let server = 1_000_000;
        // 15s residual: above 5s noise, at/under 30s suspect → elevated_ahead (FIX-2)
        let (v, d, reason) = classify_clock_skew(server + 15_000, Some(server), 0);
        assert_eq!(v, ClockVerdict::ElevatedAhead);
        assert_eq!(d, Some(15_000));
        assert!(reason.unwrap().contains("short --since"));
        let check = clock_check_from_observation(&ServerTimeObservation {
            local_before_ms: server + 15_000,
            local_after_ms: server + 15_000,
            local_mid_ms: server + 15_000,
            rtt_ms: 0,
            server_ms: Some(server),
            source: ServerTimeSource::HttpDate,
            date_header_present: true,
            date_header_parse_ok: true,
            unavailable_reason: None,
        });
        assert_eq!(check.check, CheckVerdict::Warn);
        assert!(check.guidance.as_ref().unwrap().contains("--after"));
    }

    #[test]
    fn classify_mid_band_behind_is_elevated_not_healthy() {
        let server = 1_000_000;
        let (v, d, reason) = classify_clock_skew(server - 15_000, Some(server), 0);
        assert_eq!(v, ClockVerdict::ElevatedBehind);
        assert_eq!(d, Some(-15_000));
        assert!(reason.unwrap().contains("behind"));
        assert_eq!(
            doctor_exit_code(&[CheckVerdict::Warn], false),
            1,
            "elevated mid-band must soft-fail exit"
        );
    }

    #[test]
    fn threshold_edge_exactly_suspect_bound() {
        let server = 1_000_000;
        // residual == threshold → elevated (at or under suspicion); +1 → suspected
        let (v_eq, _, _) =
            classify_clock_skew(server + CLOCK_SUSPECT_THRESHOLD_MS, Some(server), 0);
        assert_eq!(v_eq, ClockVerdict::ElevatedAhead);
        let (v_over, _, _) =
            classify_clock_skew(server + CLOCK_SUSPECT_THRESHOLD_MS + 1, Some(server), 0);
        assert_eq!(v_over, ClockVerdict::SuspectedAhead);
    }

    #[test]
    fn healthy_bound_edge_stays_healthy() {
        let server = 1_000_000;
        let (v, _, _) = classify_clock_skew(server + CLOCK_HEALTHY_BOUND_MS, Some(server), 0);
        assert_eq!(v, ClockVerdict::Healthy);
        let (v2, _, _) = classify_clock_skew(server + CLOCK_HEALTHY_BOUND_MS + 1, Some(server), 0);
        assert_eq!(v2, ClockVerdict::ElevatedAhead);
    }

    #[test]
    fn observe_missing_date_unavailable() {
        let obs = observe_server_time_from_date_header(100, 150, None);
        assert!(!obs.date_header_present);
        assert!(obs.server_ms.is_none());
        let check = clock_check_from_observation(&obs);
        assert_eq!(check.verdict, ClockVerdict::Unavailable);
        assert_eq!(check.check, CheckVerdict::Unavailable);
        assert!(check.guidance.is_none());
    }

    #[test]
    fn observe_valid_date_and_ahead_guidance() {
        // Fixed Date: use a known parseable header and fabricate local mid far ahead.
        let date = "Tue, 11 Aug 2026 12:00:00 GMT";
        let server = parse_http_date_ms(date).unwrap();
        let obs =
            observe_server_time_from_date_header(server + 120_000, server + 120_100, Some(date));
        assert!(obs.date_header_parse_ok);
        let check = clock_check_from_observation(&obs);
        assert_eq!(check.verdict, ClockVerdict::SuspectedAhead);
        assert_eq!(check.check, CheckVerdict::Warn);
        let guidance = check.guidance.as_ref().expect("suspected_ahead guidance");
        assert!(
            guidance.contains("--after") && guidance.contains("check"),
            "guidance should point at catch-up SOP, got {guidance}"
        );
    }

    #[test]
    fn doctor_exit_codes() {
        assert_eq!(doctor_exit_code(&[CheckVerdict::Pass], false), 0);
        assert_eq!(doctor_exit_code(&[CheckVerdict::Warn], false), 1);
        assert_eq!(doctor_exit_code(&[CheckVerdict::Unavailable], false), 1);
        assert_eq!(doctor_exit_code(&[CheckVerdict::Fail], false), 2);
        assert_eq!(doctor_exit_code(&[CheckVerdict::Pass], true), 2);
    }

    #[test]
    fn provider_status_class_table() {
        assert_eq!(provider_status_class(401), "credential_or_forbidden");
        assert_eq!(provider_status_class(403), "credential_or_forbidden");
        assert_eq!(provider_status_class(404), "not_found");
        assert_eq!(provider_status_class(429), "throttled");
        assert_eq!(provider_status_class(500), "server_error");
        assert_eq!(provider_status_class(418), "provider_error");
    }

    #[cfg(unix)]
    #[test]
    fn native_capability_classifier_distinguishes_supported_denied_changing_and_unavailable() {
        use crate::runtime::RuntimeError;

        let supported = classify_native_capability(Ok(()));
        assert_eq!(supported.status, NativeCapabilityStatus::Supported);
        assert!(supported.reason.is_none());
        assert!(!supported.qualification);

        for denied_reason in [
            "kernel Unix socket netns permission denied",
            "kernel Unix socket table permission denied",
            "native arguments permission denied",
            "native executable path permission denied",
            "native inventory PID list permission denied",
            "native inventory init view permission denied",
            "native inventory mountinfo permission denied",
            "native inventory namespace permission denied",
            "native inventory owner permission denied",
            "native inventory procfs permission denied",
            "native inventory self link permission denied",
            "native namespace permission denied",
            "native process birth/owner permission denied",
            "native relevance birth/owner permission denied",
        ] {
            let obs = classify_native_capability(Err(&RuntimeError::Unavailable(denied_reason)));
            assert_eq!(
                obs.status,
                NativeCapabilityStatus::Denied,
                "{denied_reason}"
            );
            assert_eq!(obs.reason.as_deref(), Some(denied_reason));
            assert!(!obs.qualification);
        }

        let io_denied = classify_native_capability(Err(&RuntimeError::Io(
            std::io::Error::from_raw_os_error(libc::EPERM),
        )));
        assert_eq!(io_denied.status, NativeCapabilityStatus::Denied);
        assert!(!io_denied.qualification);

        for changing in [
            RuntimeError::Changed("socket table generation/envelope"),
            RuntimeError::Unavailable("kernel Unix socket table overflow"),
            RuntimeError::Unavailable("kernel Unix socket table size budget"),
            RuntimeError::Unavailable("native UID list size/cap uncertainty"),
            RuntimeError::Unavailable("native arguments empty or incomplete"),
            RuntimeError::Unavailable("native executable path incomplete"),
            RuntimeError::Unavailable("native inventory deadline"),
            RuntimeError::Unavailable("native inventory truncated"),
            RuntimeError::Unavailable("native namespace incomplete"),
            RuntimeError::Unavailable("native process birth/owner incomplete"),
            RuntimeError::Unavailable("procfs metadata truncated"),
            RuntimeError::Unavailable("socket table incomplete envelope"),
            RuntimeError::Unavailable("socket table trailer missing"),
            RuntimeError::Unavailable("socket table truncated row"),
        ] {
            let obs = classify_native_capability(Err(&changing));
            assert_eq!(
                obs.status,
                NativeCapabilityStatus::IncompleteOrChanging,
                "{changing:?}"
            );
            assert!(!obs.qualification);
        }

        for unavailable_reason in [
            "authority filesystem locality unconfirmed",
            "authority initialization",
            "authority submount provider cannot be reconciled with native home",
            "bootstrap missing",
            "candidate daemon identity unavailable",
            "deleted native executable",
            "descriptor mount absent from inventory",
            "descriptor mount id",
            "duplicate descriptor mount id",
            "empty mount inventory",
            "kernel Unix socket netns unavailable",
            "kernel Unix socket table procfs type",
            "kernel Unix socket table read failed",
            "kernel Unix socket table read unavailable",
            "kernel Unix socket table size unavailable",
            "kernel Unix socket table unavailable",
            "kernel inventory byte cap",
            "kernel listener table platform unqualified",
            "kernel procfs source",
            "lock directory",
            "lock inode",
            "macOS authority filesystem provider unconfirmed",
            "mount device",
            "mount fields",
            "mount identity",
            "mount numeric field",
            "mount numeric range",
            "mount pathname NUL",
            "mount pathname escape",
            "mount pathname shape",
            "mount separator",
            "native account lookup",
            "native arguments empty",
            "native arguments platform",
            "native arguments unreadable",
            "native executable argument prefix",
            "native executable path",
            "native home pointer",
            "native home terminator",
            "native inventory PID namespace view",
            "native inventory init view",
            "native inventory mountinfo",
            "native inventory namespace view",
            "native inventory obscured process",
            "native inventory owner unreadable",
            "native inventory platform",
            "native inventory procfs root",
            "native inventory procfs type",
            "native inventory self link",
            "native local socket selector",
            "native peer PID",
            "native process platform",
            "native relevance birth/owner unreadable",
            "native relevance metadata cap",
            "native relevance platform",
            "native socket selector",
            "non-local authority filesystem",
            "overlaid procfs process view",
            "partial procfs root",
            "procfs mount fields",
            "procfs mount root ambiguous",
            "publication requires forward reconciliation",
            "publisher mutex",
            "record directory",
            "record staging inode",
            "record version",
            "recorded exited runtime",
            "referenced runtime PID omitted; reconciliation required",
            "restricted procfs visibility",
            "same-account process relevance unavailable",
            "socket root",
            "socket table header ABI",
            "socket table listener address unavailable",
            "socket table listener pathname unavailable",
            "socket table record ABI",
            "socket table socket ABI/type",
            "starting runtime requires bootstrap reconciliation",
            "unspawned starting runtime record",
            "unsupported native filesystem",
            "unsupported native platform",
        ] {
            let obs =
                classify_native_capability(Err(&RuntimeError::Unavailable(unavailable_reason)));
            assert_eq!(
                obs.status,
                NativeCapabilityStatus::Unavailable,
                "{unavailable_reason}"
            );
            assert_eq!(obs.reason.as_deref(), Some(unavailable_reason));
            assert!(!obs.qualification);
        }

        for other_unavailable in [
            RuntimeError::Invalid("socket table row count"),
            RuntimeError::Locked,
            RuntimeError::PublicationUncertain,
            RuntimeError::Io(std::io::Error::other("synthetic I/O failure")),
        ] {
            let obs = classify_native_capability(Err(&other_unavailable));
            assert_eq!(
                obs.status,
                NativeCapabilityStatus::Unavailable,
                "{other_unavailable:?}"
            );
            assert!(!obs.qualification);
        }
    }

    #[cfg(unix)]
    #[test]
    fn inspect_unreachable_socket_path_classifies_absent_non_socket_refused_accepted_and_denied() {
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::net::UnixListener;

        let temp = tempfile::tempdir().expect("tempdir");

        // 1. Missing socket in accessible parent -> Absent
        let missing = temp.path().join("absent.sock");
        let obs = inspect_unreachable_socket_path(&missing);
        assert_eq!(obs.status, SocketAccessStatus::Absent);
        assert_eq!(
            obs.reason.as_deref(),
            Some("no daemon socket present for this profile")
        );
        assert!(obs.guidance.is_none());

        // 2. Non-socket regular file -> Unreachable
        let regular = temp.path().join("not-a-socket.sock");
        std::fs::write(&regular, b"not a socket").expect("write regular file");
        let obs = inspect_unreachable_socket_path(&regular);
        assert_eq!(obs.status, SocketAccessStatus::Unreachable);
        assert_eq!(
            obs.reason.as_deref(),
            Some("configured socket path exists but is not a Unix domain socket")
        );
        assert!(obs.guidance.is_none());

        // 3. Stale socket inode on disk (listener bound and dropped without unlink) -> Unreachable (ConnectionRefused)
        let stale_sock = temp.path().join("stale.sock");
        {
            let _listener = UnixListener::bind(&stale_sock).expect("bind stale socket");
        }
        let obs = inspect_unreachable_socket_path(&stale_sock);
        assert_eq!(obs.status, SocketAccessStatus::Unreachable);
        assert_eq!(
            obs.reason.as_deref(),
            Some("daemon socket exists on disk but connection was refused")
        );
        assert!(obs.guidance.is_none());

        // 4. Listening socket that accepts connections while RPC timed out -> Unreachable (accepted connection)
        let live_sock = temp.path().join("live.sock");
        let _live_listener = UnixListener::bind(&live_sock).expect("bind live socket");
        let obs = inspect_unreachable_socket_path(&live_sock);
        assert_eq!(obs.status, SocketAccessStatus::Unreachable);
        assert_eq!(
            obs.reason.as_deref(),
            Some(
                "socket accepted a connection, but the local daemon RPC did not respond within its local budget"
            )
        );
        assert!(obs.guidance.is_none());

        // 5. Unsearchable (0o000) parent directory -> symlink_metadata(socket_path) returns EACCES -> Denied (non-root only)
        if unsafe { libc::geteuid() } != 0 {
            struct RestorePerms(std::path::PathBuf);
            impl Drop for RestorePerms {
                fn drop(&mut self) {
                    let _ =
                        std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o700));
                }
            }
            let denied_dir = temp.path().join("denied-dir");
            std::fs::create_dir_all(&denied_dir).expect("create denied dir");
            let blocked_sock = denied_dir.join("blocked.sock");
            let _guard = RestorePerms(denied_dir.clone());
            std::fs::set_permissions(&denied_dir, std::fs::Permissions::from_mode(0o000))
                .expect("chmod 000");

            let obs = inspect_unreachable_socket_path(&blocked_sock);
            assert_eq!(obs.status, SocketAccessStatus::Denied);
            assert!(obs
                .reason
                .as_deref()
                .unwrap_or("")
                .contains("permission denied"));
            assert!(obs.guidance.as_deref().unwrap_or("").contains("sandbox"));
        }
    }
}
