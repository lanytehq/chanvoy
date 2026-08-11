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
}
