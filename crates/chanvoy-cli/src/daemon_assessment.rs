//! Bounded, non-destructive assessment of local and provider health.
use chanvoy_core::recovery::{assess_daemon, DaemonDisposition, IdentityProbe, RemoteProbeOutcome};
use chanvoy_core::{DaemonStatus, Profile};
use chanvoy_daemon::{daemon_client, ping, ping_full, DaemonError};
use std::time::Duration;

pub(super) const LOCAL_BUDGET: Duration = Duration::from_millis(750);
pub(super) const REMOTE_BUDGET: Duration =
    Duration::from_millis(chanvoy_core::STATUS_PROBE_TIMEOUT_MS + 750);
pub(super) const OPERATION_BUDGET: Duration = Duration::from_secs(10);

pub(super) fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + OPERATION_BUDGET
}

pub(super) struct Assessment {
    pub disposition: DaemonDisposition,
    pub status: Option<DaemonStatus>,
    pub observation_ready: Option<bool>,
}

impl Assessment {
    pub(super) fn from_status(status: DaemonStatus, expected: &str) -> Self {
        Self {
            disposition: status_disposition(&status, expected),
            observation_ready: observation_ready(&status),
            status: Some(status),
        }
    }

    /// Later inconclusive evidence cannot erase a refusal observed during this
    /// operation. Only a verified expected identity with an explicitly clear
    /// latch can do so; a configured-name fallback is never that evidence.
    pub(super) fn absorb(&mut self, mut next: Self, expected: &str) {
        if self.disposition == DaemonDisposition::IdentityRefused
            && next.disposition != DaemonDisposition::IdentityRefused
            && !next
                .status
                .as_ref()
                .is_some_and(|s| verified_expected_identity(s, expected))
        {
            next.disposition = DaemonDisposition::IdentityRefused;
        }
        *self = next;
    }
}

pub(super) fn observed_username(status: &DaemonStatus) -> Option<&str> {
    (status.remote_probe == Some(RemoteProbeOutcome::Verified)
        && status.mattermost_ok
        && !status.mattermost_username.is_empty())
    .then_some(status.mattermost_username.as_str())
}

pub(super) fn verified_expected_identity(status: &DaemonStatus, expected: &str) -> bool {
    status.identity_refused == Some(false)
        && !status.mattermost_identity_drift.unwrap_or(false)
        && observed_username(status) == Some(expected)
}

pub(super) fn observation_ready(status: &DaemonStatus) -> Option<bool> {
    if let Some(closed) = status.ws_observation_admission_closed {
        return Some(!closed);
    }
    if status.ws_connection_state.is_none() || status.ws_reconnect_count.is_none() {
        return None;
    }
    Some(!super::daemon_ws_degraded(status))
}

pub(super) fn status_disposition(status: &DaemonStatus, expected: &str) -> DaemonDisposition {
    let outcome = if status.identity_refused.is_some() {
        status.remote_probe.unwrap_or(RemoteProbeOutcome::Unknown)
    } else {
        RemoteProbeOutcome::Unknown
    };
    let probe = IdentityProbe {
        outcome,
        observed_username: if outcome == RemoteProbeOutcome::Verified {
            observed_username(status).map(str::to_owned)
        } else {
            None
        },
    };
    assess_daemon(
        true,
        &probe,
        expected,
        status.identity_refused.unwrap_or(false)
            || status.mattermost_identity_drift.unwrap_or(false),
        observation_ready(status),
    )
}

/// Only the typed method-not-found response identifies a legacy daemon.
fn unsupported(error: &DaemonError) -> bool {
    matches!(error, DaemonError::Rpc { code: -32601, .. })
}

pub(super) async fn assess_until(profile: &Profile, deadline: tokio::time::Instant) -> Assessment {
    let client = daemon_client(&profile.name);
    let local = tokio::time::timeout(LOCAL_BUDGET, client.daemon_observation()).await;
    let (responsive, mut snapshot, legacy) = match local {
        Ok(Ok(snapshot)) => (true, Some(snapshot), false),
        Ok(Err(error)) if unsupported(&error) => {
            let responsive = matches!(
                tokio::time::timeout(LOCAL_BUDGET, ping(&profile.name)).await,
                Ok(Ok(_))
            );
            (responsive, None, true)
        }
        _ => {
            // Independently prove local liveness before classifying an absent
            // observation reply. A status-method failure is not daemon death.
            let responsive = matches!(
                tokio::time::timeout(LOCAL_BUDGET, ping(&profile.name)).await,
                Ok(Ok(_))
            );
            (responsive, None, false)
        }
    };
    if !responsive {
        return Assessment {
            disposition: DaemonDisposition::UnresponsiveLocal,
            status: None,
            observation_ready: None,
        };
    }
    // Do not start a remote RPC without enough remaining budget for the
    // daemon's classification plus transport margin. Local checks stay separate.
    let remote = if deadline.saturating_duration_since(tokio::time::Instant::now()) >= REMOTE_BUDGET
    {
        tokio::time::timeout(REMOTE_BUDGET, ping_full(&profile.name))
            .await
            .ok()
            .and_then(Result::ok)
    } else {
        None
    };
    match remote {
        Some(remote) => snapshot = Some(remote),
        _ if !legacy => {
            // Obtain the current latch/admission after the failed remote call.
            if let Ok(Ok(local)) =
                tokio::time::timeout(LOCAL_BUDGET, client.daemon_observation()).await
            {
                snapshot = Some(local);
            }
        }
        _ => {}
    }
    let disposition = snapshot
        .as_ref()
        .map(|s| status_disposition(s, &profile.bot_username))
        .unwrap_or(DaemonDisposition::DegradedRemote);
    let observation_ready = snapshot.as_ref().and_then(observation_ready);
    Assessment {
        disposition,
        status: snapshot,
        observation_ready,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fallback_requires_typed_method_not_found() {
        assert!(unsupported(&DaemonError::Rpc {
            code: -32601,
            message: "anything".into(),
            data: None
        }));
        assert!(!unsupported(&DaemonError::Rpc {
            code: -32000,
            message: "method not found".into(),
            data: None
        }));
        assert!(!unsupported(&DaemonError::NotRunning(
            "method not found".into()
        )));
    }
}
