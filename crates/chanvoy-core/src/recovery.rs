//! Independent identity, lifecycle, and observation assessments.
use crate::{CoreError, MattermostClient};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// One provider identity future per daemon. Busy callers never queue another
/// probe, including when their predecessor's client RPC has been canceled.
#[derive(Default)]
pub struct IdentityProbeGate(tokio::sync::Mutex<()>);

impl IdentityProbeGate {
    pub async fn probe(
        &self,
        client: &MattermostClient,
        budget: Duration,
        expected: &str,
        refused: &AtomicBool,
    ) -> IdentityProbe {
        self.probe_with_drift(client, budget, expected, refused, &AtomicBool::new(false))
            .await
    }

    pub async fn probe_with_drift(
        &self,
        client: &MattermostClient,
        budget: Duration,
        expected: &str,
        refused: &AtomicBool,
        drifted: &AtomicBool,
    ) -> IdentityProbe {
        let Ok(_guard) = self.0.try_lock() else {
            return IdentityProbe::unknown();
        };
        let probe = probe_identity(client, budget).await;
        let next = probe.refusal_after(expected, refused.load(Ordering::Acquire));
        if next {
            refused.store(true, Ordering::Release);
        }
        if probe.outcome == RemoteProbeOutcome::Verified && !expected.is_empty() {
            if let Some(username) = &probe.observed_username {
                drifted.store(
                    !expected.is_empty() && username != expected,
                    Ordering::Release,
                );
            }
        }
        if !next {
            refused.store(false, Ordering::Release);
        }
        probe
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RemoteProbeOutcome {
    Verified,
    RejectedCredential,
    Timeout,
    Unavailable,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityProbe {
    pub outcome: RemoteProbeOutcome,
    /// Present only after a successful provider answer, never a profile fallback.
    pub observed_username: Option<String>,
}

impl IdentityProbe {
    pub fn unknown() -> Self {
        Self {
            outcome: RemoteProbeOutcome::Unknown,
            observed_username: None,
        }
    }

    /// Inconclusive answers leave an authoritative refusal in force.
    pub fn refusal_after(&self, expected: &str, previous: bool) -> bool {
        match self.outcome {
            RemoteProbeOutcome::RejectedCredential => true,
            RemoteProbeOutcome::Verified => self
                .observed_username
                .as_deref()
                .map(|name| {
                    if expected.is_empty() {
                        previous
                    } else {
                        name != expected
                    }
                })
                .unwrap_or(previous),
            _ => previous,
        }
    }

    pub fn diagnostic(&self) -> Option<&'static str> {
        match self.outcome {
            RemoteProbeOutcome::Verified => None,
            RemoteProbeOutcome::RejectedCredential => Some("identity probe rejected credential"),
            RemoteProbeOutcome::Timeout => Some("identity probe timed out"),
            RemoteProbeOutcome::Unavailable => Some("identity probe unavailable"),
            RemoteProbeOutcome::Unknown => Some("identity probe unknown"),
        }
    }
}

/// Classify HTTP status structurally. Provider text never establishes identity.
pub fn identity_probe_error(error: &CoreError) -> RemoteProbeOutcome {
    match error {
        CoreError::Api { status, .. } if matches!(status.as_u16(), 401 | 403) => {
            RemoteProbeOutcome::RejectedCredential
        }
        _ => RemoteProbeOutcome::Unavailable,
    }
}

pub async fn probe_identity(client: &MattermostClient, budget: Duration) -> IdentityProbe {
    match tokio::time::timeout(budget, client.whoami()).await {
        Ok(Ok(identity)) => IdentityProbe {
            outcome: RemoteProbeOutcome::Verified,
            observed_username: Some(identity.username),
        },
        Ok(Err(error)) => IdentityProbe {
            outcome: identity_probe_error(&error),
            observed_username: None,
        },
        Err(_) => IdentityProbe {
            outcome: RemoteProbeOutcome::Timeout,
            observed_username: None,
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum DaemonDisposition {
    Healthy,
    DegradedRemote,
    IdentityRefused,
    UnresponsiveLocal,
}

impl DaemonDisposition {
    pub fn label(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::DegradedRemote => "degraded-remote",
            Self::IdentityRefused => "identity-refused",
            Self::UnresponsiveLocal => "unresponsive-local",
        }
    }

    pub fn successful(self) -> bool {
        matches!(self, Self::Healthy | Self::DegradedRemote)
    }
}

pub fn assess_daemon(
    responsive: bool,
    probe: &IdentityProbe,
    expected_username: &str,
    refused: bool,
    observation_ready: Option<bool>,
) -> DaemonDisposition {
    if !responsive {
        DaemonDisposition::UnresponsiveLocal
    } else if refused || probe.refusal_after(expected_username, false) {
        DaemonDisposition::IdentityRefused
    } else if probe.outcome == RemoteProbeOutcome::Verified
        && probe.observed_username.is_some()
        && observation_ready == Some(true)
    {
        DaemonDisposition::Healthy
    } else {
        DaemonDisposition::DegradedRemote
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    fn client(url: String) -> MattermostClient {
        let profile = crate::Profile {
            name: "synthetic-seat".into(),
            role: "test".into(),
            scope: "test".into(),
            provider: crate::Provider::Mattermost,
            bot_username: "expected".into(),
            team_name: "synthetic-team".into(),
            server_url: url,
            env_name: "TEST_TOKEN".into(),
            env_file: None,
            credential_mode: crate::CredentialMode::EnvName,
            capability_class: crate::CapabilityClass::Standard,
            monitored_channels: vec![],
            ipc: None,
            reduce: None,
        };
        MattermostClient::new(&profile, "test-token-probe".into()).unwrap()
    }

    #[tokio::test]
    async fn synthetic_latency_and_authoritative_refusal() {
        use wiremock::matchers::path;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        for (code, delay, budget, outcome) in [
            (200, 1000, 2000, RemoteProbeOutcome::Verified),
            (200, 1850, 2000, RemoteProbeOutcome::Verified),
            (200, 5000, 2000, RemoteProbeOutcome::Timeout),
            (401, 0, 2000, RemoteProbeOutcome::RejectedCredential),
            (403, 0, 2000, RemoteProbeOutcome::RejectedCredential),
            (503, 0, 2000, RemoteProbeOutcome::Unavailable),
        ] {
            let server = MockServer::start().await;
            Mock::given(path("/api/v4/users/me"))
                .respond_with(
                    ResponseTemplate::new(code)
                        .set_delay(Duration::from_millis(delay))
                        .set_body_json(serde_json::json!({
                            "id":"synthetic-id", "username":"expected", "is_bot":true
                        })),
                )
                .mount(&server)
                .await;
            let before = std::time::Instant::now();
            let result = probe_identity(&client(server.uri()), Duration::from_millis(budget)).await;
            assert_eq!(result.outcome, outcome);
            assert!(before.elapsed() < Duration::from_millis(budget + 750));
            assert_eq!(
                result.observed_username.is_some(),
                outcome == RemoteProbeOutcome::Verified
            );
        }
    }

    #[tokio::test]
    async fn canceled_client_and_burst_requests_do_not_accumulate_probes() {
        use std::sync::Arc;
        use wiremock::matchers::path;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(path("/api/v4/users/me"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(1))
                    .set_body_json(serde_json::json!({
                        "id":"synthetic-id", "username":"expected", "is_bot":true
                    })),
            )
            .mount(&server)
            .await;
        let gate = Arc::new(IdentityProbeGate::default());
        let client = client(server.uri());
        let refused = Arc::new(AtomicBool::new(false));
        let task = {
            let gate = gate.clone();
            let client = client.clone();
            let refused = refused.clone();
            tokio::spawn(async move {
                gate.probe(&client, Duration::from_secs(2), "expected", &refused)
                    .await
            })
        };
        // Model a canceled RPC consumer: timeout its join wait without aborting
        // the independent server handler. The probe must remain exclusively held.
        let mut task = task;
        assert!(tokio::time::timeout(Duration::from_millis(100), &mut task)
            .await
            .is_err());
        for _ in 0..30 {
            assert_eq!(
                gate.probe(&client, Duration::from_secs(2), "expected", &refused)
                    .await
                    .outcome,
                RemoteProbeOutcome::Unknown
            );
        }
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            1,
            "thirty additional callers must not launch thirty provider requests"
        );
        assert_eq!(task.await.unwrap().outcome, RemoteProbeOutcome::Verified);
        assert_eq!(
            gate.probe(&client, Duration::from_secs(2), "expected", &refused)
                .await
                .outcome,
            RemoteProbeOutcome::Verified
        );
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[test]
    fn structural_refusal_never_parses_provider_text() {
        for code in [401, 403, 404, 429, 500, 502, 503] {
            let error = CoreError::Api {
                status: StatusCode::from_u16(code).unwrap(),
                message: "401 forbidden wrong identity".into(),
            };
            assert_eq!(
                identity_probe_error(&error),
                if matches!(code, 401 | 403) {
                    RemoteProbeOutcome::RejectedCredential
                } else {
                    RemoteProbeOutcome::Unavailable
                }
            );
        }
    }

    #[test]
    fn remote_identity_and_observation_truth_table() {
        for outcome in [
            RemoteProbeOutcome::Unknown,
            RemoteProbeOutcome::Unavailable,
            RemoteProbeOutcome::Timeout,
            RemoteProbeOutcome::RejectedCredential,
            RemoteProbeOutcome::Verified,
        ] {
            for observed in [None, Some("expected"), Some("other")] {
                for admitted in [None, Some(false), Some(true)] {
                    let probe = IdentityProbe {
                        outcome,
                        observed_username: observed.map(str::to_owned),
                    };
                    let disposition = assess_daemon(true, &probe, "expected", false, admitted);
                    let identity_failed = outcome == RemoteProbeOutcome::RejectedCredential
                        || (outcome == RemoteProbeOutcome::Verified && observed == Some("other"));
                    let healthy = outcome == RemoteProbeOutcome::Verified
                        && observed == Some("expected")
                        && admitted == Some(true);
                    assert_eq!(
                        disposition,
                        if identity_failed {
                            DaemonDisposition::IdentityRefused
                        } else if healthy {
                            DaemonDisposition::Healthy
                        } else {
                            DaemonDisposition::DegradedRemote
                        }
                    );
                    assert_eq!(
                        assess_daemon(false, &probe, "expected", false, admitted),
                        DaemonDisposition::UnresponsiveLocal
                    );
                }
            }
        }
    }

    #[test]
    fn inconclusive_probe_preserves_refusal_until_authoritative_success() {
        for outcome in [
            RemoteProbeOutcome::Unknown,
            RemoteProbeOutcome::Timeout,
            RemoteProbeOutcome::Unavailable,
        ] {
            let probe = IdentityProbe {
                outcome,
                observed_username: None,
            };
            assert!(probe.refusal_after("expected", true));
            assert!(!probe.refusal_after("expected", false));
        }
        let verified = IdentityProbe {
            outcome: RemoteProbeOutcome::Verified,
            observed_username: Some("expected".into()),
        };
        assert!(!verified.refusal_after("expected", true));
        assert!(
            verified.refusal_after("", true),
            "missing expected identity cannot clear refusal"
        );
        assert!(IdentityProbe {
            observed_username: Some("other".into()),
            ..verified
        }
        .refusal_after("expected", false));
    }
}
