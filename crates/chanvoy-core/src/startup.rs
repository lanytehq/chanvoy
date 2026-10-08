//! Startup-specific deadlines and privacy-safe, opt-in phase diagnostics.
//! This helper performs no lifecycle or identity admission decisions.
use std::{fmt, future::Future, time::Duration};

use crate::CoreError;

pub const LOCAL_PING_BUDGET: Duration = Duration::from_millis(750);
pub const IDENTITY_BUDGET: Duration = Duration::from_millis(crate::STATUS_PROBE_TIMEOUT_MS);
pub const TEAM_BUDGET: Duration = Duration::from_secs(2);
pub const FINALIZATION_BUDGET: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    ExistingSocket,
    ParentIdentity,
    ParentTeam,
    FamilyIdentity,
    ForegroundIdentity,
    ChildReadiness,
    FinalReadinessProbe,
    ChildFinalization,
    ChildSocketAbsence,
}

impl Phase {
    pub fn name(self) -> &'static str {
        match self {
            Self::ExistingSocket => "existing-socket",
            Self::ParentIdentity => "parent-identity",
            Self::ParentTeam => "parent-team-access",
            Self::FamilyIdentity => "reduce-family-identity",
            Self::ForegroundIdentity => "foreground-identity",
            Self::ChildReadiness => "child-readiness-window",
            Self::FinalReadinessProbe => "final-readiness-probe",
            Self::ChildFinalization => "child-finalization",
            Self::ChildSocketAbsence => "failed-child-socket-absence",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Timeout,
    AuthoritativeRefusal,
    IdentityMismatch,
    ConnectFailure,
    ProviderUnavailable,
    InvalidInput,
    LocalUnconfirmed,
    OtherError,
}

impl Outcome {
    pub fn name(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Timeout => "timeout",
            Self::AuthoritativeRefusal => "authoritative-refusal",
            Self::IdentityMismatch => "identity-mismatch",
            Self::ConnectFailure => "connect-failure",
            Self::ProviderUnavailable => "provider-unavailable",
            Self::InvalidInput => "invalid-input",
            Self::LocalUnconfirmed => "local-unconfirmed",
            Self::OtherError => "other-error",
        }
    }
}

pub fn classify_core_error(error: &CoreError) -> Outcome {
    match error {
        CoreError::Api { status, .. } if matches!(status.as_u16(), 401 | 403) => {
            Outcome::AuthoritativeRefusal
        }
        CoreError::Api { status, .. } if status.is_server_error() => Outcome::ProviderUnavailable,
        CoreError::ProfileIdentityMismatch { .. } | CoreError::ReduceIdentityMismatch { .. } => {
            Outcome::IdentityMismatch
        }
        CoreError::Http(error) if error.is_timeout() => Outcome::Timeout,
        CoreError::Http(error) if error.is_connect() => Outcome::ConnectFailure,
        CoreError::MissingCredential(_) | CoreError::MissingEnvFile | CoreError::TomlDe(_) => {
            Outcome::InvalidInput
        }
        _ => Outcome::OtherError,
    }
}

/// Preserve typed evidence without exposing its provider body through formatting.
pub struct Failure<E> {
    pub phase: Phase,
    pub outcome: Outcome,
    cause: Option<E>,
}

impl<E> Failure<E> {
    pub fn into_cause(self) -> Option<E> {
        self.cause
    }
}

impl<E> fmt::Display for Failure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "startup {}: {}", self.phase.name(), self.outcome.name())
    }
}

impl<E> fmt::Debug for Failure<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

pub struct Diagnostic<'a> {
    phase: Phase,
    profile: &'a str,
    budget: Duration,
    started: tokio::time::Instant,
    finished: bool,
}

impl Diagnostic<'_> {
    /// Observe an existing caller-owned phase, without imposing a new deadline.
    /// The readiness window is nominal and retains its separate probe margins.
    pub fn begin(phase: Phase, profile: &str, budget: Duration) -> Diagnostic<'_> {
        tracing::info!(
            stage = phase.name(),
            budget_ms = budget.as_millis() as u64,
            elapsed_ms = 0u64,
            profile,
            pid = std::process::id(),
            "startup phase begin"
        );
        Diagnostic {
            phase,
            profile,
            budget,
            started: tokio::time::Instant::now(),
            finished: false,
        }
    }

    pub fn finish(&mut self, outcome: Outcome) {
        self.finished = true;
        tracing::info!(
            stage = self.phase.name(),
            budget_ms = self.budget.as_millis() as u64,
            elapsed_ms = self.started.elapsed().as_millis() as u64,
            outcome = outcome.name(),
            profile = self.profile,
            pid = std::process::id(),
            "startup phase completed"
        );
    }
}

impl Drop for Diagnostic<'_> {
    fn drop(&mut self) {
        if !self.finished {
            tracing::info!(
                stage = self.phase.name(),
                budget_ms = self.budget.as_millis() as u64,
                elapsed_ms = self.started.elapsed().as_millis() as u64,
                outcome = Outcome::LocalUnconfirmed.name(),
                profile = self.profile,
                pid = std::process::id(),
                "startup phase canceled"
            );
        }
    }
}

/// One directly scoped attempt. Dropping this future also drops the request;
/// it makes no assertion about work still running on a remote provider.
pub async fn bounded<T, E>(
    phase: Phase,
    profile: &str,
    budget: Duration,
    future: impl Future<Output = Result<T, E>>,
    classify: impl FnOnce(&E) -> Outcome,
) -> Result<T, Failure<E>> {
    let started = tokio::time::Instant::now();
    let deadline = started + budget;
    let mut diagnostic = Diagnostic::begin(phase, profile, budget);
    match tokio::time::timeout_at(deadline, future).await {
        Ok(result) if tokio::time::Instant::now() <= deadline => match result {
            Ok(value) => {
                diagnostic.finish(Outcome::Success);
                Ok(value)
            }
            Err(error) => {
                let outcome = classify(&error);
                diagnostic.finish(outcome);
                Err(Failure {
                    phase,
                    outcome,
                    cause: Some(error),
                })
            }
        },
        _ => {
            diagnostic.finish(Outcome::Timeout);
            Err(Failure {
                phase,
                outcome: Outcome::Timeout,
                cause: None,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct Request(Arc<AtomicUsize>);
    impl Drop for Request {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn deadline_drops_the_scoped_request() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let request = Request(dropped.clone());
        let error = tokio::time::timeout(
            Duration::from_millis(500),
            bounded(
                Phase::FamilyIdentity,
                "synthetic",
                Duration::from_millis(20),
                async move {
                    let _request = request;
                    std::future::pending::<Result<(), CoreError>>().await
                },
                classify_core_error,
            ),
        )
        .await
        .expect("phase deadline must be enforced")
        .unwrap_err();
        assert_eq!(error.outcome, Outcome::Timeout);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(error.into_cause().is_none());
    }

    #[tokio::test]
    async fn failure_format_is_safe_and_typed_status_is_preserved() {
        let cause = CoreError::Api {
            status: reqwest::StatusCode::UNAUTHORIZED,
            message: "synthetic-secret-provider-body".into(),
        };
        let error = bounded(
            Phase::ParentIdentity,
            "synthetic",
            Duration::from_secs(1),
            async { Err::<(), _>(cause) },
            classify_core_error,
        )
        .await
        .unwrap_err();
        assert_eq!(error.outcome, Outcome::AuthoritativeRefusal);
        assert!(!format!("{error:?} {error}").contains("synthetic-secret"));
        assert!(
            matches!(error.into_cause(), Some(CoreError::Api { status, .. })
            if status == reqwest::StatusCode::UNAUTHORIZED)
        );
    }
}
