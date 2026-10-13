//! Caller-side generation evidence, separate from health and lifecycle authority.
use chanvoy_core::{host_generation_match, HostBuildInfo};
use serde::Serialize;

pub(super) const EXIT_GENERATION_REFUSED: i32 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Reason {
    Match,
    CommitMismatch,
    DirtyMismatch,
    PinMissing,
    PinInvalid,
    DirtyUnknown,
    CandidateUnconfirmed,
}

impl Reason {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Match => "match",
            Self::CommitMismatch => "commit_mismatch",
            Self::DirtyMismatch => "dirty_mismatch",
            Self::PinMissing => "pin_missing",
            Self::PinInvalid => "pin_invalid",
            Self::DirtyUnknown => "dirty_unknown",
            Self::CandidateUnconfirmed => "candidate_unconfirmed",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct Evidence {
    pub cli_binary: HostBuildInfo,
    pub daemon_binary: Option<HostBuildInfo>,
    pub generation_match: Option<bool>,
    pub generation_reason: Reason,
    pub cycled: bool,
    pub reasons: Vec<String>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub predecessor: Option<Predecessor>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct Predecessor {
    pub predecessor_binary: Option<HostBuildInfo>,
    pub predecessor_generation_match: Option<bool>,
    pub predecessor_generation_reason: Reason,
}

fn valid_commit(commit: &str) -> bool {
    commit.len() == 40 && commit.bytes().all(|byte| byte.is_ascii_hexdigit())
}

impl Evidence {
    pub(super) fn compare(
        cli: HostBuildInfo,
        daemon: Option<HostBuildInfo>,
        malformed: bool,
    ) -> Self {
        let reason = if malformed {
            Reason::PinInvalid
        } else if let Some(daemon) = &daemon {
            if cli.commit.is_empty()
                || cli.commit == "unknown"
                || daemon.commit.is_empty()
                || daemon.commit == "unknown"
            {
                Reason::PinMissing
            } else if !valid_commit(&cli.commit) || !valid_commit(&daemon.commit) {
                Reason::PinInvalid
            } else if cli.dirty.is_none() || daemon.dirty.is_none() {
                Reason::DirtyUnknown
            } else if !cli.commit.eq_ignore_ascii_case(&daemon.commit) {
                Reason::CommitMismatch
            } else if cli.dirty != daemon.dirty {
                Reason::DirtyMismatch
            } else {
                Reason::Match
            }
        } else {
            Reason::PinMissing
        };
        let generation_match = match reason {
            Reason::Match => Some(true),
            Reason::CommitMismatch | Reason::DirtyMismatch => Some(false),
            _ => None,
        };
        // Unusable remote objects are not echoed. Incomplete typed metadata is
        // still useful, but cannot establish a match.
        let daemon_binary = if reason == Reason::PinInvalid {
            None
        } else {
            daemon
        };
        debug_assert!(
            reason != Reason::Match
                || host_generation_match(&cli, daemon_binary.as_ref()) == Some(true)
        );
        Self {
            cli_binary: cli,
            daemon_binary,
            generation_match,
            generation_reason: reason,
            cycled: false,
            reasons: vec![reason.label().into()],
            predecessor: None,
        }
    }

    pub(super) fn unconfirmed(mut self) -> Self {
        self.generation_match = None;
        self.generation_reason = Reason::CandidateUnconfirmed;
        self.reasons = vec![Reason::CandidateUnconfirmed.label().into()];
        self
    }

    pub(super) fn refreshed(
        mut self,
        predecessor: Self,
        profile_refresh: bool,
        identity_refresh: bool,
        confirmed_cycle: bool,
    ) -> Self {
        self.reasons.clear();
        if profile_refresh {
            self.reasons.push("profile_refresh".into());
        }
        if identity_refresh {
            self.reasons.push("bot_identity_refresh".into());
        }
        self.reasons.push(
            if confirmed_cycle {
                predecessor.generation_reason
            } else {
                self.generation_reason
            }
            .label()
            .into(),
        );
        self.predecessor = Some(Predecessor {
            predecessor_binary: predecessor.daemon_binary,
            predecessor_generation_match: predecessor.generation_match,
            predecessor_generation_reason: predecessor.generation_reason,
        });
        // Caller sets this only after the established confirmed-stop/start
        // path accepts a successor. A mere successful stop is not a cycle.
        self.cycled = confirmed_cycle;
        self
    }

    pub(super) fn error_code(&self) -> &'static str {
        if self.generation_match == Some(false) {
            "generation_mismatch"
        } else {
            "generation_unknown"
        }
    }

    pub(super) fn recovery(&self, ownable: bool) -> &'static str {
        if self.generation_reason == Reason::CandidateUnconfirmed || !ownable {
            "Daemon retained; ownership of the running daemon could not be confirmed. Inspect daemon status and resolve ownership before any retirement."
        } else {
            "Daemon retained (different or unverifiable build). Follow the confirmed per-profile retirement procedure in the operator guide, Installation and daemon migration."
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin(commit: &str, dirty: Option<bool>) -> HostBuildInfo {
        HostBuildInfo {
            version: "0.3.3".into(),
            commit: commit.into(),
            commit_short: commit.chars().take(7).collect(),
            build_date: "synthetic".into(),
            dirty,
            rustc: "synthetic".into(),
            platform: "synthetic".into(),
        }
    }

    #[test]
    fn every_reason_has_a_distinct_fixture_and_tri_state() {
        let a = pin(&"a".repeat(40), Some(false));
        let fixtures = [
            (Some(a.clone()), false, Reason::Match, Some(true)),
            (
                Some(pin(&"b".repeat(40), Some(false))),
                false,
                Reason::CommitMismatch,
                Some(false),
            ),
            (
                Some(pin(&"a".repeat(40), Some(true))),
                false,
                Reason::DirtyMismatch,
                Some(false),
            ),
            (None, false, Reason::PinMissing, None),
            (
                Some(pin("bad", Some(false))),
                false,
                Reason::PinInvalid,
                None,
            ),
            (
                Some(pin(&"a".repeat(40), None)),
                false,
                Reason::DirtyUnknown,
                None,
            ),
        ];
        for (daemon, malformed, reason, expected) in fixtures {
            let evidence = Evidence::compare(a.clone(), daemon, malformed);
            assert_eq!(evidence.generation_reason, reason);
            assert_eq!(evidence.generation_match, expected);
            let json = serde_json::to_value(&evidence).unwrap();
            assert_eq!(json["generation_reason"], reason.label());
            assert!(json.get("generation_match").is_some());
            if expected.is_none() {
                assert!(json["generation_match"].is_null());
            }
            assert_eq!(json["cycled"], false);
        }
        let changed = Evidence::compare(a.clone(), Some(a), false).unconfirmed();
        assert_eq!(changed.generation_reason, Reason::CandidateUnconfirmed);
        assert!(changed.generation_match.is_none());
        assert!(!changed.recovery(true).contains("Follow the confirmed"));
    }

    #[test]
    fn missing_dirty_on_either_or_both_sides_is_unknown() {
        for (cli_dirty, daemon_dirty) in [(None, None), (None, Some(false)), (Some(false), None)] {
            let evidence = Evidence::compare(
                pin(&"a".repeat(40), cli_dirty),
                Some(pin(&"a".repeat(40), daemon_dirty)),
                false,
            );
            assert_eq!(evidence.generation_reason, Reason::DirtyUnknown);
            assert_eq!(evidence.generation_match, None);
        }
    }

    #[test]
    fn unavailable_and_malformed_are_deterministic_separate_reasons() {
        let cli = pin(&"a".repeat(40), Some(false));
        assert_eq!(
            Evidence::compare(cli.clone(), None, false).generation_reason,
            Reason::PinMissing
        );
        assert_eq!(
            Evidence::compare(cli, None, true).generation_reason,
            Reason::PinInvalid
        );
    }

    #[test]
    fn refresh_reasons_end_with_predecessor_cause_not_result() {
        let cli = pin(&"a".repeat(40), Some(false));
        let predecessor =
            Evidence::compare(cli.clone(), Some(pin(&"b".repeat(40), Some(false))), false);
        let report = Evidence::compare(cli.clone(), Some(cli), false).refreshed(
            predecessor,
            true,
            true,
            true,
        );
        assert_eq!(
            report.reasons,
            ["profile_refresh", "bot_identity_refresh", "commit_mismatch"]
        );
        assert_eq!(report.generation_reason, Reason::Match);
        assert_eq!(report.generation_match, Some(true));
        assert!(report.cycled);
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["predecessor_generation_reason"], "commit_mismatch");
        assert_eq!(json["predecessor_generation_match"], false);
    }

    #[test]
    fn an_unproven_stop_is_not_reported_as_a_cycle() {
        let cli = pin(&"a".repeat(40), Some(false));
        let predecessor = Evidence::compare(cli.clone(), None, false);
        let report = Evidence::compare(cli.clone(), Some(cli), false).refreshed(
            predecessor,
            true,
            false,
            false,
        );
        assert!(!report.cycled);
        assert_eq!(report.reasons, ["profile_refresh", "match"]);
    }

    #[test]
    fn recovery_never_suggests_unavailable_restart_or_a_stop_chain() {
        let cli = pin(&"a".repeat(40), Some(false));
        let evidence = Evidence::compare(cli, None, false);
        for ownable in [true, false] {
            let text = evidence.recovery(ownable);
            assert!(!text.contains("daemon restart"));
            assert!(!text.contains("&&"));
            assert!(!text.contains("daemon stop"));
        }
    }
}
