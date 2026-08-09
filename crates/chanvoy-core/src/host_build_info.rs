//! Host binary identity for the process that compiled this crate.
//!
//! Used by CLI `version` / `version --extended` (CLI pin) and by the daemon
//! `daemon_status` surface (daemon pin). Comparing the two is the PER-038A
//! generation-honesty check after `make install`.
//!
//! Values are injected by `build.rs` (`FULMEN_HOST_*` rustc-env). This is a
//! local Phase-A resolver until a shared library helper lands in a later
//! rsfulmen release. Host commit is the **app** git SHA, never a dependency
//! Crucible/SSOT pin.
//!
//! Machine-readable `commit` is the full object name (typically 40-char hex).
//! `commit_short` is the 7-char display form used on human `Commit:` lines.

/// Build-time identity of this chanvoy process binary.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HostBuildInfo {
    pub version: String,
    /// Full git object name when known (prefer 40-char hex for machine pin).
    pub commit: String,
    /// Short display form (typically 7 chars). Same as `commit` when unknown.
    pub commit_short: String,
    pub build_date: String,
    /// `None` when dirty state was not injected (honest unknown).
    pub dirty: Option<bool>,
    pub rustc: String,
    pub platform: String,
}

/// Resolve host identity from compile-time env with honest unknowns.
pub fn resolve() -> HostBuildInfo {
    let commit = option_env!("FULMEN_HOST_COMMIT")
        .unwrap_or("unknown")
        .to_string();
    let commit_short = option_env!("FULMEN_HOST_COMMIT_SHORT")
        .map(str::to_string)
        .unwrap_or_else(|| short_from_full(&commit));
    HostBuildInfo {
        version: option_env!("FULMEN_HOST_VERSION")
            .unwrap_or(env!("CARGO_PKG_VERSION"))
            .to_string(),
        commit,
        commit_short,
        build_date: option_env!("FULMEN_HOST_BUILD_DATE")
            .unwrap_or("unknown")
            .to_string(),
        dirty: option_env!("FULMEN_HOST_DIRTY").and_then(|s| match s {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        }),
        rustc: option_env!("FULMEN_HOST_RUSTC")
            .unwrap_or("unknown")
            .to_string(),
        platform: format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH),
    }
}

fn short_from_full(commit: &str) -> String {
    if commit == "unknown" || commit.len() < 7 {
        return commit.to_string();
    }
    commit.chars().take(7).collect()
}

/// Whether two host pins represent the same binary generation (PER-038A).
///
/// Returns `None` when either commit is unknown — honest incomplete, not a
/// false match. Dirty flags must also agree when both commits are known.
pub fn generation_match(cli: &HostBuildInfo, daemon: Option<&HostBuildInfo>) -> Option<bool> {
    let daemon = daemon?;
    if cli.commit == "unknown" || daemon.commit == "unknown" {
        return None;
    }
    Some(cli.commit == daemon.commit && cli.dirty == daemon.dirty)
}

/// Basic line: `chanvoy <semver>`.
pub fn format_basic(info: &HostBuildInfo) -> String {
    format!("chanvoy {}", info.version)
}

/// Extended multi-line block (host lines only; no dependency pins).
///
/// Human `Commit:` keeps the short form for scanability; machine consumers
/// must use `--json` where `commit` is the full object name.
pub fn format_extended(info: &HostBuildInfo) -> String {
    let mut lines = vec![
        format_basic(info),
        format!("Commit: {}", info.commit_short),
        format!("Built: {}", info.build_date),
        format!("Rustc: {}", info.rustc),
        format!("Platform: {}", info.platform),
    ];
    if let Some(dirty) = info.dirty {
        lines.push(format!("Dirty: {}", dirty));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(commit: &str, short: &str, dirty: Option<bool>) -> HostBuildInfo {
        HostBuildInfo {
            version: "0.3.0".into(),
            commit: commit.into(),
            commit_short: short.into(),
            build_date: "2026-08-07T12:00:00Z".into(),
            dirty,
            rustc: "rustc 1.89.0".into(),
            platform: "macos/aarch64".into(),
        }
    }

    #[test]
    fn basic_line_names_the_binary() {
        let info = sample(
            "abcdef1234567890abcdef1234567890abcdef12",
            "abcdef1",
            Some(false),
        );
        assert_eq!(format_basic(&info), "chanvoy 0.3.0");
    }

    #[test]
    fn extended_uses_short_commit_for_human_line() {
        let full = "abcdef1234567890abcdef1234567890abcdef12";
        let info = sample(full, "abcdef1", Some(true));
        let text = format_extended(&info);
        assert!(text.contains("chanvoy 0.3.0"));
        assert!(text.contains("Commit: abcdef1"));
        assert!(!text.contains(&format!("Commit: {full}")));
        assert!(text.contains("Built: 2026-08-07T12:00:00Z"));
        assert!(text.contains("Rustc: rustc 1.89.0"));
        assert!(text.contains("Platform: macos/aarch64"));
        assert!(text.contains("Dirty: true"));
    }

    #[test]
    fn json_shape_includes_full_and_short_commit() {
        let full = "abcdef1234567890abcdef1234567890abcdef12";
        let info = sample(full, "abcdef1", Some(false));
        let value = serde_json::to_value(&info).expect("serialize");
        assert_eq!(value["commit"], full);
        assert_eq!(value["commit_short"], "abcdef1");
        assert_eq!(value["version"], "0.3.0");
        assert_eq!(value["dirty"], false);
    }

    #[test]
    fn short_from_full_takes_seven_when_long() {
        assert_eq!(
            short_from_full("abcdef1234567890abcdef1234567890abcdef12"),
            "abcdef1"
        );
        assert_eq!(short_from_full("unknown"), "unknown");
        assert_eq!(short_from_full("abc"), "abc");
    }

    #[test]
    fn extended_omits_dirty_when_unknown() {
        let info = HostBuildInfo {
            version: "0.3.0".into(),
            commit: "unknown".into(),
            commit_short: "unknown".into(),
            build_date: "unknown".into(),
            dirty: None,
            rustc: "unknown".into(),
            platform: "linux/x86_64".into(),
        };
        let text = format_extended(&info);
        assert!(!text.to_lowercase().contains("dirty:"));
    }

    #[test]
    fn generation_match_true_when_commit_and_dirty_agree() {
        let full = "abcdef1234567890abcdef1234567890abcdef12";
        let a = sample(full, "abcdef1", Some(false));
        let b = sample(full, "abcdef1", Some(false));
        assert_eq!(generation_match(&a, Some(&b)), Some(true));
    }

    #[test]
    fn generation_match_false_on_commit_skew() {
        let a = sample(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "aaaaaaa",
            Some(false),
        );
        let b = sample(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "bbbbbbb",
            Some(false),
        );
        assert_eq!(generation_match(&a, Some(&b)), Some(false));
    }

    #[test]
    fn generation_match_none_when_daemon_missing_or_unknown() {
        let a = sample(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "aaaaaaa",
            Some(false),
        );
        assert_eq!(generation_match(&a, None), None);
        let unknown = sample("unknown", "unknown", None);
        assert_eq!(generation_match(&a, Some(&unknown)), None);
        assert_eq!(generation_match(&unknown, Some(&a)), None);
    }
}
