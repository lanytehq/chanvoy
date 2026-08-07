//! Host binary identity for `chanvoy version` / `version --extended`.
//!
//! Values are injected by `build.rs` (`FULMEN_HOST_*` rustc-env). This is a
//! local Phase-A resolver until a shared library helper lands in a later
//! rsfulmen release. Host commit is the **app** git SHA, never a dependency
//! Crucible/SSOT pin.

/// Build-time identity of the installed chanvoy binary.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct HostBuildInfo {
    pub version: String,
    pub commit: String,
    pub build_date: String,
    /// `None` when dirty state was not injected (honest unknown).
    pub dirty: Option<bool>,
    pub rustc: String,
    pub platform: String,
}

/// Resolve host identity from compile-time env with honest unknowns.
pub fn resolve() -> HostBuildInfo {
    HostBuildInfo {
        version: option_env!("FULMEN_HOST_VERSION")
            .unwrap_or(env!("CARGO_PKG_VERSION"))
            .to_string(),
        commit: option_env!("FULMEN_HOST_COMMIT")
            .unwrap_or("unknown")
            .to_string(),
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

/// Basic line: `chanvoy <semver>`.
pub fn format_basic(info: &HostBuildInfo) -> String {
    format!("chanvoy {}", info.version)
}

/// Extended multi-line block (host lines only; no dependency pins).
pub fn format_extended(info: &HostBuildInfo) -> String {
    let mut lines = vec![
        format_basic(info),
        format!("Commit: {}", info.commit),
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

    #[test]
    fn basic_line_names_the_binary() {
        let info = HostBuildInfo {
            version: "0.3.0".into(),
            commit: "abcdef1".into(),
            build_date: "2026-08-07T12:00:00Z".into(),
            dirty: Some(false),
            rustc: "rustc 1.89.0".into(),
            platform: "macos/aarch64".into(),
        };
        assert_eq!(format_basic(&info), "chanvoy 0.3.0");
    }

    #[test]
    fn extended_includes_host_fields_and_dirty_when_known() {
        let info = HostBuildInfo {
            version: "0.3.0".into(),
            commit: "abcdef1".into(),
            build_date: "2026-08-07T12:00:00Z".into(),
            dirty: Some(true),
            rustc: "rustc 1.89.0".into(),
            platform: "macos/aarch64".into(),
        };
        let text = format_extended(&info);
        assert!(text.contains("chanvoy 0.3.0"));
        assert!(text.contains("Commit: abcdef1"));
        assert!(text.contains("Built: 2026-08-07T12:00:00Z"));
        assert!(text.contains("Rustc: rustc 1.89.0"));
        assert!(text.contains("Platform: macos/aarch64"));
        assert!(text.contains("Dirty: true"));
    }

    #[test]
    fn extended_omits_dirty_when_unknown() {
        let info = HostBuildInfo {
            version: "0.3.0".into(),
            commit: "unknown".into(),
            build_date: "unknown".into(),
            dirty: None,
            rustc: "unknown".into(),
            platform: "linux/x86_64".into(),
        };
        let text = format_extended(&info);
        assert!(!text.to_lowercase().contains("dirty:"));
    }
}
