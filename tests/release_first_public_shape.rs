//! Static contracts for the first-public release documentation and gates.

#[test]
fn release_notes_are_final_dated_and_distribution_honest() {
    let checkpoint = include_str!("../docs/releases/v0.3.0.md");
    assert!(checkpoint.contains("**Checkpoint Date**: 2026-08-27"));
    assert!(checkpoint.contains("Signed development checkpoint"));
    assert!(checkpoint.contains("no GitHub Release"));
    assert!(checkpoint.contains("no crates.io package"));

    let release = include_str!("../docs/releases/v0.3.1.md");
    assert!(release.contains("**Release Date**: 2026-08-27"));
    assert!(release.contains("First public GitHub binary release"));
    assert!(release.contains("not on crates.io"));
    assert!(!release.contains("**Release Date**: unreleased"));
    assert!(release.contains("[`/RELEASE_CHECKLIST.md`](../../RELEASE_CHECKLIST.md)"));
}

#[test]
fn changelog_has_checkpoint_and_first_public_entries() {
    let changelog = include_str!("../CHANGELOG.md");
    assert!(changelog.contains("## [0.3.1] - 2026-08-27"));
    assert!(changelog.contains("## [0.3.0] - 2026-08-27"));
    assert!(changelog.contains("Signed development checkpoint only"));
    assert!(!changelog.contains("## [0.3.1] - unreleased"));
    assert!(!changelog.contains("## [0.3.0] - unreleased"));
}

#[test]
fn root_release_notes_lead_with_first_public_and_date_the_checkpoint() {
    let notes = include_str!("../RELEASE_NOTES.md");
    let first_public = notes
        .find("## v0.3.1 - 2026-08-27")
        .expect("root notes include the first public release");
    let checkpoint = notes
        .find("## v0.3.0 - 2026-08-27")
        .expect("root notes include the dated checkpoint");
    assert!(first_public < checkpoint);
    assert!(notes.contains("First public distribution"));
    assert!(notes.contains("docs/releases/v0.3.1.md"));
    assert!(notes.contains("Signed development checkpoint, not distributed"));
    assert!(!notes.contains("v0.3.0 (unreleased)"));
    assert_eq!(
        notes
            .lines()
            .filter(|line| line.starts_with("## v"))
            .count(),
        3,
        "root notes retain only the three most recent releases"
    );
}

#[test]
fn checklist_uses_signed_separate_tag_targets_and_safe_visibility_order() {
    let checklist = include_str!("../RELEASE_CHECKLIST.md");
    assert!(!checklist.contains("git tag -a"));
    assert!(checklist.contains("make release-tag"));
    assert!(checklist.contains("make release-tag-push"));
    assert!(checklist.contains("Does not push"));
    assert!(checklist.contains("Neither target force-updates a tag"));

    let push = checklist
        .find("make release-tag-push")
        .expect("tag-only push is documented");
    let draft = checklist
        .find("## 6. GHA workflow")
        .expect("draft workflow is documented");
    let visibility = checklist
        .find("## 7. First-public visibility gate")
        .expect("visibility gate is documented");
    let undraft = visibility
        + checklist[visibility..]
            .find("make release-undraft")
            .expect("undraft is documented after visibility");
    assert!(push < draft && draft < visibility && visibility < undraft);
}

#[test]
fn follow_docs_distinguish_output_exit_and_foreground_wake() {
    for document in [
        include_str!("../docs/getting-started.md"),
        include_str!("../docs/operator-guide.md"),
    ] {
        let lower = document.to_ascii_lowercase();
        assert!(lower.contains("emitted") && lower.contains("output"));
        assert!(lower.contains("only process exit"));
        assert!(lower.contains("foreground"));
        assert!(lower.contains("backlog"));
        assert!(lower.contains("live records"));
        assert!(lower.contains("watcher") || lower.contains("doorbell"));
    }
}

#[test]
fn public_license_files_and_registry_blockers_are_explicit() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for license in ["LICENSE", "LICENSE-MIT", "LICENSE-APACHE"] {
        assert!(root.join(license).is_file(), "missing {license}");
    }

    let manifest = include_str!("../Cargo.toml");
    assert!(manifest.contains("publish = false"));
    assert!(manifest.contains("chanvoy-cli = { path = \"crates/chanvoy-cli\" }"));

    let notes = include_str!("../docs/releases/v0.3.1.md");
    assert!(notes.contains("unpublished workspace crates"));
    assert!(notes.contains("runtime git"));
}
