//! Locks the release-smoke channel-create invocation to the public CLI shape.

use std::process::Command;

fn derive_smoke_channel(version: &str, run_suffix: &str) -> std::process::Output {
    Command::new("bash")
        .args([
            "-c",
            r#"source scripts/lib-release-smoke.sh
derive_smoke_channel "$1" "$2""#,
            "derive-smoke-channel",
            version,
            run_suffix,
        ])
        .output()
        .expect("bash must execute the release-smoke helper")
}

fn validate_smoke_team(selected_team: &str, identity_team: &str) -> std::process::Output {
    Command::new("bash")
        .args([
            "-c",
            r#"source scripts/lib-release-smoke.sh
validate_smoke_team "$1" "$2""#,
            "validate-smoke-team",
            selected_team,
            identity_team,
        ])
        .output()
        .expect("bash must execute the release-smoke helper")
}

#[test]
fn semantic_version_derives_valid_smoke_slug() {
    let output = derive_smoke_channel("0.3.0", "20260827193000-12345");
    assert!(
        output.status.success(),
        "semantic version must derive a smoke slug: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout)
            .expect("derived slug must be UTF-8")
            .trim(),
        "chanvoy-smoke-v0-3-0-20260827193000-12345"
    );
}

#[test]
fn unsupported_version_characters_fail_before_channel_creation() {
    let output = derive_smoke_channel("0.3.0+candidate", "20260827193000-12345");
    assert!(
        !output.status.success(),
        "unsupported slug characters must fail closed"
    );
    assert!(
        output.stdout.is_empty(),
        "invalid derivation must not emit a channel name"
    );
}

#[test]
fn run_suffix_makes_valid_bounded_slugs_unique() {
    let first = derive_smoke_channel("0.3.0", "20260827193000-12345");
    let second = derive_smoke_channel("0.3.0", "20260827193001-12346");
    assert!(first.status.success() && second.status.success());

    let first = String::from_utf8(first.stdout).expect("first slug must be UTF-8");
    let second = String::from_utf8(second.stdout).expect("second slug must be UTF-8");
    let first = first.trim();
    let second = second.trim();

    assert_ne!(first, second, "separate runs must derive unique slugs");
    for slug in [first, second] {
        assert!(slug.starts_with("chanvoy-smoke-v0-3-0-"));
        assert!(slug.len() <= 64, "Mattermost channel slug exceeds 64 bytes");
        assert!(
            slug.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
            }),
            "derived slug contains an unsupported character: {slug}"
        );
    }
}

#[test]
fn missing_or_invalid_run_suffix_fails_closed() {
    for suffix in ["", "20260827-ABC", "20260827+123"] {
        let output = derive_smoke_channel("0.3.0", suffix);
        assert!(
            !output.status.success(),
            "invalid suffix must fail closed: {suffix}"
        );
        assert!(output.stdout.is_empty());
    }

    let overlong_suffix = "1".repeat(64);
    let output = derive_smoke_channel("0.3.0", &overlong_suffix);
    assert!(
        !output.status.success(),
        "a slug over Mattermost's 64-byte limit must fail closed"
    );
    assert!(output.stdout.is_empty());
}

#[test]
fn smoke_team_must_match_identity_primary_team() {
    assert!(
        validate_smoke_team("org-lanytehq", "org-lanytehq")
            .status
            .success(),
        "the identity profile's primary team must pass"
    );
    assert!(
        !validate_smoke_team("org-other", "org-lanytehq")
            .status
            .success(),
        "a cross-team smoke target must fail before mutation"
    );
    assert!(
        !validate_smoke_team("org-lanytehq", "").status.success(),
        "a missing authoritative identity team must fail closed"
    );
}

#[test]
fn release_smoke_uses_positional_channel_purpose() {
    let script = include_str!("../scripts/release-smoke.sh");
    let start = script
        .find("run \"channel create\"")
        .expect("release smoke must create its disposable channel");
    let end = script[start..]
        .find("SMOKE_CHANNEL_CREATED=1")
        .map(|offset| start + offset)
        .expect("release smoke must record successful channel creation");
    let invocation = &script[start..end];

    assert!(
        !invocation.contains("--purpose"),
        "channel create accepts purpose as a positional argument"
    );

    let team = invocation
        .find("--team \"${SMOKE_TEAM}\"")
        .expect("smoke invocation must select the team explicitly");
    let name = invocation
        .find("\"${SMOKE_CHANNEL}\"")
        .expect("smoke invocation must pass the channel name");
    let display = invocation
        .find("\"chanvoy smoke v${VERSION}\"")
        .expect("smoke invocation must pass the display name");
    let purpose = invocation
        .find("\"PER-032 Tier-B URL-shape smoke for chanvoy v${VERSION}. Disposable; archived at script exit.\"")
        .expect("smoke invocation must pass the positional purpose");

    assert!(
        team < name && name < display && display < purpose,
        "release smoke must follow: channel create --team TEAM NAME DISPLAY [PURPOSE]"
    );
}

#[test]
fn release_smoke_archives_without_an_unsupported_team_flag() {
    let script = include_str!("../scripts/release-smoke.sh");
    let archive_lines: Vec<_> = script
        .lines()
        .filter(|line| line.contains("chanvoy channel archive"))
        .collect();

    assert_eq!(
        archive_lines.len(),
        1,
        "trap cleanup must contain exactly one direct archive invocation"
    );
    assert!(
        !archive_lines[0].contains("--team"),
        "trap cleanup must use channel archive NAME"
    );
    assert!(
        script.contains("run \"channel archive\" channel archive \"${SMOKE_CHANNEL}\""),
        "the final smoke step must use channel archive NAME"
    );
    assert!(
        !script.contains("channel archive \"${SMOKE_CHANNEL}\" --team"),
        "no smoke archive path may pass the unsupported team flag"
    );
}
