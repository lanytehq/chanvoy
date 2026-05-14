//! PER-032 Item J Tier-B — `release-smoke.sh` log-scrub contract test.
//!
//! Locks the contract for `scrub_stream` in
//! `scripts/lib-release-smoke.sh`. The scrubbed output must never
//! contain:
//!
//! - any 26-char lowercase-alphanumeric Mattermost ID
//! - the live `${LANYTE_MM_URL}` (secrev review of PR #27 on 2026-05-13)
//! - the smoke channel / team / bot username
//!
//! Caught during secrev review: the original cut of `scrub_log` only
//! handled IDs and the smoke channel/team/bot names — `LANYTE_MM_URL`
//! leaked through into the scrubbed log, conflicting with
//! `REPOSITORY_SAFETY_PROTOCOLS.md` (no live MM URLs in committed
//! artifacts). The fix landed `${LANYTE_MM_URL:+ -e ...}` into the
//! sed pipeline and this test pins it.
//!
//! Surface that depends on this contract: the script header advertises
//! the scrubbed log as safe to include in release notes. A regression
//! that re-introduces a leak channel (e.g., logging another env var,
//! or adding a new identifier class) fails this test before the
//! release-smoke output is ever published.
//!
//! Test data: all MM URL literals in this file are RFC 2606 reserved
//! `.invalid` placeholders (`https://mattermost.test.invalid`) — never
//! the live host. Caught during secrev re-review of PR #27 on
//! 2026-05-13: the original cut of this test committed the live MM
//! host literal as test data, which is itself the exact leak channel
//! we are guarding against. Test fixtures must not be a back door for
//! the artifact-scrubbing contract — including doc comments.

#![allow(dead_code)]

use std::io::Write;
use std::process::{Command, Stdio};

/// Pipe `input` through `scrub_stream` (sourced from
/// `scripts/lib-release-smoke.sh`) with the given env. Returns
/// captured stdout. `env_overrides` are passed as
/// `(NAME, value)` pairs and exported before the function runs.
fn scrub_stream(input: &str, env_overrides: &[(&str, &str)]) -> String {
    let exports: String = env_overrides
        .iter()
        .map(|(k, v)| format!("export {k}={};\n", shell_quote(v)))
        .collect();
    let script = format!("{exports}source scripts/lib-release-smoke.sh && scrub_stream");
    let mut child = Command::new("bash")
        .arg("-c")
        .arg(&script)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bash");
    {
        let stdin = child.stdin.as_mut().expect("child stdin");
        stdin
            .write_all(input.as_bytes())
            .expect("write to child stdin");
    }
    let out = child.wait_with_output().expect("wait for child");
    assert!(
        out.status.success(),
        "bash exit non-zero: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf-8 stdout")
}

fn shell_quote(s: &str) -> String {
    let mut buf = String::with_capacity(s.len() + 2);
    buf.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            buf.push_str(r"'\''");
        } else {
            buf.push(ch);
        }
    }
    buf.push('\'');
    buf
}

/// MM IDs (26-char lowercase alphanumeric) get the `<mm-id>` placeholder.
#[test]
fn scrubs_mm_ids() {
    let input = "post=abc123def456ghi789jkl012mn channel=mnopqrstuvwx0123456789abcd\n";
    let out = scrub_stream(input, &[]);
    assert!(
        !out.contains("abc123def456ghi789jkl012mn"),
        "post id leaked: {out:?}"
    );
    assert!(
        !out.contains("mnopqrstuvwx0123456789abcd"),
        "channel id leaked: {out:?}"
    );
    assert_eq!(
        out.matches("<mm-id>").count(),
        2,
        "two replacements: {out:?}"
    );
}

/// AC #10 / secrev regression — `LANYTE_MM_URL` must be scrubbed.
/// Uses an RFC 2606 reserved `.invalid` placeholder host so the test
/// fixture itself does not commit a live MM URL.
#[test]
fn scrubs_live_mm_url() {
    let input = "[release-smoke] team=org-3leaps-test channel=chanvoy-smoke-v0.2.2 url=https://mattermost.test.invalid\n";
    let out = scrub_stream(
        input,
        &[("LANYTE_MM_URL", "https://mattermost.test.invalid")],
    );
    assert!(
        !out.contains("https://mattermost.test.invalid"),
        "LANYTE_MM_URL leaked through scrub_stream — secrev regression: {out:?}"
    );
    assert!(
        out.contains("<mm-url>"),
        "expected <mm-url> placeholder: {out:?}"
    );
}

/// Smoke channel name gets `<smoke-channel>`.
#[test]
fn scrubs_smoke_channel() {
    let input = "creating channel chanvoy-smoke-v0.2.2 on team org-3leaps-test\n";
    let out = scrub_stream(
        input,
        &[
            ("SMOKE_CHANNEL", "chanvoy-smoke-v0.2.2"),
            ("SMOKE_TEAM", "org-3leaps-test"),
        ],
    );
    assert!(
        !out.contains("chanvoy-smoke-v0.2.2"),
        "channel name leaked: {out:?}"
    );
    assert!(
        out.contains("<smoke-channel>"),
        "expected <smoke-channel>: {out:?}"
    );
    assert!(
        !out.contains("org-3leaps-test"),
        "team name leaked: {out:?}"
    );
    assert!(
        out.contains("<smoke-team>"),
        "expected <smoke-team>: {out:?}"
    );
}

/// Bot username gets `<smoke-bot>`.
#[test]
fn scrubs_smoke_bot_username() {
    let input = "as user agent-bravo-devlead\n";
    let out = scrub_stream(input, &[("SMOKE_BOT_USERNAME", "agent-bravo-devlead")]);
    assert!(
        !out.contains("agent-bravo-devlead"),
        "bot username leaked: {out:?}"
    );
    assert!(out.contains("<smoke-bot>"), "expected <smoke-bot>: {out:?}");
}

/// Unset SMOKE_BOT_USERNAME — the empty `${VAR:+ -e ...}` expansion
/// must not emit a malformed `s||<x>|g` regex (which would no-op or
/// misbehave depending on sed implementation). The rest of the
/// pipeline must still work.
#[test]
fn unset_bot_username_does_not_break_pipeline() {
    let input = "post=abc123def456ghi789jkl012mn url=https://mattermost.test.invalid\n";
    let out = scrub_stream(
        input,
        &[("LANYTE_MM_URL", "https://mattermost.test.invalid")],
    );
    assert!(
        !out.contains("abc123def456ghi789jkl012mn"),
        "id should scrub: {out:?}"
    );
    assert!(
        !out.contains("https://mattermost.test.invalid"),
        "url should scrub: {out:?}"
    );
}

/// Realistic header-line + post-id combo, all env vars set. The
/// scrubbed line should contain no live identifiers at all.
#[test]
fn full_log_line_combo_scrubs_clean() {
    let input = "[release-smoke] team=org-3leaps-test channel=chanvoy-smoke-v0.2.2 url=https://mattermost.test.invalid as=agent-bravo-devlead newest=abc123def456ghi789jkl012mn\n";
    let out = scrub_stream(
        input,
        &[
            ("LANYTE_MM_URL", "https://mattermost.test.invalid"),
            ("SMOKE_CHANNEL", "chanvoy-smoke-v0.2.2"),
            ("SMOKE_TEAM", "org-3leaps-test"),
            ("SMOKE_BOT_USERNAME", "agent-bravo-devlead"),
        ],
    );
    for leak in [
        "https://mattermost.test.invalid",
        "chanvoy-smoke-v0.2.2",
        "org-3leaps-test",
        "agent-bravo-devlead",
        "abc123def456ghi789jkl012mn",
    ] {
        assert!(
            !out.contains(leak),
            "live identifier {leak:?} leaked through scrub_stream: {out:?}"
        );
    }
    for placeholder in [
        "<mm-url>",
        "<smoke-channel>",
        "<smoke-team>",
        "<smoke-bot>",
        "<mm-id>",
    ] {
        assert!(
            out.contains(placeholder),
            "expected placeholder {placeholder:?} in scrubbed output: {out:?}"
        );
    }
}

/// Multi-line input — every leak channel must be scrubbed across the
/// whole stream, not just the first line.
#[test]
fn scrubs_across_multiple_lines() {
    let input = "\
[release-smoke] starting chanvoy v0.2.2
[release-smoke] url=https://mattermost.test.invalid
+ chanvoy whoami
{\"id\":\"abc123def456ghi789jkl012mn\"}
[release-smoke] team=org-3leaps-test channel=chanvoy-smoke-v0.2.2
";
    let out = scrub_stream(
        input,
        &[
            ("LANYTE_MM_URL", "https://mattermost.test.invalid"),
            ("SMOKE_CHANNEL", "chanvoy-smoke-v0.2.2"),
            ("SMOKE_TEAM", "org-3leaps-test"),
        ],
    );
    assert_eq!(out.matches("https://mattermost.test.invalid").count(), 0);
    assert_eq!(out.matches("abc123def456ghi789jkl012mn").count(), 0);
    assert_eq!(out.matches("chanvoy-smoke-v0.2.2").count(), 0);
    assert_eq!(out.matches("org-3leaps-test").count(), 0);
}
