//! PER-032 Item J Tier-B — `release-smoke.sh` post-id parser test.
//!
//! Locks the contract between `chanvoy --json post` (which emits a
//! `PostReceipt { id, parent_id? }` body) and the smoke harness's
//! `extract_post_id` shell function. A regression in either direction
//! — renaming the JSON key chanvoy emits, or breaking the sed
//! extraction in the shell function — fails CI before reaching live
//! MM at v0.2.x RC time.
//!
//! Caught during devrev review of PR #27 on 2026-05-12: the original
//! cut of the smoke script extracted `"post_id"`, but the actual
//! `PostReceipt` JSON key is `"id"`. That would have failed the
//! smoke at the first `post` step in production. The fix landed
//! defensive — `id` first, `post_id` fallback — and this test pins
//! both shapes.

#![allow(dead_code)]

use std::process::{Command, Stdio};

/// Invoke `bash -c 'source scripts/lib-release-smoke.sh; extract_post_id "<json>"'`
/// and capture stdout. The lib file is sourced fresh per call so any
/// state pollution is impossible.
fn extract_post_id(json: &str) -> String {
    let script = format!(
        "source scripts/lib-release-smoke.sh && extract_post_id {}",
        shell_quote(json)
    );
    let out = Command::new("bash")
        .arg("-c")
        .arg(&script)
        .stdin(Stdio::null())
        .output()
        .expect("spawn bash");
    assert!(
        out.status.success(),
        "bash exit non-zero: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf-8 stdout")
}

/// POSIX shell quoting via single-quote wrap. Single quotes inside the
/// input are escaped by closing + escaping + reopening: `'` → `'\''`.
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

/// Canonical PostReceipt shape (PER-024) — `{"id": "<post_id>"}` only.
#[test]
fn extracts_id_from_canonical_post_receipt() {
    let json = r#"{"id":"post-abc123"}"#;
    assert_eq!(extract_post_id(json), "post-abc123");
}

/// PostReceipt with a `parent_id` field (threaded-reply variant per
/// PER-024 AC #3a — `parent_id` is additive and present only when
/// the post was a thread reply). The `id` field must still extract.
#[test]
fn extracts_id_when_parent_id_present() {
    let json = r#"{"id":"reply-xyz789","parent_id":"root-post-456"}"#;
    assert_eq!(extract_post_id(json), "reply-xyz789");
}

/// Pretty-printed JSON (`serde_json::to_string_pretty` shape — extra
/// whitespace, multi-line). `chanvoy --json` uses pretty output, so
/// this is the actual on-the-wire format the script sees.
#[test]
fn extracts_id_from_pretty_printed_json() {
    let json = "{\n  \"id\": \"post-pretty-001\",\n  \"parent_id\": \"root-002\"\n}";
    assert_eq!(extract_post_id(json), "post-pretty-001");
}

/// Forward-compat fallback: if chanvoy ever renames `id` → `post_id`
/// (or adds it as a sibling key), the parser still works. Devrev's
/// review noted this defensive shape as the right move.
#[test]
fn extracts_post_id_when_only_post_id_field_present() {
    let json = r#"{"post_id":"legacy-shape-001","other_field":"ignored"}"#;
    assert_eq!(extract_post_id(json), "legacy-shape-001");
}

/// When both `id` and `post_id` are present, `id` wins (canonical
/// PER-024 shape takes precedence over the forward-compat fallback).
#[test]
fn id_field_takes_precedence_over_post_id_field() {
    let json = r#"{"id":"canonical-id","post_id":"legacy-id"}"#;
    assert_eq!(extract_post_id(json), "canonical-id");
}

/// No matching key — empty string. The caller's job to detect empty
/// and fail loudly (script does this via `if [[ -z "${POST_ID}" ]];
/// then exit 1; fi`).
#[test]
fn empty_when_no_id_field_present() {
    let json = r#"{"unrelated":"value","also_unrelated":42}"#;
    assert_eq!(extract_post_id(json), "");
}

/// Empty JSON object — empty string output. Same caller contract.
#[test]
fn empty_when_json_is_empty_object() {
    assert_eq!(extract_post_id("{}"), "");
}

/// The original devrev-reported failure shape: input was the actual
/// `chanvoy --json post` output (i.e., the canonical `{"id": "..."}`
/// shape), the OLD script regex looked for `"post_id"`, and extraction
/// silently returned empty — leading to `[release-smoke] FAIL: could
/// not extract post_id`. This test guards against re-introducing the
/// asymmetry.
#[test]
fn devrev_regression_canonical_id_must_extract() {
    // Real-shape sample matching what chanvoy emits today.
    let json = r#"{"id":"abc123def456ghi789jkl012mn"}"#;
    let got = extract_post_id(json);
    assert!(
        !got.is_empty(),
        "PostReceipt canonical `id` field must extract; got empty (devrev regression)"
    );
    assert_eq!(got, "abc123def456ghi789jkl012mn");
}
