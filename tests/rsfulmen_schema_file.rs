//! File-backed wait-params validation via rsfulmen 0.2.0.
//!
//! The schema file is an on-disk copy of the Chanvoy daemon RPC
//! `wait_channel_v3` params schema. Chanvoy does not git-pin Crucible;
//! this path is the catalog the crate does not embed.

use std::path::{Path, PathBuf};

use rsfulmen::schema_validation::{
    validate_instance_with_schema_file, FileSchemaOptions, ValidationIssue,
};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/chanvoy-daemon-rpc")
}

fn schema_path() -> PathBuf {
    fixture_dir().join("wait_channel_v3.params.schema.json")
}

fn validate_fixture(name: &str) -> Vec<ValidationIssue> {
    let bytes = std::fs::read(fixture_dir().join(name)).expect("read fixture");
    let instance: serde_json::Value = serde_json::from_slice(&bytes).expect("parse fixture");
    validate_instance_with_schema_file(&schema_path(), &instance, FileSchemaOptions::default())
        .unwrap_or_else(|err| panic!("file-backed validation failed to run: {err}"))
}

#[test]
fn wait_channel_v3_params_schema_accepts_conforming_instance() {
    let issues = validate_fixture("conforming-bare-wait.json");
    assert!(
        issues.is_empty(),
        "expected conforming wait params to pass, got {issues:?}"
    );
}

#[test]
fn wait_channel_v3_params_schema_rejects_unknown_property() {
    let issues = validate_fixture("negative-unknown-property.json");
    assert!(!issues.is_empty(), "unknown property must fail closed");
    assert!(
        issues.iter().any(|issue| {
            issue.keyword.as_deref() == Some("additionalProperties")
                && (issue.pointer.is_empty() || issue.pointer == "/force")
        }),
        "expected additionalProperties at root or /force, got {issues:?}"
    );
}
