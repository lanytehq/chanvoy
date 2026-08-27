//! Contract tests for downloaded release-binary identity verification.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn verifier_script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("verify-release-binary-identity.sh")
}

fn run(command: &mut Command, label: &str) -> Output {
    let output = command.output().expect("spawn command");
    assert!(
        output.status.success(),
        "{label} failed\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn run_git(repo: &Path, args: &[&str]) -> Output {
    run(
        Command::new("git").args(args).current_dir(repo),
        &format!("git {}", args.join(" ")),
    )
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn downloaded_binary_identity_fails_wrong_version_commit_or_dirty() {
    let root = tempfile::tempdir().expect("temporary release identity repo");
    let repo = root.path();
    run(
        Command::new("git").args(["init", "-b", "main"]).arg(repo),
        "git init",
    );
    run_git(repo, &["config", "user.name", "Release Identity Test"]);
    run_git(
        repo,
        &["config", "user.email", "release-identity@example.invalid"],
    );
    std::fs::write(repo.join("VERSION"), "0.3.1\n").expect("write VERSION");
    std::fs::write(repo.join("payload.txt"), "release\n").expect("write payload");
    run_git(repo, &["add", "VERSION", "payload.txt"]);
    run_git(repo, &["commit", "-m", "release identity commit"]);
    run_git(repo, &["tag", "-a", "v0.3.1", "-m", "v0.3.1"]);
    let commit_output = run_git(repo, &["rev-parse", "HEAD"]);
    let full_commit = String::from_utf8_lossy(&commit_output.stdout)
        .trim()
        .to_owned();
    let short_commit = &full_commit[..7];

    let release_dir = repo.join("release/v0.3.1");
    std::fs::create_dir_all(&release_dir).expect("create release directory");
    let binary = release_dir.join("chanvoy-test-host");
    std::fs::write(
        &binary,
        format!(
            r#"#!/usr/bin/env bash
case "${{FAKE_IDENTITY_STATE:-good}}" in
wrong-version) version="9.9.9"; commit="{short_commit}"; dirty="false" ;;
wrong-commit) version="0.3.1"; commit="deadbee"; dirty="false" ;;
dirty) version="0.3.1"; commit="{short_commit}"; dirty="true" ;;
*) version="0.3.1"; commit="{short_commit}"; dirty="false" ;;
esac
printf 'chanvoy %s\nCommit: %s\nDirty: %s\n' "$version" "$commit" "$dirty"
"#
        ),
    )
    .expect("write fake downloaded binary");
    let mut permissions = std::fs::metadata(&binary)
        .expect("fake binary metadata")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&binary, permissions).expect("make fake binary executable");

    let verify = |state: &str| {
        Command::new("bash")
            .arg(verifier_script())
            .args(["v0.3.1", release_dir.to_str().expect("release dir UTF-8")])
            .arg(&binary)
            .current_dir(repo)
            .env("FAKE_IDENTITY_STATE", state)
            .output()
            .expect("execute identity verifier")
    };

    let good = verify("good");
    assert!(
        good.status.success(),
        "matching binary must pass: {}",
        stderr(&good)
    );
    for state in ["wrong-version", "wrong-commit", "dirty"] {
        let output = verify(state);
        assert!(
            !output.status.success(),
            "{state} must fail closed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            stderr(&output)
        );
    }
}

#[test]
fn release_undraft_depends_on_executable_identity_gate() {
    let makefile = include_str!("../Makefile");
    assert!(makefile.contains("release-verify-identity: release-guard-release-target"));
    assert!(makefile.contains("release-undraft: release-verify-identity"));
    assert!(makefile.contains(
        "scripts/verify-release-binary-identity.sh \"$(RELEASE_TAG)\" \"$(RELEASE_DIR)\""
    ));
    assert!(!makefile.contains("RELEASE_IDENTITY_BINARY"));
}
