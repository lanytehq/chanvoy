//! Contract tests for the signed release-tag ceremony.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

struct ReleaseRepo {
    _root: TempDir,
    work: PathBuf,
    remote: PathBuf,
}

impl ReleaseRepo {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("temporary release repo");
        let work = root.path().join("work");
        let remote = root.path().join("remote.git");

        run(
            Command::new("git").args(["init", "--bare"]).arg(&remote),
            "initialize bare origin",
        );
        run(
            Command::new("git").args(["init", "-b", "main"]).arg(&work),
            "initialize release worktree",
        );
        run_git(&work, &["config", "user.name", "Release Test"]);
        run_git(&work, &["config", "user.email", "release@example.invalid"]);
        run_git(&work, &["remote", "add", "origin", path_str(&remote)]);

        std::fs::write(work.join("VERSION"), "0.3.1\n").expect("write VERSION");
        std::fs::write(work.join("payload.txt"), "first\n").expect("write payload");
        run_git(&work, &["add", "VERSION", "payload.txt"]);
        run_git(&work, &["commit", "-m", "initial release commit"]);
        run_git(&work, &["push", "-u", "origin", "main"]);

        Self {
            _root: root,
            work,
            remote,
        }
    }

    fn guard(&self, mode: &str) -> Output {
        Command::new("bash")
            .arg(guard_script())
            .arg(mode)
            .current_dir(&self.work)
            .env("CHANVOY_RELEASE_TAG", "v0.3.1")
            .env("RELEASE_TAG", "v0.3.1")
            .output()
            .expect("execute release tag guard")
    }

    fn guard_with_signer(&self, mode: &str, fingerprint: &str, homedir: &Path) -> Output {
        Command::new("bash")
            .arg(guard_script())
            .arg(mode)
            .current_dir(&self.work)
            .env("CHANVOY_RELEASE_TAG", "v0.3.1")
            .env("RELEASE_TAG", "v0.3.1")
            .env("CHANVOY_PGP_KEY_ID", fingerprint)
            .env("CHANVOY_GPG_HOMEDIR", homedir)
            .output()
            .expect("execute signed release tag guard")
    }

    fn install_fingerprint_contract(&self, fingerprint: &str) {
        self.install_raw_fingerprint_contract(&format!(
            "# Release public-key fingerprints\n\
             minisign  36a80acfa44f5cf9ac402d3ce8e51fcc083e5a1dca22180d6a0ea85b7e5340ad\n\
             gpg       {fingerprint}\n"
        ));
    }

    fn install_raw_fingerprint_contract(&self, contract: &str) {
        std::fs::create_dir_all(self.work.join("keys")).expect("create keys directory");
        std::fs::write(self.work.join("keys/expected-fingerprints.txt"), contract)
            .expect("write fingerprint contract");
        run_git(&self.work, &["add", "keys/expected-fingerprints.txt"]);
        run_git(&self.work, &["commit", "-m", "add fingerprint contract"]);
        run_git(&self.work, &["push", "origin", "main"]);
    }
}

fn guard_script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("release-guard-tag-version.sh")
}

fn github_guard_script() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("release-guard-github-release.sh")
}

fn path_str(path: &Path) -> &str {
    path.to_str().expect("temporary path is UTF-8")
}

fn run_git(repo: &Path, args: &[&str]) {
    run(
        Command::new("git").args(args).current_dir(repo),
        &format!("git {}", args.join(" ")),
    );
}

fn run(command: &mut Command, label: &str) {
    let output = command.output().expect("spawn command");
    assert!(
        output.status.success(),
        "{label} failed\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn generate_signing_key() -> (TempDir, String) {
    let homedir = tempfile::tempdir().expect("temporary GPG homedir");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(homedir.path())
            .expect("GPG homedir metadata")
            .permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(homedir.path(), permissions).expect("secure GPG homedir");
    }
    run(
        Command::new("gpg").args([
            "--homedir",
            path_str(homedir.path()),
            "--batch",
            "--passphrase",
            "",
            "--quick-generate-key",
            "Release Guard Test <release-guard@example.invalid>",
            "ed25519",
            "sign",
            "0",
        ]),
        "generate release test key",
    );
    let output = Command::new("gpg")
        .args([
            "--homedir",
            path_str(homedir.path()),
            "--batch",
            "--with-colons",
            "--fingerprint",
            "Release Guard Test",
        ])
        .output()
        .expect("read release test fingerprint");
    assert!(
        output.status.success(),
        "fingerprint lookup failed: {}",
        stderr(&output)
    );
    let fingerprint = String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|line| {
            let fields: Vec<_> = line.split(':').collect();
            (fields.first() == Some(&"fpr"))
                .then(|| fields.get(9).copied())
                .flatten()
        })
        .expect("primary fingerprint")
        .to_owned();
    (homedir, fingerprint)
}

#[test]
fn pre_create_accepts_clean_synced_main_with_absent_tag() {
    let repo = ReleaseRepo::new();
    let output = repo.guard("pre-create");
    assert!(
        output.status.success(),
        "valid pre-create state must pass: {}",
        stderr(&output)
    );
}

#[test]
fn conflicting_or_wrong_tag_overrides_fail_before_signing() {
    let repo = ReleaseRepo::new();

    let conflict = Command::new("bash")
        .arg(guard_script())
        .arg("pre-create")
        .current_dir(&repo.work)
        .env("CHANVOY_RELEASE_TAG", "v0.3.1")
        .env("RELEASE_TAG", "v0.3.2")
        .output()
        .expect("execute conflicting guard");
    assert!(!conflict.status.success());
    assert!(stderr(&conflict).contains("disagree"));

    let wrong = Command::new("bash")
        .arg(guard_script())
        .arg("pre-create")
        .current_dir(&repo.work)
        .env("CHANVOY_RELEASE_TAG", "v9.9.9")
        .env_remove("RELEASE_TAG")
        .output()
        .expect("execute mismatched guard");
    assert!(!wrong.status.success());
    assert!(stderr(&wrong).contains("does not match VERSION"));
}

#[test]
fn dirty_non_main_and_unsynced_trees_fail_closed() {
    let dirty = ReleaseRepo::new();
    std::fs::write(dirty.work.join("untracked.txt"), "dirty\n").expect("write untracked file");
    let output = dirty.guard("pre-create");
    assert!(!output.status.success());
    assert!(stderr(&output).contains("clean working tree"));

    let non_main = ReleaseRepo::new();
    run_git(&non_main.work, &["switch", "-c", "release-candidate"]);
    let output = non_main.guard("pre-create");
    assert!(!output.status.success());
    assert!(stderr(&output).contains("requires main"));

    let unsynced = ReleaseRepo::new();
    std::fs::write(unsynced.work.join("payload.txt"), "second\n").expect("update payload");
    run_git(&unsynced.work, &["add", "payload.txt"]);
    run_git(&unsynced.work, &["commit", "-m", "unpushed release commit"]);
    let output = unsynced.guard("pre-create");
    assert!(!output.status.success());
    assert!(stderr(&output).contains("not the exact live origin main"));
}

#[test]
fn stale_origin_tracking_ref_cannot_hide_newer_remote_main() {
    let repo = ReleaseRepo::new();
    let publisher = repo._root.path().join("publisher");
    run(
        Command::new("git")
            .args(["clone", "--branch", "main"])
            .arg(&repo.remote)
            .arg(&publisher),
        "clone independent publisher",
    );
    run_git(&publisher, &["config", "user.name", "Remote Publisher"]);
    run_git(
        &publisher,
        &["config", "user.email", "publisher@example.invalid"],
    );
    std::fs::write(publisher.join("payload.txt"), "remote advance\n").expect("advance remote");
    run_git(&publisher, &["add", "payload.txt"]);
    run_git(&publisher, &["commit", "-m", "advance remote main"]);
    run_git(&publisher, &["push", "origin", "main"]);

    let local_tracking = Command::new("git")
        .args(["rev-parse", "origin/main"])
        .current_dir(&repo.work)
        .output()
        .expect("read stale tracking ref");
    let local_head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(&repo.work)
        .output()
        .expect("read local head");
    assert_eq!(local_tracking.stdout, local_head.stdout);

    let output = repo.guard("pre-create");
    assert!(!output.status.success());
    assert!(stderr(&output).contains("not the exact live origin main"));
}

#[test]
fn local_or_remote_target_tag_blocks_pre_create() {
    let local = ReleaseRepo::new();
    run_git(&local.work, &["tag", "-a", "v0.3.1", "-m", "v0.3.1"]);
    let output = local.guard("pre-create");
    assert!(!output.status.success());
    assert!(stderr(&output).contains("already exists locally"));

    let remote = ReleaseRepo::new();
    run_git(&remote.work, &["tag", "-a", "v0.3.1", "-m", "v0.3.1"]);
    run_git(&remote.work, &["push", "origin", "refs/tags/v0.3.1"]);
    run_git(&remote.work, &["tag", "-d", "v0.3.1"]);
    let output = remote.guard("pre-create");
    assert!(!output.status.success());
    assert!(stderr(&output).contains("already exists on origin"));
}

#[test]
fn post_create_requires_exact_annotated_signed_tag() {
    let repo = ReleaseRepo::new();

    let missing = repo.guard("post-create");
    assert!(!missing.status.success());
    assert!(stderr(&missing).contains("missing or is not an annotated tag"));

    run_git(&repo.work, &["tag", "-a", "v0.3.1", "-m", "v0.3.1"]);
    let unsigned = repo.guard("post-create");
    assert!(!unsigned.status.success());
    assert!(
        stderr(&unsigned).contains("CHANVOY_PGP_KEY_ID is required"),
        "unsigned tag must reach the pinned-signer gate: {}",
        stderr(&unsigned)
    );
}

#[test]
fn signed_tag_must_match_repository_fingerprint_contract() {
    let (homedir, fingerprint) = generate_signing_key();

    let matching = ReleaseRepo::new();
    matching.install_fingerprint_contract(&fingerprint);
    run(
        Command::new("git")
            .args(["tag", "-s", "-u", &fingerprint, "v0.3.1", "-m", "v0.3.1"])
            .current_dir(&matching.work)
            .env("GNUPGHOME", homedir.path()),
        "sign matching release tag",
    );
    let output = matching.guard_with_signer("post-create", &fingerprint, homedir.path());
    assert!(
        output.status.success(),
        "contracted signer must pass: {}",
        stderr(&output)
    );

    let alternate = ReleaseRepo::new();
    alternate.install_fingerprint_contract("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    run(
        Command::new("git")
            .args(["tag", "-s", "-u", &fingerprint, "v0.3.1", "-m", "v0.3.1"])
            .current_dir(&alternate.work)
            .env("GNUPGHOME", homedir.path()),
        "sign alternate release tag",
    );
    let output = alternate.guard_with_signer("post-create", &fingerprint, homedir.path());
    assert!(!output.status.success());
    assert!(stderr(&output).contains("does not match release contract"));
}

#[test]
fn signed_tag_rejects_any_invalid_whole_fingerprint_contract() {
    let (homedir, fingerprint) = generate_signing_key();
    let valid_minisign = "36a80acfa44f5cf9ac402d3ce8e51fcc083e5a1dca22180d6a0ea85b7e5340ad";
    let contracts = [
        format!("minisign {valid_minisign}\ngpg {fingerprint}\nunknown value\n"),
        format!("minisign {valid_minisign}\ngpg {fingerprint}\ngpg {fingerprint}\n"),
        format!("minisign TBD-PENDING\ngpg {fingerprint}\n"),
    ];

    for contract in contracts {
        let repo = ReleaseRepo::new();
        repo.install_raw_fingerprint_contract(&contract);
        run(
            Command::new("git")
                .args(["tag", "-s", "-u", &fingerprint, "v0.3.1", "-m", "v0.3.1"])
                .current_dir(&repo.work)
                .env("GNUPGHOME", homedir.path()),
            "sign release tag with invalid contract",
        );
        let output = repo.guard_with_signer("post-create", &fingerprint, homedir.path());
        assert!(!output.status.success());
        assert!(
            stderr(&output).contains("release fingerprint contract is invalid or incomplete"),
            "invalid whole contract must fail: {}",
            stderr(&output)
        );
    }
}

#[test]
fn make_targets_keep_create_and_push_separate() {
    let makefile = include_str!("../Makefile");
    let create_start = makefile
        .find("release-tag: ##")
        .expect("release-tag target exists");
    let push_start = makefile
        .find("release-tag-push: ##")
        .expect("release-tag-push target exists");
    let create_target = &makefile[create_start..push_start];
    let push_end = makefile[push_start..]
        .find("\nrelease-clean:")
        .map(|offset| push_start + offset)
        .expect("release-tag-push target is bounded");
    let push_target = &makefile[push_start..push_end];

    assert!(create_target.contains("git tag -s"));
    assert!(create_target.contains("post-create"));
    assert!(
        !create_target.contains("git push"),
        "release-tag must never push"
    );

    assert!(push_target.contains("pre-push"));
    assert!(
        push_target.contains("refs/tags/$(CHANVOY_RELEASE_TAG):refs/tags/$(CHANVOY_RELEASE_TAG)")
    );
    assert!(
        !push_target.contains("--force"),
        "release-tag-push must not rewrite a remote tag"
    );
    let preflight_start = makefile
        .find("release-preflight:")
        .expect("release-preflight target exists");
    let preflight_end = makefile[preflight_start..]
        .find("\nrelease-guard-tag-version:")
        .map(|offset| preflight_start + offset)
        .expect("release-preflight target is bounded");
    let preflight = &makefile[preflight_start..preflight_end];
    assert!(!preflight.contains("gh release view"));
    assert!(preflight.contains("scripts/release-guard-github-release.sh"));
}

#[test]
fn github_release_absence_requires_an_authoritative_404() {
    let root = tempfile::tempdir().expect("temporary fake gh");
    let fake_gh = root.path().join("gh");
    std::fs::write(
        &fake_gh,
        r#"#!/usr/bin/env bash
endpoint="${*: -1}"
if [[ "$endpoint" == "repos/lanytehq/chanvoy" ]]; then
  case "${FAKE_GH_STATE:-}" in
  hidden)
    printf 'HTTP/2.0 404 Not Found\n' >&2
    exit 1
    ;;
  forbidden)
    printf 'HTTP/2.0 403 Forbidden\n' >&2
    exit 1
    ;;
  malformed)
    printf 'provider unavailable\n' >&2
    exit 1
    ;;
  *)
    printf 'HTTP/2.0 200 OK\n'
    exit 0
    ;;
  esac
fi
case "${FAKE_GH_STATE:-}" in
absent)
  printf 'HTTP/2.0 404 Not Found\n'
  exit 1
  ;;
present)
  printf 'HTTP/2.0 200 OK\n'
  exit 0
  ;;
forbidden)
  printf 'HTTP/2.0 404 Not Found\n' >&2
  exit 1
  ;;
hidden)
  printf 'HTTP/2.0 404 Not Found\n' >&2
  exit 1
  ;;
malformed)
  printf 'provider unavailable\n' >&2
  exit 1
  ;;
esac
"#,
    )
    .expect("write fake gh");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&fake_gh)
            .expect("fake gh metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_gh, permissions).expect("make fake gh executable");
    }

    let run_guard = |state: &str| {
        let path = format!(
            "{}:{}",
            root.path().display(),
            std::env::var("PATH").expect("test PATH")
        );
        Command::new("bash")
            .arg(github_guard_script())
            .arg("v0.3.1")
            .env("PATH", path)
            .env("FAKE_GH_STATE", state)
            .output()
            .expect("execute GitHub release guard")
    };

    let absent = run_guard("absent");
    assert!(
        absent.status.success(),
        "authoritative 404 must pass: {}",
        stderr(&absent)
    );

    for state in ["present", "forbidden", "hidden", "malformed"] {
        let output = run_guard(state);
        assert!(
            !output.status.success(),
            "{state} must fail closed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            stderr(&output)
        );
    }
}
