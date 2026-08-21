//! Fingerprint contract: decernor 0.1.4 inserter + verifier.
//!
//! Tests stub `decernor` so CI does not need the binary. A live-path
//! test runs only when `DECERNOR` (or PATH) is 0.1.4+.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn insert_script() -> PathBuf {
    repo_root().join("scripts/insert-expected-fingerprints.sh")
}

fn verify_script() -> PathBuf {
    repo_root().join("scripts/verify-public-keys.sh")
}

fn fixture(name: &str) -> PathBuf {
    repo_root()
        .join("tests/fixtures/fingerprint-contract")
        .join(name)
}

const SAMPLE_GPG_PRIMARY: &str = r#"[
  {
    "schema_version": "v0",
    "kind": "gpg",
    "class": "public",
    "algorithm": "openpgp-fingerprint",
    "fingerprint": "5D8E7478C4EA08D97D39139CCEEA5771AED0966B",
    "fingerprint_scheme": "openpgp-fingerprint-v1",
    "key_id": "CEEA5771AED0966B",
    "key_role": "primary",
    "confidence": "high"
  }
]"#;

const SAMPLE_MINI: &str = r#"[
  {
    "schema_version": "v0",
    "kind": "minisign",
    "class": "public",
    "algorithm": "minisign-key-id",
    "fingerprint": "45E379F73D967D2C",
    "fingerprint_scheme": "minisign-key-id-v1",
    "key_id": "45E379F73D967D2C",
    "confidence": "high"
  },
  {
    "schema_version": "v0",
    "kind": "minisign",
    "class": "public",
    "algorithm": "sha256",
    "fingerprint": "91f40ebe76f5af9f554c8e32ff52a46937363cc8c303bf826fa30e52f037a340",
    "fingerprint_scheme": "minisign-public-blob-sha256-v1",
    "key_id": "45E379F73D967D2C",
    "confidence": "high"
  }
]"#;

const TBD_CONTRACT: &str = "\
# comment
minisign  TBD-MINISIGN-FINGERPRINT-PENDING-DISPATCH-PROVISIONING
gpg       TBD-GPG-FINGERPRINT-PENDING-DISPATCH-PROVISIONING
";

struct StubDecernor {
    _dir: TempDir,
    bin: PathBuf,
}

impl StubDecernor {
    fn new(version: &str, gpg_json: &str, mini_json: &str) -> Self {
        let dir = TempDir::new().expect("tempdir");
        let gpg_path = dir.path().join("gpg.json");
        let mini_path = dir.path().join("mini.json");
        fs::write(&gpg_path, gpg_json).unwrap();
        fs::write(&mini_path, mini_json).unwrap();
        let bin = dir.path().join("decernor");
        let script = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
if [ "${{1:-}}" = "version" ]; then
  echo "decernor {version}"
  exit 0
fi
if [ "${{1:-}}" != "fingerprint" ]; then
  echo "unexpected: $*" >&2
  exit 2
fi
kind=""
role=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --kind) kind="$2"; shift 2 ;;
    --gpg-role) role="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [ "$kind" = "gpg" ]; then
  if [ "$role" != "primary" ]; then
    echo "stub requires --gpg-role primary" >&2
    exit 2
  fi
  cat {gpg}
  exit 0
fi
if [ "$kind" = "minisign" ]; then
  cat {mini}
  exit 0
fi
echo "unhandled kind=$kind" >&2
exit 2
"#,
            version = version,
            gpg = shell_quote(gpg_path.to_str().unwrap()),
            mini = shell_quote(mini_path.to_str().unwrap()),
        );
        fs::write(&bin, script).unwrap();
        let mut perms = fs::metadata(&bin).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&bin, perms).unwrap();
        Self { _dir: dir, bin }
    }
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\"'\"'"))
}

fn seed_publics(dir: &Path) {
    fs::copy(fixture("chanvoy.pub"), dir.join("chanvoy.pub")).unwrap();
    fs::copy(fixture("chanvoy.gpg.asc"), dir.join("chanvoy.gpg.asc")).unwrap();
}

fn run_insert(decernor: &Path, minisign: &Path, gpg: &Path, output: &Path) -> std::process::Output {
    Command::new("bash")
        .arg(insert_script())
        .arg("--minisign")
        .arg(minisign)
        .arg("--gpg")
        .arg(gpg)
        .arg("--output")
        .arg(output)
        .env("DECERNOR", decernor)
        .output()
        .expect("insert")
}

fn run_verify(decernor: &Path, release_dir: &Path, expected: &Path) -> std::process::Output {
    Command::new("bash")
        .arg(verify_script())
        .arg(release_dir)
        .env("DECERNOR", decernor)
        .env("CHANVOY_EXPECTED_FINGERPRINTS", expected)
        .output()
        .expect("verify")
}

fn stderr_of(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn stdout_of(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn happy_path_writes_both_lines_and_verify_passes() {
    let stub = StubDecernor::new("0.1.4", SAMPLE_GPG_PRIMARY, SAMPLE_MINI);
    let dir = TempDir::new().unwrap();
    seed_publics(dir.path());
    let dest = dir.path().join("expected-fingerprints.txt");
    fs::write(&dest, TBD_CONTRACT).unwrap();

    let out = run_insert(
        &stub.bin,
        &dir.path().join("chanvoy.pub"),
        &dir.path().join("chanvoy.gpg.asc"),
        &dest,
    );
    assert!(out.status.success(), "insert failed: {}", stderr_of(&out));
    let body = fs::read_to_string(&dest).unwrap();
    assert!(
        body.contains("minisign  91f40ebe76f5af9f554c8e32ff52a46937363cc8c303bf826fa30e52f037a340")
    );
    assert!(body.contains("gpg       5D8E7478C4EA08D97D39139CCEEA5771AED0966B"));
    assert!(!body.contains("TBD-"));

    let v = run_verify(&stub.bin, dir.path(), &dest);
    assert!(v.status.success(), "verify failed: {}", stderr_of(&v));
    assert!(stdout_of(&v).contains("[ok]"));
}

#[test]
fn missing_file_leaves_dest_unchanged() {
    let stub = StubDecernor::new("0.1.4", SAMPLE_GPG_PRIMARY, SAMPLE_MINI);
    let dir = TempDir::new().unwrap();
    seed_publics(dir.path());
    let dest = dir.path().join("expected-fingerprints.txt");
    fs::write(&dest, TBD_CONTRACT).unwrap();
    let out = run_insert(
        &stub.bin,
        &dir.path().join("missing.pub"),
        &dir.path().join("chanvoy.gpg.asc"),
        &dest,
    );
    assert!(!out.status.success());
    assert_eq!(fs::read_to_string(&dest).unwrap(), TBD_CONTRACT);
}

#[test]
fn private_marker_refuses_and_leaves_dest() {
    let stub = StubDecernor::new("0.1.4", SAMPLE_GPG_PRIMARY, SAMPLE_MINI);
    let dir = TempDir::new().unwrap();
    seed_publics(dir.path());
    fs::write(
        dir.path().join("chanvoy.pub"),
        "untrusted comment: minisign secret key\nAAAA\n",
    )
    .unwrap();
    let dest = dir.path().join("expected-fingerprints.txt");
    fs::write(&dest, TBD_CONTRACT).unwrap();
    let out = run_insert(
        &stub.bin,
        &dir.path().join("chanvoy.pub"),
        &dir.path().join("chanvoy.gpg.asc"),
        &dest,
    );
    assert!(!out.status.success());
    assert!(stderr_of(&out).contains("private material"));
    assert_eq!(fs::read_to_string(&dest).unwrap(), TBD_CONTRACT);
}

#[test]
fn minisign_blob_mismatch_or_duplicate_refuses() {
    let mini = format!(
        "[{}, {}]",
        r#"{"schema_version":"v0","kind":"minisign","class":"public","algorithm":"sha256","fingerprint":"91f40ebe76f5af9f554c8e32ff52a46937363cc8c303bf826fa30e52f037a340","fingerprint_scheme":"minisign-public-blob-sha256-v1","confidence":"high"}"#,
        r#"{"schema_version":"v0","kind":"minisign","class":"public","algorithm":"sha256","fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","fingerprint_scheme":"minisign-public-blob-sha256-v1","confidence":"high"}"#
    );
    let stub = StubDecernor::new("0.1.4", SAMPLE_GPG_PRIMARY, &mini);
    let dir = TempDir::new().unwrap();
    seed_publics(dir.path());
    let dest = dir.path().join("expected-fingerprints.txt");
    fs::write(&dest, TBD_CONTRACT).unwrap();
    let out = run_insert(
        &stub.bin,
        &dir.path().join("chanvoy.pub"),
        &dir.path().join("chanvoy.gpg.asc"),
        &dest,
    );
    assert!(!out.status.success());
    assert!(stderr_of(&out).contains("exactly one minisign-public-blob-sha256-v1"));
    assert_eq!(fs::read_to_string(&dest).unwrap(), TBD_CONTRACT);
}

#[test]
fn gpg_not_unique_primary_refuses() {
    let gpg = r#"[
  {
    "schema_version": "v0",
    "kind": "gpg",
    "class": "public",
    "algorithm": "openpgp-fingerprint",
    "fingerprint": "5D8E7478C4EA08D97D39139CCEEA5771AED0966B",
    "fingerprint_scheme": "openpgp-fingerprint-v1",
    "key_role": "primary",
    "confidence": "high"
  },
  {
    "schema_version": "v0",
    "kind": "gpg",
    "class": "public",
    "algorithm": "openpgp-fingerprint",
    "fingerprint": "FE0BA9FD92A29EAF886DA4F9BE0CE6453D42D18F",
    "fingerprint_scheme": "openpgp-fingerprint-v1",
    "key_role": "primary",
    "confidence": "high"
  }
]"#;
    let stub = StubDecernor::new("0.1.4", gpg, SAMPLE_MINI);
    let dir = TempDir::new().unwrap();
    seed_publics(dir.path());
    let dest = dir.path().join("expected-fingerprints.txt");
    fs::write(&dest, TBD_CONTRACT).unwrap();
    let out = run_insert(
        &stub.bin,
        &dir.path().join("chanvoy.pub"),
        &dir.path().join("chanvoy.gpg.asc"),
        &dest,
    );
    assert!(!out.status.success());
    assert!(stderr_of(&out).contains("exactly one GPG primary"));
    assert_eq!(fs::read_to_string(&dest).unwrap(), TBD_CONTRACT);
}

#[test]
fn old_decernor_version_is_refused() {
    let stub = StubDecernor::new("0.1.3", SAMPLE_GPG_PRIMARY, SAMPLE_MINI);
    let dir = TempDir::new().unwrap();
    seed_publics(dir.path());
    let dest = dir.path().join("expected-fingerprints.txt");
    fs::write(&dest, TBD_CONTRACT).unwrap();
    let out = run_insert(
        &stub.bin,
        &dir.path().join("chanvoy.pub"),
        &dir.path().join("chanvoy.gpg.asc"),
        &dest,
    );
    assert!(!out.status.success());
    assert!(stderr_of(&out).contains("too old"));
    assert_eq!(fs::read_to_string(&dest).unwrap(), TBD_CONTRACT);
}

#[test]
fn verify_fails_closed_on_tbd_placeholders() {
    let stub = StubDecernor::new("0.1.4", SAMPLE_GPG_PRIMARY, SAMPLE_MINI);
    let dir = TempDir::new().unwrap();
    seed_publics(dir.path());
    let dest = dir.path().join("expected-fingerprints.txt");
    fs::write(&dest, TBD_CONTRACT).unwrap();
    let v = run_verify(&stub.bin, dir.path(), &dest);
    assert!(!v.status.success());
    assert!(stderr_of(&v).contains("TBD placeholder"));
}

#[test]
fn live_decernor_014_on_fixtures() {
    let override_bin = std::env::var("DECERNOR")
        .ok()
        .filter(|p| Path::new(p).is_file());
    let mut ver_cmd = Command::new(override_bin.as_deref().unwrap_or("decernor"));
    ver_cmd.arg("version");
    let ver = ver_cmd.output().ok();
    let Some(ver) = ver else {
        eprintln!("skip: no decernor binary");
        return;
    };
    if !ver.status.success() {
        eprintln!("skip: decernor version failed");
        return;
    }
    let text = String::from_utf8_lossy(&ver.stdout);
    if !text.contains("0.1.4") && !text.contains("0.1.5") && !text.contains("0.2.") {
        eprintln!("skip: decernor is not >= 0.1.4 ({text:?})");
        return;
    }
    let dir = TempDir::new().unwrap();
    seed_publics(dir.path());
    let dest = dir.path().join("expected-fingerprints.txt");
    fs::write(&dest, TBD_CONTRACT).unwrap();
    let mut insert = Command::new("bash");
    insert
        .arg(insert_script())
        .arg("--minisign")
        .arg(dir.path().join("chanvoy.pub"))
        .arg("--gpg")
        .arg(dir.path().join("chanvoy.gpg.asc"))
        .arg("--output")
        .arg(&dest);
    if let Some(b) = &override_bin {
        insert.env("DECERNOR", b);
    } else {
        insert.env_remove("DECERNOR");
    }
    let out = insert.output().unwrap();
    assert!(
        out.status.success(),
        "live insert failed: {}",
        stderr_of(&out)
    );
    let body = fs::read_to_string(&dest).unwrap();
    assert!(
        body.contains("minisign  91f40ebe76f5af9f554c8e32ff52a46937363cc8c303bf826fa30e52f037a340")
    );
    assert!(body.contains("gpg       5D8E7478C4EA08D97D39139CCEEA5771AED0966B"));
    let mut verify = Command::new("bash");
    verify
        .arg(verify_script())
        .arg(dir.path())
        .env("CHANVOY_EXPECTED_FINGERPRINTS", &dest);
    if let Some(b) = &override_bin {
        verify.env("DECERNOR", b);
    } else {
        verify.env_remove("DECERNOR");
    }
    let v = verify.output().unwrap();
    assert!(v.status.success(), "live verify failed: {}", stderr_of(&v));
}
