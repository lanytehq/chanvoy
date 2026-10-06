//! Canonical version guard; complete tag/signing controls run in the shell corpus.
use std::path::PathBuf;
use std::process::{Command, Output};
fn github_guard_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/release-guard-github-release.sh")
}
fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
#[test]
fn version_guard_rejects_wrong_cut_and_conflicting_alias() {
    let root = tempfile::tempdir().unwrap();
    let scripts = root.path().join("scripts");
    std::fs::create_dir(&scripts).unwrap();
    std::fs::write(root.path().join("VERSION"), "1.2.3\n").unwrap();
    let script = scripts.join("release-guard-tag-version.sh");
    std::fs::copy(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/release-guard-tag-version.sh"),
        &script,
    )
    .unwrap();
    let guard = |tag: &str, alias: Option<&str>| {
        let mut cmd = Command::new("bash");
        cmd.arg(&script)
            .env("CHANVOY_RELEASE_TAG", tag)
            .env_remove("RELEASE_TAG")
            .env_remove("CHANVOY_REQUIRE_TAG");
        if let Some(alias) = alias {
            cmd.env("RELEASE_TAG", alias);
        }
        cmd.output().unwrap()
    };
    assert!(guard("v1.2.3", None).status.success());
    for tag in ["v9.9.9", "v01.2.3", "1.2.3", "v1.2.3-rc1"] {
        assert!(
            !guard(tag, None).status.success(),
            "unexpected accepted cut {tag}"
        );
    }
    assert!(!guard("v1.2.3", Some("v9.9.9")).status.success());
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
