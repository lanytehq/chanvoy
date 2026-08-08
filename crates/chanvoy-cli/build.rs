//! Inject host binary identity for `chanvoy version` / `version --extended`.
//!
//! Compile-time env names follow the Fulmen host-identity convention so a
//! future shared resolver can read the same keys. Values are best-effort:
//! missing git or a non-git tree yields "unknown" / omitted dirty — never panic.

use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo"),
    );
    // Workspace root is two levels up from crates/chanvoy-cli.
    let workspace_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| manifest_dir.clone());

    let version = read_version(&workspace_root);
    // Machine identity uses the full object name (40-char hex when on a
    // normal commit). Short form is display-only for human version lines.
    let commit = git(&workspace_root, &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let commit_short = git(&workspace_root, &["rev-parse", "--short=7", "HEAD"])
        .unwrap_or_else(|| short_from_full(&commit));
    let dirty = git(&workspace_root, &["status", "--porcelain"])
        .map(|s| !s.trim().is_empty())
        .unwrap_or(false);
    let built = utc_now_rfc3339();
    let rustc = rustc_version().unwrap_or_else(|| "unknown".into());

    println!("cargo:rustc-env=FULMEN_HOST_VERSION={version}");
    println!("cargo:rustc-env=FULMEN_HOST_COMMIT={commit}");
    println!("cargo:rustc-env=FULMEN_HOST_COMMIT_SHORT={commit_short}");
    println!("cargo:rustc-env=FULMEN_HOST_BUILD_DATE={built}");
    println!("cargo:rustc-env=FULMEN_HOST_DIRTY={dirty}");
    println!("cargo:rustc-env=FULMEN_HOST_RUSTC={rustc}");

    println!(
        "cargo:rerun-if-changed={}",
        workspace_root.join("VERSION").display()
    );
    // Best-effort: rebuild when HEAD moves. Plain repos use `.git/HEAD`;
    // linked worktrees use a `gitdir:` pointer — watch the real HEAD there
    // or install can keep a stale Commit: pin from a prior tip.
    for head in git_head_paths(&workspace_root) {
        if head.exists() {
            println!("cargo:rerun-if-changed={}", head.display());
        }
    }
}

/// Paths to git HEAD files that should invalidate host-identity env.
fn git_head_paths(workspace_root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let git_meta = workspace_root.join(".git");
    if git_meta.is_dir() {
        out.push(git_meta.join("HEAD"));
        return out;
    }
    if git_meta.is_file() {
        // worktree: "gitdir: /path/to/.git/worktrees/<name>"
        if let Ok(contents) = std::fs::read_to_string(&git_meta) {
            for line in contents.lines() {
                if let Some(dir) = line.strip_prefix("gitdir:") {
                    let gitdir = PathBuf::from(dir.trim());
                    out.push(gitdir.join("HEAD"));
                    if let Some(parent) = gitdir.parent().and_then(|p| p.parent()) {
                        out.push(parent.join("HEAD"));
                    }
                }
            }
        }
    }
    out
}

fn read_version(workspace_root: &Path) -> String {
    let path = workspace_root.join("VERSION");
    std::fs::read_to_string(&path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
}

fn git(cwd: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8(output.stdout).ok()?;
    Some(s.trim().to_string())
}

/// Best-effort short form when only the full object name is known.
fn short_from_full(commit: &str) -> String {
    if commit == "unknown" || commit.len() < 7 {
        return commit.to_string();
    }
    commit.chars().take(7).collect()
}

fn rustc_version() -> Option<String> {
    let output = Command::new("rustc").arg("-V").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let s = String::from_utf8(output.stdout).ok()?;
    Some(s.trim().to_string())
}

fn utc_now_rfc3339() -> String {
    // Prefer shell date so build.rs stays free of chrono.
    if let Ok(output) = Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
    {
        if output.status.success() {
            if let Ok(s) = String::from_utf8(output.stdout) {
                let t = s.trim();
                if !t.is_empty() {
                    return t.to_string();
                }
            }
        }
    }
    "unknown".into()
}
