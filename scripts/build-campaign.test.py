#!/usr/bin/env python3
"""Storage admission, process ownership and real build-identity regressions."""

import importlib.util
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parent.parent
DRIVER = ROOT / "scripts/build-campaign.py"
spec = importlib.util.spec_from_file_location("campaign", DRIVER)
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)


def wait_for(path, process):
    deadline = time.monotonic() + 10
    while not path.exists() and time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(f"child exited before readiness: {process.returncode}")
        time.sleep(.01)
    if not path.exists():
        raise AssertionError("child did not reach readiness")


class CampaignTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="chanvoy-build-test-")
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name).resolve()
        self.source = self.base / "source"
        self.source.mkdir()
        self.cache = self.base / "cache"
        self.log = self.base / "cargo.jsonl"
        self.env = {k: v for k, v in os.environ.items()
                    if k != campaign.FD_ENV and k not in campaign.SELECTORS
                    and not k.startswith("CARGO_PROFILE_") and k != "CARGO_INCREMENTAL"}
        self.env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                        TEST_CARGO_LOG=str(self.log))
        self.git("init", "-q")
        (self.source / ".gitignore").write_text("target/\n")
        self.git("add", ".")
        self.commit()
        self.tools = self.base / "tools"
        self.tools.mkdir()
        cargo = self.tools / "cargo"
        cargo.write_text('''#!/usr/bin/env python3
import json, os, pathlib, sys
target = os.environ.get('CARGO_TARGET_DIR', str(pathlib.Path.cwd() / 'target'))
if 'metadata' in sys.argv:
    print(json.dumps({'target_directory': os.environ.get('TEST_METADATA_TARGET', target)}))
else:
    with open(os.environ['TEST_CARGO_LOG'], 'a') as out:
        out.write(json.dumps({'argv':sys.argv[1:], 'target':target,
                             'debug':os.environ.get('CARGO_PROFILE_DEV_DEBUG'),
                             'source':os.environ.get('CHANVOY_BUILD_SOURCE_ID')})+'\\n')
    if os.environ.get('TEST_GROW'):
        pathlib.Path(target, 'growth').write_bytes(b'x' * 32768)
    sys.exit(int(os.environ.get('TEST_EXIT', '0')))
''')
        cargo.chmod(0o755)
        self.env["PATH"] = str(self.tools) + os.pathsep + self.env["PATH"]

    def git(self, *args):
        return subprocess.check_output(["git", "-c", "user.name=Fixture", "-c",
                                        "user.email=fixture@example.invalid", *args],
                                       cwd=self.source, env=self.env, text=True).strip()

    def commit(self):
        self.git("commit", "-qm", "Fixture state")

    def invocation(self, command, *options):
        return [sys.executable, "-B", str(DRIVER), "run", "--cache-root", str(self.cache),
                "--min-free-gib", "0", *options, "--", *command]

    def step(self, *args):
        return [sys.executable, "-B", str(DRIVER), "step", "--", "cargo", *args]

    def run_command(self, command, *options, env=None):
        return subprocess.run(self.invocation(command, *options), cwd=self.source,
                              env=env or self.env, text=True, capture_output=True, timeout=60)

    def rows(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def test_sources_share_target_and_controls_are_separate(self):
        for lane in ("dev", "dev", "controls"):
            result = self.run_command(self.step("build"), "--lane", lane)
            self.assertEqual(result.returncode, 0, result.stderr)
            if lane == "dev":
                second = self.base / "second"
                if not second.exists():
                    self.git("worktree", "add", "-q", "--detach", str(second))
                    self.source = second
        rows = self.rows()
        self.assertEqual(rows[0]["target"], rows[1]["target"])
        self.assertNotEqual(rows[1]["target"], rows[2]["target"])
        self.assertNotEqual(rows[0]["source"], rows[1]["source"])
        self.assertTrue(all(row["debug"] == "0" for row in rows))
        self.assertFalse((self.source / "target").exists())

    def test_direct_step_and_invalid_descriptor_refuse(self):
        for extra in ({}, {campaign.FD_ENV: "999999"}):
            result = subprocess.run(self.step("build"), cwd=self.source,
                                    env={**self.env, **extra}, capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
        self.assertFalse(self.log.exists())

    def test_budget_and_free_space_refuse_before_cargo(self):
        for options in (("--budget-gib", "0"), ("--min-free-gib", "9999999")):
            result = self.run_command(self.step("build"), *options)
            self.assertEqual(result.returncode, 2, result.stderr)
        self.assertFalse(self.log.exists())

    def test_selectors_and_profile_conflicts_refuse(self):
        for key, value in (("CARGO_TARGET_DIR", "/somewhere"),
                           ("CARGO_PROFILE_DEV_DEBUG", "2"),
                           ("CARGO_BUILD_BUILD_DIR", "/somewhere")):
            result = self.run_command(self.step("build"), env={**self.env, key: value})
            self.assertEqual(result.returncode, 2, result.stderr)
        result = self.run_command(self.step("build", "--target-dir", "elsewhere"))
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertFalse(self.log.exists())

    def test_metadata_disagreement_refuses_before_compilation(self):
        result = self.run_command(self.step("build"),
                                  env={**self.env, "TEST_METADATA_TARGET": str(self.base / "wrong")})
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("different target directory", result.stderr)
        self.assertFalse(self.log.exists())

    def test_growth_refuses_next_step_and_preserves_failure(self):
        for code in (0, 7):
            if self.cache.exists():
                shutil.rmtree(self.cache)
            result = self.run_command(self.step("build"), "--budget-gib", str(16384 / campaign.GIB),
                                      env={**self.env, "TEST_GROW": "1", "TEST_EXIT": str(code)})
            self.assertEqual(result.returncode, code or 2, result.stderr)
            self.assertIn("cache budget reached", result.stderr)
        result = self.run_command(self.step("build"), "--budget-gib", str(16384 / campaign.GIB))
        self.assertEqual(result.returncode, 2)
        self.assertEqual(len(self.rows()), 2)

    def test_nonzero_command_is_preserved_and_next_owner_can_start(self):
        result = self.run_command(self.step("test"), env={**self.env, "TEST_EXIT": "19"})
        self.assertEqual(result.returncode, 19, result.stderr)
        self.assertEqual(self.run_command(self.step("check")).returncode, 0)

    def test_direct_cargo_campaign_routes_through_a_managed_step(self):
        result = self.run_command(["cargo", "build"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.rows()[0]["target"], str(self.cache / "dev"))

    def held_owner(self, ignore_term=False):
        ready = self.base / "ready"
        release = self.base / "release"
        code = ("import pathlib,time,signal; "
                + ("signal.signal(signal.SIGTERM,signal.SIG_IGN); " if ignore_term else "")
                + f"pathlib.Path({str(ready)!r}).touch(); "
                + f"\nwhile not pathlib.Path({str(release)!r}).exists(): time.sleep(.01)\n")
        process = subprocess.Popen(self.invocation([sys.executable, "-c", code]),
                                   cwd=self.source, env=self.env, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL)
        self.addCleanup(lambda: process.poll() is None and process.kill())
        wait_for(ready, process)
        return process, release

    def test_competing_owner_is_refused_including_other_lane(self):
        process, release = self.held_owner()
        try:
            result = self.run_command(self.step("build"), "--lane", "controls")
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn("another campaign", result.stderr)
        finally:
            release.touch()
            process.wait(timeout=10)
        self.assertEqual(self.run_command(self.step("build")).returncode, 0)

    def test_termination_keeps_lock_until_child_exits(self):
        process, release = self.held_owner(ignore_term=True)
        try:
            process.send_signal(signal.SIGTERM)
            result = self.run_command(self.step("build"))
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIsNone(process.poll())
        finally:
            release.touch()
            process.wait(timeout=10)
        self.assertEqual(process.returncode, 128 + signal.SIGTERM)
        self.assertEqual(self.run_command(self.step("build")).returncode, 0)

    def test_killed_supervisor_leaves_child_holding_the_lease(self):
        process, release = self.held_owner()
        try:
            process.kill()
            process.wait(timeout=10)
            result = self.run_command(self.step("build"))
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn("another campaign", result.stderr)
        finally:
            release.touch()
        deadline = time.monotonic() + 10
        with (self.cache / "campaign.lock").open("rb") as lock:
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        self.fail("child did not release its lease after completing")
                    time.sleep(.01)
        self.assertEqual(self.run_command(self.step("build")).returncode, 0)

    def test_symlink_lane_refuses_without_writing_destination(self):
        self.cache.mkdir()
        destination = self.base / "destination"
        destination.mkdir()
        (self.cache / "dev").symlink_to(destination, target_is_directory=True)
        result = self.run_command(self.step("build"))
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(list(destination.iterdir()), [])

    def test_make_public_targets_keep_one_campaign_and_scoped_environment(self):
        shutil.copy(ROOT / "Makefile", self.source / "Makefile")
        (self.source / "scripts").mkdir()
        shutil.copy(DRIVER, self.source / "scripts/build-campaign.py")
        (self.source / "Cargo.toml").write_text('rust-version = "1.89.0"\n')
        args = f"--cache-root {self.cache} --min-free-gib 0"
        result = subprocess.run(["make", "check", f"BUILD_CAMPAIGN_ARGS={args}"],
                                cwd=self.source, env=self.env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr.count('"build_campaign": "started"'), 1)
        self.assertEqual([row["argv"][0] for row in self.rows()], ["fmt", "clippy", "test"])
        # A nested public target reuses the descriptor without reacquiring it.
        nested = self.run_command(["make", "test"])
        self.assertEqual(nested.returncode, 0, nested.stderr)

    def test_release_guard_refuses_managed_or_redirected_install(self):
        result = self.run_command([sys.executable, str(DRIVER), "local-release"])
        self.assertEqual(result.returncode, 2, result.stderr)
        result = subprocess.run([sys.executable, str(DRIVER), "local-release"],
                                env={**self.env, "CARGO_TARGET_DIR": str(self.cache)},
                                cwd=self.source, capture_output=True, text=True)
        self.assertEqual(result.returncode, 2, result.stderr)

    def test_source_stamp_changes_on_same_branch_commit_and_dirty_transition(self):
        self.assertEqual(self.run_command(self.step("build")).returncode, 0)
        (self.source / "file").write_text("changed\n")
        self.assertEqual(self.run_command(self.step("build")).returncode, 0)
        self.git("add", ".")
        self.commit()
        self.assertEqual(self.run_command(self.step("build")).returncode, 0)
        self.assertEqual(len({row["source"] for row in self.rows()}), 3)

    def test_actual_build_script_identity_across_a_b_a_and_commit_move(self):
        # No dependencies and tiny outputs: exercise the production build.rs,
        # not a second full chanvoy build/test campaign.
        self.env["PATH"] = os.environ["PATH"]
        (self.source / "Cargo.toml").write_text('[workspace]\nmembers=["crates/probe"]\nresolver="2"\n')
        crate = self.source / "crates/probe"
        (crate / "src").mkdir(parents=True)
        (crate / "Cargo.toml").write_text('[package]\nname="identity-probe"\nversion="0.0.0"\nedition="2021"\n')
        shutil.copy(ROOT / "crates/chanvoy-core/build.rs", crate / "build.rs")
        (crate / "src/main.rs").write_text('fn main() { println!("{} {}", env!("FULMEN_HOST_COMMIT"), env!("FULMEN_HOST_DIRTY")); }\n')
        (self.source / "VERSION").write_text("0.0.0\n")
        # Generate and commit the lock so the first Cargo run stays clean.
        subprocess.run(["cargo", "generate-lockfile", "--offline"], cwd=self.source,
                       env=self.env, check=True, capture_output=True)
        self.git("add", ".")
        self.commit()
        a = self.source
        b = self.base / "other-worktree"
        self.git("worktree", "add", "-qb", "other", str(b))
        self.source = b
        (b / "VERSION").write_text("0.0.1\n")
        self.git("add", ".")
        self.commit()
        for root in (a, b, a):
            self.source = root
            expected = self.git("rev-parse", "HEAD") + " false"
            result = self.run_command(self.step("run", "--quiet", "--offline", "--locked"))
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), expected)
            self.assertFalse((root / "target").exists())
        self.git("commit", "--allow-empty", "-qm", "Advance same branch")
        result = self.run_command(self.step("run", "--quiet", "--offline", "--locked"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), self.git("rev-parse", "HEAD") + " false")


if __name__ == "__main__":
    unittest.main()
