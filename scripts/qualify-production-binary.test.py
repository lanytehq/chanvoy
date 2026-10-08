#!/usr/bin/env python3
"""Synthetic subprocess oracles; never builds or runs application fixtures."""

import fnmatch
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from types import SimpleNamespace


REPO = Path(__file__).resolve().parent.parent
DRIVER = REPO / "scripts/qualify-production-binary.py"
HEAD = "a" * 40
TREE = "b" * 40
RUST = "rustc 1.89.0 (29483883e 2025-08-04)"
TARGETS = {
    "linux-x86_64": ("x86_64-unknown-linux-gnu", "linux/x86_64"),
    "linux-aarch64": ("aarch64-unknown-linux-gnu", "linux/aarch64"),
    "macos-aarch64": ("aarch64-apple-darwin", "macos/aarch64"),
}


def case_lists():
    # Independently take declared Rust test names, including non-ignored smoke
    # cases. No application test is executed by this orchestration harness.
    result = {}
    for suite in ("startup_diagnostics", "restart_harness", "per_043_wait_follow",
                  "wait_direct_message", "wait_inbox"):
        source = (REPO / "tests" / (suite + ".rs")).read_text()
        cases = []
        all_cases = []
        for attrs, name in re.findall(r"((?:#\[[^\n]*\]\s*)+)(?:async\s+)?fn\s+(\w+)\s*\(", source):
            if "#[tokio::test" in attrs or "#[test]" in attrs:
                all_cases.append(name)
                if suite == "startup_diagnostics" or "#[ignore" in attrs:
                    cases.append(name)
        result[suite] = {"selected": sorted(cases), "all": sorted(all_cases)}
    return result


def executable(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/usr/bin/env python3\n" + textwrap.dedent(body))
    path.chmod(0o755)


class Synthetic:
    def __init__(self, directory, platform="linux-x86_64", **changes):
        self.root = directory / "source"
        self.out = directory / "evidence"
        self.root.mkdir()
        self.tools = directory / "tools"
        self.tools.mkdir()
        self.state_path = directory / "control.json"
        target, pin_platform = TARGETS[platform]
        self.state = dict(root=str(self.root), target=target, pin_platform=pin_platform,
                          platform=platform, head=HEAD, tree=TREE, dirty=False,
                          cases=case_lists(), rust=RUST, pin_commit=HEAD)
        self.state.update(changes)
        self.state_path.write_text(json.dumps(self.state))
        (self.root / "Cargo.toml").write_text('[package]\nname="chanvoy"\nversion="0.3.2"\n')
        (self.root / "Cargo.lock").write_text("synthetic locked packages\n")
        (self.root / "VERSION").write_text("0.3.2\n")
        self.cli = self.root / "target/release/chanvoy"
        self.package_id = "path+file:///synthetic#chanvoy@0.3.2"
        prefix = "import json,pathlib,sys,time\nstate=json.loads(pathlib.Path(%r).read_text())\n" % str(self.state_path)
        executable(self.cli, prefix + """
pin={'commit':state['pin_commit'],'dirty':False,'platform':state['pin_platform'],
     'rustc':state['rust'],'version':'0.3.2'}
print(json.dumps(dict(pin,cli=pin,daemon=None,generation_scored=False)))
""")
        self.normal_bytes = self.cli.read_bytes()
        self.hash = hashlib.sha256(self.normal_bytes).hexdigest()
        executable(self.tools / "git", prefix + """
args=sys.argv[1:]
if args==['rev-parse','HEAD']:print(state['head'])
elif args==['rev-parse','HEAD^{tree}']:print(state['tree'])
elif args==['status','--porcelain=v1']:
 if state.get('dirty'):print(' M source.rs')
elif args==['cat-file','commit','HEAD']:print('tree '+state['tree']+'\\nparent '+'c'*40+'\\n\\nSynthetic source')
else:raise SystemExit(8)
""")
        executable(self.tools / "rustc", prefix + """
print(state['rust']+'\\nhost: '+state['target']+'\\nrelease: 1.89.0')
""")
        cargo_body = prefix + """
root=pathlib.Path(state['root']); target=root/'target'; package='path+file:///synthetic#chanvoy@0.3.2'
def event(name,kind,path,test):
 return dict(reason='compiler-artifact',package_id=package,target=dict(name=name,kind=[kind]),
             profile=dict(test=test,opt_level='0',debug_assertions=True),
             executable=str(path),features=[])
if sys.argv[1]=='metadata':
 print(json.dumps(dict(packages=[dict(name='chanvoy',id=package,manifest_path=str(root/'Cargo.toml'))],
                       target_directory=str(target))))
elif sys.argv[1]=='test':
 time.sleep(state.get('compile_sleep',0))
 cli=target/'debug/chanvoy';cli.parent.mkdir(parents=True,exist_ok=True)
 cli.write_text('original synthetic debug CLI');cli.chmod(0o755)
 print(json.dumps(event('chanvoy','bin',cli,False)))
 for index,arg in enumerate(sys.argv):
  if arg!='--test':continue
  suite=sys.argv[index+1];exe=target/'debug/deps'/suite;exe.parent.mkdir(parents=True,exist_ok=True)
  body="import json,pathlib,sys\\nstate=json.loads(pathlib.Path("+repr(str(CONTROL)) +").read_text())\\nsuite="+repr(suite)+"\\n"
  body+=FIXTURE
  exe.write_text('#!/usr/bin/env python3\\n'+body);exe.chmod(0o755)
  print(json.dumps(event(suite,'test',exe,True)))
 print(json.dumps(dict(reason='build-finished',success=True)))
else:raise SystemExit(9)
"""
        fixture = """
if '--list' in sys.argv:
 names=state['cases'][suite]['all']
 if state.get('missing_list')==suite:names=names[1:]
 for name in names:print(name+': test')
else:
 if state.get('zero_case')==suite:print('test result: ok. 0 passed; 0 failed; 0 ignored;')
 else:
  for name in state['cases'][suite]['selected']:print('test '+name+' ... ok')
  print('test result: ok. '+str(len(state['cases'][suite]['selected']))+' passed; 0 failed; 0 ignored;')
 if state.get('tamper_cli')==suite:
  with (pathlib.Path(state['root'])/'target/debug/chanvoy').open('a') as f:f.write('changed')
 if state.get('dirty_after')==suite:
  state['dirty']=True;pathlib.Path(CONTROL).write_text(json.dumps(state))
 if state.get('fail_suite')==suite:raise SystemExit(7)
"""
        cargo_body = cargo_body.replace("FIXTURE", repr(textwrap.dedent(fixture).replace("CONTROL", repr(str(self.state_path)))))
        cargo_body = cargo_body.replace("CONTROL", repr(str(self.state_path)))
        executable(self.tools / "cargo", cargo_body)
        self.normal = directory / "normal-build.jsonl"
        normal_event = dict(reason="compiler-artifact", package_id=self.package_id,
                            target=dict(name="chanvoy", kind=["bin"]),
                            profile=dict(test=False, opt_level="3", debug_assertions=False),
                            executable=str(self.cli), features=[])
        if changes.get("debug_profile"):
            normal_event["profile"]["opt_level"] = "0"
        if changes.get("test_feature"):
            normal_event["features"] = ["test-seam"]
        events = [normal_event]
        if changes.get("mixed_test"):
            events.append(dict(reason="compiler-artifact", profile=dict(test=True),
                               target=dict(kind=["test"])))
        events.append(dict(reason="build-finished", success=True))
        self.normal.write_text("".join(json.dumps(x) + "\n" for x in events))
        if changes.get("symlink_payload"):
            destination = self.cli.with_suffix(".owned-original")
            self.cli.rename(destination)
            self.cli.symlink_to(destination)
        if changes.get("missing_completion"):
            self.normal.write_text(json.dumps(normal_event) + "\n")
        if changes.get("duplicate_compiler"):
            self.normal.write_text("".join(json.dumps(x) + "\n" for x in [normal_event, *events]))

    def run(self, **overrides):
        env = os.environ.copy()
        for name in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER"):
            env.pop(name, None)
        env["PATH"] = str(self.tools) + os.pathsep + env["PATH"]
        env.update(overrides.get("env", {}))
        mode = overrides.get("mode", "candidate" if self.state["platform"] == "linux-x86_64" else "shipping")
        args = [sys.executable, str(DRIVER), "--root", str(self.root), "--binary", str(self.cli),
                "--normal-build", str(self.normal), "--expected-commit", HEAD,
                "--expected-sha256", overrides.get("hash", self.hash),
                "--platform", self.state["platform"], "--mode", mode, "--output", str(self.out)]
        if mode == "shipping":
            args.extend(["--tag", "v0.3.2", "--tag-object", "d" * 40])
        if "operation_seconds" in overrides:
            args.extend(["--operation-seconds", str(overrides["operation_seconds"])])
        result = subprocess.run(args, env=env, capture_output=True, text=True, timeout=20)
        receipt = json.loads((self.out / "qualification.json").read_text())
        return result, receipt


class QualificationTests(unittest.TestCase):
    def check(self, failure=None, platform="linux-x86_64", **changes):
        with tempfile.TemporaryDirectory(prefix="cv-qualify-synthetic-") as directory:
            fixture = Synthetic(Path(directory), platform, **changes)
            result, receipt = fixture.run()
            if failure is None:
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(receipt["status"], "pass")
                self.assertEqual(fixture.cli.read_bytes(), fixture.normal_bytes)
                self.assertEqual(receipt["fixture_cli_sha256"], fixture.hash)
                self.assertEqual(receipt["restored_fixture_cli_sha256"], receipt["original_fixture_cli_sha256"])
                self.assertEqual((fixture.root / "target/debug/chanvoy").read_text(), "original synthetic debug CLI")
                self.assertTrue(all(x["status"] == "pass" for x in receipt["suites"]))
            else:
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(receipt["status"], "failed")
                self.assertIn(failure, receipt["failure"])
            return receipt

    def test_three_platforms_execute_declared_normal_bytes(self):
        for platform in TARGETS:
            with self.subTest(platform=platform):
                self.check(platform=platform)

    def test_stale_checkout_and_dirty_source_refuse(self):
        self.check("declared commit", head="e" * 40)
        self.check("dirty", dirty=True)

    def test_stale_pin_and_wrong_native_target_refuse(self):
        self.check("build pin", pin_commit="e" * 40)
        self.check("native target", target="wrong-target")

    def test_debug_test_feature_and_mixed_normal_capture_refuse(self):
        self.check("optimized release", debug_profile=True)
        self.check("production features", test_feature=True)
        self.check("fixture or test artifact", mixed_test=True)

    def test_compiled_name_list_cannot_silently_lose_a_case(self):
        self.check("exact case list", missing_list="startup_diagnostics")

    def test_symlink_and_incomplete_or_ambiguous_build_capture_refuse(self):
        self.check("regular file", symlink_payload=True)
        self.check("Cargo completion", missing_completion=True)
        self.check("duplicate compiled executable", duplicate_compiler=True)

    def test_successful_zero_case_run_is_not_evidence(self):
        self.check("zero-case", zero_case="startup_diagnostics")

    def test_payload_mutation_and_source_mutation_are_refused(self):
        self.check("payload changed", tamper_cli="startup_diagnostics")
        self.check("dirty", dirty_after="startup_diagnostics")

    def test_first_failure_is_retained_and_stops_later_suites(self):
        receipt = self.check("owned command failed", fail_suite="startup_diagnostics")
        self.assertEqual([x["name"] for x in receipt["suites"]], ["startup_diagnostics"])
        self.assertEqual(receipt["suites"][0]["status"], "incomplete")
        self.assertEqual(receipt["restored_fixture_cli_sha256"], receipt["original_fixture_cli_sha256"])

    def test_wrong_hash_and_compiler_override_are_refused(self):
        with tempfile.TemporaryDirectory(prefix="cv-qualify-negative-") as directory:
            fixture = Synthetic(Path(directory))
            result, receipt = fixture.run(hash="0" * 64)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("hash mismatch", receipt["failure"])
        with tempfile.TemporaryDirectory(prefix="cv-qualify-negative-") as directory:
            fixture = Synthetic(Path(directory))
            result, receipt = fixture.run(env={"RUSTFLAGS": "--cfg test"})
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("compiler override", receipt["failure"])

    def test_process_horizon_is_failed_not_a_suite_assertion(self):
        with tempfile.TemporaryDirectory(prefix="cv-qualify-horizon-") as directory:
            fixture = Synthetic(Path(directory))
            spec = importlib.util.spec_from_file_location("qualification_driver", DRIVER)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            args = SimpleNamespace(root=fixture.root, output=fixture.out, operation_seconds=20,
                                   mode="candidate", platform="linux-x86_64", expected_commit=HEAD,
                                   tag=None, tag_object=None)
            driver = module.Driver(args)
            with self.assertRaisesRegex(module.QualificationError, "process horizon"):
                driver.command("owned-blocked-command", [sys.executable, "-c", "import time; time.sleep(5)"], 1)
            receipt = json.loads((fixture.out / "qualification.json").read_text())
            self.assertNotEqual(receipt["status"], "pass")
            self.assertTrue(receipt["commands"][0]["process_horizon_expired"])
            self.assertFalse(receipt["commands"][0]["owned_cleanup_confirmed"])
            self.assertEqual(receipt["suites"], [])

    def test_workflow_admission_and_failed_evidence_are_distinct(self):
        workflow = (REPO / ".github/workflows/release.yml").read_text()
        candidate = (REPO / ".github/workflows/check.yml").read_text()
        self.assertIn("github.event.pull_request.head.sha || github.sha", candidate)
        self.assertNotIn("pull_request_target", candidate + workflow)
        self.assertIn("needs: [validate, build, sbom]", workflow)
        qualifier = workflow.index("Qualify actual tag-run payload")
        upload = workflow.index("Upload exact binary artifact")
        self.assertLess(qualifier, upload)
        block = workflow[upload:workflow.index("\n  sbom:", upload)]
        self.assertIn("if: success()", block)
        self.assertNotIn("always()", block)
        self.assertIn("Preserve shipping qualification and first-failure evidence\n        if: always()", workflow)
        self.assertNotIn("continue-on-error", candidate + workflow)
        for name in ("qualification-shipping-linux-x86_64-1", "qualification-candidate-linux-x86_64-1"):
            self.assertFalse(fnmatch.fnmatchcase(name, "binary-*"))
            self.assertFalse(fnmatch.fnmatchcase(name, "release-packages-*"))


if __name__ == "__main__":
    unittest.main()
