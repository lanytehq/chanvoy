#!/usr/bin/env python3
"""Synthetic subprocess oracles; never builds or runs application fixtures."""

import fnmatch
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock
from types import SimpleNamespace


REPO = Path(__file__).resolve().parent.parent
DRIVER = REPO / "scripts/qualify-production-binary.py"
HEAD = "a" * 40
TREE = "b" * 40
RUST = "rustc 1.89.0 (29483883e 2025-08-04)"
native_spec = importlib.util.spec_from_file_location("native_synthetic", REPO / "scripts/production-build-policy.test.py")
native = importlib.util.module_from_spec(native_spec)
native_spec.loader.exec_module(native)

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
        directory = directory.resolve()
        self.root = directory / "source"
        self.out = directory / "evidence"
        self.root.mkdir()
        self.tools = directory / "tools"
        self.tools.mkdir()
        self.state_path = directory / "control.json"
        target, pin_platform = TARGETS[platform]
        self.state = dict(root=str(self.root), target=target, pin_platform=pin_platform,
                          platform=platform, head=HEAD, tree=TREE, dirty=False,
                          cases=case_lists(), rust=RUST, pin_commit=HEAD, evidence=str(self.out))
        self.state.update(changes)
        self.state_path.write_text(json.dumps(self.state))
        (self.root / "Cargo.toml").write_text('[package]\nname="chanvoy"\nversion="0.3.2"\n')
        (self.root / "Cargo.lock").write_text("version = 4\n" + native.native_lock())
        (self.root / "scripts").mkdir()
        for name in native.policy.SOURCE_FILES:
            (self.root / "scripts" / name).write_bytes((REPO / "scripts" / name).read_bytes())
        (self.root / "VERSION").write_text("0.3.2\n")
        self.cargo_home = directory / "owned-cargo-home"
        for key, location in (("cargo_config", self.root / ".cargo"),
                              ("cargo_parent_config", directory / ".cargo"), ("cargo_home_config", self.cargo_home)):
            if changes.get(key):
                location.mkdir(parents=True, exist_ok=True)
                (location / changes[key]).write_text('[build]\nrustc="unsupported-owned-selector"\n')
        self.cli = self.root / "target/release/chanvoy"
        self.package_id = "path+file:///synthetic#chanvoy@0.3.2"
        prefix = "import json,pathlib,sys,time\nstate=json.loads(pathlib.Path(%r).read_text())\n" % str(self.state_path)
        executable(self.cli, prefix + """
pin={'commit':state['pin_commit'],'dirty':False,'platform':state['pin_platform'],
     'rustc':state['rust'],'version':'0.3.2'}
print(json.dumps(dict(pin,cli=pin,daemon=None,generation_scored=False)))
if state.get('human_stderr'):print('synthetic pin warning',file=sys.stderr)
if state.get('malformed_stdout')=='build-pin':print('invalid machine output')
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
        for name in ("readelf", "otool"):
            executable(self.tools / name, prefix + """
if state.get('fail_native'):raise SystemExit(7)
if '--version' in sys.argv:print('synthetic native metadata tool version 1')
elif '-L' in sys.argv:
 print(sys.argv[-1]+':\\n\\t/usr/lib/libSystem.B.dylib (compatibility version 1.0.0, current version 1.0.0)')
elif '-d' in sys.argv:print('Dynamic section: (NEEDED) Shared library: [libc.so.6]')
elif '-l' in sys.argv:print('Program Headers: [Requesting program interpreter: /lib64/synthetic-loader]')
else:raise SystemExit(8)
""")
        cargo_body = prefix + """
import os
root=pathlib.Path(state['root']); target=root/'target'; package='path+file:///synthetic#chanvoy@0.3.2'
with (root.parent/'qualifier-cargo-controls.jsonl').open('a') as controls:
 controls.write(json.dumps(dict(operation=sys.argv[1],control={k:v for k,v in os.environ.items() if k.startswith(('AWS_LC_SYS_','HOST_AWS_LC_SYS_','TARGET_AWS_LC_SYS_'))}))+'\\n')
def event(name,kind,path,test):
 return dict(reason='compiler-artifact',package_id=package,target=dict(name=name,kind=[kind]),
             profile=dict(test=test,opt_level='0',debug_assertions=True),
             executable=str(path),features=[])
if sys.argv[1]=='metadata':
 print(json.dumps(dict(packages=[dict(name='chanvoy',id=package,manifest_path=str(root/'Cargo.toml')), state['aws_package']],
                       target_directory=str(target))))
 if state.get('human_stderr'):print('warning: synthetic metadata diagnostic',file=sys.stderr)
 if state.get('malformed_stdout')=='metadata':print('invalid machine output')
elif sys.argv[1]=='test':
 if state.get('human_stderr'):print('   Compiling synthetic fixture; warning: owned diagnostic',file=sys.stderr)
 if state.get('malformed_stdout')=='fixture-compile':print('invalid machine output')
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
 if state.get('human_stderr'):print('    Finished synthetic test profile',file=sys.stderr)
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
 if state.get('tamper_native')==suite:
  with (pathlib.Path(state['evidence'])/'normal-build-inputs.json').open('a') as f:f.write('changed')
 if state.get('fail_suite')==suite:
  if state.get('restore_failure'):
   cli=pathlib.Path(state['root'])/'target/debug/chanvoy';cli.unlink();cli.mkdir()
  raise SystemExit(7)
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

        metadata = {"packages": [{"name": "chanvoy", "id": self.package_id,
                                   "manifest_path": str(self.root / "Cargo.toml")}]}
        complete = [json.loads(x) for x in self.normal.read_text().splitlines()]
        native_out = native.add_native(complete, metadata, self.root)
        native_out.mkdir(parents=True)
        (native_out.parent / "output").write_text(native.native_output(native_out))
        (native_out / native.policy.ARCHIVE).write_bytes(b"!<arch>\nowned synthetic source archive\n")
        self.state["aws_package"] = native.native_package()
        self.state_path.write_text(json.dumps(self.state))
        mode = "candidate" if platform == "linux-x86_64" else "shipping"
        expected = {"commit": HEAD, "tree": TREE, "lock_sha256": native.inputs.digest(self.root / "Cargo.lock"),
                    "platform": platform, "target": target, "mode": mode,
                    "tag": "v0.3.2" if mode == "shipping" else None,
                    "tag_object": "d" * 40 if mode == "shipping" else None,
                    "workflow": {"ref": os.environ.get("GITHUB_WORKFLOW_REF"), "sha": os.environ.get("GITHUB_WORKFLOW_SHA"),
                                 "event_sha": os.environ.get("GITHUB_SHA"), "run_id": os.environ.get("GITHUB_RUN_ID"),
                                 "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT")}}
        self.producer = native.seal_synthetic(self.out, self.root, complete, metadata, self.normal_bytes, expected)
        (self.out / "normal-Cargo.lock").write_bytes((self.root / "Cargo.lock").read_bytes())
        self.normal.write_bytes((self.out / "normal-build.jsonl").read_bytes())

    def owned_environment(self):
        # The provider/roots/tools belong to this harness. Never borrow the
        # launcher's Make/native/compiler selections or ambient credentials.
        # Intentional negative controls are added below without normalization.
        env = {"PATH": str(self.tools) + os.pathsep + os.environ["PATH"],
               "CARGO_HOME": str(self.cargo_home)}
        for name in ("GITHUB_WORKFLOW_REF", "GITHUB_WORKFLOW_SHA", "GITHUB_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"):
            if name in os.environ:
                env[name] = os.environ[name]
        return env

    def run(self, **overrides):
        env = self.owned_environment()
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

    def test_owned_provider_environment_works_under_ambient_make_and_native_context(self):
        with tempfile.TemporaryDirectory(prefix="cv-owned-launcher-") as directory:
            fixture = Synthetic(Path(directory))
            ambient = {"MAKEFLAGS": "--jobs=2 --no-print-directory", "CC": "owned-unsupported-caller-compiler",
                       "CMAKE_TOOLCHAIN_FILE": "owned-unsupported-toolchain", "AWS_LC_SYS_USE_SYSTEM": "1",
                       "CARGO_TARGET_DIR": "owned-unsupported-caller-output"}
            with mock.patch.dict(os.environ, ambient):
                before = dict(os.environ)
                result, receipt = fixture.run()
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(receipt["status"], "pass")
                # Boolean assertion cannot disclose the ambient environment.
                self.assertTrue(dict(os.environ) == before, "synthetic run changed the parent environment")

    def test_explicit_empty_makeflags_and_native_negative_are_not_removed(self):
        for override in ({"MAKEFLAGS": ""}, {"AWS_LC_SYS_USE_SYSTEM": "0"}):
            with self.subTest(selector=next(iter(override))), tempfile.TemporaryDirectory(prefix="cv-owned-negative-") as directory:
                fixture = Synthetic(Path(directory))
                result, receipt = fixture.run(env=override)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(receipt["status"], "failed")
                self.assertIn(next(iter(override)), receipt["failure"])
                self.assertEqual(receipt["commands"], [])
                self.assertEqual(receipt["suites"], [])

    def test_fixture_control_follows_verified_policy_and_metadata_has_none(self):
        with tempfile.TemporaryDirectory(prefix="cv-fixture-control-") as directory:
            fixture = Synthetic(Path(directory))
            result, receipt = fixture.run()
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            controls = [json.loads(x) for x in (fixture.root.parent / "qualifier-cargo-controls.jsonl").read_text().splitlines()]
            self.assertEqual(controls, [{"operation": "metadata", "control": {}},
                                       {"operation": "test", "control": {"AWS_LC_SYS_USE_SYSTEM": "0"}}])
            self.assertEqual(receipt["status"], "pass")
        with tempfile.TemporaryDirectory(prefix="cv-fixture-control-refusal-") as directory:
            fixture = Synthetic(Path(directory))
            (fixture.out / "native-build-policy.json").unlink()
            result, receipt = fixture.run()
            self.assertNotEqual(result.returncode, 0)
            controls = [json.loads(x) for x in (fixture.root.parent / "qualifier-cargo-controls.jsonl").read_text().splitlines()]
            self.assertEqual(controls, [{"operation": "metadata", "control": {}}])
            self.assertEqual(receipt["suites"], [])

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

    def test_native_capture_is_required_and_its_receipt_cannot_change(self):
        receipt = self.check("owned command failed: native-tool-version", fail_native=True)
        self.assertEqual(receipt["suites"], [])
        self.check("normal build input receipt changed", tamper_native="startup_diagnostics")

    def test_first_failure_is_retained_and_stops_later_suites(self):
        receipt = self.check("owned command failed", fail_suite="startup_diagnostics")
        self.assertEqual([x["name"] for x in receipt["suites"]], ["startup_diagnostics"])
        self.assertEqual(receipt["suites"][0]["status"], "incomplete")
        self.assertEqual(receipt["restored_fixture_cli_sha256"], receipt["original_fixture_cli_sha256"])

    def test_restoration_failure_keeps_the_first_command_failure(self):
        receipt = self.check("owned command failed: startup_diagnostics",
                             fail_suite="startup_diagnostics", restore_failure=True)
        self.assertEqual([x["name"] for x in receipt["suites"]], ["startup_diagnostics"])
        self.assertEqual(receipt["suites"][0]["status"], "incomplete")
        self.assertTrue(any("regular file" in x for x in receipt["secondary_failures"]))

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

    def test_substituted_normal_payload_after_legacy_hash_recompute_refuses_before_fixtures(self):
        with tempfile.TemporaryDirectory(prefix="cv-producer-payload-") as directory:
            fixture = Synthetic(Path(directory))
            fixture.cli.write_bytes(fixture.normal_bytes + b"\n# owned substituted payload\n")
            replacement = hashlib.sha256(fixture.cli.read_bytes()).hexdigest()
            result, receipt = fixture.run(hash=replacement)
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("producer normal executable association", receipt["failure"])
            self.assertEqual(receipt["suites"], [])
            self.assertNotIn("fixture-compile", [x["stage"] for x in receipt["commands"]])

    def test_correct_normal_payload_hash_with_wrong_producer_size_refuses(self):
        with tempfile.TemporaryDirectory(prefix="cv-producer-size-") as directory:
            fixture = Synthetic(Path(directory))
            fixture.producer["normal_executable"]["bytes"] += 1
            native.json_file(fixture.out / "native-build-policy.json", fixture.producer)
            result, receipt = fixture.run()
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("producer normal executable association", receipt["failure"])
            self.assertEqual(receipt["suites"], [])

    def test_missing_wrong_size_mode_and_policy_fields_refuse_before_fixtures(self):
        for field, wrong in (("normal_executable", None), ("normal_executable", {"sha256": "1" * 64, "bytes": 1}),
                             ("mode", "local"), ("target", "wrong"), ("source_files_sha256", {})):
            with self.subTest(field=field), tempfile.TemporaryDirectory(prefix="cv-producer-association-") as directory:
                fixture = Synthetic(Path(directory))
                fixture.producer[field] = wrong
                native.json_file(fixture.out / "native-build-policy.json", fixture.producer)
                result, receipt = fixture.run()
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(receipt["suites"], [])
                self.assertNotIn("fixture-compile", [x["stage"] for x in receipt["commands"]])

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

    def test_json_stdout_and_human_stderr_are_separate_retained_evidence(self):
        with tempfile.TemporaryDirectory(prefix="cv-qualify-streams-") as directory:
            fixture = Synthetic(Path(directory), human_stderr=True)
            result, receipt = fixture.run()
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(receipt["status"], "pass")
            for stage in ("metadata", "build-pin", "fixture-compile"):
                record = next(x for x in receipt["commands"] if x["stage"] == stage)
                stdout = (fixture.out / record["stdout"]).read_text()
                stderr = (fixture.out / record["stderr"]).read_text()
                self.assertIn("synthetic", stderr)
                for line in stdout.splitlines():
                    self.assertIsInstance(json.loads(line), dict)
            self.assertIn("Compiling", (fixture.out / "fixture-compile.stderr.log").read_text())
            self.assertIn("Finished", (fixture.out / "fixture-compile.stderr.log").read_text())

    def test_malformed_machine_stdout_is_not_filtered_into_success(self):
        for stage in ("metadata", "build-pin", "fixture-compile"):
            with self.subTest(stage=stage):
                cause = "Expecting value" if stage == "fixture-compile" else "Extra data"
                receipt = self.check(cause, human_stderr=True, malformed_stdout=stage)
                self.assertEqual(receipt["suites"], [])

    def test_compiler_selectors_and_config_wrapper_equivalents_refuse(self):
        for name in ("RUSTC", "CARGO_BUILD_RUSTC", "CARGO_BUILD_RUSTC_WRAPPER",
                     "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER"):
            with self.subTest(variable=name), tempfile.TemporaryDirectory(prefix="cv-qualify-selector-") as directory:
                fixture = Synthetic(Path(directory))
                result, receipt = fixture.run(env={name: "unsupported-owned-selector"})
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(receipt["failure"], "unsupported compiler override: " + name)
                self.assertEqual(receipt["commands"], [])
                self.assertEqual(receipt["suites"], [])

    def test_target_host_unstable_and_empty_selectors_refuse_before_admission(self):
        names = ("CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS",
                 "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER",
                 "CARGO_TARGET_AARCH64_APPLE_DARWIN_RUNNER",
                 "CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTDOCFLAGS",
                 "CARGO_TARGET_APPLIES_TO_HOST", "CARGO_HOST_RUSTFLAGS", "CARGO_UNSTABLE_BUILD_STD")
        for name in (*names, "RUSTC", "CARGO_PROFILE_RELEASE_OPT_LEVEL"):
            for value in ("", "unsupported-owned-selector"):
                with self.subTest(variable=name, value=value), tempfile.TemporaryDirectory(prefix="cv-target-selector-") as directory:
                    fixture = Synthetic(Path(directory))
                    result, receipt = fixture.run(env={name: value})
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(receipt["status"], "failed")
                    self.assertIn("unsupported", receipt["failure"])
                    self.assertEqual(receipt["commands"], [])
                    self.assertEqual(receipt["suites"], [])
        with tempfile.TemporaryDirectory(prefix="cv-output-placement-") as directory:
            fixture = Synthetic(Path(directory))
            result, receipt = fixture.run(env={"CARGO_TARGET_DIR": str(fixture.root / "target"),
                                              "CARGO_TARGET_TMPDIR": str(Path(directory) / "owned-output")})
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(receipt["failure"], "unsupported Cargo selector: CARGO_TARGET_DIR")
            self.assertEqual(receipt["commands"], [])

    def test_unapproved_cargo_config_cannot_select_a_hidden_compiler(self):
        for location in ("cargo_config", "cargo_parent_config", "cargo_home_config"):
            for filename in ("config", "config.toml"):
                with self.subTest(location=location, filename=filename):
                    receipt = self.check("unsupported Cargo configuration", **{location: filename})
                    self.assertEqual(receipt["commands"], [])
                    self.assertEqual(receipt["suites"], [])
        with tempfile.TemporaryDirectory(prefix="cv-qualify-config-link-") as directory:
            fixture = Synthetic(Path(directory))
            config_dir = fixture.root / ".cargo"
            config_dir.mkdir()
            (config_dir / "config.toml").symlink_to(config_dir / "owned-missing-config")
            result, receipt = fixture.run()
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(receipt["failure"], "unsupported Cargo configuration")
            self.assertEqual(receipt["commands"], [])

    def test_unreaped_denied_signal_and_reap_error_remain_bounded_unknown(self):
        for failure_mode in ("never-reap", "denied-signal", "reap-error"):
            with self.subTest(mode=failure_mode), tempfile.TemporaryDirectory(prefix="cv-qualify-reap-") as directory:
                fixture = Synthetic(Path(directory))
                spec = importlib.util.spec_from_file_location("qualification_driver", DRIVER)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                clock = [100.0]
                waits, signals, children = [], [], []
                outer = self

                class UncollectableChild:
                    pid = 424242  # Simulated only; killpg is replaced below.

                    def __init__(self, argv, **kwargs):
                        children.append(argv)
                        outer.assertTrue(kwargs["start_new_session"])
                        outer.assertNotEqual(kwargs["stderr"], subprocess.STDOUT)

                    def wait(self, *, timeout):
                        outer.assertGreater(timeout, 0)
                        waits.append((clock[0], timeout))
                        if failure_mode == "reap-error" and len(waits) > 1:
                            raise OSError("owned synthetic reap error")
                        clock[0] += timeout
                        raise subprocess.TimeoutExpired("owned synthetic child", timeout)

                def owned_signal(pid, sig):
                    outer.assertEqual(pid, UncollectableChild.pid)
                    before = json.loads((fixture.out / "qualification.json").read_text())
                    outer.assertEqual(before["status"], "failed")
                    outer.assertIn("process horizon", before["failure"])
                    outer.assertTrue(before["commands"][0]["process_horizon_expired"])
                    outer.assertFalse(before["commands"][0]["owned_cleanup_confirmed"])
                    outer.assertIsNone(before["commands"][0]["exit"])
                    signals.append(sig)
                    clock[0] += 1  # Signals consume the SAME absolute budget.
                    if failure_mode == "denied-signal":
                        raise PermissionError("owned synthetic signal denial")

                args = SimpleNamespace(root=fixture.root, output=fixture.out, operation_seconds=20,
                                       mode="candidate", platform="linux-x86_64", expected_commit=HEAD,
                                       tag=None, tag_object=None)
                with mock.patch.dict(module.os.environ, fixture.owned_environment(), clear=True), \
                        mock.patch.object(module.time, "monotonic", side_effect=lambda: clock[0]), \
                        mock.patch.object(module.subprocess, "Popen", UncollectableChild), \
                        mock.patch.object(module.os, "killpg", side_effect=owned_signal):
                    driver = module.Driver(args)
                    with self.assertRaisesRegex(module.QualificationError, "process horizon"):
                        driver.qualify()
                    driver.fail("owned synthetic restoration failure")
                receipt = json.loads((fixture.out / "qualification.json").read_text())
                record = receipt["commands"][0]
                self.assertEqual(len(children), 1)
                self.assertEqual(len(receipt["commands"]), 1)
                self.assertEqual(receipt["suites"], [])
                self.assertFalse((fixture.out / "payload").exists())
                self.assertEqual(receipt["failure"], "owned command exceeded its process horizon")
                self.assertEqual(receipt["secondary_failures"], ["owned synthetic restoration failure"])
                self.assertIsNone(record["exit"])
                self.assertFalse(record["child_collected"])
                self.assertFalse(record["owned_cleanup_confirmed"])
                self.assertLessEqual(clock[0], driver.deadline)
                self.assertEqual(record["cleanup_deadline"], 120)
                for began, timeout in waits[1:]:
                    self.assertLessEqual(began + timeout, record["cleanup_deadline"])
                if failure_mode == "never-reap":
                    self.assertEqual(signals, [signal.SIGTERM, signal.SIGKILL])
                    self.assertEqual([x[1] for x in waits], [10, 5, 3])
                    self.assertTrue(all(x["reap_horizon_expired"] for x in record["teardown"]))
                    self.assertTrue(record["cleanup_horizon_expired"])
                elif failure_mode == "denied-signal":
                    self.assertEqual(signals, [signal.SIGTERM])
                    self.assertEqual(record["teardown"][0]["signal_error_class"], "PermissionError")
                else:
                    self.assertEqual(signals, [signal.SIGTERM])
                    self.assertEqual(record["teardown"][0]["reap_error_class"], "OSError")

    def test_cleanup_reserve_prevents_late_command_launch(self):
        with tempfile.TemporaryDirectory(prefix="cv-qualify-reserve-") as directory:
            fixture = Synthetic(Path(directory))
            spec = importlib.util.spec_from_file_location("qualification_driver", DRIVER)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            args = SimpleNamespace(root=fixture.root, output=fixture.out, operation_seconds=10,
                                   mode="candidate", platform="linux-x86_64", expected_commit=HEAD,
                                   tag=None, tag_object=None)
            with mock.patch.object(module.time, "monotonic", return_value=100), \
                    mock.patch.object(module.subprocess, "Popen") as launch:
                driver = module.Driver(args)
                with self.assertRaisesRegex(module.QualificationError, "operation horizon"):
                    driver.command("forbidden-late-child", ["owned-synthetic-command"], 1)
                launch.assert_not_called()
            self.assertEqual(driver.receipt["commands"], [])
            self.assertEqual(driver.receipt["status"], "failed")

    def test_workflow_admission_and_failed_evidence_are_distinct(self):
        workflow = (REPO / ".github/workflows/release.yml").read_text()
        candidate = (REPO / ".github/workflows/check.yml").read_text()
        self.assertIn("github.event.pull_request.head.sha || github.sha", candidate)
        self.assertNotIn("pull_request_target", candidate + workflow)
        self.assertIn("needs: [validate, build, sbom]", workflow)
        qualifier = workflow.index("Qualify actual tag-run payload")
        upload = workflow.index("Upload exact binary artifact")
        self.assertLess(qualifier, upload)
        block = workflow[upload:workflow.index("\n  sbom_tool_route:", upload)]
        self.assertIn("if: success()", block)
        self.assertNotIn("always()", block)
        self.assertIn("Preserve shipping qualification and first-failure evidence\n        if: always()", workflow)
        self.assertNotIn("continue-on-error", candidate + workflow)
        for name in ("qualification-shipping-linux-x86_64-1", "qualification-candidate-linux-x86_64-1"):
            self.assertFalse(fnmatch.fnmatchcase(name, "binary-*"))
            self.assertFalse(fnmatch.fnmatchcase(name, "release-packages-*"))


if __name__ == "__main__":
    unittest.main()
