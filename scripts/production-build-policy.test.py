#!/usr/bin/env python3
"""Owned synthetic native producers and portable evidence; no application builds."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.dont_write_bytecode = True
import production_build_policy as policy
import production_build_inputs as inputs
from bounded_evidence import EvidenceError

REPO = Path(__file__).resolve().parent.parent
AWS_ID = "registry+https://github.com/rust-lang/crates.io-index#aws-lc-sys@0.45.0"
AWS_CHECKSUM = "7" * 64


def json_file(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def native_output(out, builder="CC"):
    directory = out if builder == "CC" else out / "build/artifacts"
    return ("cargo:warning=Environment Variable found 'AWS_LC_SYS_USE_SYSTEM': '0'\n"
            'cargo:warning=Building with: ' + builder + '\n'
            'cargo:warning=Symbol Prefix: Some("aws_lc_0_45_0")\n'
            'cargo:libdir=' + str(directory) + '\ncargo:link_kind=static\n'
            'cargo:libcrypto=aws_lc_0_45_0_crypto\ncargo:libcrypto_path=' + str(directory / policy.ARCHIVE) + '\n'
            'cargo:system_libs=\ncargo:rustc-link-lib=' + policy.LIBRARY + '\n'
            'cargo:rustc-link-search=native=' + str(directory) + '\n')


def native_package():
    return {"name": "aws-lc-sys", "version": "0.45.0", "source": policy.REGISTRY, "id": AWS_ID,
            "manifest_path": "/unavailable-registry/aws-lc-sys/Cargo.toml", "license": "ISC AND MIT",
            "targets": [{"kind": ["lib"]}, {"kind": ["custom-build"]}]}


def native_lock():
    return ('\n[[package]]\nname = "aws-lc-sys"\nversion = "0.45.0"\nsource = "' + policy.REGISTRY +
            '"\nchecksum = "' + AWS_CHECKSUM + '"\n')


def add_native(events, metadata, source_root, builder="CC"):
    """Positive fixture source layout is deliberately explicit, not discovered."""
    out = Path(source_root) / "target/release/build/aws-lc-sys-owned/out"
    directory = out if builder == "CC" else out / "build/artifacts"
    metadata["target_directory"] = str(Path(source_root) / "target")
    metadata["packages"].append(native_package())
    events.extend([
        {"reason": "compiler-artifact", "package_id": AWS_ID, "target": {"name": "build-script-main", "kind": ["custom-build"]},
         "profile": {"test": False}, "features": ["prebuilt-nasm"], "fresh": False},
        {"reason": "compiler-artifact", "package_id": AWS_ID, "target": {"name": "aws_lc_sys", "kind": ["lib"]},
         "profile": {"test": False}, "features": ["prebuilt-nasm"], "fresh": False},
        {"reason": "build-script-executed", "package_id": AWS_ID, "out_dir": str(out),
         "linked_libs": [policy.LIBRARY], "linked_paths": ["native=" + str(directory)], "cfgs": []},
    ])
    return out


def seal_synthetic(folder, source_root, events, metadata, payload, expected, builder="CC"):
    """Mint only harness-owned observations; original paths need not exist."""
    folder.mkdir(parents=True, exist_ok=True)
    script_events = [(n, e) for n, e in enumerate(events)
                     if e.get("reason") == "build-script-executed" and e.get("package_id") == AWS_ID]
    rows = []
    for ordinal, event in script_events:
        out = Path(event["out_dir"])
        directory = out if builder == "CC" else out / "build/artifacts"
        row = {"event_ordinal": ordinal, "package_id": AWS_ID,
               "script_compiler_artifact_ordinals": [n for n, e in enumerate(events)
                    if e.get("reason") == "compiler-artifact" and e.get("package_id") == AWS_ID
                    and e.get("target", {}).get("kind") == ["custom-build"]], "out_dir": str(out),
               "out_dir_relative": str(out.relative_to(Path(source_root) / "target")), "builder": builder,
               "linked_libs": [policy.LIBRARY], "linked_paths": ["native=" + str(directory)],
               "output_source": str(out.parent / "output"), "archive_source": str(directory / policy.ARCHIVE),
               "archive_name": policy.ARCHIVE}
        directory = folder / ("native-snapshots/event-%02d" % ordinal)
        directory.mkdir(parents=True)
        for kind, name, data in (("output", "output.log", native_output(out, builder).encode()),
                                 ("archive", policy.ARCHIVE, b"!<arch>\nowned synthetic source archive\n")):
            path = directory / name
            path.write_bytes(data)
            row[kind] = {"path": str(path.relative_to(folder)), "sha256": policy.sha(data), "bytes": len(data)}
        rows.append(row)
    normal = b"".join((json.dumps(e) + "\n").encode() for e in events)
    (folder / "normal-build.jsonl").write_bytes(normal)
    (folder / "normal-build.stderr.log").write_bytes(b"")
    receipt = {"schema": "native-build-policy-v1", "status": "pass", "policy": "bundled-source-v1",
               "control": dict(policy.CONTROL), "fresh_target_at_entry": True, "cargo_configuration": "absent",
               **{k: copy.deepcopy(expected.get(k)) for k in policy.BINDINGS},
               "source_files_sha256": inputs.policy_source_hashes(REPO),
               "normal_build_messages_sha256": policy.sha(normal), "normal_build_stderr_sha256": policy.sha(b""),
               "source_root": str(source_root), "target_directory": metadata["target_directory"],
               "package_id": next(p["id"] for p in metadata["packages"] if p["name"] == "chanvoy"),
               "normal_executable": {"sha256": policy.sha(payload), "bytes": len(payload)},
               "native_package": {**{k: v for k, v in native_package().items() if k in ("name", "version", "source")},
                                  "checksum": AWS_CHECKSUM, "package_id": AWS_ID},
               "native_events": rows, "native_snapshot_manifest": "native-snapshots.json"}
    manifest = {"schema": "native-snapshots-v1", "association":
                {k: receipt[k] for k in (*policy.BINDINGS, "source_files_sha256", "normal_build_messages_sha256")}, "events": rows}
    json_file(folder / "native-snapshots.json", manifest)
    receipt["native_snapshot_manifest_sha256"] = inputs.digest(folder / "native-snapshots.json")
    json_file(folder / "native-build-policy.json", receipt)
    return receipt


class NativeFixture:
    def __init__(self, directory, builder="CC"):
        directory = directory.resolve()
        self.directory = directory
        self.root = directory / "owned-source"
        self.root.mkdir()
        self.out = directory / "portable-evidence"
        self.out.mkdir()
        self.payload = directory / "owned-payload"
        self.payload.write_bytes(b"owned normal payload")
        self.lock = "version = 4\n" + native_lock()
        self.expected = {"commit": "a" * 40, "tree": "b" * 40, "lock_sha256": policy.sha(self.lock.encode()),
                         "platform": "linux-x86_64", "target": policy.PLATFORMS["linux-x86_64"][0],
                         "mode": "shipping", "tag": "v1.2.3", "tag_object": "c" * 40,
                         "workflow": {"ref": "owned/tag-workflow", "sha": "a" * 40, "event_sha": "a" * 40,
                                      "run_id": "1", "run_attempt": "1"}}
        self.metadata = {"packages": [{"id": "root", "name": "chanvoy", "manifest_path": str(self.root / "Cargo.toml")}]}
        self.events = [{"reason": "compiler-artifact", "package_id": "root", "target": {"name": "chanvoy", "kind": ["bin"]},
                        "profile": {"test": False, "opt_level": "3", "debug_assertions": False}, "features": [],
                        "executable": str(self.root / "target/release/chanvoy")}]
        add_native(self.events, self.metadata, self.root, builder)
        self.events.append({"reason": "build-finished", "success": True})
        (self.out / "normal-Cargo.lock").write_text(self.lock)
        self.receipt = seal_synthetic(self.out, self.root, self.events, self.metadata, self.payload.read_bytes(), self.expected, builder)

    def verify(self):
        return inputs.verify_native_policy(self.out, self.expected, REPO, self.events, self.metadata, self.payload)

    def rewrite(self, receipt=None, manifest=None):
        if manifest is not None:
            json_file(self.out / "native-snapshots.json", manifest)
            self.receipt["native_snapshot_manifest_sha256"] = inputs.digest(self.out / "native-snapshots.json")
        json_file(self.out / "native-build-policy.json", receipt or self.receipt)


class PolicyTests(unittest.TestCase):
    def test_all_caller_native_controls_and_values_are_refused_without_echo(self):
        names = ("AWS_LC_SYS_USE_SYSTEM", "AWS_LC_SYS_USE_SYSTEM_x86_64_unknown_linux_gnu",
                 "HOST_AWS_LC_SYS_USE_SYSTEM", "TARGET_AWS_LC_SYS_USE_SYSTEM", "AWS_LC_SYS_NO_PREFIX",
                 "AWS_LC_SYS_STATIC", "AWS_LC_SYS_WHOLE_ARCHIVE", "AWS_LC_SYS_FUTURE_CONTROL",
                 "OPENSSL_DIR", "X86_64_UNKNOWN_LINUX_GNU_OPENSSL_LIB_DIR", "OPENSSL_INCLUDE_DIR_aarch64_apple_darwin")
        for name in names:
            for value in ("", "0", "1", "unrecognized-owned-selector"):
                with self.subTest(name=name, value=value):
                    environment = {name: value}
                    with self.assertRaises(EvidenceError) as error:
                        policy.child_environment(environment, policy.PLATFORMS["linux-x86_64"][0])
                    self.assertEqual(environment, {name: value})
                    self.assertNotIn("unrecognized-owned-selector", str(error.exception))

    def test_owned_child_control_is_exact_and_parent_is_unchanged(self):
        parent = {"PATH": "owned-path"}
        child = policy.child_environment(parent, policy.PLATFORMS["linux-x86_64"][0])
        self.assertEqual(child, {"PATH": "owned-path", "AWS_LC_SYS_USE_SYSTEM": "0"})
        self.assertEqual(parent, {"PATH": "owned-path"})

    def test_existing_compiler_native_target_and_empty_directory_guards(self):
        for name in ("CARGO_TARGET_DIR", "RUSTFLAGS", "CARGO_BUILD_RUSTC_WRAPPER", "CARGO_PROFILE_RELEASE_OPT_LEVEL",
                     "CARGO_HOST_RUSTFLAGS", "CARGO_UNSTABLE_BUILD_STD", "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER",
                     "CC", "HOST_AR", "CFLAGS_x86_64_unknown_linux_gnu", "CMAKE_TOOLCHAIN_FILE", "MAKEFLAGS"):
            with self.subTest(name=name), self.assertRaises(EvidenceError):
                policy.admit_selectors({name: ""}, policy.PLATFORMS["linux-x86_64"][0])

    def test_cc_and_cmake_portable_snapshots_do_not_read_original_runner(self):
        for builder in ("CC", "CMake"):
            with self.subTest(builder=builder), tempfile.TemporaryDirectory() as temporary:
                fixture = NativeFixture(Path(temporary), builder)
                # Source runner and caches never exist; only copies/payload are read.
                fixture.root.rmdir()
                fixture.verify()

    def test_every_invocation_and_exact_builder_path_are_required(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = NativeFixture(Path(temporary))
            texts = {3: native_output(Path(fixture.events[3]["out_dir"]))}
            policy.backend_records(fixture.events, fixture.metadata, fixture.lock, str(fixture.root), texts)
            for wrong in ("/owned/system", str(fixture.root / "target/release/other/out"),
                          str(fixture.root / "target/release/build/aws-lc-sys-owned/out/build/artifacts")):
                events = copy.deepcopy(fixture.events)
                events[3]["linked_paths"] = ["native=" + wrong]
                with self.assertRaises(EvidenceError):
                    policy.backend_records(events, fixture.metadata, fixture.lock, str(fixture.root), texts)
            events = copy.deepcopy(fixture.events)
            events.append(copy.deepcopy(events[3]))
            with self.assertRaises(EvidenceError):
                policy.backend_records(events, fixture.metadata, fixture.lock, str(fixture.root), texts)

    def test_consistent_but_wrong_cc_and_cmake_source_directories_refuse(self):
        for builder in ("CC", "CMake"):
            with self.subTest(builder=builder), tempfile.TemporaryDirectory() as temporary:
                fixture = NativeFixture(Path(temporary), builder)
                out = Path(fixture.events[3]["out_dir"])
                expected = out if builder == "CC" else out / "build/artifacts"
                wrong = fixture.root / "target/release/other-owned-archive"
                events = copy.deepcopy(fixture.events)
                events[3]["linked_paths"] = ["native=" + str(wrong)]
                text = native_output(out, builder).replace(str(expected), str(wrong))
                with self.assertRaisesRegex(EvidenceError, "source metadata"):
                    policy.backend_records(events, fixture.metadata, fixture.lock, str(fixture.root), {3: text})

    def test_capture_detects_changed_source_and_exclusive_snapshot_creation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            fixture = NativeFixture(root)
            owned = root / "changing-owned-source"
            owned.write_bytes(b"original owned evidence")
            count = 0
            def change():
                nonlocal count
                count += 1
                if count == 2:owned.write_bytes(b"changed owned evidence of another size")
            with self.assertRaisesRegex(EvidenceError, "changed"):
                inputs.bounded_file(owned, root, 1024, change, destination=root / "exclusive-copy")
            with self.assertRaises(FileExistsError):
                inputs.bounded_file(fixture.payload, root, 1024, destination=root / "exclusive-copy")

    def test_output_executable_and_total_caps_are_deciding(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = NativeFixture(Path(temporary))
            for limit in (policy.OUTPUT_LIMIT, policy.EXECUTABLE_LIMIT):
                sparse = fixture.directory / ("owned-sparse-%s" % limit)
                with sparse.open("wb") as output:output.truncate(limit + 1)
                with self.assertRaisesRegex(EvidenceError, "byte bound"):
                    inputs.bounded_file(sparse, fixture.directory, limit)
            receipt = copy.deepcopy(fixture.receipt)
            manifest = json.loads((fixture.out / "native-snapshots.json").read_text())
            rows, snapshots = [], {}
            for ordinal in range(5):
                row = copy.deepcopy(manifest["events"][0]);row["event_ordinal"] = ordinal
                for kind in ("output", "archive"):
                    row[kind]["path"] = "native-snapshots/event-%02d/" % ordinal + ("output.log" if kind == "output" else policy.ARCHIVE)
                    if kind == "archive":row[kind]["bytes"] = policy.ARCHIVE_LIMIT
                    snapshots[row[kind]["path"]] = {"observation": {k: row[kind][k] for k in ("sha256", "bytes")},
                                                   "text": native_output(Path(row["out_dir"])) if kind == "output" else None}
                rows.append(row)
            manifest["events"] = rows;receipt["native_events"] = rows
            with self.assertRaisesRegex(EvidenceError, "over-limit native snapshot inventory"):
                policy.admit_receipt(receipt, manifest, fixture.expected, inputs.policy_source_hashes(REPO),
                    fixture.events, fixture.metadata, fixture.lock, receipt["normal_build_messages_sha256"],
                    receipt["normal_build_stderr_sha256"], receipt["native_snapshot_manifest_sha256"], snapshots,
                    receipt["normal_executable"])

    def test_dynamic_whole_archive_unprefixed_unknown_and_missing_exports_refuse(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = NativeFixture(Path(temporary))
            out = Path(fixture.events[3]["out_dir"])
            normal = native_output(out)
            for text in (normal.replace(policy.LIBRARY, "dylib=crypto"), normal.replace(policy.LIBRARY, "static:+whole-archive=aws_lc_0_45_0_crypto"),
                         normal.replace(policy.PREFIX, "crypto"), normal.replace("Building with: CC", "Building with: System"),
                         normal.replace("libcrypto_path=", "unknown_export="), normal.replace("'0'", "'false'"),
                         normal.replace("USE_SYSTEM': '0'", "USE_SYSTEM_x86_64_unknown_linux_gnu': ''")):
                with self.subTest(text=text), self.assertRaises(EvidenceError):
                    policy.backend_records(fixture.events, fixture.metadata, fixture.lock, str(fixture.root), {3: text})

    def test_stale_build_script_cannot_claim_fresh_invocation(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = NativeFixture(Path(temporary))
            fixture.events[1]["fresh"] = True
            with self.assertRaisesRegex(EvidenceError, "fresh"):
                fixture.verify()

    def test_snapshot_mutation_after_recomputed_outer_receipts_still_refuses(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = NativeFixture(Path(temporary))
            row = fixture.receipt["native_events"][0]
            path = fixture.out / row["archive"]["path"]
            path.write_bytes(b"changed owned archive")
            # Recompute outer receipt/manifest hashes, retain original event observation.
            manifest = json.loads((fixture.out / "native-snapshots.json").read_text())
            fixture.rewrite(manifest=manifest)
            with self.assertRaisesRegex(EvidenceError, "bytes/hash"):
                fixture.verify()

    def test_swapped_payload_with_legacy_associations_recomputed_refuses(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = NativeFixture(Path(temporary))
            fixture.payload.write_bytes(b"different normal payload with new outer hashes")
            with self.assertRaisesRegex(EvidenceError, "normal executable"):
                fixture.verify()

    def test_correct_payload_hash_with_wrong_producer_size_refuses(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = NativeFixture(Path(temporary))
            fixture.receipt["normal_executable"]["bytes"] += 1
            fixture.rewrite()
            with self.assertRaisesRegex(EvidenceError, "normal executable"):
                fixture.verify()

    def test_missing_wrong_size_and_source_target_mode_policy_refuse(self):
        for field, wrong in (("normal_executable", None), ("normal_executable", {"sha256": "1" * 64, "bytes": 3}),
                             ("commit", "f" * 40), ("target", "wrong"), ("mode", "candidate"),
                             ("control", {"AWS_LC_SYS_USE_SYSTEM": "false"})):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temporary:
                fixture = NativeFixture(Path(temporary))
                fixture.receipt[field] = wrong
                fixture.rewrite()
                with self.assertRaises(EvidenceError):
                    fixture.verify()

    def test_missing_extra_symlink_duplicate_and_escaping_snapshots_refuse(self):
        for change in ("missing", "extra", "symlink", "duplicate", "escape", "absolute"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as temporary:
                fixture = NativeFixture(Path(temporary))
                row = fixture.receipt["native_events"][0]
                path = fixture.out / row["archive"]["path"]
                manifest = json.loads((fixture.out / "native-snapshots.json").read_text())
                if change == "missing":path.unlink()
                elif change == "extra":(path.parent / "unexpected").write_text("owned extra")
                elif change == "symlink":
                    path.unlink();path.symlink_to(fixture.payload)
                elif change == "duplicate":manifest["events"].append(copy.deepcopy(row))
                elif change == "escape":manifest["events"][0]["archive"]["path"] = "../owned-payload"
                else:manifest["events"][0]["archive"]["path"] = str(fixture.payload)
                fixture.rewrite(manifest=manifest)
                with self.assertRaises((EvidenceError, OSError)):
                    fixture.verify()

    def test_fixed_byte_event_and_total_limits_are_not_renewable(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = NativeFixture(Path(temporary))
            oversized = fixture.directory / "owned-sparse"
            with oversized.open("wb") as output:output.truncate(policy.ARCHIVE_LIMIT + 1)
            with self.assertRaisesRegex(EvidenceError, "byte bound"):
                inputs.bounded_file(oversized, fixture.directory, policy.ARCHIVE_LIMIT)
            check = mock.Mock(side_effect=EvidenceError("owned budget depleted"))
            with self.assertRaisesRegex(EvidenceError, "budget depleted"):
                inputs.bounded_file(fixture.payload, fixture.directory, policy.EXECUTABLE_LIMIT, check)
            manifest = json.loads((fixture.out / "native-snapshots.json").read_text())
            manifest["events"] *= 17
            fixture.rewrite(manifest=manifest)
            with self.assertRaisesRegex(EvidenceError, "over-limit"):
                fixture.verify()


class ProducerFixture:
    def __init__(self, directory, builder="CC", **changes):
        directory = directory.resolve()
        self.root, self.out, self.tools = directory / "source", directory / "evidence", directory / "tools"
        self.root.mkdir();self.tools.mkdir()
        (self.root / "scripts").mkdir()
        for name in policy.SOURCE_FILES:
            shutil.copyfile(REPO / "scripts" / name, self.root / "scripts" / name)
        self.lock = "version = 4\n" + native_lock()
        (self.root / "Cargo.lock").write_text(self.lock)
        (self.root / "VERSION").write_text("1.2.3\n")
        (self.root / "Cargo.toml").write_text('[package]\nname="chanvoy"\nversion="1.2.3"\n')
        self.payload = b"owned synthetic release executable bytes"
        self.metadata = {"packages": [{"id": "root", "name": "chanvoy", "manifest_path": str(self.root / "Cargo.toml")}]}
        self.events = [{"reason": "compiler-artifact", "package_id": "root", "target": {"name": "chanvoy", "kind": ["bin"]},
                        "profile": {"test": False, "opt_level": "3", "debug_assertions": False}, "features": [],
                        "executable": str(self.root / "target/release/chanvoy")}]
        native_out = add_native(self.events, self.metadata, self.root, builder)
        self.events.append({"reason": "build-finished", "success": True})
        self.calls = directory / "cargo-calls.jsonl"
        state = directory / "state.json"
        json_file(state, {"root": str(self.root), "metadata": self.metadata, "events": self.events, "out": str(native_out),
                          "output": native_output(native_out, builder), "payload": self.payload.decode(),
                          "calls": str(self.calls), "builder": builder, **changes})
        prefix = "import json,os,pathlib,sys,time\nstate=json.loads(pathlib.Path(%r).read_text())\n" % str(state)
        def executable(name, body):
            path = self.tools / name
            path.write_text("#!/usr/bin/env python3\n" + prefix + body)
            path.chmod(0o755)
        executable("git", """args=sys.argv[1:]
if args==['rev-parse','HEAD']:print('a'*40)
elif args==['rev-parse','HEAD^{tree}']:print('b'*40)
elif args==['status','--porcelain=v1']:pass
else:raise SystemExit(8)
""")
        executable("rustc", "print('rustc 1.89.0 (owned synthetic)\\nhost: x86_64-unknown-linux-gnu')\n")
        executable("cargo", """with pathlib.Path(state['calls']).open('a') as calls:
 calls.write(json.dumps({'argv':sys.argv[1:],'control':{k:v for k,v in os.environ.items() if k.startswith(('AWS_LC_SYS_','HOST_AWS_LC_SYS_','TARGET_AWS_LC_SYS_'))}})+'\\n')
if sys.argv[1]=='fetch':pass
elif sys.argv[1]=='metadata':print(json.dumps(state['metadata']))
elif sys.argv[1]=='build':
 out=pathlib.Path(state['out']);out.mkdir(parents=True)
 native_dir=out if state['builder']=='CC' else out/'build/artifacts';native_dir.mkdir(parents=True,exist_ok=True)
 (native_dir/'libaws_lc_0_45_0_crypto.a').write_bytes(b'!<arch>\\nowned synthetic source archive\\n')
 output=state['output']
 if os.environ.get('AWS_LC_SYS_USE_SYSTEM')!='0':output=output.replace('Building with: CC','Building with: System').replace('Building with: CMake','Building with: System')
 (out.parent/'output').write_text(output)
 cli=pathlib.Path(state['root'])/'target/release/chanvoy';cli.write_text(state['payload'])
 for event in state['events']:print(json.dumps(event))
else:raise SystemExit(9)
""")
        self.environment = {"PATH": str(self.tools) + os.pathsep + os.environ["PATH"],
                            "CARGO_HOME": str(directory / "cargo-home"), "RUSTUP_TOOLCHAIN": "1.89.0"}

    def run(self, **environment):
        result = subprocess.run([sys.executable, str(REPO / "scripts/build-production-binary.py"),
            "--root", str(self.root), "--platform", "linux-x86_64", "--expected-commit", "a" * 40,
            "--mode", "local", "--output", str(self.out)], env={**self.environment, **environment},
            capture_output=True, text=True, timeout=10)
        receipt = json.loads((self.out / "native-build-policy.json").read_text())
        return result, receipt


class ProducerTests(unittest.TestCase):
    def test_fresh_cc_and_cmake_produce_only_owned_child_control_and_observation(self):
        for builder in ("CC", "CMake"):
            with self.subTest(builder=builder), tempfile.TemporaryDirectory() as temporary:
                fixture = ProducerFixture(Path(temporary), builder)
                result, receipt = fixture.run()
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(receipt["normal_executable"], {"sha256": policy.sha(fixture.payload), "bytes": len(fixture.payload)})
                self.assertEqual(receipt["status"], "pass")
                calls = [json.loads(x) for x in fixture.calls.read_text().splitlines()]
                self.assertEqual([x["argv"][0] for x in calls], ["fetch", "metadata", "build"])
                self.assertTrue(all(x["control"] == {"AWS_LC_SYS_USE_SYSTEM": "0"} for x in calls))
                self.assertEqual(calls[-1]["argv"], ["build", "--release", "--locked", "--package", "chanvoy",
                                                   "--message-format=json-render-diagnostics"])
                self.assertNotIn("AWS_LC_SYS_USE_SYSTEM", fixture.environment)

    def test_presence_and_config_refusal_precede_first_cargo(self):
        for selector in ("AWS_LC_SYS_USE_SYSTEM", "AWS_LC_SYS_USE_SYSTEM_x86_64_unknown_linux_gnu",
                         "HOST_AWS_LC_SYS_USE_SYSTEM", "TARGET_AWS_LC_SYS_NO_PREFIX", "CARGO_TARGET_DIR", "OPENSSL_LIB_DIR", "MAKEFLAGS"):
            with self.subTest(selector=selector), tempfile.TemporaryDirectory() as temporary:
                fixture = ProducerFixture(Path(temporary))
                result, receipt = fixture.run(**{selector: ""})
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(receipt["status"], "failed")
                self.assertFalse(fixture.calls.exists())
                self.assertEqual(receipt["commands"], [])
        for config in ("root", "parent", "home", "symlink"):
            with self.subTest(config=config), tempfile.TemporaryDirectory() as temporary:
                fixture = ProducerFixture(Path(temporary))
                directory = (fixture.root / ".cargo" if config in ("root", "symlink") else
                             fixture.root.parent / ".cargo" if config == "parent" else Path(fixture.environment["CARGO_HOME"]))
                directory.mkdir()
                if config == "symlink":(directory / "config.toml").symlink_to("missing-owned-file")
                else:(directory / "config.toml").write_text("owned unsupported configuration")
                result, receipt = fixture.run()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("configuration", receipt["failure"])
                self.assertFalse(fixture.calls.exists())

    def test_stale_target_is_retained_and_cannot_mint_receipt(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = ProducerFixture(Path(temporary))
            target = fixture.root / "target";target.mkdir()
            residue = target / "owned-residue";residue.write_text("retain this")
            result, receipt = fixture.run()
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("absent owned target", receipt["failure"])
            self.assertEqual(residue.read_text(), "retain this")
            self.assertFalse(fixture.calls.exists())

    def test_make_receipt_is_guidance_only_and_ordinary_install_stays_cached(self):
        makefile = (REPO / "Makefile").read_text()
        self.assertIn("build-release:\n\tcargo build --release --locked --package chanvoy", makefile)
        self.assertIn("install: build-release", makefile)
        guidance = makefile.split("build-release-receipt:", 1)[1].split("\n#", 1)[0]
        self.assertIn("does not invoke Cargo or the producer", guidance)
        self.assertIn("@exit 1", guidance)
        self.assertNotIn("\tpython3", guidance)
        for name in ("check", "release"):
            workflow = (REPO / ".github/workflows" / (name + ".yml")).read_text()
            self.assertIn("python3 scripts/build-production-binary.py", workflow)
            self.assertNotIn("AWS_LC_SYS_USE_SYSTEM:", workflow)
            self.assertNotIn("cargo build --release", workflow)


if __name__ == "__main__":
    unittest.main()
