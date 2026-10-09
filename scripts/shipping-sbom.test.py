#!/usr/bin/env python3
"""Owned synthetic association, inclusion and immutable schema controls."""

import copy
import errno
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import tempfile
import tarfile
import time
from types import SimpleNamespace
import unittest
from unittest import mock

sys.dont_write_bytecode = True
import offline_schema as schema
import bounded_evidence as bounded
import shipping_sbom as sbom
import production_build_inputs as build_inputs
from bounded_evidence import EvidenceCommands, EvidenceError, json_write
from sbom_evidence import Controller, scanner


native_spec = importlib.util.spec_from_file_location("native_synthetic", sbom.SCRIPTS / "production-build-policy.test.py")
native_fixture = importlib.util.module_from_spec(native_spec)
native_spec.loader.exec_module(native_fixture)

REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
GIT = "git+https://github.com/example/synthetic?tag=v1.0.0#" + "d" * 40


class ShippingFixture:
    def __init__(self, directory):
        directory = directory.resolve()
        self.root = directory
        self.binaries = directory / "binaries"
        self.binaries.mkdir()
        self.expected = {"version": "1.2.3", "commit": "a" * 40, "tree": "b" * 40,
                         "tag": "v1.2.3", "tag_object": "c" * 40,
                         "workflow": {"ref": "example/repository/.github/workflows/release.yml@refs/tags/v1.2.3",
                                      "sha": "a" * 40, "event_sha": "a" * 40,
                                      "run_id": "111", "run_attempt": "1"}}
        self.folders, self.scans = {}, {}
        for platform, target in sbom.PLATFORMS.items():
            folder = directory / platform
            folder.mkdir()
            self.folders[platform] = folder
            packages = []
            for identity, name, source, kind in (("root", "chanvoy", None, "bin"),
                                                 ("shared", "shared", REGISTRY, "lib"),
                                                 ("fork", "shared", GIT, "lib"),
                                                 ("builder", "cc", REGISTRY, "lib"),
                                                 ("macro", "owned-macro", REGISTRY, "proc-macro"),
                                                 ("dev", "dev-fixture", REGISTRY, "lib"),
                                                 ("inactive", "ring", REGISTRY, "lib")):
                packages.append({"id": identity, "name": name, "version": "1.2.3" if identity == "root" else "1.0.0",
                                 "source": source, "manifest_path": "/source/Cargo.toml",
                                 "license": "MIT OR Apache-2.0", "targets": [{"kind": [kind]}]})
            metadata = {"packages": packages, "workspace_members": ["root"], "resolve": {"nodes": []}}
            for package in packages:
                deps = []
                if package["id"] == "root":
                    for identity, kind in (("shared", None), ("shared", "build"), ("fork", None),
                                           ("builder", "build"), ("macro", None), ("dev", "dev")):
                        deps.append({"pkg": identity, "dep_kinds": [{"kind": kind, "target": None}]})
                metadata["resolve"]["nodes"].append({"id": package["id"], "deps": deps})
            events = []
            for identity, kind, features in (("root", "bin", []), ("shared", "lib", ["runtime"]),
                                             ("shared", "lib", ["build"]), ("fork", "lib", []),
                                             ("builder", "lib", []), ("macro", "proc-macro", [])):
                events.append({"reason": "compiler-artifact", "package_id": identity,
                               "target": {"name": "chanvoy" if identity == "root" else identity, "kind": [kind]},
                               "profile": {"test": False, "opt_level": "3", "debug_assertions": False},
                               "features": features})
            native_script = {"reason": "build-script-executed", "package_id": "root",
                             "linked_libs": ["static=owned_native"], "linked_paths": ["native=/source/target/release/out"],
                             "cfgs": []}
            events += [native_script]
            native_fixture.add_native(events, metadata, "/source")
            events[0]["executable"] = "/source/target/release/chanvoy"
            metadata["resolve"]["nodes"][0]["deps"].append({"pkg": native_fixture.AWS_ID, "dep_kinds": [{"kind": None}]})
            metadata["resolve"]["nodes"].append({"id": native_fixture.AWS_ID, "deps": []})
            events.append({"reason": "build-finished", "success": True})
            lock = "version = 4\n"
            for package in packages:
                lock += '\n[[package]]\nname = ' + json.dumps(package["name"]) + '\nversion = ' + json.dumps(package["version"]) + '\n'
                if package["source"]:
                    lock += 'source = ' + json.dumps(package["source"]) + '\n'
                    if package["source"] == REGISTRY:
                        lock += 'checksum = "' + (native_fixture.AWS_CHECKSUM if package["name"] == "aws-lc-sys" else "1" * 64) + '"\n'
            (folder / "normal-Cargo.lock").write_text(lock)
            self.expected["lock_sha256"] = schema.sha(lock.encode())
            self.write_json(folder / "metadata.log", metadata)
            self.write_events(folder, events)
            asset = "chanvoy-v1.2.3-" + platform
            payload = b"owned synthetic payload: " + platform.encode()
            (self.binaries / asset).write_bytes(payload)
            (folder / "payload").mkdir()
            (folder / "payload" / asset).write_bytes(payload)
            native_logs = {}
            stages = ["native-tool-version", "native-dynamic"]
            stages += ["native-build-cc-version", "native-build-c++-version"]
            if platform.startswith("linux-"):
                stages.append("native-program-headers")
            for stage in stages:
                for suffix in (".log", ".stderr.log"):
                    name = stage + suffix
                    data = ("owned native evidence " + name).encode()
                    (folder / name).write_bytes(data)
                    native_logs[name] = schema.sha(data)
            captured = {"schema": "normal-build-inputs-v2", "before_fixture_compilation": True, "mode": "shipping",
                        "platform": platform, "target": target, "rustc": "rustc 1.89.0 (owned synthetic)",
                        "source_root": "/source", "package_id": "root", "payload": asset,
                        "payload_sha256": schema.sha(payload), **{k: copy.deepcopy(v) for k, v in self.expected.items() if k != "version"},
                        "native": {"tool": "otool" if platform == "macos-aarch64" else "readelf",
                                   "tool_sha256": "9" * 64, "tool_version": "owned synthetic native tool",
                                   "logs_sha256": native_logs, "capture_status": "success", "external_requirements": [],
                                   "empty_observation": True, "build_scripts": [{k: v for k, v in native_script.items() if k != "reason"}]}}
            captured["native"]["build_toolchain"] = {"selected_builder_package_ids": ["builder"],
                "observations": [{"tool": name, "sha256": "8" * 64, "version": "owned default tool",
                                  "evidence_class": "host/build-tool discovery/query observation",
                                  "selection": "selection-unconfirmed; not compiler invocation proof"} for name in ("cc", "c++")]}
            captured["native"]["build_scripts"][0]["archives"] = [{"name": "libowned_native.a", "sha256": "2" * 64}]
            producer = native_fixture.seal_synthetic(folder, "/source", events, metadata, payload,
                        {**self.expected, "mode": "shipping", "platform": platform, "target": target})
            captured.update(native_build_policy="native-build-policy.json",
                            native_build_policy_sha256=build_inputs.digest(folder / "native-build-policy.json"),
                            native_snapshot_manifest_sha256=producer["native_snapshot_manifest_sha256"],
                            normal_executable=producer["normal_executable"])
            native_event = next(e for e in events if e.get("reason") == "build-script-executed" and e["package_id"] == native_fixture.AWS_ID)
            captured["native"]["build_scripts"].append({**{k: v for k, v in native_event.items() if k != "reason"},
                "archives": [{"name": native_fixture.policy.ARCHIVE, "sha256": producer["native_events"][0]["archive"]["sha256"]}]})
            q = {**{k: copy.deepcopy(v) for k, v in captured.items() if k not in ("schema", "native", "source_root")},
                 "status": "pass", "expected_commit": self.expected["commit"], "normal_build_inputs": "normal-build-inputs.json",
                 "qualification_driver_sha256": schema.sha(schema.regular_bytes(sbom.SCRIPTS / "qualify-production-binary.py"))}
            self.write_json(folder / "normal-build-inputs.json", captured)
            self.write_json(folder / "qualification.json", q)
            self.refresh(folder)
            self.scans[platform] = {"descriptor": {"name": "syft", "version": "1.33.0",
                                                   "configuration": {"catalogers": {"requested": {"default": ["directory", "file"]},
                                                                                      "used": sbom.TOOLS["syft"]["catalogers"]}}},
                                    "source": {"type": "file", "name": asset,
                                               "metadata": {"digests": [{"algorithm": "sha256", "value": schema.sha(payload)}]}},
                                    "artifacts": []}

    @staticmethod
    def write_json(path, value):
        path.write_text(json.dumps(value, sort_keys=True) + "\n")

    @staticmethod
    def write_events(folder, events):
        (folder / "normal-build.jsonl").write_text("".join(json.dumps(x) + "\n" for x in events))

    def refresh(self, folder):
        captured = sbom.object_file(folder / "normal-build-inputs.json")
        q = sbom.object_file(folder / "qualification.json")
        captured["normal_build_messages_sha256"] = schema.sha(schema.regular_bytes(folder / "normal-build.jsonl"))
        captured["metadata_sha256"] = schema.sha(schema.regular_bytes(folder / "metadata.log"))
        self.write_json(folder / "normal-build-inputs.json", captured)
        q.update(normal_build_messages_sha256=captured["normal_build_messages_sha256"],
                 metadata_messages_sha256=captured["metadata_sha256"],
                 normal_build_inputs_sha256=schema.sha(schema.regular_bytes(folder / "normal-build-inputs.json")))
        self.write_json(folder / "qualification.json", q)

    def assemble(self):
        return sbom.assemble(self.folders, self.binaries, self.scans, self.expected)


class ShippingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="cv-shipping-synthetic-")
        self.fixture = ShippingFixture(Path(self.directory.name))

    def tearDown(self):
        self.directory.cleanup()

    def test_positive_roles_sources_native_uncertainty_and_empty_scan(self):
        value = self.fixture.assemble()
        applications = [x for x in value["components"] if x["type"] == "application"]
        self.assertEqual(len(applications), 3)
        names = {x["name"] for x in value["components"]}
        self.assertNotIn("ring", names)
        self.assertNotIn("dev-fixture", names)
        for component in value["components"]:
            props = {x["name"]: x["value"] for x in component["properties"]}
            if component["name"] in ("cc", "owned-macro"):
                self.assertEqual(props["chanvoy:roles"], "build-input")
            if component["name"] == "shared":
                self.assertIn("source-identity", " ".join(props))
            if component["name"].startswith("chanvoy-"):
                self.assertIn("not established", props["chanvoy:native-build-inputs"])
        shared = [x for x in value["components"] if x["name"] == "shared"]
        self.assertEqual(len(shared), 6)  # Registry and git fork on each platform.
        self.assertEqual(len({x["bom-ref"] for x in shared}), 6)

    def test_reordered_semantically_identical_inputs_keep_inventory(self):
        before = self.fixture.assemble()
        for folder in self.fixture.folders.values():
            events = [json.loads(x) for x in (folder / "normal-build.jsonl").read_text().splitlines()]
            # Normal compiler messages are an immutable producer observation.
            # Metadata package/node ordering remains semantically irrelevant.
            metadata = sbom.object_file(folder / "metadata.log")
            metadata["packages"].reverse()
            metadata["resolve"]["nodes"].reverse()
            self.fixture.write_json(folder / "metadata.log", metadata)
            self.fixture.refresh(folder)
        self.assertEqual(self.fixture.assemble(), before)

    def test_payload_platform_and_same_run_association_fail_closed(self):
        platform = "linux-x86_64"
        folder = self.fixture.folders[platform]
        original = schema.regular_bytes(folder / "qualification.json")
        for field, wrong in (("commit", "f" * 40), ("tree", "f" * 40), ("tag_object", "f" * 40),
                              ("target", "wrong"), ("mode", "candidate"), ("status", "failed")):
            with self.subTest(field=field):
                q = json.loads(original)
                q[field] = wrong
                self.fixture.write_json(folder / "qualification.json", q)
                with self.assertRaises(EvidenceError):
                    self.fixture.assemble()
        (folder / "qualification.json").write_bytes(original)
        q = json.loads(original)
        q["workflow"]["run_attempt"] = "2"
        self.fixture.write_json(folder / "qualification.json", q)
        with self.assertRaisesRegex(EvidenceError, "run/attempt"):
            self.fixture.assemble()
        (folder / "qualification.json").write_bytes(original)
        (self.fixture.binaries / "chanvoy-v1.2.3-linux-x86_64").write_bytes(b"different owned payload")
        with self.assertRaisesRegex(EvidenceError, "payload association"):
            self.fixture.assemble()

    def test_substituted_shipping_payload_rehashed_legacy_receipts_still_refuses(self):
        platform = "linux-x86_64"
        folder = self.fixture.folders[platform]
        captured = sbom.object_file(folder / "normal-build-inputs.json")
        asset = captured["payload"]
        replacement = b"owned substituted shipping payload with revised legacy hashes"
        for path in (self.fixture.binaries / asset, folder / "payload" / asset):
            path.write_bytes(replacement)
        captured["payload_sha256"] = schema.sha(replacement)
        self.fixture.write_json(folder / "normal-build-inputs.json", captured)
        q = sbom.object_file(folder / "qualification.json")
        q["payload_sha256"] = schema.sha(replacement)
        self.fixture.write_json(folder / "qualification.json", q)
        self.fixture.refresh(folder)
        self.fixture.scans[platform]["source"]["metadata"]["digests"][0]["value"] = schema.sha(replacement)
        with self.assertRaisesRegex(EvidenceError, "producer normal executable association"):
            self.fixture.assemble()

    def test_correct_shipping_payload_hash_with_wrong_producer_size_refuses(self):
        folder = self.fixture.folders["linux-x86_64"]
        producer = sbom.object_file(folder / "native-build-policy.json")
        producer["normal_executable"]["bytes"] += 1
        self.fixture.write_json(folder / "native-build-policy.json", producer)
        for name in ("normal-build-inputs.json", "qualification.json"):
            value = sbom.object_file(folder / name)
            value["normal_executable"] = copy.deepcopy(producer["normal_executable"])
            value["native_build_policy_sha256"] = schema.sha(schema.regular_bytes(folder / "native-build-policy.json"))
            self.fixture.write_json(folder / name, value)
        self.fixture.refresh(folder)
        with self.assertRaisesRegex(EvidenceError, "producer normal executable association"):
            self.fixture.assemble()

    def test_old_input_version_and_missing_producer_binding_cannot_upgrade(self):
        folder = self.fixture.folders["linux-x86_64"]
        original = schema.regular_bytes(folder / "normal-build-inputs.json")
        for field, wrong in (("schema", "normal-build-inputs-v1"), ("normal_executable", None),
                             ("native_build_policy_sha256", "0" * 64), ("native_snapshot_manifest_sha256", None)):
            with self.subTest(field=field):
                captured = json.loads(original)
                captured[field] = wrong
                self.fixture.write_json(folder / "normal-build-inputs.json", captured)
                self.fixture.refresh(folder)
                with self.assertRaises(EvidenceError):
                    self.fixture.assemble()
        (folder / "normal-build-inputs.json").write_bytes(original)
        self.fixture.refresh(folder)
        (folder / "native-build-policy.json").unlink()
        with self.assertRaises(OSError):
            self.fixture.assemble()

    def test_departed_runner_paths_are_not_opened_by_aggregation(self):
        original = Path.open
        def open_owned(path, *args, **kwargs):
            if str(path).startswith(("/source/", "/unavailable-registry/")):
                self.fail("aggregation dereferenced a departed runner path")
            return original(path, *args, **kwargs)
        with mock.patch.object(Path, "open", open_owned):
            self.fixture.assemble()

    def test_normal_messages_cannot_be_reordered_after_producer_capture(self):
        folder = self.fixture.folders["linux-x86_64"]
        events = [json.loads(x) for x in (folder / "normal-build.jsonl").read_text().splitlines()]
        self.fixture.write_events(folder, list(reversed(events)))
        self.fixture.refresh(folder)
        with self.assertRaisesRegex(EvidenceError, "native producer policy"):
            self.fixture.assemble()

    def test_missing_platform_and_extra_canonical_asset_refuse(self):
        removed = self.fixture.folders.pop("linux-aarch64")
        with self.assertRaisesRegex(EvidenceError, "platform"):
            self.fixture.assemble()
        self.fixture.folders["linux-aarch64"] = removed
        (self.fixture.binaries / "owned-unexpected.log").write_text("not a release asset")
        with self.assertRaisesRegex(EvidenceError, "inventory"):
            self.fixture.assemble()

    def test_injected_dev_only_or_unknown_compiler_package_refuse(self):
        folder = self.fixture.folders["linux-x86_64"]
        original = schema.regular_bytes(folder / "normal-build.jsonl")
        events = [json.loads(x) for x in original.splitlines()]
        for identity in ("dev", "unknown-package"):
            with self.subTest(identity=identity):
                injected = copy.deepcopy(events[1])
                injected["package_id"] = identity
                self.fixture.write_events(folder, [*events, injected])
                self.fixture.refresh(folder)
                with self.assertRaises(EvidenceError):
                    self.fixture.assemble()

    def test_missing_native_build_evidence_or_log_tamper_refuse(self):
        folder = self.fixture.folders["linux-x86_64"]
        captured = sbom.object_file(folder / "normal-build-inputs.json")
        captured["native"]["build_scripts"] = []
        self.fixture.write_json(folder / "normal-build-inputs.json", captured)
        self.fixture.refresh(folder)
        with self.assertRaisesRegex(EvidenceError, "native build-script"):
            self.fixture.assemble()
        (folder / "native-dynamic.log").write_text("changed owned evidence")
        with self.assertRaisesRegex(EvidenceError, "evidence changed"):
            self.fixture.assemble()

    def test_native_receipt_shape_is_required_after_correct_rehash(self):
        for platform in ("linux-x86_64", "macos-aarch64"):
            folder = self.fixture.folders[platform]
            original = (folder / "normal-build-inputs.json").read_bytes()
            for change in ("missing-log-name", "wrong-tool"):
                with self.subTest(platform=platform, change=change):
                    captured = json.loads(original)
                    if change == "missing-log-name":
                        captured["native"]["logs_sha256"].pop("native-dynamic.log")
                    else:
                        captured["native"]["tool"] = "readelf" if platform == "macos-aarch64" else "otool"
                    self.fixture.write_json(folder / "normal-build-inputs.json", captured)
                    self.fixture.refresh(folder)
                    with self.assertRaisesRegex(EvidenceError, "required native metadata receipt"):
                        self.fixture.assemble()
            (folder / "normal-build-inputs.json").write_bytes(original)
            self.fixture.refresh(folder)

    def test_scanner_hash_version_selection_and_private_paths_refuse(self):
        platform = "linux-x86_64"
        original = copy.deepcopy(self.fixture.scans[platform])
        for change in ("hash", "version", "selection", "private"):
            with self.subTest(change=change):
                scan = copy.deepcopy(original)
                if change == "hash":scan["source"]["metadata"]["digests"][0]["value"] = "0" * 64
                elif change == "version":scan["descriptor"]["version"] = "1.42.1"
                elif change == "selection":scan["descriptor"]["configuration"]["catalogers"]["used"] = []
                else:scan["artifacts"] = [{"id": "owned", "name": "/home/synthetic/private", "version": "1", "foundBy": "binary-classifier-cataloger"}]
                self.fixture.scans[platform] = scan
                with self.assertRaises(EvidenceError):
                    self.fixture.assemble()

    def test_complete_mock_generator_admits_only_validated_bytes(self):
        fixture = self.fixture
        env = dict(zip(("GITHUB_WORKFLOW_REF", "GITHUB_WORKFLOW_SHA", "GITHUB_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"),
                       fixture.expected["workflow"].values()))
        names = ["valid", "invalid-type", "invalid-spdx", "invalid-jsf", "malformed-data", "missing-spdx", "missing-jsf",
                 "missing-meta", "tampered-schema", "symlink-schema", "invalid-isolation-setup", "unknown-ref", "unknown-schema",
                 "probe", "unresolved-reference", "valid-bom-snapshot-tamper"]
        route = fixture.root / "owned-route.json"
        self.fixture.write_json(route, {"schema": "sbom-tool-route-v1", "status": "pass", "commit": fixture.expected["commit"],
                                       "platform": "Linux", "workflow": env, "cases": [{"name": name, "matched": True} for name in names],
                                       "isolation": "unshare user/map-root-user/network; no fallback",
                                       "tool": {"archive_sha256": sbom.TOOLS["goneat"]["archive_sha256"]},
                                       "runner": {"os_release": {"ID": "ubuntu", "VERSION_ID": "22.04"}}})
        tool = fixture.root / "owned-validator"
        tool.write_text("owned validator fixture")
        setup = fixture.root / "owned-setup.json"
        self.fixture.write_json(setup, {"status": "pass", "tool": {"binary": str(tool), "binary_sha256": schema.sha(tool.read_bytes()),
                                                                 "archive_sha256": sbom.TOOLS["goneat"]["archive_sha256"]}})
        companions = fixture.root / "companions"
        companions.mkdir()
        for name, folder in fixture.folders.items():
            folder.rename(companions / name)
        original_bytes = sbom.regular_bytes

        def bytes_at(path):
            if Path(path) == sbom.SCRIPTS.parent / "Cargo.lock":
                return schema.regular_bytes(companions / "linux-x86_64/normal-Cargo.lock")
            return original_bytes(path)

        def command(controller, name, *_args, **_kwargs):
            return {"head": fixture.expected["commit"], "tree": fixture.expected["tree"], "clean": "", "final-clean": ""}[name]

        def scan(_controller, name, argv, *_args):
            if name == "syft-version":return json.dumps({"version": "1.33.0"})
            if name == "syft-config":
                return "check-for-app-update: " + ("false" if "--load" in argv else "true") + "\n"
            return json.dumps(fixture.scans[name.removeprefix("syft-")])

        for failure in (False, True):
            output = fixture.root / ("blocked-generation" if failure else "accepted-generation")
            argv = ["shipping_sbom.py", "--binaries", str(fixture.binaries), "--evidence", str(companions),
                    "--tool-route", str(route), "--tool-receipt", str(setup), "--expected-commit", fixture.expected["commit"],
                    "--tag-object", fixture.expected["tag_object"], "--tag", "v1.2.3", "--version", "1.2.3", "--output", str(output)]
            def validate(_controller, data, _tool):
                if failure:raise EvidenceError("owned synthetic schema refusal")
                return Path(data).read_bytes()
            with mock.patch.dict(os.environ, env), mock.patch.object(sbom.sys, "argv", argv), \
                    mock.patch.object(sbom, "regular_bytes", side_effect=bytes_at), \
                    mock.patch.object(Controller, "command", command), mock.patch.object(sbom, "scanner", side_effect=scan), \
                    mock.patch.object(sbom, "validate_schema", side_effect=validate):
                self.assertEqual(sbom.main(), 1 if failure else 0)
            asset = output / "sbom-1.2.3.cdx.json"
            self.assertEqual(asset.exists(), not failure)
            receipt = sbom.object_file(output / "evidence.json")
            self.assertEqual(receipt["status"], "failed" if failure else "pass")
            if not failure:
                self.assertEqual(asset.read_bytes(), (output / "candidate-bom.json").read_bytes())

    def test_effective_scanner_configuration_controls_admission(self):
        for version, config, accepted in (
            ("1.33.0", "check-for-app-update: false\n", True),
            ("1.33.0", "check-for-app-update: true\n", False),
            ("1.33.0", "log: {}\n", False),
            ("1.32.0", "check-for-app-update: false\n", False),
        ):
            with self.subTest(version=version, config=config):
                def observe(_controller, name, argv, _pin):
                    if name == "syft-version":
                        return json.dumps({"version": version})
                    # The configuration command emits defaults unless asked to load.
                    return config if "--load" in argv else "check-for-app-update: true\n"
                with mock.patch.object(sbom, "scanner", side_effect=observe):
                    if accepted:
                        observed, text = sbom.scanner_preflight(object())
                        self.assertEqual(observed["version"], version)
                        self.assertEqual(text, config)
                    else:
                        with self.assertRaises(EvidenceError):
                            sbom.scanner_preflight(object())

    def test_workflow_dependency_and_inventory_separation(self):
        release = (sbom.SCRIPTS.parent / ".github/workflows/release.yml").read_text()
        check = (sbom.SCRIPTS.parent / ".github/workflows/check.yml").read_text()
        self.assertIn("needs: [validate, build, sbom_tool_route]", release)
        self.assertIn("needs: [validate, build, sbom]", release)
        self.assertNotIn("dir:.", release)
        self.assertNotIn("pull_request_target", check + release)
        for platform in sbom.PLATFORMS:
            self.assertIn("qualification-shipping-" + platform + "-${{ github.run_attempt }}", release)
        upload = release[release.index("      - name: Upload exact SBOM artifact"):release.index("\n  packages:")]
        self.assertIn("if: success()", upload)
        self.assertNotIn("always()", upload)


class SchemaTests(unittest.TestCase):
    def snapshot_control(self, change=None):
        with tempfile.TemporaryDirectory(prefix="cv-schema-snapshot-") as directory:
            root = Path(directory)
            out = root / "evidence"
            out.mkdir()
            data = root / "owned-bom.json"
            data.write_text('{"bomFormat":"CycloneDX","specVersion":"1.6","version":1,"components":[]}')
            goneat = root / "owned-validator"
            goneat.write_text("owned synthetic tool bytes")
            controller = EvidenceCommands()
            controller.root, controller.out = root, out
            controller.deadline = time.monotonic() + 30
            controller.receipt_path = out / "schema-receipt.json"
            controller.receipt = {"schema": "owned-synthetic-schema-control", "commands": [], "status": "incomplete"}
            calls = []

            def command(name, argv, horizon, **kwargs):
                calls.append((name, argv, horizon))
                self.assertEqual(set(kwargs["expected_images"]), {"tool", "wrapper"})
                if change == "setup-failure":
                    raise EvidenceError("owned isolation setup refusal")
                marker = Path(argv[argv.index("--marker") + 1])
                json_write(marker, {"exec_requested": True, "tool_sha256": schema.sha(goneat.read_bytes()),
                                    "platform": "synthetic-only", "scope": "not an actual namespace proof"})
                if name == "validator-version":
                    if change == "version-shape":
                        return "[]"
                    return json.dumps({"binaryVersion": "v0.6.1"})
                if change == "bom":
                    snapshot = Path(argv[argv.index("--data") + 1])
                    snapshot.chmod(0o644)
                    value = json.loads(snapshot.read_text())
                    value["version"] = 2
                    snapshot.write_text(json.dumps(value))
                elif change == "source":
                    data.write_text("{}")
                elif change == "schema":
                    snapshot = Path(argv[argv.index("--schema-dir") + 1]) / "spdx.schema.json"
                    snapshot.chmod(0o644)
                    snapshot.write_text("{}")
                return "owned synthetic validator success"

            with mock.patch.object(controller, "command", side_effect=command):
                if change == "hosted-snapshot-seam":
                    spec = importlib.util.spec_from_file_location("owned_hash_route", sbom.SCRIPTS / "sbom-tool-route.py")
                    route = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(route)
                    route.arm_snapshot_hash_negative(controller)
                if change:
                    with self.assertRaises(EvidenceError):
                        schema.validate(controller, data, goneat)
                    self.assertNotIn("schema_validation", controller.receipt)
                else:
                    validated = schema.validate(controller, data, goneat)
                    self.assertEqual(validated, data.read_bytes())
                    receipt = controller.receipt["schema_validation"]
                    self.assertEqual(receipt["closed_references"], 344)
                    self.assertEqual(receipt["bom_sha256"], schema.sha(validated))
                    self.assertEqual(set(receipt["schemas_sha256"]), set(schema.SCHEMAS))
                    self.assertEqual((out / "validation-snapshot/bom.json").stat().st_mode & 0o777, 0o444)
            if change == "hosted-snapshot-seam":
                evidence = controller.receipt["owned_snapshot_hash_negative"]
                self.assertNotEqual(evidence["before_sha256"], evidence["after_sha256"])
                self.assertEqual(json.loads((out / "validation-snapshot/bom.json").read_text())["version"], 2)
            self.assertEqual(len(calls), 1 if change in ("setup-failure", "version-shape") else 2)
            self.assertTrue((out / "validation-snapshot/bom.json").exists())
            self.assertFalse((root / "canonical-release-bom.json").exists())

    def test_bom_and_closure_snapshots_are_bound_and_tampering_refuses(self):
        self.snapshot_control()
        for change in ("bom", "source", "schema", "setup-failure", "version-shape", "hosted-snapshot-seam"):
            with self.subTest(change=change):
                self.snapshot_control(change)

    def test_inactive_namespace_stops_before_validator_exec(self):
        with tempfile.TemporaryDirectory(prefix="cv-schema-inactive-") as directory:
            root = Path(directory)
            tool = root / "owned-validator"
            tool.write_text("synthetic tool")
            marker = root / "child.json"
            argv = ["offline_schema.py", "--child", "--goneat", str(tool), "--tool-sha256", schema.sha(tool.read_bytes()),
                    "--parent-namespace", "net:[owned-parent]", "--marker", str(marker)]
            with mock.patch.object(schema.sys, "argv", argv), mock.patch.object(schema.platform, "system", return_value="Linux"), \
                    mock.patch.object(schema.os, "readlink", return_value="net:[owned-parent]"), \
                    mock.patch.object(schema.os, "execv") as launch:
                self.assertEqual(schema.child(), 65)
                launch.assert_not_called()
                self.assertFalse(marker.exists())

    def test_exact_fixed_closure_and_unknown_uri_or_fragment(self):
        data, count = schema.preflight()
        self.assertEqual(count, 344)
        documents = {k: json.loads(v) for k, v in data.items()}
        for key, value in (("$ref", "http://synthetic.invalid/unknown"), ("$schema", "http://synthetic.invalid/unknown"),
                           ("$ref", "#/missing-owned-fragment")):
            with self.subTest(key=key, value=value):
                changed = copy.deepcopy(documents)
                changed["bom-1.6.schema.json"][key] = value
                with self.assertRaises(EvidenceError):
                    schema.reference_closure(changed)

    def test_missing_tampered_and_symlink_closure_never_launch(self):
        for case in ("missing", "tampered", "symlink"):
            with self.subTest(case=case), tempfile.TemporaryDirectory(prefix="cv-schema-owned-") as directory:
                target = Path(directory)
                for name in schema.SCHEMAS:
                    shutil.copyfile(schema.SCHEMA_ROOT / name, target / name)
                path = target / "spdx.schema.json"
                if case == "missing":path.unlink()
                elif case == "tampered":
                    value = json.loads(path.read_text())
                    value["title"] = "Owned ID-preserving schema hash negative"
                    path.write_text(json.dumps(value))
                else:
                    path.unlink()
                    path.symlink_to(schema.SCHEMA_ROOT / path.name)
                with mock.patch.object(schema.os, "execv") as launch:
                    with self.assertRaises((EvidenceError, OSError)):
                        schema.preflight(target)
                    launch.assert_not_called()


class ScannerTests(unittest.TestCase):
    def controlled_scanner(self, mode=None):
        with tempfile.TemporaryDirectory(prefix="cv-scanner-owned-") as directory:
            root = Path(directory)
            payload = root / "owned-payload"
            payload.write_bytes(b"owned synthetic scanner input")
            controller = Controller(sbom.SCRIPTS.parent, root / "evidence", "synthetic-scanner-v1", 180)
            calls = []
            container = None

            def command(name, argv, horizon):
                nonlocal container
                calls.append((name, argv, horizon))
                if name.endswith("-create"):
                    self.assertIn("--network", argv)
                    self.assertIn("--security-opt", argv)
                    self.assertIn("--cap-drop", argv)
                    self.assertEqual(argv[argv.index("--network") + 1], "none")
                    self.assertEqual(argv[argv.index("--security-opt") + 1], "no-new-privileges")
                    self.assertIn("--read-only", argv)
                    self.assertEqual(argv[argv.index("--cap-drop") + 1], "ALL")
                    self.assertIn("SYFT_CHECK_FOR_APP_UPDATE=false", argv)
                    self.assertIn(sbom.TOOLS["syft"]["image"], argv)
                    owner = argv[argv.index("--label") + 1].split("=", 1)[1]
                    mount = argv[argv.index("--mount") + 1]
                    source = mount.split("src=", 1)[1].split(",", 1)[0]
                    container = {"Id": "c" * 64, "Config": {"Image": sbom.TOOLS["syft"]["image"],
                                                              "Labels": {"chanvoy.sbom-owner": owner}},
                                 "HostConfig": {"NetworkMode": "none", "ReadonlyRootfs": True,
                                                "CapDrop": ["ALL"], "SecurityOpt": ["no-new-privileges"]},
                                 "Mounts": [{"Source": source, "Destination": "/payload", "RW": False}],
                                 "State": {"Status": "exited", "ExitCode": 0}}
                    options = {"enabled-explicit": ["no-new-privileges=true"],
                               "disabled-nnp": ["no-new-privileges=false"],
                               "unknown-nnp": ["no-new-privileges=owned-unknown"],
                               "contradictory-nnp": ["no-new-privileges", "no-new-privileges=false"],
                               "missing-nnp": [], "malformed-nnp": [True]}
                    if mode in options:
                        container["HostConfig"]["SecurityOpt"] = options[mode]
                    return "" if mode == "unknown-create" else container["Id"]
                if "-before-remove" in name and mode == "changed-owner":
                    container["Config"]["Labels"]["chanvoy.sbom-owner"] = "another-owned-fixture"
                if argv[1] == "inspect":
                    if mode == "malformed-inspect":
                        return "null"
                    return json.dumps([container])
                if argv[1] == "start":
                    if mode in ("timeout", "timeout-denied-remove"):
                        controller.fail("owned command exceeded its process horizon")
                        raise EvidenceError("owned command exceeded its process horizon")
                    return '{"owned":"scanner result"}'
                if argv[1] == "rm":
                    if mode in ("denied-remove", "timeout-denied-remove"):
                        raise EvidenceError("owned synthetic removal refusal")
                    return container["Id"]
                if argv[1] == "ps":
                    return json.dumps({"ID": container["Id"]}) if mode == "survivor" else ""
                raise AssertionError("unexpected owned scanner command")

            with mock.patch.object(controller, "command", side_effect=command):
                if mode not in (None, "enabled-explicit"):
                    with self.assertRaises(EvidenceError):
                        scanner(controller, "owned-scan", ["file:/payload/owned-payload", "-o", "syft-json"], sbom.TOOLS["syft"], payload)
                else:
                    self.assertEqual(json.loads(scanner(controller, "owned-scan", ["file:/payload/owned-payload", "-o", "syft-json"],
                                                        sbom.TOOLS["syft"], payload)), {"owned": "scanner result"})
            record = controller.receipt["containers"][0]
            self.assertEqual(record["cleanup"], "confirmed-absent" if mode in (None, "enabled-explicit", "timeout") else "unknown")
            if mode in ("disabled-nnp", "unknown-nnp", "contradictory-nnp", "missing-nnp", "malformed-nnp"):
                self.assertFalse(any(argv[1] in ("start", "rm") for _, argv, _ in calls))
                self.assertNotIn("completed", record)
            if mode == "unknown-create":
                self.assertIsNone(record["id"])
                self.assertEqual(len(calls), 1)
            elif mode == "changed-owner":
                self.assertFalse(any(x[1][1] == "rm" for x in calls))
            if mode == "timeout-denied-remove":
                self.assertEqual(controller.receipt["failure"], "owned command exceeded its process horizon")
                self.assertTrue(controller.receipt["secondary_failures"])
            for _, argv, horizon in calls:
                self.assertGreater(horizon, 0)
                if argv[1] in ("start", "inspect", "rm"):
                    self.assertIn("c" * 64, argv)
            self.assertFalse((root / "canonical-bom.json").exists())

    def test_owned_container_cleanup_and_negative_receipts(self):
        for mode in (None, "enabled-explicit", "disabled-nnp", "unknown-nnp", "contradictory-nnp", "missing-nnp",
                     "malformed-nnp", "unknown-create", "changed-owner", "malformed-inspect", "denied-remove", "survivor", "timeout", "timeout-denied-remove"):
            with self.subTest(mode=mode):
                self.controlled_scanner(mode)

    def test_verified_archive_extracts_only_one_regular_owned_binary(self):
        spec = importlib.util.spec_from_file_location("setup_validator", sbom.SCRIPTS / "setup-sbom-validator.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory(prefix="cv-validator-archive-") as directory:
            root = Path(directory)
            for mode in ("valid", "symlink", "duplicate", "mismatch"):
                with self.subTest(mode=mode):
                    buffer = io.BytesIO()
                    with tarfile.open(fileobj=buffer, mode="w:gz") as package:
                        info = tarfile.TarInfo("goneat")
                        if mode == "symlink":
                            info.type = tarfile.SYMTYPE
                            info.linkname = "owned-missing"
                            package.addfile(info)
                        else:
                            data = b"owned synthetic validator"
                            info.size = len(data)
                            package.addfile(info, io.BytesIO(data))
                            if mode == "duplicate":package.addfile(info, io.BytesIO(data))
                    archive = root / (mode + ".tar.gz")
                    archive.write_bytes(buffer.getvalue())
                    output = root / ("tool-" + mode)
                    pin = {**module.PIN, "archive_sha256": "0" * 64 if mode == "mismatch" else schema.sha(buffer.getvalue())}
                    with mock.patch.object(module, "PIN", pin):
                        if mode == "valid":
                            receipt = module.unpack(archive, output)
                            self.assertEqual(receipt["binary_sha256"], schema.sha(b"owned synthetic validator"))
                            self.assertEqual(output.stat().st_mode & 0o777, 0o555)
                        else:
                            with self.assertRaises(EvidenceError):module.unpack(archive, output)
                            self.assertFalse(output.exists())

    def test_release_requires_exact_completed_hosted_tool_route(self):
        expected = {"commit": "a" * 40, "hosted_workflow": {"owned-run": "1"}}
        names = ["valid", "invalid-type", "invalid-spdx", "invalid-jsf", "malformed-data", "missing-spdx", "missing-jsf",
                 "missing-meta", "tampered-schema", "symlink-schema", "invalid-isolation-setup", "unknown-ref", "unknown-schema",
                 "probe", "unresolved-reference", "valid-bom-snapshot-tamper"]
        receipt = {"schema": "sbom-tool-route-v1", "status": "pass", "commit": "a" * 40, "platform": "Linux",
                   "isolation": "unshare user/map-root-user/network; no fallback", "workflow": {"owned-run": "1"},
                   "cases": [{"name": name, "matched": True} for name in names],
                   "tool": {"archive_sha256": sbom.TOOLS["goneat"]["archive_sha256"]},
                   "runner": {"os_release": {"ID": "ubuntu", "VERSION_ID": "22.04"}}}
        sbom.tool_route_admission(receipt, expected)
        for mutation in ("missing", "failed", "commit", "run", "case", "runner"):
            with self.subTest(mutation=mutation):
                changed = copy.deepcopy(receipt)
                if mutation == "missing":changed["cases"].pop()
                elif mutation == "failed":changed["status"] = "failed"
                elif mutation == "commit":changed["commit"] = "b" * 40
                elif mutation == "run":changed["workflow"] = {"owned-run": "2"}
                elif mutation == "case":changed["cases"][0]["matched"] = False
                else:changed["runner"]["os_release"]["VERSION_ID"] = "24.04"
                with self.assertRaises(EvidenceError):sbom.tool_route_admission(changed, expected)


class RouteFixtureTests(unittest.TestCase):
    def test_named_fetch_fixture_binds_tool_and_keeps_meta_local(self):
        spec = importlib.util.spec_from_file_location("owned_tool_route", sbom.SCRIPTS / "sbom-tool-route.py")
        route = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(route)
        with tempfile.TemporaryDirectory(prefix="cv-fetch-fixture-") as directory:
            root = Path(directory)
            fixture, data = route.denied_fetch_fixture(root, 12345)
            self.assertEqual(schema.regular_bytes(fixture.parent / "draft07.schema.json"),
                             schema.regular_bytes(schema.SCHEMA_ROOT / "draft07.schema.json"))
            self.assertEqual(json.loads(fixture.read_text())["$ref"], "http://127.0.0.1:12345/validator-ref")
            tool = root / "owned-validator"
            tool.write_bytes(b"owned synthetic tool")
            marker = root / "namespace.json"
            args = SimpleNamespace(fixture_child="unresolved-reference", parent_namespace="net:[parent]",
                                   marker=marker, goneat=tool, tool_sha256="0" * 64,
                                   fixture_schema=fixture, fixture_data=data)
            witness = {"platform": "Linux", "parent_network_namespace": "net:[parent]",
                       "child_network_namespace": "net:[child]"}
            with mock.patch.object(route.schema, "namespace_witness", return_value=witness), \
                    mock.patch.object(route.os, "execv") as launch:
                with self.assertRaises(EvidenceError):
                    route.fixture_child(args)
                launch.assert_not_called()
                self.assertFalse(marker.exists())
                args.tool_sha256 = schema.sha(tool.read_bytes())
                route.fixture_child(args)
                launch.assert_called_once()
                self.assertIn("id-strict", launch.call_args.args[1])
                self.assertTrue(json.loads(marker.read_text())["exec_requested"])


class NativeDiscoveryTests(unittest.TestCase):
    def test_selected_default_tools_and_absent_builder_are_distinct(self):
        with tempfile.TemporaryDirectory(prefix="cv-native-discovery-") as directory:
            root = Path(directory)
            tool = root / "owned-tool"
            tool.write_text("owned synthetic executable bytes")
            out = root / "evidence"
            out.mkdir()
            driver = mock.Mock(out=out, receipt={"target": "x86_64-unknown-linux-gnu"})
            calls = []

            def command(stage, argv, horizon):
                calls.append((stage, argv, horizon))
                (out / (stage + ".stderr.log")).write_text("")
                return "owned default tool version 1"

            driver.command.side_effect = command
            metadata = {"packages": [{"id": "cc-id", "name": "cc"}, {"id": "cmake-id", "name": "cmake"}],
                        "target_directory": str(root / "target")}
            with mock.patch.object(build_inputs.shutil, "which", return_value=str(tool)), mock.patch.dict(os.environ, {}, clear=True):
                absent = build_inputs.native_toolchain(driver, [], metadata)
                self.assertEqual(absent["observations"], [])
                self.assertEqual(calls, [])
                events = [{"reason": "compiler-artifact", "package_id": "cc-id"},
                          {"reason": "compiler-artifact", "package_id": "cmake-id"}]
                selected = build_inputs.native_toolchain(driver, events, metadata)
            self.assertEqual(selected["selected_builder_package_ids"], ["cc-id", "cmake-id"])
            self.assertEqual({x["tool"] for x in selected["observations"]}, {"cc", "c++", "cmake"})
            self.assertTrue(all("unconfirmed" in x["selection"] for x in selected["observations"]))
            self.assertEqual(len(calls), 3)
            self.assertTrue(all(x[2] == 10 for x in calls))

    def test_target_host_wrapper_flag_and_producer_selectors_refuse_before_query(self):
        target = "x86_64-unknown-linux-gnu"
        keys = ["CC", "CC_" + target, "CXX_x86_64_unknown_linux_gnu", "HOST_CC", "TARGET_CFLAGS",
                "ARFLAGS", "CRATE_CC_NO_DEFAULTS", "CC_KNOWN_WRAPPER_CUSTOM", "CMAKE_TOOLCHAIN_FILE",
                "CMAKE_TOOLCHAIN_FILE_x86_64_unknown_linux_gnu", "HOST_CMAKE_GENERATOR", "TARGET_CMAKE",
                "AWS_LC_SYS_CC", "AWS_LC_SYS_CFLAGS_x86_64_unknown_linux_gnu", "TARGET_AWS_LC_SYS_CC",
                "AWS_LC_SYS_CMAKE_TOOLCHAIN_FILE"]
        metadata = {"packages": [{"id": "cc-id", "name": "cc"}, {"id": "cmake-id", "name": "cmake"},
                                 {"id": "native-id", "name": "aws-lc-sys"}]}
        events = [{"reason": "compiler-artifact", "package_id": p["id"]} for p in metadata["packages"]]
        for key in keys:
            with self.subTest(key=key), mock.patch.dict(os.environ, {key: ""}, clear=True):
                driver = mock.Mock(receipt={"target": target})
                with self.assertRaisesRegex(EvidenceError, "override"):
                    build_inputs.native_toolchain(driver, events, metadata)
                driver.command.assert_not_called()

    def test_same_normal_owned_cache_is_retained_without_invocation_inference(self):
        with tempfile.TemporaryDirectory(prefix="cv-native-cache-") as directory:
            root = Path(directory)
            out = root / "evidence"
            out.mkdir()
            tool = root / "owned-tool"
            tool.write_text("owned discovery bytes")
            build = root / "target/release/build/owned/out"
            (build / "build").mkdir(parents=True)
            (build / "build/CMakeCache.txt").write_text("CMAKE_C_COMPILER:FILEPATH=" + str(tool) + "\n")
            metadata = {"packages": [{"id": "cmake", "name": "cmake"}, {"id": "producer", "name": "owned-producer"}],
                        "target_directory": str(root / "target")}
            events = [{"reason": "compiler-artifact", "package_id": "cmake"},
                      {"reason": "build-script-executed", "package_id": "producer", "out_dir": str(build)}]
            driver = mock.Mock(out=out, receipt={"target": "x86_64-unknown-linux-gnu"})
            def command(name, *_):
                (out / (name + ".stderr.log")).write_text("")
                return "owned default tool"
            driver.command.side_effect = command
            with mock.patch.object(build_inputs.shutil, "which", return_value=str(tool)), mock.patch.dict(os.environ, {}, clear=True):
                capture = build_inputs.native_toolchain(driver, events, metadata)
            self.assertEqual(capture["normal_build_caches"][0]["package_id"], "producer")
            self.assertEqual(capture["normal_build_caches"][0]["choices"]["CMAKE_C_COMPILER"], str(tool))
            self.assertIn("unconfirmed", capture["selection"])


class ExecObservationTests(unittest.TestCase):
    def exercise(self, image="tool", pgid="match", presence="present", depleted=False, collected=False,
                 platform_name="Linux"):
        with tempfile.TemporaryDirectory(prefix="cv-owned-image-") as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            controller = Controller(source, root / "evidence", "owned-image-test", 30)
            controller.deadline = 120
            record = {"stage": "owned-version"}
            controller.receipt["commands"].append(record)
            child = SimpleNamespace(pid=424242, returncode=0 if collected else None)
            clock, signals, reads = [100.0], [], []

            def durable_first():
                receipt = json.loads(controller.receipt_path.read_text())
                self.assertEqual(receipt["failure"], "owned command exceeded its process horizon")
                self.assertTrue(receipt["commands"][0]["process_horizon_expired"])
                self.assertFalse(receipt["commands"][0]["owned_cleanup_confirmed"])

            def image_stat(path):
                durable_first()
                self.assertEqual(path, "/proc/424242/exe")
                reads.append(path)
                if depleted:
                    clock[0] = 110.5  # The already fixed cleanup deadline is 110.
                if image == "missing":
                    raise FileNotFoundError(errno.ENOENT, "owned synthetic missing image")
                if image == "denied":
                    raise PermissionError(errno.EACCES, "owned synthetic image denial")
                device, inode = {"tool": (1, 2), "wrapper": (3, 4), "other": (5, 6)}[image]
                return SimpleNamespace(st_dev=device, st_ino=inode)

            def group_of(pid):
                durable_first()
                self.assertEqual(pid, child.pid)
                if pgid == "denied":
                    raise PermissionError(errno.EPERM, "owned synthetic PGID denial")
                return child.pid if pgid == "match" else 0

            def signal_owned(pid, sig):
                durable_first()
                self.assertEqual(pid, child.pid)
                signals.append(sig)
                if sig == 0 and presence != "present":
                    raise OSError(errno.ESRCH if presence == "absent" else errno.EPERM,
                                  "owned synthetic presence error")

            def reap(*, timeout):
                self.assertGreater(timeout, 0)
                self.assertLessEqual(clock[0] + timeout, 110)
                child.returncode = -signal.SIGTERM
                return child.returncode

            child.wait = reap
            with mock.patch.object(bounded.platform, "system", return_value=platform_name), \
                    mock.patch.object(bounded.time, "monotonic", side_effect=lambda: clock[0]), \
                    mock.patch.object(bounded.os, "stat", side_effect=image_stat), \
                    mock.patch.object(bounded.os, "getpgid", side_effect=group_of), \
                    mock.patch.object(bounded.os, "killpg", side_effect=signal_owned):
                with self.assertRaisesRegex(EvidenceError, "process horizon"):
                    controller.expired_command(child, record, 99,
                                               {"tool": (1, 2), "wrapper": (3, 4)})
                if depleted:
                    with mock.patch.object(bounded.subprocess, "Popen") as launch:
                        with self.assertRaisesRegex(EvidenceError, "operation horizon"):
                            controller.command("forbidden-late-command", ["synthetic"], 1)
                        launch.assert_not_called()
            result = json.loads(controller.receipt_path.read_text())
            self.assertEqual(result["status"], "failed")
            self.assertFalse(record["owned_cleanup_confirmed"])
            self.assertEqual(record["exec_observation"]["other_live_member"], "unknown")
            self.assertEqual(record["cleanup_deadline"], 110)
            self.assertLessEqual(len(reads), 1)
            self.assertLessEqual(signals.count(0), 1)
            return record, signals

    def test_images_and_unavailable_reads_are_diagnostic_only(self):
        for image, expected in (("tool", "expected-image-observed"), ("wrapper", "wrapper-image-observed"),
                                ("other", "different-image-observed"), ("missing", "unknown"),
                                ("denied", "unknown")):
            with self.subTest(image=image):
                record, signals = self.exercise(image=image)
                self.assertEqual(record["exec_observation"]["image"], expected)
                self.assertEqual(signals, [0, signal.SIGTERM])
                self.assertTrue(record["child_collected"])

    def test_group_match_errors_and_mismatch_never_prove_other_members_absent(self):
        for pgid, presence, expected, probes in (("match", "present", "present", 1),
                                               ("match", "absent", "not-present-at-observation", 1),
                                               ("match", "denied", "unknown", 1),
                                               ("mismatch", "present", "unknown", 0),
                                               ("denied", "present", "unknown", 0)):
            with self.subTest(pgid=pgid, presence=presence):
                record, signals = self.exercise(pgid=pgid, presence=presence)
                self.assertEqual(record["exec_observation"]["group_presence"], expected)
                self.assertEqual(signals.count(0), probes)

    def test_depleted_observation_budget_and_collected_child_do_not_probe(self):
        record, signals = self.exercise(depleted=True)
        self.assertTrue(record["exec_observation"]["budget_depleted"])
        self.assertEqual(record["exec_observation"]["pgid_attempts"], 0)
        self.assertEqual(signals, [])
        self.assertTrue(record["cleanup_horizon_expired"])
        record, signals = self.exercise(collected=True)
        self.assertTrue(record["exec_observation"]["ownership_unavailable"])
        self.assertEqual(record["exec_observation"]["image_attempts"], 0)
        self.assertNotIn(0, signals)

    def test_unsupported_platform_keeps_unknown_without_metadata_reads(self):
        for name in ("Darwin", "unsupported-synthetic"):
            with self.subTest(platform=name):
                record, signals = self.exercise(platform_name=name)
                observation = record["exec_observation"]
                self.assertTrue(observation["unsupported_platform"])
                self.assertEqual(observation["image"], "unknown")
                self.assertEqual(observation["image_attempts"], 0)
                self.assertEqual(observation["pgid_attempts"], 0)
                self.assertNotIn(0, signals)

    def test_normal_exit_and_omitted_option_do_not_observe(self):
        with tempfile.TemporaryDirectory(prefix="cv-owned-normal-") as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            controller = Controller(source, root / "evidence", "owned-normal-test", 30)
            identity = Path(sys.executable).stat()
            images = {"tool": (identity.st_dev, identity.st_ino), "wrapper": (identity.st_dev, identity.st_ino)}
            with mock.patch.object(controller, "observe_expired_image") as observe:
                self.assertEqual(controller.command("normal", [sys.executable, "-c", "print('{}')"], 2,
                                                    expected_images=images).strip(), "{}")
                with self.assertRaisesRegex(EvidenceError, "process horizon"):
                    controller.command("no-option", [sys.executable, "-c", "import time; time.sleep(5)"], 0.1)
                observe.assert_not_called()
            self.assertTrue(all("exec_observation" not in r for r in controller.receipt["commands"]))

    def test_exec_refusal_has_typed_errno_without_schema_admission(self):
        with tempfile.TemporaryDirectory(prefix="cv-owned-exec-refusal-") as directory:
            root = Path(directory)
            tool = root / "nonexecutable-owned-tool"
            tool.write_text("owned synthetic nonexecutable bytes")
            tool.chmod(0o600)
            marker = root / "version-witness.json"
            argv = ["offline_schema.py", "--child", "--goneat", str(tool), "--tool-sha256",
                    schema.sha(tool.read_bytes()), "--parent-namespace", "owned-parent", "--marker", str(marker)]
            witness = {"platform": "synthetic-only", "scope": "not namespace proof"}
            with mock.patch.object(schema.sys, "argv", argv), \
                    mock.patch.object(schema, "namespace_witness", return_value=witness):
                self.assertEqual(schema.child(), 65)
            value = json.loads(marker.read_text())
            self.assertEqual(value["exec_error"], {"error_class": "PermissionError", "errno": errno.EACCES})
            self.assertTrue(value["exec_requested"])
            self.assertNotIn("schema_validation", value)

    @unittest.skipUnless(sys.platform == "linux", "Linux /proc native-image proof requires Linux")
    def test_owned_native_image_and_owned_descendant_stay_failed(self):
        with tempfile.TemporaryDirectory(prefix="cv-owned-native-") as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            native = root / "owned-sleep"
            shutil.copy2(shutil.which("sleep"), native)
            target, wrapper = native.stat(), Path(sys.executable).stat()
            images = {"tool": (target.st_dev, target.st_ino), "wrapper": (wrapper.st_dev, wrapper.st_ino)}
            commands = (([str(native), "5"], "expected-image-observed"),
                        ([sys.executable, "-c", "import subprocess; subprocess.run(['" + str(native) + "','5'])"],
                         "wrapper-image-observed"))
            for index, (argv, expected) in enumerate(commands):
                controller = Controller(source, root / ("evidence-" + str(index)), "owned-native-test", 30)
                with self.assertRaisesRegex(EvidenceError, "process horizon"):
                    controller.command("silent-owned-image", argv, 0.5, expected_images=images)
                record = controller.receipt["commands"][0]
                self.assertEqual(record["exec_observation"]["image"], expected)
                self.assertEqual(record["exec_observation"]["group_presence"], "present")
                self.assertEqual(record["exec_observation"]["other_live_member"], "unknown")
                self.assertFalse(record["owned_cleanup_confirmed"])
                self.assertTrue(record["child_collected"])
                self.assertEqual(controller.receipt["status"], "failed")


if __name__ == "__main__":
    unittest.main()
