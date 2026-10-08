#!/usr/bin/env python3
"""Disposable schema/namespace proof; never builds or scans application code."""

import argparse
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import sys
import threading
from urllib.request import urlopen

sys.dont_write_bytecode = True
from bounded_evidence import EvidenceError, json_write
import offline_schema as schema
from sbom_evidence import Controller


ROOT = Path(__file__).resolve().parent.parent


def fixture_child(args):
    # Only this named synthetic fixture bypasses closure to exercise a failed
    # validator fetch. Production validation has no bypass parameter.
    witness = schema.namespace_witness(args.parent_namespace)
    if args.fixture_child == "probe":
        json_write(args.marker, witness)
        try:
            with socket.create_connection(("127.0.0.1", args.port), timeout=2):
                print(json.dumps({"connected": True, **witness}))
        except OSError as error:
            print(json.dumps({"connected": False, "errno": error.errno, **witness}))
        return 0
    if schema.sha(schema.regular_bytes(args.goneat)) != args.tool_sha256:
        raise EvidenceError("owned fixture validator changed")
    json_write(args.marker, {**witness, "exec_requested": True, "tool_sha256": args.tool_sha256})
    os.execv(str(args.goneat), [str(args.goneat), "schema", "validate-data", "--schema-file", str(args.fixture_schema),
                              "--ref-dir", str(args.fixture_schema.parent), "--schema-resolution", "id-strict",
                              "--data", str(args.fixture_data), "--format", "json"])


def denied_fetch_fixture(case_root, port):
    """Keep the meta-reference local so this named negative targets the canary."""
    closure = case_root / "owned-closure"
    closure.mkdir(mode=0o700)
    shutil.copyfile(schema.SCHEMA_ROOT / "draft07.schema.json", closure / "draft07.schema.json")
    fixture = closure / "owned-schema.json"
    fixture.write_text(json.dumps({"$schema": "http://json-schema.org/draft-07/schema#",
                                   "$id": "https://synthetic.invalid/root.json",
                                   "$ref": "http://127.0.0.1:" + str(port) + "/validator-ref"}))
    data = case_root / "owned-data.json"
    data.write_text("{}")
    return fixture, data


def arm_snapshot_hash_negative(controller):
    """Named harness seam: change valid owned data only after validator success."""
    original = controller.command

    def command(stage, argv, *args, **kwargs):
        result = original(stage, argv, *args, **kwargs)
        if stage == "schema-validation":
            snapshot = Path(argv[argv.index("--data") + 1])
            before = schema.regular_bytes(snapshot)
            value = json.loads(before)
            if value.get("version") != 1:
                raise EvidenceError("owned snapshot negative requires version 1")
            value["version"] = 2  # Both versions are valid CycloneDX data.
            after = (json.dumps(value, sort_keys=True) + "\n").encode()
            snapshot.chmod(0o644)
            try:
                snapshot.write_bytes(after)
            finally:
                snapshot.chmod(0o444)
            controller.receipt["owned_snapshot_hash_negative"] = {
                "timing": "after successful isolated schema-validation command",
                "before_sha256": schema.sha(before), "after_sha256": schema.sha(after),
                "before_version": 1, "after_version": 2}
            controller.save()
        return result

    controller.command = command


def matrix(root, goneat):
    cases = []
    base = {"bomFormat": "CycloneDX", "specVersion": "1.6", "version": 1,
            "components": [{"type": "application", "name": "owned-synthetic",
                            "licenses": [{"license": {"id": "MIT"}}]}],
            "signature": {"algorithm": "HS256", "value": "owned-synthetic-signature"}}
    changes = [("valid", None, True), ("invalid-type", "type", False),
               ("invalid-spdx", "spdx", False), ("invalid-jsf", "jsf", False),
               ("malformed-data", "malformed", False), ("missing-spdx", "spdx.schema.json", False),
               ("missing-jsf", "jsf-0.82.schema.json", False), ("missing-meta", "draft07.schema.json", False),
               ("tampered-schema", "tamper", False), ("symlink-schema", "symlink", False),
               ("invalid-isolation-setup", "setup", False),
               ("valid-bom-snapshot-tamper", "snapshot", False)]
    for name, change, success in changes:
        case_root = root / name
        case_root.mkdir(mode=0o700)
        data = case_root / "owned-bom.json"
        value = copy.deepcopy(base)
        if change == "type":value["components"][0]["type"] = "invalid-owned-type"
        elif change == "spdx":value["components"][0]["licenses"][0]["license"]["id"] = "NOT-AN-SPDX-LICENSE"
        elif change == "jsf":value["signature"]["value"] = 42
        data.write_text("{" if change == "malformed" else json.dumps(value))
        closure = case_root / "closure"
        closure.mkdir()
        for filename in schema.SCHEMAS:
            shutil.copyfile(schema.SCHEMA_ROOT / filename, closure / filename)
        if change in schema.SCHEMAS:(closure / change).unlink()
        elif change == "tamper":
            path = closure / "spdx.schema.json"
            changed = json.loads(path.read_text())
            changed["title"] = "Owned ID-preserving schema hash negative"
            path.write_text(json.dumps(changed))
        elif change == "symlink":
            target = closure / "spdx.schema.json"
            target.unlink()
            target.symlink_to(schema.SCHEMA_ROOT / target.name)
        controller = Controller(ROOT, case_root / "evidence", "sbom-tool-route-case-v1", 30)
        if change == "snapshot":
            arm_snapshot_hash_negative(controller)
        original_prefix = schema.isolation_prefix
        if change == "setup":
            schema.isolation_prefix = lambda: ["/usr/bin/unshare", "--owned-invalid-option"]
        try:
            schema.validate(controller, data, goneat, closure)
            actual = True
        except (EvidenceError, OSError, ValueError, KeyError, TypeError) as error:
            actual = False
            controller.fail(str(error))
        finally:
            schema.isolation_prefix = original_prefix
        marker = controller.out / "validator-child.json"
        launched = marker.exists()
        preflight_negative = change in (*schema.SCHEMAS, "tamper", "symlink", "malformed", "setup")
        outcome = actual == success and (not preflight_negative or not launched)
        if change == "snapshot":
            commands = controller.receipt["commands"]
            mutation = controller.receipt.get("owned_snapshot_hash_negative", {})
            outcome = (outcome and launched and commands[-1].get("exit") == 0
                       and not commands[-1].get("process_horizon_expired")
                       and mutation.get("before_version") == 1 and mutation.get("after_version") == 2
                       and mutation.get("before_sha256") != mutation.get("after_sha256")
                       and "schema_validation" not in controller.receipt)
        elif not success and not preflight_negative:
            commands = controller.receipt["commands"]
            outcome = outcome and launched and commands[-1].get("exit") == 1 and not commands[-1].get("process_horizon_expired")
        cases.append({"name": name, "expected_success": success, "matched": outcome, "validator_marker": launched})
    for keyword in ("$ref", "$schema"):
        docs = {k: json.loads(v) for k, v in schema.preflight()[0].items()}
        docs["bom-1.6.schema.json"][keyword] = "http://owned.invalid/missing"
        try:
            schema.reference_closure(docs)
            matched = False
        except EvidenceError:
            matched = True
        cases.append({"name": "unknown-" + keyword[1:], "matched": matched, "validator_marker": False})
    return cases


def network_proof(root, goneat):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            self.send_response(200 if self.path.startswith("/health") else 503)
            self.end_headers()
            self.wfile.write(b"owned alive")

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    results = []
    try:
        with urlopen("http://127.0.0.1:" + str(port) + "/health-before", timeout=2) as response:
            if response.status != 200:
                raise EvidenceError("owned canary is not alive")
        for name in ("probe", "unresolved-reference"):
            case_root = root / name
            case_root.mkdir(mode=0o700)
            controller = Controller(ROOT, case_root / "evidence", "sbom-network-route-case-v1", 30)
            marker = controller.out / "namespace.json"
            argv = [*schema.isolation_prefix(), sys.executable, "-B", str(Path(__file__).resolve()),
                    "--fixture-child", name, "--parent-namespace", schema.parent_namespace(),
                    "--marker", str(marker), "--port", str(port)]
            if name == "unresolved-reference":
                fixture, data = denied_fetch_fixture(case_root, port)
                argv += ["--goneat", str(goneat), "--tool-sha256", schema.sha(schema.regular_bytes(goneat)),
                         "--fixture-schema", str(fixture), "--fixture-data", str(data)]
            try:
                text = controller.command(name, argv, 20)
                matched = name == "probe" and json.loads(text).get("connected") is False
            except EvidenceError:
                last = controller.receipt["commands"][-1]
                stderr = (controller.out / (name + ".stderr.log")).read_text()
                matched = (name == "unresolved-reference" and last.get("exit") == 1
                           and not last.get("process_horizon_expired") and marker.exists()
                           and "127.0.0.1" in stderr)
            if marker.exists():
                witness = json.loads(marker.read_text())
                matched = matched and witness.get("platform") == "Linux" and (
                    witness.get("parent_network_namespace") != witness.get("child_network_namespace"))
            else:
                matched = False
            results.append({"name": name, "matched": matched})
        with urlopen("http://127.0.0.1:" + str(port) + "/health-after", timeout=2) as response:
            if response.status != 200:
                raise EvidenceError("owned canary lost liveness")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    json_write(root / "canary.json", {"requests": requests, "validator_fetches": requests.count("/validator-ref")})
    for value in results:
        value["matched"] = value["matched"] and requests == ["/health-before", "/health-after"]
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture-child", choices=("probe", "unresolved-reference"))
    parser.add_argument("--parent-namespace")
    parser.add_argument("--marker", type=Path)
    parser.add_argument("--port", type=int)
    parser.add_argument("--fixture-schema", type=Path)
    parser.add_argument("--fixture-data", type=Path)
    parser.add_argument("--tool-sha256")
    parser.add_argument("--goneat", type=Path)
    parser.add_argument("--tool-receipt", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--expected-commit")
    args = parser.parse_args()
    if args.fixture_child:
        return fixture_child(args)
    if platform.system() != "Linux" or not all((args.output, args.tool_receipt, args.expected_commit)):
        parser.error("Offline tool route requires Linux and exact source/tool/output inputs")
    controller = Controller(ROOT, args.output, "sbom-tool-route-v1", 540)
    try:
        head = controller.command("head", ["git", "rev-parse", "HEAD"], 10).strip()
        if head != args.expected_commit or controller.command("clean", ["git", "status", "--porcelain=v1"], 10).strip():
            raise EvidenceError("Offline tool route source boundary mismatch")
        setup = json.loads(schema.regular_bytes(args.tool_receipt))
        tool = setup["tool"]
        pin = json.loads(schema.regular_bytes(ROOT / "scripts/sbom-tools.json"))["goneat"]
        goneat = Path(tool["binary"])
        if (setup.get("status") != "pass" or tool["archive_sha256"] != pin["archive_sha256"]
                or schema.sha(schema.regular_bytes(goneat)) != tool["binary_sha256"]):
            raise EvidenceError("Offline tool route validator pin mismatch")
        cases = matrix(controller.out, goneat) + network_proof(controller.out, goneat)
        controller.receipt.update(commit=head, platform="Linux", cases=cases,
                                  tool=tool, isolation="unshare user/map-root-user/network; no fallback",
                                  runner={"platform": platform.platform(), "kernel": platform.release(),
                                          "os_release": dict(line.split("=", 1) for line in Path("/etc/os-release").read_text().splitlines()
                                                             if "=" in line and not line.startswith("#"))},
                                  workflow={k: os.environ.get(k) for k in ("GITHUB_WORKFLOW_REF", "GITHUB_WORKFLOW_SHA",
                                                                       "GITHUB_SHA", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT")})
        controller.receipt["runner"]["os_release"] = {
            k: v.strip('"') for k, v in controller.receipt["runner"]["os_release"].items()}
        if not all(x["matched"] for x in cases):
            raise EvidenceError("Offline tool route incomplete or failed synthetic route")
        if controller.command("final-clean", ["git", "status", "--porcelain=v1"], 10).strip():
            raise EvidenceError("Offline tool route source changed")
        controller.receipt["status"] = "pass"
        controller.save()
    except (EvidenceError, OSError, ValueError, KeyError, TypeError) as error:
        controller.fail(str(error))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
