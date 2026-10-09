"""Source-aware conservative inventory for three qualified shipping payloads."""

import argparse
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlsplit

sys.dont_write_bytecode = True
from bounded_evidence import EvidenceError
from offline_schema import regular_bytes, sha
from offline_schema import validate as validate_schema
from sbom_evidence import Controller, scanner
import production_build_policy as policy
from production_build_inputs import verify_native_policy, bounded_file, budget_check


PLATFORMS = {"linux-x86_64": "x86_64-unknown-linux-gnu",
             "linux-aarch64": "aarch64-unknown-linux-gnu",
             "macos-aarch64": "aarch64-apple-darwin"}
SCRIPTS = Path(__file__).resolve().parent
TOOLS = json.loads(regular_bytes(SCRIPTS / "sbom-tools.json"))


def scanner_preflight(controller):
    version = json.loads(scanner(controller, "syft-version", ["version", "-o", "json"], TOOLS["syft"]))
    if not isinstance(version, dict) or version.get("version") != TOOLS["syft"]["version"]:
        raise EvidenceError("pinned scanner reports wrong version")
    config = scanner(controller, "syft-config", ["config", "--load"], TOOLS["syft"])
    if not re.search(r"^check-for-app-update:\s*false\s*$", config, re.MULTILINE):
        raise EvidenceError("scanner application update check is not disabled")
    return version, config


def object_file(path):
    value = json.loads(regular_bytes(path))
    if not isinstance(value, dict):
        raise EvidenceError("expected evidence object")
    return value


def properties(values):
    return [{"name": "chanvoy:" + name, "value": str(value)} for name, value in sorted(values.items())]


def ref(*values):
    return "urn:sha256:" + sha(json.dumps(values, sort_keys=True).encode())


def lock_packages(data):
    """Read only reviewed Cargo.lock package identity/checksum scalar fields."""
    result = {}
    text = data.decode()
    if not re.search(r"^version\s*=\s*4\s*$", text, re.MULTILINE):
        raise EvidenceError("unsupported Cargo lock format")
    for block in re.split(r"^\[\[package\]\]\s*$", text, flags=re.MULTILINE)[1:]:
        fields = {}
        for name in ("name", "version", "source", "checksum"):
            matches = re.findall(r"^" + name + r"\s*=\s*(\"[^\n]*\")\s*$", block, re.MULTILINE)
            if len(matches) > 1:
                raise EvidenceError("ambiguous lock package field")
            if matches:
                fields[name] = json.loads(matches[0])
        key = (fields["name"], fields["version"], fields.get("source"))
        if key in result:
            raise EvidenceError("duplicate lock source identity")
        result[key] = fields
    return result


def source_identity(package, locks, commit, workspace, source_root):
    name, version, source = package["name"], package["version"], package.get("source")
    if not re.fullmatch(r"[A-Za-z0-9_.+-]+", name):
        raise EvidenceError("unsafe package name")
    if source is None:
        if package["id"] not in workspace or not Path(package["manifest_path"]).is_relative_to(source_root):
            raise EvidenceError("unassociated path package")
        return {"origin": "workspace-source", "commit": commit, "name": name, "version": version}
    locked = locks.get((name, version, source))
    if not locked:
        raise EvidenceError("compiled source absent from same lock")
    if source.startswith("registry+"):
        if source != "registry+https://github.com/rust-lang/crates.io-index":
            raise EvidenceError("unsupported registry origin")
        checksum = locked.get("checksum", "")
        if not re.fullmatch(r"[0-9a-f]{64}", checksum):
            raise EvidenceError("missing registry source checksum")
        return {"origin": source, "archive_sha256": checksum, "name": name, "version": version}
    if source.startswith("git+"):
        url = urlsplit(source[4:])
        if (url.scheme != "https" or url.hostname != "github.com" or url.username or url.password
                or not re.fullmatch(r"[0-9a-f]{40}", url.fragment)):
            raise EvidenceError("unsupported git source identity")
        return {"origin": source, "resolved_commit": url.fragment, "name": name, "version": version}
    raise EvidenceError("unsupported compiled package origin")


def normal_projection(events, metadata, root):
    selected, units, scripts = set(), {}, {}
    finished = [x for x in events if x.get("reason") == "build-finished"]
    if len(finished) != 1 or finished[0].get("success") is not True:
        raise EvidenceError("normal compilation did not finish successfully")
    for event in events:
        if event.get("reason") not in ("compiler-artifact", "build-script-executed"):
            continue
        identity = event["package_id"]
        selected.add(identity)
        if event["reason"] == "compiler-artifact":
            kind = event["target"]["kind"]
            if event["profile"].get("test") is not False or set(kind) & {"test", "bench", "example"}:
                raise EvidenceError("fixture-only or mixed compilation input")
            units.setdefault(identity, []).append({"kind": sorted(kind), "features": sorted(event["features"])})
        else:
            scripts.setdefault(identity, []).append(event)
    packages = {p["id"]: p for p in metadata["packages"]}
    if len(packages) != len(metadata["packages"]) or not selected <= packages.keys() or root not in selected:
        raise EvidenceError("compiled package association is incomplete")
    nodes = {p["id"]: p for p in metadata["resolve"]["nodes"]}
    if len(nodes) != len(metadata["resolve"]["nodes"]):
        raise EvidenceError("ambiguous normal dependency metadata")
    roles, visited, todo = {}, set(), [(root, "target-candidate")]
    while todo:
        identity, role = todo.pop()
        if identity not in selected:
            continue
        if any("proc-macro" in t["kind"] for t in packages[identity]["targets"]):
            role = "build-input"
        if (identity, role) in visited:
            continue
        visited.add((identity, role))
        roles.setdefault(identity, set()).add(role)
        if identity not in nodes:
            raise EvidenceError("compiled package missing dependency node")
        for dep in nodes[identity]["deps"]:
            for edge in dep["dep_kinds"]:
                if edge["kind"] is None:
                    todo.append((dep["pkg"], role))
                elif edge["kind"] == "build":
                    todo.append((dep["pkg"], "build-input"))
                elif edge["kind"] != "dev":
                    raise EvidenceError("unsupported dependency context")
    if set(roles) != selected:
        raise EvidenceError("unreachable fixture/inactive compiled package")
    for identity in scripts:
        roles[identity].add("build-input")
    return packages, roles, units, scripts


def bind_platform(platform, folder, payload, expected, check=lambda: None):
    qualification = object_file(folder / "qualification.json")
    captured = object_file(folder / "normal-build-inputs.json")
    if (qualification.get("status") != "pass" or qualification.get("mode") != "shipping"
            or captured.get("schema") != "normal-build-inputs-v2"
            or captured.get("before_fixture_compilation") is not True or captured.get("mode") != "shipping"):
        raise EvidenceError("missing successful shipping qualification/input receipt")
    for field in ("commit", "tree", "tag", "tag_object", "lock_sha256"):
        if qualification.get(field) != expected[field] or captured.get(field) != expected[field]:
            raise EvidenceError("stale shipping source association: " + field)
    if qualification.get("expected_commit") != expected["commit"]:
        raise EvidenceError("candidate source substituted for shipping source")
    if (qualification.get("platform") != platform or captured.get("platform") != platform
            or qualification.get("target") != PLATFORMS[platform] or captured.get("target") != PLATFORMS[platform]):
        raise EvidenceError("wrong shipping native target")
    for value in (qualification, captured):
        if value.get("workflow") != expected["workflow"]:
            raise EvidenceError("mixed shipping workflow/run/attempt")
        if not value.get("rustc", "").startswith("rustc 1.89.0 "):
            raise EvidenceError("unsupported shipping compiler")
    asset = "chanvoy-v" + expected["version"] + "-" + platform
    payload_observation = bounded_file(payload, payload.parent, policy.EXECUTABLE_LIMIT, check)["observation"]
    payload_hash = payload_observation["sha256"]
    if (payload.name != asset or qualification.get("payload") != asset or captured.get("payload") != asset
            or qualification.get("payload_sha256") != payload_hash or captured.get("payload_sha256") != payload_hash
            or bounded_file(folder / "payload" / asset, folder, policy.EXECUTABLE_LIMIT, check)["observation"]
                != payload_observation):
        raise EvidenceError("shipping payload association mismatch")
    if (qualification.get("normal_build_inputs") != "normal-build-inputs.json"
            or qualification.get("normal_build_inputs_sha256") != sha(regular_bytes(folder / "normal-build-inputs.json"))
            or qualification.get("qualification_driver_sha256") != sha(regular_bytes(SCRIPTS / "qualify-production-binary.py"))):
        raise EvidenceError("input/qualification adapter association mismatch")
    inputs = {"normal-build.jsonl": captured["normal_build_messages_sha256"],
              "metadata.log": captured["metadata_sha256"], "normal-Cargo.lock": captured["lock_sha256"]}
    if (qualification.get("normal_build_messages_sha256") != inputs["normal-build.jsonl"]
            or qualification.get("metadata_messages_sha256") != inputs["metadata.log"]):
        raise EvidenceError("normal compiler/metadata evidence mismatch")
    native = captured["native"]
    required = {"native-tool-version.log", "native-tool-version.stderr.log",
                "native-dynamic.log", "native-dynamic.stderr.log"}
    if platform.startswith("linux-"):
        required |= {"native-program-headers.log", "native-program-headers.stderr.log"}
    toolchain = native.get("build_toolchain")
    if not isinstance(toolchain, dict):
        raise EvidenceError("missing native build-tool discovery receipt")
    tools = [x["tool"] for x in toolchain.get("observations", [])]
    if len(set(tools)) != len(tools) or not set(tools) <= {"cc", "c++", "cmake"}:
        raise EvidenceError("unexpected native build-tool discovery")
    for tool in tools:
        required |= {"native-build-" + tool + "-version.log", "native-build-" + tool + "-version.stderr.log"}
    if (native.get("capture_status") != "success" or not native.get("tool_version")
            or native.get("tool") != ("otool" if platform == "macos-aarch64" else "readelf")
            or not re.fullmatch(r"[0-9a-f]{64}", native.get("tool_sha256", ""))
            or set(native.get("logs_sha256", {})) != required):
        raise EvidenceError("missing required native metadata receipt")
    inputs.update(native["logs_sha256"])
    if any(sha(regular_bytes(folder / name)) != digest for name, digest in inputs.items()):
        raise EvidenceError("shipping input evidence changed")
    events = [json.loads(line) for line in regular_bytes(folder / "normal-build.jsonl").splitlines() if line.strip()]
    metadata = object_file(folder / "metadata.log")
    root = qualification["package_id"]
    if captured.get("package_id") != root:
        raise EvidenceError("root package association mismatch")
    normal_roots = [x for x in events if x.get("reason") == "compiler-artifact" and x.get("package_id") == root
                    and x["target"].get("kind") == ["bin"] and x["target"].get("name") == "chanvoy"]
    if (len(normal_roots) != 1 or normal_roots[0]["profile"].get("test") is not False
            or normal_roots[0]["profile"].get("opt_level") not in ("1", "2", "3", "s", "z")
            or normal_roots[0]["profile"].get("debug_assertions") or normal_roots[0].get("features") != []):
        raise EvidenceError("not the approved optimized root build")
    expected_policy = {**expected, "mode": "shipping", "platform": platform, "target": PLATFORMS[platform]}
    producer, producer_hash, manifest_hash = verify_native_policy(
        folder, expected_policy, SCRIPTS.parent, events, metadata, payload, check)
    policy.bind_payload(producer, bounded_file(folder / "payload" / asset, folder,
                                             policy.EXECUTABLE_LIMIT, check)["observation"])
    for value in (qualification, captured):
        if (value.get("native_build_policy") != "native-build-policy.json"
                or value.get("native_build_policy_sha256") != producer_hash
                or value.get("native_snapshot_manifest_sha256") != manifest_hash
                or value.get("normal_executable") != producer["normal_executable"]):
            raise EvidenceError("shipping native producer association mismatch")
    aws_rows = [r for r in native["build_scripts"] if r["package_id"] == producer["native_package"]["package_id"]]
    if len(aws_rows) != len(producer["native_events"]):
        raise EvidenceError("missing native build-script producer association")
    for row, observed in zip(aws_rows, producer["native_events"]):
        archives = [{"name": a.get("name"), "sha256": a.get("sha256")} for a in row.get("archives", [])]
        if (archives != [{"name": policy.ARCHIVE, "sha256": observed["archive"]["sha256"]}]
                or row.get("unresolved_static_inputs")):
            raise EvidenceError("native producer archive/input association mismatch")
    return captured, events, metadata, payload_hash


def add_license(component, expression):
    if expression:
        component["licenses"] = [{"expression": expression}]
    return component


def inventory(platform, captured, events, metadata, payload_hash, expected, scan):
    root = captured["package_id"]
    packages, roles, units, scripts = normal_projection(events, metadata, root)
    builders = {p: packages[p]["name"] for p in roles if packages[p]["name"] in ("cc", "cmake")}
    discovery = captured["native"]["build_toolchain"]
    expected_tools = ({"cc", "c++"} | ({"cmake"} if "cmake" in builders.values() else set())) if builders else set()
    if (discovery.get("selected_builder_package_ids") != sorted(builders)
            or {x["tool"] for x in discovery.get("observations", [])} != expected_tools):
        raise EvidenceError("default-tool discovery does not match positive normal-builder selection")
    locks = lock_packages(regular_bytes(expected["evidence_folders"][platform] / "normal-Cargo.lock"))
    artifact_ref = ref("payload", platform, payload_hash)
    public_discovery = [{"tool": x["tool"], "sha256": x["sha256"], "version": x["version"].splitlines()[0],
                         "selection": x["selection"], "evidence_class": x["evidence_class"]}
                        for x in discovery["observations"]]
    components = [add_license({"type": "application", "name": "chanvoy-" + platform,
                              "version": expected["version"], "bom-ref": artifact_ref,
                              "hashes": [{"alg": "SHA-256", "content": payload_hash}],
                              "properties": properties({"evidence-class": "artifact-observation",
                                                        "target": PLATFORMS[platform], "source-commit": expected["commit"],
                                                        "source-tree": expected["tree"],
                                                        "native-build-tool-discovery": json.dumps(public_discovery, sort_keys=True),
                                                        "native-backend-policy": "bundled-source-v1",
                                                        "native-build-policy-sha256": captured["native_build_policy_sha256"],
                                                        "native-snapshot-manifest-sha256": captured["native_snapshot_manifest_sha256"]})}, packages[root].get("license"))]
    if packages[root]["version"] != expected["version"]:
        raise EvidenceError("root version differs from shipping version")
    native_by_package = {}
    expected_scripts = [json.dumps({"package_id": event["package_id"],
                                    "linked_libs": event.get("linked_libs", []),
                                    "linked_paths": event.get("linked_paths", []),
                                    "cfgs": event.get("cfgs", [])}, sort_keys=True)
                        for event in events if event.get("reason") == "build-script-executed"]
    actual_scripts = [json.dumps({key: row[key] for key in ("package_id", "linked_libs", "linked_paths", "cfgs")},
                                sort_keys=True) for row in captured["native"]["build_scripts"]]
    if sorted(expected_scripts) != sorted(actual_scripts):
        raise EvidenceError("missing or changed native build-script receipt")
    for row in captured["native"]["build_scripts"]:
        if row["package_id"] not in scripts:
            raise EvidenceError("unassociated native build input")
        native_by_package.setdefault(row["package_id"], []).append(row)
    for identity in sorted(roles):
        package = packages[identity]
        source = source_identity(package, locks, expected["commit"], metadata["workspace_members"],
                                 Path(captured["source_root"]))
        classes = []
        if "target-candidate" in roles[identity]:
            classes.append("conservative-target-candidate")
        if "build-input" in roles[identity]:
            classes.append("build-input")
        normal_units = sorted(units.get(identity, []), key=lambda x: json.dumps(x, sort_keys=True))
        native = [{"linked_libs": row["linked_libs"],
                   "archives": sorted([{"name": x["name"], "sha256": x["sha256"]} for x in row["archives"]],
                                      key=lambda x: (x["name"], x["sha256"])),
                   "unresolved_static_inputs": row.get("unresolved_static_inputs", []),
                   "upstream_native_version": "not established"} for row in native_by_package.get(identity, [])]
        component = {"type": "library", "name": package["name"], "version": package["version"],
                     "bom-ref": ref(source, platform, sorted(roles[identity])),
                     "properties": properties({"artifact-ref": artifact_ref, "evidence-class": ";".join(classes),
                                               "roles": ";".join(sorted(roles[identity])),
                                               "source-identity": json.dumps(source, sort_keys=True),
                                               "compiler-feature-records": json.dumps(normal_units, sort_keys=True),
                                               "feature-context": "conservative per-context association; no flattened linkage claim",
                                               "build-context-ambiguous": len(roles[identity]) > 1 or len(normal_units) > 1,
                                               "native-build-inputs": json.dumps(sorted(native, key=lambda x: json.dumps(x, sort_keys=True)), sort_keys=True),
                                               "declared-license": package.get("license") or "unknown"})}
        if identity == root:
            # The root package is associated with the known payload already;
            # merge its build roles without inventing another application.
            values = {x["name"]: x["value"] for x in [*components[0]["properties"], *component["properties"]]}
            values["chanvoy:evidence-class"] = "artifact-observation;" + ";".join(classes)
            components[0]["properties"] = [{"name": k, "value": v} for k, v in sorted(values.items())]
        else:
            components.append(add_license(component, package.get("license")))
    descriptor = scan["descriptor"]
    selection = descriptor["configuration"]["catalogers"]
    observed_hashes = {x["value"] for x in scan["source"]["metadata"]["digests"] if x["algorithm"] == "sha256"}
    if (descriptor.get("name") != "syft" or descriptor.get("version") != TOOLS["syft"]["version"]
            or selection.get("requested") != {"default": TOOLS["syft"]["requested_default"]}
            or sorted(selection.get("used", [])) != sorted(TOOLS["syft"]["catalogers"])
            or scan["source"].get("type") != "file" or scan["source"].get("name") != captured["payload"]
            or observed_hashes != {payload_hash}):
        raise EvidenceError("scanner identity/config/source association mismatch")
    seen = set()
    for observation in scan["artifacts"]:
        identity = observation["id"]
        if identity in seen or observation.get("foundBy") not in selection["used"]:
            raise EvidenceError("ambiguous scanner observation")
        seen.add(identity)
        # Name/version alone cannot join a scanner observation to a Cargo source.
        # Preserve it separately with its cataloger and exact payload association.
        component = {"type": "library", "name": observation["name"],
                     "bom-ref": ref("syft-observation", platform, payload_hash, identity),
                     "properties": properties({"artifact-ref": artifact_ref, "evidence-class": "artifact-observation",
                                               "cataloger": observation["foundBy"], "source-identity": "not established"})}
        if observation.get("version"):
            component["version"] = observation["version"]
        components.append(component)
    for requirement in captured["native"]["external_requirements"]:
        if requirement.startswith("/") and not requirement.startswith(("/usr/lib/", "/System/Library/", "/lib/", "/lib64/")):
            requirement = "unresolved private install-name; raw evidence retained"
        components.append({"type": "library", "name": requirement,
                           "bom-ref": ref("external-requirement", platform, payload_hash, requirement),
                           "properties": properties({"artifact-ref": artifact_ref, "evidence-class": "artifact-observation",
                                                     "role": "external dynamic-load requirement; not bundled host version"})})
    return components


def assemble(evidence, binaries, scans, expected, check=lambda: None):
    if set(evidence) != set(PLATFORMS) or set(scans) != set(PLATFORMS):
        raise EvidenceError("missing or duplicate shipping platform")
    expected_assets = {"chanvoy-v" + expected["version"] + "-" + p for p in PLATFORMS}
    if {p.name for p in binaries.iterdir()} != expected_assets:
        raise EvidenceError("unexpected canonical binary inventory")
    expected = {**expected, "evidence_folders": evidence}
    components = []
    for platform in sorted(PLATFORMS):
        payload = binaries / ("chanvoy-v" + expected["version"] + "-" + platform)
        captured, events, metadata, digest = bind_platform(platform, evidence[platform], payload, expected, check)
        components.extend(inventory(platform, captured, events, metadata, digest, expected, scans[platform]))
    components.sort(key=lambda x: x["bom-ref"])
    if len({x["bom-ref"] for x in components}) != len(components):
        raise EvidenceError("duplicate SBOM component identity")
    value = {"bomFormat": "CycloneDX", "specVersion": "1.6", "version": 1, "components": components,
             "metadata": {"properties": properties({"inventory-claim": "artifact-associated conservative build inventory; not linkage completeness",
                                                     "static-coverage": "unknown; an empty binary scan is not an absence proof",
                                                     "generator-source-commit": expected["commit"],
                                                     "generator-sha256": sha(regular_bytes(Path(__file__)))})}}
    text = json.dumps(value, sort_keys=True)
    if re.search(r"/Users/|/home/|/private/|/tmp/|[A-Za-z]:\\\\", text):
        raise EvidenceError("private path in public inventory")
    return value


def tool_route_admission(receipt, expected):
    names = {"valid", "invalid-type", "invalid-spdx", "invalid-jsf", "malformed-data", "missing-spdx",
             "missing-jsf", "missing-meta", "tampered-schema", "symlink-schema", "invalid-isolation-setup",
             "unknown-ref", "unknown-schema", "probe", "unresolved-reference", "valid-bom-snapshot-tamper"}
    if (receipt.get("schema") != "sbom-tool-route-v1" or receipt.get("status") != "pass"
            or receipt.get("commit") != expected["commit"] or receipt.get("platform") != "Linux"
            or receipt.get("isolation") != "unshare user/map-root-user/network; no fallback"
            or receipt.get("workflow") != expected["hosted_workflow"]
            or len(receipt.get("cases", [])) != len(names)
            or {x.get("name") for x in receipt["cases"]} != names
            or any(x.get("matched") is not True for x in receipt["cases"])
            or receipt.get("tool", {}).get("archive_sha256") != TOOLS["goneat"]["archive_sha256"]
            or receipt.get("runner", {}).get("os_release", {}).get("ID") != "ubuntu"
            or receipt.get("runner", {}).get("os_release", {}).get("VERSION_ID") != "22.04"):
        raise EvidenceError("required same-source/run Ubuntu offline-tool-route proof is unresolved")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", required=True, type=Path)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--tool-route", required=True, type=Path)
    parser.add_argument("--tool-receipt", required=True, type=Path)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--tag-object", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    controller = Controller(SCRIPTS.parent, args.output, "shipping-sbom-generation-v1", 720)
    try:
        if (not re.fullmatch(r"[0-9a-f]{40}", args.expected_commit)
                or not re.fullmatch(r"[0-9a-f]{40}", args.tag_object)
                or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?", args.version)
                or args.tag != "v" + args.version):
            raise EvidenceError("invalid verified shipping identity")
        head = controller.command("head", ["git", "rev-parse", "HEAD"], 10).strip()
        tree = controller.command("tree", ["git", "rev-parse", "HEAD^{tree}"], 10).strip()
        if head != args.expected_commit or controller.command("clean", ["git", "status", "--porcelain=v1"], 10).strip():
            raise EvidenceError("generator checkout/source boundary mismatch")
        hosted = {k: os.environ.get(k) for k in ("GITHUB_WORKFLOW_REF", "GITHUB_WORKFLOW_SHA", "GITHUB_SHA",
                                               "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT")}
        if not all(hosted.values()):
            raise EvidenceError("missing hosted workflow identity")
        expected = {"version": args.version, "commit": head, "tree": tree, "tag": args.tag,
                    "tag_object": args.tag_object, "lock_sha256": sha(regular_bytes(SCRIPTS.parent / "Cargo.lock")),
                    "hosted_workflow": hosted,
                    "workflow": dict(zip(("ref", "sha", "event_sha", "run_id", "run_attempt"), hosted.values()))}
        route = object_file(args.tool_route)
        tool_route_admission(route, expected)
        setup = object_file(args.tool_receipt)
        tool = setup["tool"]
        goneat = Path(tool["binary"])
        if (setup.get("status") != "pass" or tool.get("archive_sha256") != TOOLS["goneat"]["archive_sha256"]
                or sha(regular_bytes(goneat)) != tool.get("binary_sha256")):
            raise EvidenceError("release validator setup/pin mismatch")
        evidence = {p: args.evidence / p for p in PLATFORMS}
        if {x.name for x in args.evidence.iterdir()} != set(PLATFORMS):
            raise EvidenceError("unexpected shipping companion inventory")
        expected_assets = {"chanvoy-v" + args.version + "-" + p for p in PLATFORMS}
        if {p.name for p in args.binaries.iterdir()} != expected_assets:
            raise EvidenceError("unexpected canonical binary inventory")
        # Bind every native input before any artifact scan is admitted.
        for p in PLATFORMS:
            bind_platform(p, evidence[p], args.binaries / ("chanvoy-v" + args.version + "-" + p), expected,
                          lambda: budget_check(controller))
        version, config = scanner_preflight(controller)
        scans = {}
        for p in sorted(PLATFORMS):
            payload = args.binaries / ("chanvoy-v" + args.version + "-" + p)
            scans[p] = json.loads(scanner(controller, "syft-" + p,
                                          ["file:/payload/" + payload.name, "-o", "syft-json"], TOOLS["syft"], payload))
        value = assemble(evidence, args.binaries, scans, expected, lambda: budget_check(controller))
        candidate = controller.out / "candidate-bom.json"
        candidate.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        validated = validate_schema(controller, candidate, goneat)
        if controller.command("final-clean", ["git", "status", "--porcelain=v1"], 10).strip():
            raise EvidenceError("generator source changed")
        asset = controller.out / ("sbom-" + args.version + ".cdx.json")
        with asset.open("xb") as output:
            output.write(validated)
        controller.receipt.update(status="pass", source=expected, sbom=asset.name, sbom_sha256=sha(validated),
                                  tool_route_sha256=sha(regular_bytes(args.tool_route)), scanner=version,
                                  scanner_config_sha256=sha(config.encode()))
        controller.save()
    except (EvidenceError, OSError, ValueError, KeyError, TypeError) as error:
        controller.fail(str(error))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
