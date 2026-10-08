"""Fixed CycloneDX closure and immutable, network-isolated validation."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import stat
import sys
from urllib.parse import unquote, urldefrag, urljoin

sys.dont_write_bytecode = True
from bounded_evidence import EvidenceError, json_write


SCHEMA_ROOT = Path(__file__).resolve().parent.parent / "schemas/cyclonedx-1.6"
SOURCE_COMMIT = "2aea6eacad7dcb7d93172a427e0d37a2cfcf590d"
SCHEMAS = {
    "bom-1.6.schema.json": ("cdd5eb70e4359ed9efd67effc91aaf1b28d5c606ce1b81e194755631d7bb17c1",
                            "http://cyclonedx.org/schema/bom-1.6.schema.json"),
    "spdx.schema.json": ("54a6288292bc6c90b0d3952f5f939f17436fa76704ffe68a46e5b78539c7cc1b",
                         "http://cyclonedx.org/schema/spdx.schema.json"),
    "jsf-0.82.schema.json": ("8bae002c25e723db7ee1f26afde680ae1a2b1a8f6b4b4b0fd65dc3becb090aae",
                           "http://cyclonedx.org/schema/jsf-0.82.schema.json"),
    "draft07.schema.json": ("43eb3a96aee6151fed730fa450050303bf9b04e0f369272ad85cd09299931f88",
                           "http://json-schema.org/draft-07/schema#"),
}


def regular_bytes(path):
    path = Path(path)
    if not stat.S_ISREG(path.lstat().st_mode):
        raise EvidenceError("schema/data input must be a non-symlink regular file")
    return path.read_bytes()


def sha(data):
    return hashlib.sha256(data).hexdigest()


def reference_closure(documents):
    """Check this fixed closure, not arbitrary schemas or remote retrieval."""
    by_id = {}
    for document in documents.values():
        identity = urldefrag(document["$id"])[0]
        if identity in by_id:
            raise EvidenceError("duplicate schema identity")
        by_id[identity] = document
    count = 0

    def walk(node, base):
        nonlocal count
        if isinstance(node, list):
            for child in node:
                walk(child, base)
        elif isinstance(node, dict):
            if isinstance(node.get("$id"), str):
                base = urljoin(base, node["$id"])
            for key, value in node.items():
                if key in ("$ref", "$schema") and isinstance(value, str):
                    count += 1
                    identity, fragment = urldefrag(urljoin(base, value))
                    if identity not in by_id:
                        raise EvidenceError("schema reference outside the fixed closure")
                    target = by_id[identity]
                    if fragment:
                        if not fragment.startswith("/"):
                            raise EvidenceError("unsupported schema fragment")
                        try:
                            for part in unquote(fragment)[1:].split("/"):
                                part = part.replace("~1", "/").replace("~0", "~")
                                target = target[int(part)] if isinstance(target, list) else target[part]
                        except (KeyError, IndexError, ValueError, TypeError) as error:
                            raise EvidenceError("unresolved schema fragment") from error
                walk(value, base)
    for document in documents.values():
        walk(document, document["$id"])
    return count


def preflight(directory=SCHEMA_ROOT):
    # This trusted manifest is a reviewed source input, never regenerated from
    # the received directory. Hard-coded identities/hashes also bind its shape.
    manifest = json.loads(regular_bytes(SCHEMA_ROOT / "manifest.json"))
    if (manifest.get("source_commit") != SOURCE_COMMIT
            or manifest.get("source_repository") != "https://github.com/fulmenhq/goneat"
            or set(manifest.get("files", {})) != set(SCHEMAS)):
        raise EvidenceError("unexpected schema manifest")
    documents, data = {}, {}
    for name, (expected, identity) in SCHEMAS.items():
        entry = manifest["files"][name]
        if entry.get("sha256") != expected or entry.get("id") != identity:
            raise EvidenceError("schema manifest changed")
        content = regular_bytes(Path(directory) / name)
        if sha(content) != expected:
            raise EvidenceError("schema hash mismatch: " + name)
        document = json.loads(content)
        if document.get("$id") != identity:
            raise EvidenceError("schema identity mismatch")
        documents[name], data[name] = document, content
    return data, reference_closure(documents)


def parent_namespace():
    if platform.system() == "Linux":
        return os.readlink("/proc/self/ns/net")
    return "macos-sandbox-exec"


def isolation_prefix():
    if platform.system() == "Linux":
        return ["/usr/bin/unshare", "--user", "--map-root-user", "--net", "--"]
    if platform.system() == "Darwin":
        return ["/usr/bin/sandbox-exec", "-p", "(version 1) (allow default) (deny network*)"]
    raise EvidenceError("unsupported network isolation platform")


def namespace_witness(parent):
    if platform.system() == "Linux":
        current = os.readlink("/proc/self/ns/net")
        if current == parent:
            raise EvidenceError("network namespace isolation is inactive")
        return {"platform": "Linux", "parent_network_namespace": parent,
                "child_network_namespace": current}
    if platform.system() == "Darwin" and parent == "macos-sandbox-exec":
        return {"platform": "Darwin", "enforcement": "sandbox-exec deny network profile",
                "scope": "local adapter only; not Linux namespace proof"}
    raise EvidenceError("missing network isolation witness")


def child():
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", action="store_true", required=True)
    parser.add_argument("--goneat", type=Path, required=True)
    parser.add_argument("--tool-sha256", required=True)
    parser.add_argument("--parent-namespace", required=True)
    parser.add_argument("--marker", type=Path, required=True)
    parser.add_argument("--schema-dir", type=Path)
    parser.add_argument("--data", type=Path)
    parser.add_argument("--data-sha256")
    args = parser.parse_args()
    try:
        witness = namespace_witness(args.parent_namespace)
        if sha(regular_bytes(args.goneat)) != args.tool_sha256:
            raise EvidenceError("validator tool changed")
        if args.data:
            preflight(args.schema_dir)
            if sha(regular_bytes(args.data)) != args.data_sha256:
                raise EvidenceError("validation data changed")
            argv = [str(args.goneat), "schema", "validate-data", "--schema-file",
                    str(args.schema_dir / "bom-1.6.schema.json"), "--ref-dir", str(args.schema_dir),
                    "--schema-resolution", "id-strict", "--data", str(args.data)]
        else:
            argv = [str(args.goneat), "version", "--json"]
        json_write(args.marker, {**witness, "exec_requested": True, "tool_sha256": args.tool_sha256})
        # exec preserves the owning outer Popen process group. There is no
        # nested unbounded wait or detached validator requiring a foreign scan.
        try:
            os.execv(str(args.goneat), argv)
        except OSError as error:
            # Only an actual exec failure gets this typed errno receipt.
            json_write(args.marker, {**witness, "exec_requested": True,
                                    "tool_sha256": args.tool_sha256,
                                    "exec_error": {"error_class": type(error).__name__,
                                                   "errno": error.errno}})
            print("schema exec failed: " + type(error).__name__, file=sys.stderr)
            return 65
    except (EvidenceError, OSError, ValueError, KeyError, TypeError) as error:
        print("schema isolation/exec setup failed: " + type(error).__name__, file=sys.stderr)
        return 65


def validate(controller, data, goneat, schema_dir=SCHEMA_ROOT):
    source_data = regular_bytes(data)
    json.loads(source_data)  # Reject malformed data before launching anything.
    closure, references = preflight(schema_dir)
    snapshot = controller.out / "validation-snapshot"
    snapshot.mkdir(mode=0o700)  # Never replace a prior failed snapshot.
    schemas = snapshot / "schemas"
    schemas.mkdir(mode=0o700)
    for name, content in closure.items():
        path = schemas / name
        path.write_bytes(content)
        path.chmod(0o444)
    bom = snapshot / "bom.json"
    bom.write_bytes(source_data)
    bom.chmod(0o444)
    schemas.chmod(0o555)
    snapshot.chmod(0o555)
    tool = Path(goneat).resolve()
    tool_hash = sha(regular_bytes(tool))
    parent = parent_namespace()

    def argv(marker):
        return [*isolation_prefix(), sys.executable, "-B", str(Path(__file__).resolve()), "--child",
                "--goneat", str(tool), "--tool-sha256", tool_hash,
                "--parent-namespace", parent, "--marker", str(marker)]

    def expected_images():
        target = tool.stat()
        wrapper = Path(sys.executable).stat()
        return {"tool": (target.st_dev, target.st_ino),
                "wrapper": (wrapper.st_dev, wrapper.st_ino)}

    version_marker = controller.out / "validator-version-child.json"
    version = json.loads(controller.command("validator-version", argv(version_marker), 10,
                                            expected_images=expected_images()))
    if not isinstance(version, dict) or version.get("binaryVersion") != "v0.6.1":
        raise EvidenceError("unsupported validator version")
    marker = controller.out / "validator-child.json"
    controller.command("schema-validation", [*argv(marker), "--schema-dir", str(schemas),
                                            "--data", str(bom), "--data-sha256", sha(source_data)], 20,
                       expected_images=expected_images())
    if (sha(regular_bytes(data)) != sha(source_data) or sha(regular_bytes(bom)) != sha(source_data)
            or sha(regular_bytes(tool)) != tool_hash):
        raise EvidenceError("validation source/data/tool changed")
    for name, content in closure.items():
        if regular_bytes(schemas / name) != content or regular_bytes(Path(schema_dir) / name) != content:
            raise EvidenceError("validation schema changed")
    witnesses = [json.loads(regular_bytes(path)) for path in (version_marker, marker)]
    if any(not isinstance(x, dict) or x.get("exec_requested") is not True
           or x.get("tool_sha256") != tool_hash for x in witnesses):
        raise EvidenceError("missing validator invocation witness")
    controller.receipt["schema_validation"] = {
        "validator": version, "validator_sha256": tool_hash,
        "bom_sha256": sha(source_data), "schemas_sha256": {k: sha(v) for k, v in closure.items()},
        "schema_source_commit": SOURCE_COMMIT, "closed_references": references,
        "isolation_witnesses": witnesses, "status": "pass",
    }
    controller.save()
    return source_data


if __name__ == "__main__":
    raise SystemExit(child())
