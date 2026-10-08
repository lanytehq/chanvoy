"""Pure admission rules for fresh bundled-source production build evidence.

No filesystem, subprocess, provider or target discovery belongs in this module.
"""

import hashlib
import json
from pathlib import PurePosixPath
import re

from bounded_evidence import EvidenceError

PLATFORMS = {
    "linux-x86_64": ("x86_64-unknown-linux-gnu", "linux/x86_64"),
    "linux-aarch64": ("aarch64-unknown-linux-gnu", "linux/aarch64"),
    "macos-aarch64": ("aarch64-apple-darwin", "macos/aarch64"),
}
CONTROL = {"AWS_LC_SYS_USE_SYSTEM": "0"}
PREFIX = "aws_lc_0_45_0"
LIBRARY = "static=" + PREFIX + "_crypto"
ARCHIVE = "lib" + PREFIX + "_crypto.a"
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
MAX_EVENTS = 16
OUTPUT_LIMIT = 1024 * 1024
ARCHIVE_LIMIT = 64 * 1024 * 1024
SNAPSHOT_LIMIT = 256 * 1024 * 1024
EXECUTABLE_LIMIT = 64 * 1024 * 1024
BINDINGS = ("commit", "tree", "lock_sha256", "platform", "target", "mode", "tag", "tag_object", "workflow")
SOURCE_FILES = ("build-production-binary.py", "production_build_policy.py")


def sha(data):
    return hashlib.sha256(data).hexdigest()


def admit_selectors(environment, target):
    """Reject presence, including empty values. Never read/report caller values."""
    compiler = {"RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC", "RUSTC_WRAPPER",
                "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_RUSTC", "CARGO_BUILD_RUSTC_WRAPPER",
                "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_TARGET", "CARGO_BUILD_RUSTFLAGS"}
    native = {"CC", "CXX", "AR", "CFLAGS", "CXXFLAGS", "ARFLAGS", "CXXSTDLIB", "RANLIB", "RANLIBFLAGS",
              "CRATE_CC_NO_DEFAULTS", "CC_KNOWN_WRAPPER_CUSTOM", "CC_SHELL_ESCAPED_FLAGS", "CC_FORCE_DISABLE",
              "CROSS_COMPILE", "CMAKE", "CMAKE_TOOLCHAIN_FILE", "CMAKE_GENERATOR", "CMAKE_PREFIX_PATH",
              "EMCMAKE", "EMMAKE", "MAKEFLAGS"}
    native_names = {key for name in native for key in
                    (name, "HOST_" + name, "TARGET_" + name, name + "_" + target,
                     name + "_" + target.replace("-", "_").replace(".", "_"))}
    for name in sorted(environment):
        if name in compiler:
            raise EvidenceError("unsupported compiler override: " + name)
        if name.startswith("CARGO_PROFILE_"):
            raise EvidenceError("unsupported Cargo profile override")
        if (name == "CARGO_TARGET_DIR" or name == "CARGO_TARGET_APPLIES_TO_HOST"
                or name.startswith(("CARGO_HOST_", "CARGO_UNSTABLE_"))
                or (name.startswith("CARGO_TARGET_")
                    and name.endswith(("_RUSTFLAGS", "_LINKER", "_RUNNER", "_RUSTDOCFLAGS")))):
            raise EvidenceError("unsupported Cargo selector: " + name)
        if (name.startswith(("AWS_LC_SYS_", "HOST_AWS_LC_SYS_", "TARGET_AWS_LC_SYS_", "OPENSSL_"))
                or any(name.endswith("_" + base) or name.startswith(base + "_")
                       for base in ("OPENSSL_DIR", "OPENSSL_INCLUDE_DIR", "OPENSSL_LIB_DIR"))):
            raise EvidenceError("unsupported native producer override: " + name)
        if name in native_names:
            raise EvidenceError("unsupported native build override: " + name)


def child_environment(environment, target):
    admit_selectors(environment, target)
    return {**environment, **CONTROL}


def relative_name(value):
    if not isinstance(value, str) or not value or "\\" in value:
        raise EvidenceError("unsafe native evidence path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(x in (".", "..", "") for x in value.split("/")):
        raise EvidenceError("unsafe native evidence path")
    return path


def absolute_owned(value, parent):
    if not isinstance(value, str) or not value.startswith("/") or "\\" in value:
        raise EvidenceError("unsupported native source path")
    path = PurePosixPath(value)
    if str(path) != value or ".." in path.parts or not path.is_relative_to(parent):
        raise EvidenceError("native source path is outside owned output")
    return path


def lock_identity(text):
    found = []
    for block in re.split(r"^\[\[package\]\]\s*$", text, flags=re.MULTILINE)[1:]:
        fields = {}
        for name in ("name", "version", "source", "checksum"):
            matches = re.findall(r"^" + name + r"\s*=\s*(\"[^\n]*\")\s*$", block, re.MULTILINE)
            if len(matches) > 1:
                raise EvidenceError("ambiguous native lock identity")
            if matches:
                fields[name] = json.loads(matches[0])
        if fields.get("name") == "aws-lc-sys":
            found.append(fields)
    if (len(found) != 1 or found[0].get("version") != "0.45.0" or found[0].get("source") != REGISTRY
            or not re.fullmatch(r"[0-9a-f]{64}", found[0].get("checksum", ""))):
        raise EvidenceError("unsupported native locked source")
    return found[0]


def backend_records(events, metadata, lock, source_root, output_texts):
    """Classify every selected invocation using its own output; paths are data."""
    identity = lock_identity(lock)
    packages = [p for p in metadata["packages"] if p.get("name") == "aws-lc-sys"]
    if len(packages) != 1 or any(packages[0].get(k) != identity[k] for k in ("name", "version", "source")):
        raise EvidenceError("native package metadata disagrees with locked source")
    package_id = packages[0]["id"]
    artifacts = [e for e in events if e.get("reason") == "compiler-artifact" and e.get("package_id") == package_id]
    builders = [e for e in artifacts if e.get("target", {}).get("kind") == ["custom-build"]]
    if (not builders or any(e.get("fresh") is not False for e in builders)
            or any(set(e.get("features", [])) & {"fips", "ssl", "fips-link-precompiled"} for e in artifacts)):
        raise EvidenceError("missing fresh supported native build-script compilation")
    selected = [(n, e) for n, e in enumerate(events)
                if e.get("reason") == "build-script-executed" and e.get("package_id") == package_id]
    if not 1 <= len(selected) <= MAX_EVENTS or set(output_texts) != {n for n, _ in selected}:
        raise EvidenceError("missing, extra or over-limit native invocations")
    root = PurePosixPath(source_root)
    if not root.is_absolute() or str(root) != source_root:
        raise EvidenceError("unsupported native source root")
    target = root / "target"
    if metadata.get("target_directory") != str(target):
        raise EvidenceError("native target directory association mismatch")
    result, seen = [], set()
    for ordinal, event in selected:
        out = absolute_owned(event.get("out_dir"), target / "release" / "build")
        if out.name != "out" or out == target / "release" / "build" / "out" or str(out) in seen:
            raise EvidenceError("duplicate or unsupported native invocation output")
        seen.add(str(out))
        text = output_texts[ordinal]
        if not isinstance(text, str) or len(text.encode()) > OUTPUT_LIMIT:
            raise EvidenceError("native output exceeds fixed bound")
        lines = text.replace("cargo::", "cargo:").splitlines()
        controls = [x for x in lines if x.startswith("cargo:warning=Environment Variable found '")
                    and "AWS_LC_SYS_" in x and "USE_SYSTEM" in x]
        if controls != ["cargo:warning=Environment Variable found 'AWS_LC_SYS_USE_SYSTEM': '0'"]:
            raise EvidenceError("native output lacks exact owned child control")
        reported = [x.removeprefix("cargo:warning=Building with: ") for x in lines
                    if x.startswith("cargo:warning=Building with: ")]
        if len(reported) != 1 or reported[0] not in ("CC", "CMake"):
            raise EvidenceError("unsupported native source builder")
        if [x for x in lines if x.startswith("cargo:warning=Symbol Prefix:")] != [
                'cargo:warning=Symbol Prefix: Some("' + PREFIX + '")']:
            raise EvidenceError("unsupported native symbol prefix")
        directory = out if reported[0] == "CC" else out / "build" / "artifacts"
        archive = directory / ARCHIVE
        required = {"libdir": str(directory), "link_kind": "static", "libcrypto": PREFIX + "_crypto",
                    "libcrypto_path": str(archive), "system_libs": ""}
        for key, expected in required.items():
            values = [x.split("=", 1)[1] for x in lines if x.startswith("cargo:" + key + "=")]
            if values != [expected]:
                raise EvidenceError("unsupported or missing native source metadata: " + key)
        links = [x.split("=", 1)[1] for x in lines if x.startswith("cargo:rustc-link-lib=")]
        paths = [x.split("=", 1)[1] for x in lines if x.startswith("cargo:rustc-link-search=")]
        if (links != [LIBRARY] or paths != ["native=" + str(directory)]
                or event.get("linked_libs") != links or event.get("linked_paths") != paths):
            raise EvidenceError("unsupported native link/archive association")
        result.append({"event_ordinal": ordinal, "package_id": package_id,
                       "script_compiler_artifact_ordinals": [n for n, e in enumerate(events) if e in builders],
                       "out_dir": str(out),
                       "out_dir_relative": str(out.relative_to(target)), "builder": reported[0],
                       "linked_libs": links, "linked_paths": paths,
                       "output_source": str(out.parent / "output"), "archive_source": str(archive),
                       "archive_name": ARCHIVE})
    return {**identity, "package_id": package_id}, result


def executable_event(events, package_id, target_directory):
    for event in events:
        if event.get("reason") == "compiler-artifact" and (event.get("profile", {}).get("test")
                or set(event.get("target", {}).get("kind", [])) & {"test", "bench", "example"}):
            raise EvidenceError("fixture or test artifact in normal build capture")
    roots = [e for e in events if e.get("reason") == "compiler-artifact" and e.get("package_id") == package_id
             and e.get("target", {}).get("name") == "chanvoy" and e.get("target", {}).get("kind") == ["bin"]
             and e.get("profile", {}).get("test") is False and e.get("executable")]
    if len(roots) != 1:
        raise EvidenceError("missing or duplicate compiled executable association")
    event = roots[0]
    if event["profile"].get("opt_level") not in ("1", "2", "3", "s", "z") or event["profile"].get("debug_assertions"):
        raise EvidenceError("normal build receipt is not optimized release")
    if event.get("features") != []:
        raise EvidenceError("unexpected root production features")
    if event["executable"] != str(PurePosixPath(target_directory) / "release" / "chanvoy"):
        raise EvidenceError("normal executable is not the declared native release path")
    finished = [e for e in events if e.get("reason") == "build-finished"]
    if len(finished) != 1 or finished[0].get("success") is not True:
        raise EvidenceError("missing or duplicate successful Cargo completion")
    return event


def bind_payload(receipt, observation):
    actual = receipt.get("normal_executable")
    if (not isinstance(actual, dict) or set(actual) != {"sha256", "bytes"}
            or not re.fullmatch(r"[0-9a-f]{64}", actual.get("sha256", ""))
            or type(actual.get("bytes")) is not int or not 0 < actual["bytes"] <= EXECUTABLE_LIMIT
            or actual != observation):
        raise EvidenceError("producer normal executable association mismatch")


def admit_receipt(receipt, manifest, expected, source_hashes, events, metadata, lock,
                  normal_hash, stderr_hash, manifest_hash, snapshots, payload):
    if (receipt.get("schema") != "native-build-policy-v1" or receipt.get("status") != "pass"
            or receipt.get("policy") != "bundled-source-v1" or receipt.get("control") != CONTROL
            or receipt.get("fresh_target_at_entry") is not True or receipt.get("cargo_configuration") != "absent"
            or receipt.get("source_files_sha256") != source_hashes
            or receipt.get("normal_build_messages_sha256") != normal_hash
            or receipt.get("normal_build_stderr_sha256") != stderr_hash
            or receipt.get("native_snapshot_manifest") != "native-snapshots.json"
            or receipt.get("native_snapshot_manifest_sha256") != manifest_hash):
        raise EvidenceError("missing or mismatched native producer policy")
    if any(receipt.get(k) != expected.get(k) for k in BINDINGS):
        raise EvidenceError("native producer source/target/mode/run association mismatch")
    if (manifest.get("schema") != "native-snapshots-v1" or manifest.get("association") !=
            {k: receipt[k] for k in (*BINDINGS, "source_files_sha256", "normal_build_messages_sha256")}
            or receipt.get("target_directory") != metadata.get("target_directory")):
        raise EvidenceError("native snapshot manifest association mismatch")
    bind_payload(receipt, payload)
    roots = [p for p in metadata["packages"] if p.get("name") == "chanvoy"
             and p.get("manifest_path") == receipt.get("source_root", "") + "/Cargo.toml"]
    if len(roots) != 1 or roots[0]["id"] != receipt.get("package_id"):
        raise EvidenceError("native producer root package association mismatch")
    executable_event(events, receipt.get("package_id"), receipt.get("target_directory"))
    records = manifest.get("events")
    if not isinstance(records, list) or not 1 <= len(records) <= MAX_EVENTS:
        raise EvidenceError("missing or over-limit native snapshots")
    names, output_texts, total = set(), {}, 0
    for record in records:
        ordinal = record.get("event_ordinal")
        if type(ordinal) is not int or ordinal in output_texts:
            raise EvidenceError("duplicate native snapshot event")
        for kind, limit, suffix in (("output", OUTPUT_LIMIT, "output.log"), ("archive", ARCHIVE_LIMIT, ARCHIVE)):
            item = record.get(kind, {})
            expected_name = "native-snapshots/event-%02d/" % ordinal + suffix
            if item.get("path") != expected_name:
                raise EvidenceError("unsupported native snapshot path")
            relative_name(item["path"])
            if item["path"] in names or type(item.get("bytes")) is not int or not 0 < item["bytes"] <= limit:
                raise EvidenceError("duplicate or over-limit native snapshot")
            names.add(item["path"])
            total += item["bytes"]
            observation = snapshots.get(item["path"])
            if not observation or {k: item.get(k) for k in ("sha256", "bytes")} != observation["observation"]:
                raise EvidenceError("native snapshot bytes/hash association mismatch")
        output_texts[ordinal] = snapshots[record["output"]["path"]]["text"]
    if total > SNAPSHOT_LIMIT or set(snapshots) != names:
        raise EvidenceError("extra or over-limit native snapshot inventory")
    package, classified = backend_records(events, metadata, lock, receipt["source_root"], output_texts)
    if receipt.get("native_package") != package or receipt.get("native_events") != records:
        raise EvidenceError("native producer event association mismatch")
    for observed, classified_record in zip(records, classified):
        if {k: observed.get(k) for k in classified_record} != classified_record:
            raise EvidenceError("native snapshot source/event association mismatch")
    return records
