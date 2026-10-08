"""Capture normal-build inputs and native metadata before fixture compilation."""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import time

from bounded_evidence import CLEANUP_SECONDS, EvidenceError, json_write
import production_build_policy as policy


def regular(path):
    path = Path(path)
    if not stat.S_ISREG(path.lstat().st_mode):
        raise EvidenceError("build input is not a regular file")
    return path


def digest(path):
    with regular(path).open("rb") as source:
        value = hashlib.sha256()
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def native_inputs(events, target_dir):
    """Only inspect explicitly requested archives in owned release output."""
    result = []
    release_dir = (target_dir / "release").resolve()
    for event in events:
        if event.get("reason") != "build-script-executed":
            continue
        record = {"package_id": event["package_id"],
                  "linked_libs": event.get("linked_libs", []),
                  "linked_paths": event.get("linked_paths", []),
                  "cfgs": event.get("cfgs", []), "archives": [],
                  "interpretation": "build-input evidence; not proof of linked members"}
        for library in record["linked_libs"]:
            if not library.startswith("static="):
                continue
            name = library.split("=", 1)[1].split(":", 1)[-1]
            if not re.fullmatch(r"[A-Za-z0-9_.+-]+", name):
                raise EvidenceError("unsupported native archive name")
            found = []
            for search in record["linked_paths"]:
                if not search.startswith("native="):
                    continue
                directory = Path(search.split("=", 1)[1]).resolve()
                if not directory.is_relative_to(release_dir):
                    continue  # Never read arbitrary runner-installed libraries.
                archive = directory / ("lib" + name + ".a")
                if archive.exists() or archive.is_symlink():
                    regular(archive)
                    found.append({"name": archive.name,
                                  "relative_path": str(archive.relative_to(release_dir)),
                                  "sha256": digest(archive)})
            record["archives"].extend(found)
            if not found:
                record.setdefault("unresolved_static_inputs", []).append(name)
        result.append(record)
    return result


def native_toolchain(driver, events, metadata):
    selected = {x["package_id"] for x in events if x.get("reason") in ("compiler-artifact", "build-script-executed")}
    selected_names = {p["name"] for p in metadata["packages"] if p["id"] in selected}
    builders = {p["id"]: p["name"] for p in metadata["packages"] if p["id"] in selected and p["name"] in ("cc", "cmake")}
    if "aws-lc-sys" in selected_names:
        for key in os.environ:
            if key.startswith(("AWS_LC_SYS_", "HOST_AWS_LC_SYS_", "TARGET_AWS_LC_SYS_")):
                raise EvidenceError("unsupported native producer override: " + key)
    if not builders:
        return {"selected_builder_package_ids": [], "observations": [], "selection": "no selected builder; no discovery record"}
    bases = {"CC", "CXX", "AR", "CFLAGS", "CXXFLAGS", "ARFLAGS", "CXXSTDLIB", "RANLIB", "RANLIBFLAGS",
             "CRATE_CC_NO_DEFAULTS", "CC_KNOWN_WRAPPER_CUSTOM", "CC_SHELL_ESCAPED_FLAGS", "CC_FORCE_DISABLE", "CROSS_COMPILE"}
    if "cmake" in builders.values():
        bases |= {"CMAKE", "CMAKE_TOOLCHAIN_FILE", "CMAKE_GENERATOR", "CMAKE_PREFIX_PATH", "EMCMAKE", "EMMAKE", "MAKEFLAGS"}
    target = driver.receipt["target"]
    for name in sorted(bases):
        for key in (name, "HOST_" + name, "TARGET_" + name, name + "_" + target, name + "_" + target.replace("-", "_").replace(".", "_")):
            if key in os.environ:
                raise EvidenceError("unsupported native build override: " + key)
    names = ["cc", "c++"] + (["cmake"] if "cmake" in builders.values() else [])
    observations = []
    for name in names:
        selected_tool = shutil.which(name)
        if not selected_tool:
            raise EvidenceError("required default build tool unavailable: " + name)
        tool = regular(Path(selected_tool).resolve())
        tool_hash = digest(tool)
        stage = "native-build-" + name + "-version"
        version = driver.command(stage, [str(tool), "--version"], 10)
        stderr = (driver.out / (stage + ".stderr.log")).read_text()
        version = "\n".join(x.strip() for x in (version, stderr) if x.strip())
        if not version or digest(tool) != tool_hash:
            raise EvidenceError("missing/changed native build tool identity")
        observations.append({"tool": name, "resolved_executable": str(tool), "sha256": tool_hash, "version": version,
                             "evidence_class": "host/build-tool discovery/query observation",
                             "selection": "selection-unconfirmed; query is not build-script invocation proof"})
    # Same normal-build cache choices are observations, not reconstructed commands.
    caches = []
    release = (Path(metadata["target_directory"]) / "release").resolve()
    for event in events:
        if event.get("reason") != "build-script-executed" or not event.get("out_dir"):
            continue
        directory = Path(event["out_dir"]).resolve()
        if not directory.is_relative_to(release):
            raise EvidenceError("native build cache is outside owned release output")
        for path in directory.rglob("CMakeCache.txt"):
            if len(caches) >= 8 or regular(path).stat().st_size > 1024 * 1024:
                raise EvidenceError("native build cache capture exceeds fixed bound")
            content = path.read_text()
            choices = dict(re.findall(r"^(CMAKE_C_COMPILER|CMAKE_CXX_COMPILER|CMAKE_COMMAND|CMAKE_MAKE_PROGRAM):[^=\n]+=([^\n]*)$",
                                      content, re.MULTILINE))
            caches.append({"package_id": event["package_id"], "relative_path": str(path.relative_to(release)),
                           "sha256": digest(path), "choices": choices,
                           "interpretation": "normal-build cache choices; no invocation reconstruction"})
    return {"selected_builder_package_ids": sorted(builders), "observations": observations,
            "normal_build_caches": caches, "selection": "unconfirmed unless independently associated with normal build choices"}


def capture(driver, events, metadata, payload):
    toolchain = native_toolchain(driver, events, metadata)
    tool_name = "otool" if driver.args.platform == "macos-aarch64" else "readelf"
    selected = shutil.which(tool_name)
    if not selected:
        raise EvidenceError("required native metadata tool is unavailable")
    tool = regular(Path(selected).resolve())
    tool_hash = digest(tool)
    version = driver.command("native-tool-version", [str(tool), "--version"], 10)
    version_stderr = (driver.out / "native-tool-version.stderr.log").read_text()
    version = "\n".join(x.strip() for x in (version, version_stderr) if x.strip())
    if not version:
        raise EvidenceError("missing native metadata tool identity")
    if tool_name == "otool":
        dynamic = driver.command("native-dynamic", [str(tool), "-L", str(payload)], 10)
        if not dynamic.splitlines() or not dynamic.splitlines()[0].endswith(":"):
            raise EvidenceError("malformed Mach-O native metadata")
        requirements = re.findall(r"^\s+(\S+) \(compatibility version [^)]*\)", dynamic, re.MULTILINE)
        observation = "Mach-O install-name requirements"
    else:
        dynamic = driver.command("native-dynamic", [str(tool), "-d", str(payload)], 10)
        headers = driver.command("native-program-headers", [str(tool), "-l", str(payload)], 10)
        if not dynamic.strip() or not headers.strip():
            raise EvidenceError("missing ELF native metadata capture")
        requirements = re.findall(r"\(NEEDED\).*Shared library: \[([^\]]+)\]", dynamic)
        requirements += re.findall(r"Requesting program interpreter: ([^\]]+)\]", headers)
        observation = "ELF dynamic-load requirements"
    if digest(tool) != tool_hash:
        raise EvidenceError("native metadata tool changed during capture")
    normal = driver.out / "normal-build.jsonl"
    if driver.args.normal_build.resolve() != normal.resolve():
        shutil.copyfile(regular(driver.args.normal_build), normal)
    lock = driver.out / "normal-Cargo.lock"
    if lock.exists():
        if digest(lock) != digest(driver.root / "Cargo.lock"):
            raise EvidenceError("normal lock evidence changed before capture")
    else:
        with lock.open("xb") as output:
            output.write(regular(driver.root / "Cargo.lock").read_bytes())
    metadata_path = driver.out / "metadata.log"
    if (digest(normal) != driver.receipt["normal_build_messages_sha256"]
            or digest(lock) != driver.receipt["lock_sha256"]
            or digest(metadata_path) != driver.receipt["metadata_messages_sha256"]
            or digest(payload) != driver.receipt["payload_sha256"]):
        raise EvidenceError("normal build input changed during capture")
    native_logs = {
        p.name: digest(p) for p in driver.out.glob("native-*.log") if p.is_file()
    }
    scripts = native_inputs(events, Path(metadata["target_directory"]))
    aws_rows = [r for r in scripts if r["package_id"] == driver.native_policy["native_package"]["package_id"]]
    if len(aws_rows) != len(driver.native_policy["native_events"]):
        raise EvidenceError("missing native build-script producer association")
    for row, observed in zip(aws_rows, driver.native_policy["native_events"]):
        if ([{"name": a.get("name"), "sha256": a.get("sha256")} for a in row["archives"]]
                != [{"name": policy.ARCHIVE, "sha256": observed["archive"]["sha256"]}]
                or row.get("unresolved_static_inputs")):
            raise EvidenceError("native producer archive/input association mismatch before fixtures")
    policy.bind_payload(driver.native_policy, bounded_file(payload, payload.parent, policy.EXECUTABLE_LIMIT,
                                                         lambda: budget_check(driver))["observation"])
    value = {
        "schema": "normal-build-inputs-v2", "before_fixture_compilation": True,
        "mode": driver.args.mode, "platform": driver.args.platform,
        "target": driver.receipt["target"], "rustc": driver.receipt["rustc"],
        "source_root": str(driver.root), "commit": driver.receipt["commit"],
        "tree": driver.receipt["tree"], "tag": driver.args.tag, "tag_object": driver.args.tag_object,
        "workflow": driver.receipt["workflow"], "package_id": driver.receipt["package_id"],
        "payload": payload.name, "payload_sha256": digest(payload),
        "normal_executable": dict(driver.native_policy["normal_executable"]),
        "native_build_policy": "native-build-policy.json",
        "native_build_policy_sha256": driver.receipt["native_build_policy_sha256"],
        "native_snapshot_manifest_sha256": driver.receipt["native_snapshot_manifest_sha256"],
        "normal_build_messages_sha256": digest(normal),
        "metadata_sha256": digest(metadata_path), "lock_sha256": digest(lock),
        "native": {"tool": tool_name, "tool_sha256": tool_hash, "tool_version": version,
                   "logs_sha256": native_logs, "capture_status": "success",
                   "observation": observation, "external_requirements": sorted(set(requirements)),
                   "empty_observation": not requirements,
                   "coverage": "dynamic metadata only; static native completeness unknown",
                   "build_toolchain": toolchain,
                   "build_scripts": scripts},
    }
    path = driver.out / "normal-build-inputs.json"
    json_write(path, value)
    return path


# Native producer snapshots are portable, bounded build-input observations.
# These routines deliberately do not reconstruct final linked archive members.
def budget_check(driver):
    if time.monotonic() >= driver.deadline - CLEANUP_SECONDS:
        raise EvidenceError("owned operation horizon expired during native evidence")


def safe_owned(path, root):
    path, root = Path(path).absolute(), Path(root).absolute()
    if not path.is_relative_to(root) or path == root or ".." in path.parts or root.is_symlink():
        raise EvidenceError("native evidence escapes owned root")
    relative = path.relative_to(root)
    # Canonicalize the caller's enclosing root (e.g. macOS /var -> /private/var),
    # but never resolve/accept aliases in any owned descendant.
    root = root.resolve()
    path = root / relative
    current = root
    for component in relative.parts:
        current = current / component
        if current.is_symlink():
            raise EvidenceError("native evidence contains a symlink")
    regular(path)
    if path.resolve() != path:
        raise EvidenceError("native evidence contains a path alias")
    return path


def bounded_file(path, root, limit, check=lambda: None, *, destination=None, text=False):
    check()
    path = safe_owned(path, root)
    before = path.stat()
    if not 0 < before.st_size <= limit:
        raise EvidenceError("native evidence exceeds fixed byte bound")
    value, size, blocks = hashlib.sha256(), 0, []
    output = None
    try:
        if destination is not None:
            output = Path(destination).open("xb")
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as source:
            opened = os.fstat(source.fileno())
            if (opened.st_dev, opened.st_ino, opened.st_size) != (before.st_dev, before.st_ino, before.st_size):
                raise EvidenceError("native evidence changed before capture")
            while True:
                check()
                block = source.read(min(1024 * 1024, limit + 1 - size))
                if not block:
                    break
                size += len(block)
                if size > limit:
                    raise EvidenceError("native evidence exceeds fixed byte bound")
                value.update(block)
                if text:
                    blocks.append(block)
                if output is not None:
                    output.write(block)
            after = os.fstat(source.fileno())
        if output is not None:
            output.close()
            output = None
        safe_owned(path, root)
        current = path.stat()
        signature = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        if signature(before) != signature(after) or signature(before) != signature(current) or size != before.st_size:
            raise EvidenceError("native evidence changed during capture")
        check()
        observation = {"sha256": value.hexdigest(), "bytes": size}
        return {"observation": observation, **({"text": b"".join(blocks).decode()} if text else {})}
    finally:
        if output is not None:
            output.close()


def cargo_configuration_absent(root):
    root = Path(root)
    cargo_home = Path(os.environ.get("CARGO_HOME") or Path.home() / ".cargo")
    if not cargo_home.is_absolute():
        cargo_home = root / cargo_home
    for directory in (*[p / ".cargo" for p in (root, *root.parents)], cargo_home):
        for name in ("config", "config.toml"):
            path = directory / name
            if path.exists() or path.is_symlink():
                raise EvidenceError("unsupported Cargo configuration")


def policy_source_hashes(root):
    return {name: digest(safe_owned(Path(root) / "scripts" / name, root)) for name in policy.SOURCE_FILES}


def capture_native_snapshots(driver, events, metadata):
    check = lambda: budget_check(driver)
    selected = {p["id"] for p in metadata["packages"] if p.get("name") == "aws-lc-sys"}
    outputs = {}
    for ordinal, event in enumerate(events):
        if event.get("reason") == "build-script-executed" and event.get("package_id") in selected:
            if len(outputs) >= policy.MAX_EVENTS:
                raise EvidenceError("native invocation count exceeds fixed bound")
            out = policy.absolute_owned(event.get("out_dir"), driver.root / "target/release/build")
            path = Path(out).parent / "output"
            outputs[ordinal] = bounded_file(path, driver.root / "target", policy.OUTPUT_LIMIT, check, text=True)["text"]
    package, classified = policy.backend_records(events, metadata, (driver.root / "Cargo.lock").read_text(),
                                                 str(driver.root), outputs)
    snapshot_root = driver.out / "native-snapshots"
    snapshot_root.mkdir(mode=0o700)  # Exclusive: no old snapshot can be reused.
    records, total = [], 0
    for row in classified:
        directory = snapshot_root / ("event-%02d" % row["event_ordinal"])
        directory.mkdir(mode=0o700)
        record = dict(row)
        for kind, source_name, filename, limit in (
                ("output", "output_source", "output.log", policy.OUTPUT_LIMIT),
                ("archive", "archive_source", policy.ARCHIVE, policy.ARCHIVE_LIMIT)):
            source = Path(row[source_name])
            destination = directory / filename
            before = bounded_file(source, driver.root / "target", limit, check)["observation"]
            copied = bounded_file(source, driver.root / "target", limit, check, destination=destination)["observation"]
            after = bounded_file(source, driver.root / "target", limit, check)["observation"]
            snapshot = bounded_file(destination, driver.out, limit, check, text=kind == "output")
            if before != copied or after != copied or snapshot["observation"] != copied:
                raise EvidenceError("native source/copy changed during snapshot capture")
            if kind == "output" and snapshot["text"] != outputs[row["event_ordinal"]]:
                raise EvidenceError("native script output changed after classification")
            total += copied["bytes"]
            if total > policy.SNAPSHOT_LIMIT:
                raise EvidenceError("native snapshot total exceeds fixed bound")
            record[kind] = {"path": str(destination.relative_to(driver.out)), **copied}
            destination.chmod(0o444)
        records.append(record)
    manifest = {"schema": "native-snapshots-v1", "association":
                {k: driver.receipt[k] for k in (*policy.BINDINGS, "source_files_sha256", "normal_build_messages_sha256")},
                "events": records}
    path = driver.out / "native-snapshots.json"
    with path.open("x") as output:
        output.write(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
    return package, records, digest(path)


def verify_native_policy(folder, expected, source_root, events, metadata, payload, check=lambda: None):
    """Rehash only relative snapshots/payload; original runner paths are data."""
    folder = Path(folder).absolute()
    receipt_path = safe_owned(folder / "native-build-policy.json", folder)
    manifest_path = safe_owned(folder / "native-snapshots.json", folder)
    # Evidence objects are themselves small and bounded; no unbounded JSON input.
    receipt = json.loads(bounded_file(receipt_path, folder, 1024 * 1024, check, text=True)["text"])
    manifest_result = bounded_file(manifest_path, folder, 1024 * 1024, check, text=True)
    manifest = json.loads(manifest_result["text"])
    records = manifest.get("events", [])
    if not isinstance(records, list) or not 1 <= len(records) <= policy.MAX_EVENTS:
        raise EvidenceError("missing or over-limit native snapshots")
    snapshots, total = {}, 0
    for row in records:
        for kind, limit in (("output", policy.OUTPUT_LIMIT), ("archive", policy.ARCHIVE_LIMIT)):
            name = row.get(kind, {}).get("path")
            policy.relative_name(name)
            if name in snapshots:
                raise EvidenceError("duplicate native snapshot path")
            item = bounded_file(folder / name, folder, limit, check, text=kind == "output")
            snapshots[name] = item
            total += item["observation"]["bytes"]
            if total > policy.SNAPSHOT_LIMIT:
                raise EvidenceError("native snapshot total exceeds fixed bound")
    # Inventory only this artifact's fixed native-snapshot subtree, never OUT_DIR.
    root = folder / "native-snapshots"
    if root.is_symlink() or not root.is_dir():
        raise EvidenceError("unsafe native snapshot directory")
    files, directories, entries = set(), set(), 0
    for directory, child_dirs, child_files in os.walk(root, followlinks=False):
        for name in (*child_dirs, *child_files):
            entries += 1
            if entries > policy.MAX_EVENTS * 3:
                raise EvidenceError("extra native snapshot inventory")
            path = Path(directory) / name
            if path.is_symlink():
                raise EvidenceError("native snapshot inventory contains a symlink")
            if name in child_dirs:
                directories.add(str(path.relative_to(folder)))
            else:
                files.add(str(path.relative_to(folder)))
    if files != set(snapshots) or directories != {str(Path(name).parent) for name in snapshots}:
        raise EvidenceError("extra or missing native snapshot inventory")
    normal = bounded_file(folder / "normal-build.jsonl", folder, 64 * 1024 * 1024, check)["observation"]
    # Empty stderr is legitimate; digest checks regularity without a positive size requirement.
    stderr = safe_owned(folder / "normal-build.stderr.log", folder)
    if stderr.stat().st_size > 64 * 1024 * 1024:
        raise EvidenceError("normal stderr exceeds fixed bound")
    stderr_hash = digest(stderr)
    actual_payload = bounded_file(payload, Path(payload).absolute().parent, policy.EXECUTABLE_LIMIT, check)["observation"]
    if digest(safe_owned(folder / "normal-Cargo.lock", folder)) != expected.get("lock_sha256"):
        raise EvidenceError("native producer lock evidence changed")
    policy.admit_receipt(receipt, manifest, expected, policy_source_hashes(source_root), events, metadata,
                         (folder / "normal-Cargo.lock").read_text(), normal["sha256"], stderr_hash,
                         manifest_result["observation"]["sha256"], snapshots, actual_payload)
    check()
    return receipt, digest(receipt_path), manifest_result["observation"]["sha256"]
