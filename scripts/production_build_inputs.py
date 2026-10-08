"""Capture normal-build inputs and native metadata before fixture compilation."""

import hashlib
import os
from pathlib import Path
import re
import shutil
import stat

from bounded_evidence import EvidenceError, json_write


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
    shutil.copyfile(regular(driver.root / "Cargo.lock"), lock)
    metadata_path = driver.out / "metadata.log"
    if (digest(normal) != driver.receipt["normal_build_messages_sha256"]
            or digest(lock) != driver.receipt["lock_sha256"]
            or digest(metadata_path) != driver.receipt["metadata_messages_sha256"]
            or digest(payload) != driver.receipt["payload_sha256"]):
        raise EvidenceError("normal build input changed during capture")
    native_logs = {
        p.name: digest(p) for p in driver.out.glob("native-*.log") if p.is_file()
    }
    value = {
        "schema": "normal-build-inputs-v1", "before_fixture_compilation": True,
        "mode": driver.args.mode, "platform": driver.args.platform,
        "target": driver.receipt["target"], "rustc": driver.receipt["rustc"],
        "source_root": str(driver.root), "commit": driver.receipt["commit"],
        "tree": driver.receipt["tree"], "tag": driver.args.tag, "tag_object": driver.args.tag_object,
        "workflow": driver.receipt["workflow"], "package_id": driver.receipt["package_id"],
        "payload": payload.name, "payload_sha256": digest(payload),
        "normal_build_messages_sha256": digest(normal),
        "metadata_sha256": digest(metadata_path), "lock_sha256": digest(lock),
        "native": {"tool": tool_name, "tool_sha256": tool_hash, "tool_version": version,
                   "logs_sha256": native_logs, "capture_status": "success",
                   "observation": observation, "external_requirements": sorted(set(requirements)),
                   "empty_observation": not requirements,
                   "coverage": "dynamic metadata only; static native completeness unknown",
                   "build_toolchain": toolchain,
                   "build_scripts": native_inputs(events, Path(metadata["target_directory"]))},
    }
    path = driver.out / "normal-build-inputs.json"
    json_write(path, value)
    return path
