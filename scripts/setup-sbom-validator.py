#!/usr/bin/env python3
"""Stage the fixed Linux validator at an explicit job-local path."""

import argparse
import json
from pathlib import Path
import sys
import tarfile
import time

sys.dont_write_bytecode = True
from bounded_evidence import EvidenceCommands, EvidenceError
from offline_schema import regular_bytes, sha


ROOT = Path(__file__).resolve().parent.parent
PIN = json.loads(regular_bytes(ROOT / "scripts/sbom-tools.json"))["goneat"]


def unpack(archive, output):
    if sha(regular_bytes(archive)) != PIN["archive_sha256"]:
        raise EvidenceError("validator archive checksum mismatch")
    with tarfile.open(archive, "r:gz") as package:
        members = [x for x in package.getmembers() if x.name in ("goneat", "./goneat")]
        if len(members) != 1 or not members[0].isfile():
            raise EvidenceError("ambiguous/nonregular validator archive entry")
        binary = package.extractfile(members[0])
        if binary is None:
            raise EvidenceError("missing validator archive contents")
        data = binary.read()
    output = Path(output)
    with output.open("xb") as target:
        target.write(data)
    output.chmod(0o555)
    return {"tool": "goneat", "version": "0.6.1", "platform": "linux-amd64",
            "archive_url": PIN["archive_url"], "archive_sha256": PIN["archive_sha256"],
            "binary_sha256": sha(data), "binary": str(output.resolve())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    out = args.output.parent.resolve() / args.output.name
    if out.exists() or out.is_symlink() or out.is_relative_to(ROOT):
        parser.error("validator staging requires a new external job-local directory")
    out.mkdir(parents=True, mode=0o700)
    controller = EvidenceCommands()
    controller.root, controller.out = ROOT, out
    controller.deadline = time.monotonic() + 180
    controller.receipt_path = out / "setup-evidence.json"
    controller.receipt = {"schema": "sbom-validator-setup-v1", "status": "incomplete", "commands": []}
    controller.save()
    try:
        archive = out / "goneat.tar.gz"
        controller.command("download", ["curl", "--disable", "--fail", "--location", "--proto", "=https",
                                        "--connect-timeout", "10", "--max-time", "120", "--retry", "0",
                                        "--output", str(archive), PIN["archive_url"]], 130)
        controller.receipt["tool"] = unpack(archive, out / "goneat")
        controller.receipt["status"] = "pass"
        controller.save()
    except (EvidenceError, OSError, ValueError, tarfile.TarError) as error:
        controller.fail(str(error))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
