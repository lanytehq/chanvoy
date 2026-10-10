#!/usr/bin/env python3
"""Qualify scanner configuration and payload access in owned containers."""

import argparse
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from bounded_evidence import EvidenceError
from offline_schema import sha
from sbom_evidence import Controller, scanner_access_probe
from shipping_sbom import TOOLS, scanner_preflight


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    controller = Controller(Path(__file__).resolve().parent.parent, args.output,
                            "sbom-scanner-preflight-v1", 240)
    try:
        version, config = scanner_preflight(controller)
        access = scanner_access_probe(controller, TOOLS["syft"])
        controller.receipt.update(status="pass", scanner=version,
                                  scanner_config_sha256=sha(config.encode()),
                                  payload_access_probe=access)
        controller.save()
    except (EvidenceError, OSError, ValueError, KeyError, TypeError) as error:
        controller.fail(str(error))
        print("scanner preflight failed: " + str(error), file=sys.stderr)
        return 1
    print("[ok] pinned scanner version, effective configuration and payload access")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
