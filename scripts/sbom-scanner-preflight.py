#!/usr/bin/env python3
"""Qualify the pinned scanner's effective configuration in owned containers."""

import argparse
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from bounded_evidence import EvidenceError
from offline_schema import sha
from sbom_evidence import Controller
from shipping_sbom import scanner_preflight


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    controller = Controller(Path(__file__).resolve().parent.parent, args.output,
                            "sbom-scanner-preflight-v1", 240)
    try:
        version, config = scanner_preflight(controller)
        controller.receipt.update(status="pass", scanner=version,
                                  scanner_config_sha256=sha(config.encode()))
        controller.save()
    except (EvidenceError, OSError, ValueError, KeyError, TypeError) as error:
        controller.fail(str(error))
        print("scanner preflight failed: " + str(error), file=sys.stderr)
        return 1
    print("[ok] pinned scanner version and effective configuration")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
