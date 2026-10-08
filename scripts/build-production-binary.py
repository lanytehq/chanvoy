#!/usr/bin/env python3
"""Build one fresh standalone release payload with bundled native receipts."""

import argparse
import json
import os
from pathlib import Path
import re
import sys
import time

sys.dont_write_bytecode = True
from bounded_evidence import EvidenceCommands, EvidenceError
import production_build_policy as policy
from production_build_inputs import (bounded_file, budget_check, cargo_configuration_absent,
                                     capture_native_snapshots, digest, policy_source_hashes,
                                     safe_owned, verify_native_policy)


class Producer(EvidenceCommands):
    def __init__(self, args):
        self.args = args
        self.root = args.root.resolve()
        output = args.output.absolute()
        if output.is_symlink():
            raise EvidenceError("unsafe producer evidence directory")
        self.out = output.parent.resolve() / output.name
        if self.out.is_relative_to(self.root):
            raise EvidenceError("evidence must be outside the source checkout")
        self.out.mkdir(parents=True, exist_ok=True, mode=0o700)
        for name in ("native-build-policy.json", "native-snapshots.json", "native-snapshots", "normal-build.jsonl",
                     "normal-build.log", "normal-build.stderr.log", "normal-Cargo.lock", "metadata.log"):
            if (self.out / name).exists() or (self.out / name).is_symlink():
                raise EvidenceError("producer evidence already exists")
        self.deadline = time.monotonic() + args.operation_seconds
        self.receipt_path = self.out / "native-build-policy.json"
        self.receipt = {"schema": "native-build-policy-v1", "status": "incomplete", "policy": "bundled-source-v1",
                        "mode": args.mode, "platform": args.platform, "target": policy.PLATFORMS[args.platform][0],
                        "expected_commit": args.expected_commit, "tag": args.tag, "tag_object": args.tag_object,
                        "commands": [], "workflow": dict(zip(("ref", "sha", "event_sha", "run_id", "run_attempt"),
                            (os.environ.get(k) for k in ("GITHUB_WORKFLOW_REF", "GITHUB_WORKFLOW_SHA", "GITHUB_SHA",
                                                        "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT"))))}
        self.save()

    def boundary(self, suffix):
        head = self.command("head-" + suffix, ["git", "rev-parse", "HEAD"], 10).strip()
        tree = self.command("tree-" + suffix, ["git", "rev-parse", "HEAD^{tree}"], 10).strip()
        clean = self.command("status-" + suffix, ["git", "status", "--porcelain=v1"], 10).strip()
        if head != self.args.expected_commit or clean:
            raise EvidenceError("producer source boundary mismatch")
        if suffix == "before":
            self.receipt.update(commit=head, tree=tree, lock_sha256=digest(self.root / "Cargo.lock"),
                                source_files_sha256=policy_source_hashes(self.root))
        elif (tree != self.receipt["tree"] or digest(self.root / "Cargo.lock") != self.receipt["lock_sha256"]
              or policy_source_hashes(self.root) != self.receipt["source_files_sha256"]):
            raise EvidenceError("producer source changed during operation")
        self.save()

    def produce(self):
        target = self.receipt["target"]
        # ALL caller/config selectors are refused before even fetch/metadata.
        child = policy.child_environment(os.environ, target)
        cargo_configuration_absent(self.root)
        effective_target = self.root / "target"
        if effective_target.exists() or effective_target.is_symlink():
            raise EvidenceError("receipt build requires an absent owned target directory")
        self.receipt.update(cargo_configuration="absent", fresh_target_at_entry=True,
                            target_directory=str(effective_target), source_root=str(self.root), control=dict(policy.CONTROL))
        self.boundary("before")
        rust = self.command("rust", ["rustc", "-vV"], 10)
        if not rust.startswith("rustc 1.89.0 ") or "host: " + target not in rust.splitlines():
            raise EvidenceError("wrong Rust compiler or native target")
        self.receipt["rustc"] = rust.splitlines()[0]
        if self.args.mode in ("candidate", "shipping") and not all(self.receipt["workflow"].values()):
            raise EvidenceError("missing hosted producer workflow identity")
        if self.args.mode == "shipping" and self.args.tag != "v" + (self.root / "VERSION").read_text().strip():
            raise EvidenceError("shipping tag disagrees with source version")
        self.command("fetch", ["cargo", "fetch", "--locked"], 120, child)
        metadata = json.loads(self.command("metadata", ["cargo", "metadata", "--locked", "--offline",
                              "--format-version", "1", "--filter-platform", target], 30, child))
        if metadata.get("target_directory") != str(effective_target):
            raise EvidenceError("effective Cargo target directory mismatch")
        roots = [p for p in metadata["packages"] if p.get("name") == "chanvoy"
                 and p.get("manifest_path") == str(self.root / "Cargo.toml")]
        if len(roots) != 1:
            raise EvidenceError("ambiguous producer root package")
        self.receipt["package_id"] = roots[0]["id"]
        self.command("normal-build", ["cargo", "build", "--release", "--locked", "--package", "chanvoy",
                                     "--message-format=json-render-diagnostics"], 600, child)
        normal = self.out / "normal-build.jsonl"
        (self.out / "normal-build.log").rename(normal)
        self.receipt["commands"][-1]["stdout"] = normal.name
        # Capture actual normal executable immediately after the normal build.
        check = lambda: budget_check(self)
        stream = bounded_file(normal, self.out, 64 * 1024 * 1024, check, text=True)
        events = [json.loads(line) for line in stream["text"].splitlines() if line.strip()]
        event = policy.executable_event(events, roots[0]["id"], str(effective_target))
        executable = Path(event["executable"])
        observed = bounded_file(executable, effective_target, policy.EXECUTABLE_LIMIT, check)["observation"]
        self.receipt.update(normal_executable=observed, normal_build_messages_sha256=stream["observation"]["sha256"],
                            normal_build_stderr_sha256=digest(self.out / "normal-build.stderr.log"))
        # Preserve the locked native source association alongside portable evidence.
        with (self.out / "normal-Cargo.lock").open("xb") as output:
            output.write(safe_owned(self.root / "Cargo.lock", self.root).read_bytes())
        package, records, manifest_hash = capture_native_snapshots(self, events, metadata)
        self.receipt.update(native_package=package, native_events=records, native_snapshot_manifest="native-snapshots.json",
                            native_snapshot_manifest_sha256=manifest_hash)
        policy.bind_payload(self.receipt, bounded_file(executable, effective_target, policy.EXECUTABLE_LIMIT, check)["observation"])
        self.boundary("after")
        self.receipt["status"] = "pass"
        self.save()
        verify_native_policy(self.out, self.receipt, self.root, events, metadata, executable, check)
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--platform", required=True, choices=policy.PLATFORMS)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--mode", required=True, choices=("local", "candidate", "shipping"))
    parser.add_argument("--tag")
    parser.add_argument("--tag-object")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--operation-seconds", type=float, default=710)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_commit) or not 0 < args.operation_seconds <= 710:
        parser.error("invalid source identity or bounded producer horizon")
    if args.mode == "shipping":
        if not args.tag or not re.fullmatch(r"[0-9a-f]{40}", args.tag_object or ""):
            parser.error("shipping requires verified tag and tag object")
    elif args.tag or args.tag_object:
        parser.error("untagged producer mode cannot carry shipping tag fields")
    if args.mode == "candidate" and args.platform != "linux-x86_64":
        parser.error("candidate mode requires the hosted native x86 target")
    producer = None
    try:
        producer = Producer(args)
        producer.produce()
    except (EvidenceError, OSError, ValueError, KeyError, TypeError) as error:
        if producer:
            producer.fail(str(error))
        print("production build failed: " + str(error))
        return 1
    print("production build receipt passed for " + args.platform)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
