#!/usr/bin/env python3
"""Associate owned integration fixtures with preserved normal release bytes."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from bounded_evidence import EvidenceCommands, EvidenceError as QualificationError, json_write
from production_build_inputs import (capture as capture_build_inputs, cargo_configuration_absent,
                                     bounded_file, budget_check, verify_native_policy)
import production_build_policy as policy


PLATFORMS = {
    "linux-x86_64": ("x86_64-unknown-linux-gnu", "linux/x86_64"),
    "linux-aarch64": ("aarch64-unknown-linux-gnu", "linux/aarch64"),
    "macos-aarch64": ("aarch64-apple-darwin", "macos/aarch64"),
}
SUITES = {
    "startup_diagnostics": [
        "bootstrap_uncertainty_and_owned_poison_have_distinct_safe_receipts",
        "dangling_or_nonregular_handoff_refuses_foreground_before_whoami",
        "family_identity_failure_is_private_and_never_creates_runtime",
        "foreground_cancellation_before_bind_leaves_no_listener",
        "foreground_failures_are_bounded_private_and_do_not_bind",
        "one_second_foreground_identity_success_keeps_json_on_stdout",
        "parent_team_failure_keeps_typed_status_but_not_provider_body",
        "parent_username_mismatch_is_structural_and_private",
        "retained_foreign_handoff_blocks_spawn_and_doctor_names_path",
        "scoped_cancellation_has_measured_request_count_and_peak",
        "successful_family_identity_is_not_logged",
        "unresponsive_existing_socket_is_bounded_and_retained"
    ],
    "restart_harness": [
        "auto_setup_daemon_detaches_into_new_session",
        "auto_setup_detached_daemon_state_survives_session_transition",
        "auto_setup_preserves_daemon_when_observation_admission_is_closed",
        "auto_setup_promotes_reuse_to_refreshed_on_bot_username_drift",
        "auto_setup_recovers_from_stale_socket",
        "auto_setup_retains_socket_without_readable_predecessor_pid",
        "auto_setup_reuses_clean_generation_matched_daemon_without_pid_change",
        "auto_setup_stops_zombie_and_respawns",
        "daemon_persists_successful_legacy_migration_while_serving",
        "daemon_serve_remains_attached_to_invoking_session",
        "daemon_serve_with_info_logging_reports_startup_stages",
        "daemon_start_classifies_child_startup_failure",
        "daemon_start_detaches_into_new_session",
        "daemon_start_is_ready_while_legacy_attention_migration_is_slow",
        "daemon_start_recovers_from_stale_socket_and_dead_pid",
        "daemon_start_refuses_on_bot_identity_mismatch",
        "daemon_start_requires_explicit_profile_selection",
        "daemon_start_sweeps_residue_when_child_exits_after_binding",
        "daemon_start_timeout_leaves_no_live_child_and_retry_yields_one_daemon",
        "harness_smoke_daemon_spawns_and_stops_cleanly",
        "notifications_cursor_survives_clean_restart",
        "post_cursor_survives_clean_restart",
        "post_cursor_survives_sigkill_restart",
        "process_count_is_scoped_to_invocation_runtime",
        "stale_cursor_path_preserved_across_restart"
    ],
    "per_043_wait_follow": [
        "bare_follow_without_explicit_sink_is_exit_two",
        "broken_stdout_sink_exits_two_and_releases_the_held_owner",
        "client_eof_releases_the_held_owner",
        "coalesce_against_old_daemon_is_hard_capability",
        "coalesce_mention_ignores_non_mentions",
        "coalesce_omitted_still_emits_one_message_v1",
        "coalesce_two_posts_in_window_are_one_record",
        "follow_emits_backlog_without_rearming_then_terminal",
        "follow_refuses_existing_sink_that_is_not_mode_0600",
        "follow_refuses_symlink_sink_before_daemon_admission",
        "follow_stdout_jsonl_escapes_newline_and_keeps_stderr_static",
        "old_daemon_is_hard_capability_without_fallback",
        "provider_failure_writes_failed_terminal_before_exit_two",
        "replacing_follow_writes_terminal_line_and_releases_owner",
        "sigint_writes_canceled_and_releases_the_held_owner",
        "skewed_invalid_event_is_refused_before_sink_output"
    ],
    "wait_direct_message": [
        "cli_refuses_uuid_user_id_dm_name_self_and_mixes_before_provider",
        "direct_create_403_is_not_a_peer_and_does_not_acquire",
        "direct_create_404_is_not_a_peer",
        "expired_deadline_refuses_before_direct_create",
        "extreme_timeout_is_hard_input_for_oneshot_and_follow",
        "old_daemon_follow_is_hard_capability",
        "old_daemon_is_hard_capability_without_fallback",
        "positional_dm_and_username_wait_share_owner",
        "wait_dm_follow_emits_peer",
        "wait_dm_follow_recovers_racing_post",
        "wait_dm_match_names_peer_and_unknown_is_not_a_peer",
        "wait_dm_recovers_post_that_races_direct_create",
        "wait_dm_skips_bot_authored_post_and_matches_peer",
        "wait_help_documents_dm"
    ],
    "wait_inbox": [
        "bad_cursors_do_not_touch_catalog",
        "broken_stdout_after_one_inbox_line_exits_two",
        "cli_refuses_post_id_after_and_selector_mixes_before_provider",
        "disconnected_ws_refuses_before_catalog",
        "old_coalesce_follow_daemon_is_hard_capability",
        "old_daemon_is_hard_capability",
        "old_follow_daemon_is_hard_capability",
        "sigint_after_one_inbox_line_keeps_acked_cursor"
    ]
}


def regular(path):
    path = Path(path).absolute()
    if not stat.S_ISREG(path.lstat().st_mode):
        raise QualificationError("input is not a regular file")
    return path


def digest(path):
    return hashlib.sha256(regular(path).read_bytes()).hexdigest()


def messages(path):
    result = []
    for line in regular(path).read_text().splitlines():
        if line.strip():
            value = json.loads(line)
            if not isinstance(value, dict):
                raise QualificationError("invalid Cargo message")
            result.append(value)
    if sum(x.get("reason") == "build-finished" for x in result) != 1:
        raise QualificationError("missing or duplicate Cargo completion")
    if not next(x for x in result if x.get("reason") == "build-finished").get("success"):
        raise QualificationError("Cargo build did not succeed")
    return result


def compiler_executable(events, package_id, name, kind, test):
    candidates = [
        x for x in events
        if x.get("reason") == "compiler-artifact"
        and x.get("package_id") == package_id
        and x.get("target", {}).get("name") == name
        and x.get("target", {}).get("kind") == [kind]
        and x.get("profile", {}).get("test") is test
        and x.get("executable")
    ]
    if len(candidates) != 1:
        raise QualificationError("missing or duplicate compiled executable association")
    return candidates[0]


class Driver(EvidenceCommands):
    def __init__(self, args):
        self.args = args
        self.root = args.root.resolve()
        raw_output = args.output.absolute()
        if raw_output.is_symlink():
            raise QualificationError("unsafe evidence directory")
        self.out = raw_output.parent.resolve() / raw_output.name
        if self.out.is_relative_to(self.root):
            raise QualificationError("evidence must be outside the source checkout")
        self.out.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.out.is_symlink() or not self.out.is_dir():
            raise QualificationError("unsafe evidence directory")
        self.deadline = time.monotonic() + args.operation_seconds
        self.receipt_path = self.out / "qualification.json"
        self.receipt = {
            "schema": "production-qualification-v1",
            "status": "incomplete",
            "mode": args.mode,
            "platform": args.platform,
            "expected_commit": args.expected_commit,
            "tag": args.tag,
            "tag_object": args.tag_object,
            "commands": [],
            "suites": [],
            "workflow": {
                "ref": os.environ.get("GITHUB_WORKFLOW_REF"),
                "sha": os.environ.get("GITHUB_WORKFLOW_SHA"),
                "event_sha": os.environ.get("GITHUB_SHA"),
                "run_id": os.environ.get("GITHUB_RUN_ID"),
                "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
            },
        }
        self.save()

    def command(self, name, argv, horizon, env=None, **kwargs):
        if argv and argv[0] == "cargo" and argv[1] != "metadata":
            if not getattr(self, "native_policy", None):
                raise QualificationError("native fixture control requires verified producer association")
            env = policy.child_environment(os.environ, PLATFORMS[self.args.platform][0])
        return super().command(name, argv, horizon, env, **kwargs)

    def git(self, name, *args):
        return self.command(name, ["git", *args], 10).strip()

    def boundary(self, suffix):
        head = self.git("head-" + suffix, "rev-parse", "HEAD")
        tree = self.git("tree-" + suffix, "rev-parse", "HEAD^{tree}")
        if head != self.args.expected_commit:
            raise QualificationError("checkout is not the declared commit")
        if self.git("status-" + suffix, "status", "--porcelain=v1"):
            raise QualificationError("source checkout is dirty")
        if suffix == "before":
            header = self.git("commit-header", "cat-file", "commit", "HEAD").split("\n\n", 1)[0]
            self.receipt["parents"] = re.findall(r"^parent ([0-9a-f]{40})$", header, re.MULTILINE)
            self.receipt.update(commit=head, tree=tree, lock_sha256=digest(self.root / "Cargo.lock"))
        elif tree != self.receipt["tree"] or digest(self.root / "Cargo.lock") != self.receipt["lock_sha256"]:
            raise QualificationError("source or lock changed during qualification")
        self.save()

    def qualify(self):
        policy.admit_selectors(os.environ, PLATFORMS[self.args.platform][0])
        cargo_configuration_absent(self.root)
        self.receipt["cargo_configuration"] = "absent"
        self.boundary("before")
        target, pin_platform = PLATFORMS[self.args.platform]
        rust = self.command("rust", ["rustc", "-vV"], 10)
        if not rust.startswith("rustc 1.89.0 ") or "host: " + target not in rust.splitlines():
            raise QualificationError("wrong Rust compiler or native target")
        self.receipt.update(target=target, rustc=rust.splitlines()[0])
        metadata = json.loads(self.command(
            "metadata", ["cargo", "metadata", "--locked", "--offline", "--format-version", "1",
                         "--filter-platform", target], 30,
        ))
        self.receipt["metadata_messages_sha256"] = digest(self.out / "metadata.log")
        roots = [x for x in metadata["packages"]
                 if x["name"] == "chanvoy" and Path(x["manifest_path"]).resolve() == self.root / "Cargo.toml"]
        if len(roots) != 1:
            raise QualificationError("ambiguous root package")
        package_id = roots[0]["id"]
        normal_events = messages(self.args.normal_build)
        for event in normal_events:
            if event.get("reason") == "compiler-artifact" and (
                    event.get("profile", {}).get("test")
                    or any(kind in ("test", "bench", "example")
                           for kind in event.get("target", {}).get("kind", []))):
                raise QualificationError("fixture or test artifact in normal build capture")
        normal = compiler_executable(normal_events, package_id, "chanvoy", "bin", False)
        profile = normal["profile"]
        if profile.get("opt_level") not in ("1", "2", "3", "s", "z") or profile.get("debug_assertions"):
            raise QualificationError("normal build receipt is not optimized release")
        # The standalone root currently has no production Cargo features.
        # Introducing any requires an explicit updated build contract.
        if normal.get("features") != []:
            raise QualificationError("unexpected root production features")
        original = regular(normal["executable"])
        target_dir = Path(metadata["target_directory"]).resolve()
        if target_dir != (self.root / "target").resolve() or not target_dir.is_relative_to(self.root):
            raise QualificationError("compiler target directory is not owned by this checkout")
        if original.resolve() != target_dir / "release" / "chanvoy":
            raise QualificationError("normal executable is not the declared native release path")
        supplied = regular(self.args.binary)
        expected_hash = self.args.expected_sha256
        normal_observation = bounded_file(original, target_dir, policy.EXECUTABLE_LIMIT,
                                          lambda: budget_check(self))["observation"]
        supplied_observation = bounded_file(supplied, supplied.parent, policy.EXECUTABLE_LIMIT,
                                            lambda: budget_check(self))["observation"]
        if normal_observation["sha256"] != expected_hash or supplied_observation["sha256"] != expected_hash:
            raise QualificationError("normal payload hash mismatch")
        # Mandatory producer binding precedes even pin/suite admission. Original
        # runner paths are read only here for the actual normal executable.
        self.receipt.update(package_id=package_id)
        expected_policy = {**self.receipt, "mode": self.args.mode, "tag": self.args.tag,
                           "tag_object": self.args.tag_object, "platform": self.args.platform}
        self.native_policy, policy_hash, manifest_hash = verify_native_policy(
            self.out, expected_policy, self.root, normal_events, metadata, original,
            lambda: budget_check(self))
        if digest(self.args.normal_build) != self.native_policy["normal_build_messages_sha256"]:
            raise QualificationError("normal producer/compiler message association mismatch")
        policy.bind_payload(self.native_policy, supplied_observation)
        self.receipt.update(native_build_policy="native-build-policy.json", native_build_policy_sha256=policy_hash,
                            native_snapshot_manifest_sha256=manifest_hash,
                            normal_executable=dict(self.native_policy["normal_executable"]))
        version = (self.root / "VERSION").read_text().strip()
        if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?", version):
            raise QualificationError("invalid source version")
        if self.args.mode == "shipping" and self.args.tag != "v" + version:
            raise QualificationError("shipping tag disagrees with source version")
        payload_dir = self.out / "payload"
        payload_dir.mkdir(mode=0o700)
        preserved = payload_dir / ("chanvoy-v" + version + "-" + self.args.platform)
        shutil.copy2(supplied, preserved)
        preserved.chmod(0o555)
        policy.bind_payload(self.native_policy, bounded_file(
            preserved, payload_dir, policy.EXECUTABLE_LIMIT, lambda: budget_check(self))["observation"])
        self.receipt.update(
            payload=preserved.name, payload_sha256=expected_hash,
            normal_build_messages_sha256=digest(self.args.normal_build),
            normal_build_profile=profile, normal_root_features=normal["features"],
            package_id=package_id, qualification_driver_sha256=digest(Path(__file__)),
        )
        self.save()
        with tempfile.TemporaryDirectory(prefix="cv-q-pin-") as temporary:
            config = Path(temporary) / "config"
            runtime = Path(temporary) / "runtime"
            config.mkdir(mode=0o700)
            runtime.mkdir(mode=0o700)
            pin = json.loads(self.command(
                "build-pin", [str(preserved), "--profile", "synthetic-build-pin",
                              "version", "--extended", "--json"], 15,
                {"PATH": os.environ["PATH"], "CHANVOY_CONFIG_DIR": str(config),
                 "CHANVOY_RUNTIME_DIR": str(runtime)},
            ))
        for value in (pin, pin.get("cli", {})):
            if (value.get("commit") != self.args.expected_commit or value.get("dirty") is not False
                    or value.get("platform") != pin_platform or value.get("rustc") != self.receipt["rustc"]
                    or value.get("version") != version):
                raise QualificationError("stale or mismatched production build pin")
        if pin.get("daemon") is not None or pin.get("generation_scored") is not False:
            raise QualificationError("isolated pin unexpectedly scored a daemon")
        json_write(self.out / "build-pin.json", pin)
        inputs_path = capture_build_inputs(self, normal_events, metadata, preserved)
        self.receipt.update(normal_build_inputs=inputs_path.name, normal_build_inputs_sha256=digest(inputs_path))
        self.save()
        suites = ["startup_diagnostics", "per_043_wait_follow"]
        if self.args.platform.startswith("linux-"):
            suites.insert(1, "restart_harness")
        else:
            suites.extend(["wait_direct_message", "wait_inbox"])
        compile_argv = ["cargo", "test", "--locked", "--offline", "--package", "chanvoy", "--no-run",
                        "--message-format=json"]
        for suite in suites:
            compile_argv.extend(["--test", suite])
        self.command("fixture-compile", compile_argv, 300)
        compiled_path = self.out / "fixture-compile.log"
        fixture_events = messages(compiled_path)
        fixture_cli = regular(compiler_executable(fixture_events, package_id, "chanvoy", "bin", False)["executable"])
        if fixture_cli.resolve() != target_dir / "debug" / "chanvoy":
            raise QualificationError("unexpected compiler-associated fixture CLI path")
        executables = {}
        executable_hashes = {}
        for suite in suites:
            exe = regular(compiler_executable(fixture_events, package_id, suite, "test", True)["executable"])
            if not exe.resolve().is_relative_to(target_dir):
                raise QualificationError("fixture executable is outside the declared target directory")
            executables[suite] = exe
            executable_hashes[suite] = digest(exe)
            listing = self.command(suite + "-list", [str(exe), "--list"], 10)
            if digest(exe) != executable_hashes[suite]:
                raise QualificationError("compiled fixture changed during enumeration")
            discovered = sorted(re.findall(r"^(\S+): test$", listing, re.MULTILINE))
            expected = SUITES[suite]
            # Non-ignored helper smoke tests are deliberately not selected.
            allowed_extra = {
                "wait_direct_message": ["rpc_request_helper_compiles_for_wait_dm"],
                "wait_inbox": ["help_names_inbox_without_channel_id"],
            }.get(suite, [])
            if discovered != sorted(expected + allowed_extra):
                raise QualificationError("compiled exact case list changed: " + suite)
        if digest(preserved) != expected_hash:
            raise QualificationError("preserved payload changed during fixture compilation")
        backup = self.out / "original-fixture-cli"
        shutil.copy2(fixture_cli, backup)
        self.receipt["original_fixture_cli_sha256"] = digest(backup)
        try:
            shutil.copy2(preserved, fixture_cli)
            # The preserved payload stays read-only. The independently copied
            # harness path must be writable for byte-verified restoration.
            fixture_cli.chmod(0o755)
            if digest(fixture_cli) != expected_hash:
                raise QualificationError("fixture CLI is not the preserved normal payload")
            self.receipt.update(fixture_cli=str(fixture_cli), fixture_cli_sha256=expected_hash)
            self.save()
            for suite in suites:
                exe = executables[suite]
                if digest(exe) != executable_hashes[suite]:
                    raise QualificationError("compiled fixture changed before execution")
                record = {"name": suite, "cases": SUITES[suite], "threads": 8,
                          "executable_sha256": executable_hashes[suite], "status": "incomplete"}
                self.receipt["suites"].append(record)
                self.save()
                argv = [str(exe), "--test-threads=8", "--nocapture"]
                if suite != "startup_diagnostics":
                    argv.append("--ignored")
                text = self.command(suite, argv, 120)
                passed_names = sorted(re.findall(r"^test (\S+) \.\.\. ok$", text, re.MULTILINE))
                summaries = re.findall(r"^test result: ok\. (\d+) passed; 0 failed;", text, re.MULTILINE)
                if passed_names != SUITES[suite] or summaries != [str(len(SUITES[suite]))]:
                    raise QualificationError("missing or zero-case successful execution: " + suite)
                if digest(exe) != record["executable_sha256"] or digest(fixture_cli) != expected_hash:
                    raise QualificationError("executed fixture or payload changed")
                record["status"] = "pass"
                self.save()
        except (QualificationError, OSError, ValueError, KeyError, TypeError, StopIteration) as error:
            # Restoration may fail too; preserve the actual qualification
            # failure before finally runs instead of replacing its cause.
            self.fail(str(error))
            raise
        finally:
            shutil.copy2(backup, fixture_cli)
            restored = digest(fixture_cli)
            self.receipt["restored_fixture_cli_sha256"] = restored
            self.save()
            if restored != self.receipt["original_fixture_cli_sha256"]:
                raise QualificationError("fixture CLI restoration mismatch")
            backup.unlink()
        if any(digest(path) != expected_hash for path in (preserved, original, supplied)):
            raise QualificationError("normal payload changed during qualification")
        if digest(inputs_path) != self.receipt["normal_build_inputs_sha256"]:
            raise QualificationError("normal build input receipt changed")
        captured = json.loads(inputs_path.read_text())
        inputs = {"normal-build.jsonl": captured["normal_build_messages_sha256"],
                  "metadata.log": captured["metadata_sha256"], "normal-Cargo.lock": captured["lock_sha256"],
                  **captured["native"]["logs_sha256"]}
        if any(digest(self.out / name) != value for name, value in inputs.items()):
            raise QualificationError("normal build evidence changed during qualification")
        _, final_policy_hash, final_manifest_hash = verify_native_policy(
            self.out, expected_policy, self.root, normal_events, metadata, preserved,
            lambda: budget_check(self))
        if (final_policy_hash != self.receipt["native_build_policy_sha256"]
                or final_manifest_hash != self.receipt["native_snapshot_manifest_sha256"]):
            raise QualificationError("native producer evidence changed during qualification")
        self.boundary("after")
        self.receipt["status"] = "pass"
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--normal-build", required=True, type=Path)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--platform", required=True, choices=PLATFORMS)
    parser.add_argument("--mode", required=True, choices=["local", "candidate", "shipping"])
    parser.add_argument("--tag")
    parser.add_argument("--tag-object")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--operation-seconds", type=float, default=840)
    args = parser.parse_args()
    if (not re.fullmatch(r"[0-9a-f]{40}", args.expected_commit)
            or not re.fullmatch(r"[0-9a-f]{64}", args.expected_sha256)
            or not 0 < args.operation_seconds <= 840):
        parser.error("invalid identity or bounded operation horizon")
    if args.mode == "shipping" and (
            not args.tag or not re.fullmatch(r"[0-9a-f]{40}", args.tag_object or "")):
        parser.error("shipping requires the already-verified tag and tag object")
    if args.mode == "local" and (args.tag or args.tag_object):
        parser.error("local mode is an untagged final-candidate proof")
    if args.mode == "candidate" and (
            args.platform != "linux-x86_64" or args.tag or args.tag_object):
        parser.error("candidate mode is the untagged native x86 proof")
    driver = None
    try:
        driver = Driver(args)
        driver.qualify()
    except (QualificationError, OSError, ValueError, KeyError, TypeError, StopIteration) as error:
        if driver:
            driver.fail(str(error))
        print("production qualification failed: " + str(error))
        return 1
    print("production qualification passed for " + args.platform)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
