"""Bounded owned-command evidence collection shared by qualification adapters."""

import json
import os
from pathlib import Path
import signal
import subprocess
import time


class EvidenceError(Exception):
    pass


CLEANUP_SECONDS = 10
TERM_REAP_SECONDS = 5


def json_write(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


class EvidenceCommands:
    """Caller supplies its owned root, output, deadline, receipt and path."""

    def save(self):
        json_write(self.receipt_path, self.receipt)

    def fail(self, cause):
        self.receipt["status"] = "failed"
        first = self.receipt.setdefault("failure", cause)
        if cause != first:
            self.receipt.setdefault("secondary_failures", []).append(cause)
        self.save()

    def expired_command(self, child, record, started):
        cause = "owned command exceeded its process horizon"
        cleanup_deadline = min(self.deadline, time.monotonic() + CLEANUP_SECONDS)
        record.update(process_horizon_expired=True, owned_cleanup_confirmed=False,
                      exit=None, child_collected=False, cleanup_deadline=cleanup_deadline,
                      elapsed_seconds=time.monotonic() - started, teardown=[])
        # Persist the first failure and unknown cleanup BEFORE any signal/reap.
        self.fail(cause)
        for stage, sig, cap in (("term", signal.SIGTERM, TERM_REAP_SECONDS),
                                ("kill", signal.SIGKILL, CLEANUP_SECONDS)):
            remaining = cleanup_deadline - time.monotonic()
            if remaining <= 0:
                record["cleanup_horizon_expired"] = True
                break
            step = {"stage": stage}
            record["teardown"].append(step)
            # Only the process group created by this Popen invocation is used.
            # Even a collected child does not prove whole-group death.
            try:
                os.killpg(child.pid, sig)
            except OSError as error:
                step["signal_error_class"] = type(error).__name__
                break
            remaining = cleanup_deadline - time.monotonic()
            if remaining <= 0:
                record["cleanup_horizon_expired"] = True
                break
            step["reap_horizon_seconds"] = min(cap, remaining)
            self.save()
            try:
                record["exit"] = child.wait(timeout=step["reap_horizon_seconds"])
                record["child_collected"] = True
                break
            except subprocess.TimeoutExpired:
                step["reap_horizon_expired"] = True
                if stage == "kill":
                    record["cleanup_horizon_expired"] = True
            except OSError as error:
                step["reap_error_class"] = type(error).__name__
                break
        record["elapsed_seconds"] = time.monotonic() - started
        self.save()
        raise EvidenceError(cause)

    def command(self, name, argv, horizon, env=None):
        # Cleanup is reserved INSIDE the total operation deadline. A depleted
        # execution budget starts no further command and never renews cleanup.
        remaining = self.deadline - time.monotonic() - CLEANUP_SECONDS
        if remaining <= 0:
            cause = "owned operation horizon expired"
            self.fail(cause)
            raise EvidenceError(cause)
        horizon = min(horizon, remaining)
        log = self.out / (name + ".log")
        stderr_log = self.out / (name + ".stderr.log")
        record = {"stage": name, "argv": argv, "horizon_seconds": horizon,
                  "stdout": log.name, "stderr": stderr_log.name}
        self.receipt["commands"].append(record)
        self.save()
        started = time.monotonic()
        with log.open("wb") as output, stderr_log.open("wb") as errors:
            child = subprocess.Popen(
                argv, cwd=self.root, env=env, stdout=output,
                stderr=errors, start_new_session=True,
            )
            try:
                code = child.wait(timeout=horizon)
            except subprocess.TimeoutExpired:
                self.expired_command(child, record, started)
        record.update(exit=code, elapsed_seconds=time.monotonic() - started)
        self.save()
        if code:
            cause = "owned command failed: " + name
            self.fail(cause)
            raise EvidenceError(cause)
        return log.read_text()
