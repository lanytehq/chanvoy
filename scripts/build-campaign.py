#!/usr/bin/env python3
"""Bound ordinary Cargo campaigns without changing production provenance policy."""

import argparse
import contextlib
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import sys

GIB = 1024 ** 3
FD_ENV = "CHANVOY_BUILD_CAMPAIGN_FD"
COMPACT = {"CARGO_PROFILE_DEV_DEBUG": "0", "CARGO_PROFILE_TEST_DEBUG": "0",
           "CARGO_INCREMENTAL": "0"}
SELECTORS = {"CARGO_TARGET_DIR", "CARGO_BUILD_TARGET_DIR", "CARGO_BUILD_BUILD_DIR"}


class Refusal(Exception):
    pass


def default_cache():
    if sys.platform == "darwin":
        return Path.home() / "Library/Caches/chanvoy-cargo"
    return Path(os.environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache"))) / "chanvoy-cargo"


def report(event, **fields):
    print(json.dumps({"build_campaign": event, **fields}, sort_keys=True), file=sys.stderr, flush=True)


def usage(root):
    """Allocated bytes, counting hard links once; never traverse symlinks."""
    total, seen = 0, set()
    for lane in ("dev", "controls"):
        path = root / lane
        if path.is_symlink():
            raise Refusal("cache lane must not be a symlink")
        if not path.exists():
            continue

        def unreadable(error):
            raise error

        for directory, dirs, files in os.walk(path, followlinks=False, onerror=unreadable):
            for name in [".", *dirs, *files]:
                item = Path(directory) / name
                meta = item.lstat()
                key = (meta.st_dev, meta.st_ino)
                if key not in seen:
                    total += meta.st_blocks * 512
                    seen.add(key)
    return total


def check_space(state):
    root = Path(state["cache"])
    used = usage(root)
    free = shutil.disk_usage(root).free
    report("space", cache=str(root), used_bytes=used, free_bytes=free,
           budget_bytes=state["budget"], minimum_free_bytes=state["min_free"])
    if used >= state["budget"]:
        raise Refusal("development/control cache budget reached; review retained outputs before building")
    if free < state["min_free"]:
        raise Refusal("insufficient free space for another build stage")


def caller_environment():
    for key in SELECTORS:
        if key in os.environ:
            raise Refusal(f"caller selector {key} conflicts with managed placement")
    for key, value in os.environ.items():
        if key.startswith("CARGO_PROFILE_") or key == "CARGO_INCREMENTAL":
            if COMPACT.get(key) != value:
                raise Refusal(f"caller profile {key} conflicts with the compact development profile")


def inherited():
    try:
        fd = int(os.environ[FD_ENV])
        meta = os.fstat(fd)
        if not stat.S_ISREG(meta.st_mode):
            raise ValueError("not regular")
        state = json.loads(os.pread(fd, 16384, 0))
        path_meta = (Path(state["cache"]) / "campaign.lock").lstat()
        if (meta.st_dev, meta.st_ino) != (path_meta.st_dev, path_meta.st_ino):
            raise ValueError("lock replaced")
        if state["lane"] not in ("dev", "controls") or state["schema"] != 1:
            raise ValueError("invalid context")
        # This descriptor is the inherited open-file description, not a new
        # pathname open. It remains locked while the parent or child owns it.
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd, state
    except (KeyError, ValueError, OSError, TypeError) as error:
        raise Refusal("no valid inherited campaign; start through a public Make target or 'run'") from error


def execute(argv, fd, environment):
    if not argv:
        raise Refusal("missing command")
    child = subprocess.Popen(argv, env=environment, pass_fds=(fd,), start_new_session=True)
    previous = {}
    interrupted = []

    def forward(signum, _frame):
        # Keep the lease and wait for the actual direct child after forwarding.
        interrupted.append(signum)
        with contextlib.suppress(ProcessLookupError):
            os.killpg(child.pid, signum)

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, forward)
        code = child.wait()
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    if code == 0 and interrupted:
        return 128 + interrupted[0]
    return code if code >= 0 else 128 - code


def observed_execute(argv, fd, state, environment):
    check_space(state)
    code = execute(argv, fd, environment)
    try:
        check_space(state)
    except (Refusal, OSError) as error:
        report("refused", reason=str(error))
        return code or 2
    return code


def run(args):
    caller_environment()
    if args.command and Path(args.command[0]).name == "cargo":
        args.command = [sys.executable, "-B", str(Path(__file__).resolve()), "step", "--", *args.command]
    if FD_ENV in os.environ:
        fd, state = inherited()
        if any(value is not None for value in (args.cache_root, args.budget_gib, args.min_free_gib)):
            raise Refusal("nested campaign cannot replace placement or limits")
        if args.lane is not None and args.lane != state["lane"]:
            raise Refusal("nested campaign cannot switch lanes")
        return observed_execute(args.command, fd, state, dict(os.environ))
    root = (args.cache_root or default_cache()).expanduser().absolute()
    if root.is_symlink():
        raise Refusal("cache root must not be a symlink")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root = root.resolve()
    state = {"schema": 1, "cache": str(root), "lane": args.lane or "dev", "pid": os.getpid(),
             "source": str(Path.cwd()), "budget": int((12 if args.budget_gib is None else args.budget_gib) * GIB),
             "min_free": int((20 if args.min_free_gib is None else args.min_free_gib) * GIB)}
    fd = os.open(root / "campaign.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise Refusal("another campaign owns this cache; wait for its actual completion") from error
        os.ftruncate(fd, 0)
        os.write(fd, json.dumps(state).encode())
        os.fsync(fd)
        report("started", **state)
        environment = dict(os.environ, **{FD_ENV: str(fd)})
        return observed_execute(args.command, fd, state, environment)
    finally:
        # Do not unlink or explicitly unlock: descendants may still hold the
        # inherited description if the supervising process is interrupted.
        os.close(fd)


def source_id():
    root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], text=True).strip())
    return hashlib.sha256(json.dumps([root, head, dirty]).encode()).hexdigest()


def step(args):
    fd, state = inherited()
    caller_environment()
    argv = args.command
    if not argv or Path(argv[0]).name != "cargo":
        raise Refusal("managed steps must invoke cargo")
    if any(a == "--config" or a.startswith(("--config=", "--target-dir")) for a in argv):
        raise Refusal("per-command configuration cannot override managed placement")
    target = Path(state["cache"]) / state["lane"]
    check_space(state)
    target.mkdir(exist_ok=True, mode=0o700)
    environment = dict(os.environ, **COMPACT, CARGO_TARGET_DIR=str(target),
                       CHANVOY_BUILD_SOURCE_ID=source_id())
    toolchain = [argv[1]] if len(argv) > 1 and argv[1].startswith("+") else []
    metadata = subprocess.check_output(
        [argv[0], *toolchain, "metadata", "--no-deps", "--offline", "--format-version", "1"],
        env=environment, text=True)
    if Path(json.loads(metadata)["target_directory"]).resolve() != target:
        raise Refusal("Cargo resolved a different target directory")
    report("step", lane=state["lane"], target=str(target), command=argv)
    return observed_execute(argv, fd, state, environment)


def local_release():
    if FD_ENV in os.environ or any(key in os.environ for key in SELECTORS):
        raise Refusal("release/install requires an unmanaged local target; leave the development campaign")
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--offline", "--format-version", "1"], text=True))
    if Path(metadata["target_directory"]).resolve() != Path.cwd().resolve() / "target":
        raise Refusal("release/install expects the local target directory")
    return 0


def limit(value):
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("limit must be finite and nonnegative")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="operation", required=True)
    runner = commands.add_parser("run", help="own a complete ordinary build/test campaign")
    runner.add_argument("--lane", choices=("dev", "controls"))
    runner.add_argument("--cache-root", type=Path)
    runner.add_argument("--budget-gib", type=limit)
    runner.add_argument("--min-free-gib", type=limit)
    runner.add_argument("command", nargs=argparse.REMAINDER)
    stage = commands.add_parser("step", help="run one Cargo stage inside an inherited campaign")
    stage.add_argument("command", nargs=argparse.REMAINDER)
    commands.add_parser("check", help="validate inherited campaign and available storage")
    commands.add_parser("local-release", help="refuse a redirected release/install artifact path")
    args = parser.parse_args()
    if hasattr(args, "command") and args.command[:1] == ["--"]:
        args.command = args.command[1:]
    try:
        if args.operation == "run":
            return run(args)
        if args.operation == "step":
            return step(args)
        if args.operation == "local-release":
            return local_release()
        _, state = inherited()
        check_space(state)
        return 0
    except (Refusal, OSError, subprocess.CalledProcessError, ValueError) as error:
        report("refused", reason=str(error))
        return 2


if __name__ == "__main__":
    sys.exit(main())
