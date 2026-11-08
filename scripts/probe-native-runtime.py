#!/usr/bin/env python3
"""Read-only host capability probe plus an isolated Unix-listener reference fixture.

Does not inspect credentials, signal daemons, or alter installed runtime state.
This is capability evidence, not a qualified parser or an absence/death proof.
Only counts/error codes are emitted; raw socket-table contents are not retained.
"""
import array
import argparse
import ctypes
import fcntl
import json
import os
import platform
import socket
import subprocess
import sys
import tempfile

CAP = 8 * 1024 * 1024
DARWIN_TABLES = ("net.local.stream.pcblist", "net.local.stream.pcblist64", "net.local.stream.pcblist_n")
NATIVE_OBSERVER = None


def table(name):
    if name == "/proc/net/unix":
        try:
            with open(name, "rb") as source:
                data = source.read(CAP + 1)
        except OSError as error:
            return None, {"stage": "read", "errno": error.errno}
        if len(data) > CAP:
            return None, {"stage": "read", "reason": "outside-byte-budget"}
        return data, {"bytes": len(data), "scope": "current_network_namespace"}
    native = ctypes.CDLL(None, use_errno=True)
    native.sysctlbyname.argtypes = [ctypes.c_char_p, ctypes.c_void_p,
                                    ctypes.POINTER(ctypes.c_size_t), ctypes.c_void_p, ctypes.c_size_t]
    size = ctypes.c_size_t()
    ctypes.set_errno(0)
    if native.sysctlbyname(name.encode(), None, ctypes.byref(size), None, 0):
        return None, {"stage": "size", "errno": ctypes.get_errno()}
    if not 0 < size.value <= CAP:
        return None, {"stage": "size", "reason": "outside-byte-budget", "bytes": size.value}
    data = ctypes.create_string_buffer(size.value)
    capacity = size.value
    ctypes.set_errno(0)
    if native.sysctlbyname(name.encode(), data, ctypes.byref(size), None, 0):
        return None, {"stage": "read", "errno": ctypes.get_errno()}
    if size.value > capacity:
        return None, {"stage": "read", "reason": "oversized-result"}
    return data.raw[:size.value], {"bytes": size.value}


def namespace_identity(fd=None):
    metadata = os.stat("/proc/self/ns/net") if fd is None else os.fstat(fd)
    return [metadata.st_dev, metadata.st_ino]


def observe(stage, needle, socket_inode=None, namespace_fd=None):
    rows = []
    tables = DARWIN_TABLES if platform.system() == "Darwin" else ("/proc/net/unix",)
    for name in tables:
        if namespace_fd is not None and namespace_identity(namespace_fd) != namespace_identity():
            raise RuntimeError("fixture network namespace changed before observation")
        data, facts = table(name)
        if namespace_fd is not None and namespace_identity(namespace_fd) != namespace_identity():
            raise RuntimeError("fixture network namespace changed during observation")
        row = {"fixture_phase": stage, "table": name, **facts}
        if namespace_fd is not None:
            row["namespace_matches_retained_reference"] = True
        if data is not None:
            # Deliberately a visibility diagnostic, not a parsed table verdict.
            row["fixture_path_occurrences"] = data.count(needle)
            if name == "/proc/net/unix" and socket_inode is not None:
                # Diagnostic only: match the socket object's inode, including
                # rows without paths. No UID, state or pathname filter.
                lines = data.splitlines()
                complete = bool(lines) and lines[0].split() == [
                    b"Num", b"RefCount", b"Protocol", b"Flags", b"Type", b"St", b"Inode", b"Path"]
                count = 0
                for line in lines[1:]:
                    fields = line.split(None, 7)
                    if len(fields) < 7 or not fields[6].isdigit():
                        complete = False
                        continue
                    count += int(fields[6]) == socket_inode
                row["fixture_socket_inode_occurrences"] = count
                row["inode_scan_complete"] = complete
            if platform.system() == "Darwin":
                lengths = {}
                offset = 0
                while offset + 4 <= len(data):
                    length = int.from_bytes(data[offset:offset + 4], "little")
                    if length < 4 or length > len(data) - offset:
                        break
                    lengths[str(length)] = lengths.get(str(length), 0) + 1
                    offset += length
                row["record_length_counts"] = lengths
                row["length_walk_consumed_all"] = offset == len(data)
        rows.append(row)
    print(json.dumps({"observations": rows}), flush=True)
    if NATIVE_OBSERVER is not None:
        result = subprocess.run(
            [NATIVE_OBSERVER, "--fixture-path", os.fsdecode(needle)],
            env={}, stdin=subprocess.DEVNULL, capture_output=True, timeout=5)
        # This optional adapter exercises the actual Rust observer against the
        # fixture. No table-byte export, private runtime or lifecycle mutation.
        if len(result.stdout) > 4096:
            raise RuntimeError("native observer exceeded output budget")
        decoded = json.loads(result.stdout)
        print(json.dumps({"fixture_phase": stage, "native_decoder": decoded}), flush=True)
        expected = "not_observed" if stage.endswith("closed") else "present"
        if (result.returncode != 0 or decoded.get("qualification") is not False
                or decoded.get("observation") != expected):
            raise RuntimeError("native observer refused or disagreed with fixture phase")


def receive_descriptor(receiver):
    width = array.array("i").itemsize
    body, controls, flags, _ = receiver.recvmsg(1, socket.CMSG_SPACE(width))
    received = []
    for level, kind, payload in controls:
        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
            descriptors = array.array("i")
            descriptors.frombytes(payload[:len(payload) - len(payload) % width])
            received.extend(descriptors)
    if body != b"L" or flags & socket.MSG_CTRUNC or len(received) != 1:
        for fd in received:
            os.close(fd)
        raise RuntimeError("invalid fixture descriptor transfer")
    return received[0]


def listener_child(path, descriptor):
    # Only invoked by the parent below, with its own isolated temporary path and
    # explicitly inherited socketpair endpoint. No installed daemon participates.
    with socket.socket(fileno=descriptor) as sender, \
            socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener, \
            open(path + ".lease", "wb") as lease:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        listener.bind(path)
        listener.listen(1)
        print(json.dumps({"socket_inode": os.fstat(listener.fileno()).st_ino,
                          "network_namespace": namespace_identity() if platform.system() == "Linux" else None}),
              flush=True)
        sender.sendmsg([b"L"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                array.array("i", [listener.fileno()]))])
    return 0


def creator_exit_fixture():
    with tempfile.TemporaryDirectory(prefix="cv-exit-", dir="/tmp") as root:
        path = os.path.join(root, "listener")
        sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
        receiver.settimeout(2)
        received = None
        child = None
        namespace_fd = None
        try:
            if platform.system() == "Linux":
                namespace_fd = os.open("/proc/self/ns/net", os.O_RDONLY | os.O_CLOEXEC)
            child = subprocess.Popen(
                [sys.executable, __file__, "--listener-child", path, str(sender.fileno())],
                pass_fds=(sender.fileno(),), env={}, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            sender.close()
            if child.wait(timeout=5) != 0:
                raise RuntimeError("isolated listener creator failed")
            claim = json.loads(child.stdout.read(1024))
            socket_inode = claim["socket_inode"]
            if not isinstance(socket_inode, int) or (platform.system() == "Linux" and socket_inode <= 0):
                raise RuntimeError("fixture socket inode unavailable")
            if namespace_fd is not None and claim["network_namespace"] != namespace_identity(namespace_fd):
                raise RuntimeError("fixture creator network namespace mismatch")
            os.unlink(path)
            with open(path + ".lease", "rb") as lease:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            print(json.dumps({"fixture_creator_exited": True,
                              "fixture_lease_free": True, "qualification": False}), flush=True)
            observe("creator_exited_listener_queued", os.fsencode(path), socket_inode, namespace_fd)
            received = receive_descriptor(receiver)
            if os.fstat(received).st_ino != socket_inode:
                raise RuntimeError("fixture received a different socket inode")
            observe("creator_exited_listener_received", os.fsencode(path), socket_inode, namespace_fd)
            os.close(received)
            received = None
            observe("creator_exited_final_reference_closed", os.fsencode(path), socket_inode, namespace_fd)
        finally:
            if child is not None:
                if child.poll() is None:
                    # Timeout cleanup is limited to this Popen-owned fixture.
                    child.kill()
                    child.wait()
                if child.stderr is not None:
                    child.stderr.close()
                if child.stdout is not None:
                    child.stdout.close()
            if received is not None:
                os.close(received)
            if namespace_fd is not None:
                os.close(namespace_fd)
            sender.close()
            receiver.close()


def main():
    global NATIVE_OBSERVER
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-observer", help="compiled native-listener-observe fixture adapter")
    args = parser.parse_args()
    if args.native_observer is not None:
        NATIVE_OBSERVER = os.path.abspath(args.native_observer)
    print(json.dumps({"probe": "native-runtime-capability", "system": platform.system(),
                      "release": platform.release(), "machine": platform.machine(),
                      "uid": os.geteuid(), "qualification": False}), flush=True)
    if platform.system() not in ("Darwin", "Linux"):
        print(json.dumps({"unsupported_probe_platform": True}))
        return 2
    # A short unique path is required by sockaddr_un. No installed runtime root.
    with tempfile.TemporaryDirectory(prefix="cv-pcb-", dir="/tmp") as root:
        path = os.path.join(root, "listener")
        needle = os.fsencode(path)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
        receiver.settimeout(2)
        received = None
        try:
            listener.bind(path)
            listener.listen(1)
            observe("listener_bound", needle)
            descriptor = array.array("i", [listener.fileno()])
            sender.sendmsg([b"L"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, descriptor)])
            os.unlink(path)
            listener.close()
            # No numeric descriptor now owns the listener: its reference is
            # queued in SCM_RIGHTS. The table must still expose it if usable.
            observe("unlinked_listener_queued", needle)
            received = receive_descriptor(receiver)
            observe("unlinked_listener_received", needle)
            os.close(received)
            received = None
            observe("final_reference_closed", needle)
        finally:
            if received is not None:
                os.close(received)
            listener.close()
            sender.close()
            receiver.close()
    creator_exit_fixture()
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--listener-child":
        raise SystemExit(listener_child(sys.argv[2], int(sys.argv[3])))
    raise SystemExit(main())
