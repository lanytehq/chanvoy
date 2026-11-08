#!/usr/bin/env python3
"""Isolated Darwin listener retained by a fileport after creator exit.

No installed runtime, credentials, signals to existing processes, or lifecycle
authority participates. Optional Rust observation remains qualification=false.
"""
import argparse
import ctypes
import fcntl
import importlib.util
import json
import os
import platform
import socket
import subprocess
import sys
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-observer")
    args = parser.parse_args()
    if platform.system() != "Darwin":
        print(json.dumps({"unsupported_probe_platform": True, "qualification": False}))
        return 2
    # Reuse the frozen capability fixture's transfer and observer paths. Keep
    # source trees free of import bytecode as well as build outputs.
    sys.dont_write_bytecode = True
    source = Path(__file__).with_name("probe-native-runtime.py")
    spec = importlib.util.spec_from_file_location("native_runtime_probe", source)
    probe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)
    if args.native_observer:
        probe.NATIVE_OBSERVER = os.path.abspath(args.native_observer)
    native = ctypes.CDLL(None, use_errno=True)
    native.fileport_makeport.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_uint)]
    native.fileport_makeport.restype = ctypes.c_int
    native.fileport_makefd.argtypes = [ctypes.c_uint]
    native.fileport_makefd.restype = ctypes.c_int
    native.mach_port_deallocate.argtypes = [ctypes.c_uint, ctypes.c_uint]
    native.mach_port_deallocate.restype = ctypes.c_int
    task = ctypes.c_uint.in_dll(native, "mach_task_self_").value
    if not task:
        raise RuntimeError("fixture Mach task unavailable")
    print(json.dumps({"probe": "native-fileport-listener", "system": "Darwin",
                      "release": platform.release(), "uid": os.geteuid(),
                      "qualification": False}), flush=True)
    with tempfile.TemporaryDirectory(prefix="cv-port-", dir="/tmp") as root:
        path = os.path.join(root, "listener")
        sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
        receiver.settimeout(2)
        child = None
        received = None
        restored = None
        port = ctypes.c_uint()
        try:
            child = subprocess.Popen(
                [sys.executable, str(source), "--listener-child", path, str(sender.fileno())],
                pass_fds=(sender.fileno(),), env={}, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            sender.close()
            if child.wait(timeout=5) != 0:
                raise RuntimeError("isolated listener creator failed")
            claim = json.loads(child.stdout.read(1024))
            with open(path + ".lease", "rb") as lease:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            received = probe.receive_descriptor(receiver)
            if os.fstat(received).st_ino != claim["socket_inode"]:
                raise RuntimeError("fixture received a different socket inode")
            ctypes.set_errno(0)
            if native.fileport_makeport(received, ctypes.byref(port)) != 0 or not port.value:
                raise OSError(ctypes.get_errno(), "fixture fileport creation failed")
            os.unlink(path)
            os.close(received)
            received = None
            # The original creator has exited. No numeric fd or queued
            # SCM_RIGHTS now retains this listener: only our Mach send right.
            print(json.dumps({"fixture_creator_exited": True,
                              "synthetic_lease_free": True,
                              "fileport_only_reference": True,
                              "qualification": False}), flush=True)
            probe.observe("creator_exited_listener_fileport", os.fsencode(path))
            fd = native.fileport_makefd(port.value)
            if fd < 0:
                raise OSError(ctypes.get_errno(), "fixture fileport descriptor recovery failed")
            received = fd
            fcntl.fcntl(received, fcntl.F_SETFD,
                        fcntl.fcntl(received, fcntl.F_GETFD) | fcntl.FD_CLOEXEC)
            restored = socket.socket(fileno=received)
            received = None
            if (restored.getsockname() != path
                    or restored.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) != socket.SOCK_STREAM
                    or os.fstat(restored.fileno()).st_ino != claim["socket_inode"]):
                raise RuntimeError("fixture recovered a different listener")
            if native.mach_port_deallocate(task, port.value) != 0:
                raise RuntimeError("fixture fileport release failed")
            port.value = 0
            probe.observe("creator_exited_fileport_released_fd_retained", os.fsencode(path))
            restored.close()
            restored = None
            probe.observe("creator_exited_fileport_final_reference_closed", os.fsencode(path))
        finally:
            if child is not None:
                if child.poll() is None:
                    child.kill()
                    child.wait()
                child.stdout.close()
                child.stderr.close()
            if received is not None:
                os.close(received)
            if restored is not None:
                restored.close()
            sender.close()
            receiver.close()
            if port.value and native.mach_port_deallocate(task, port.value) != 0:
                raise RuntimeError("fixture fileport cleanup unconfirmed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
