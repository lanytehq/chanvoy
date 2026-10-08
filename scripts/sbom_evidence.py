"""Owned evidence controller and bounded pinned-scanner containers."""

import json
from pathlib import Path
import shutil
import sys
import time
import uuid

sys.dont_write_bytecode = True
from bounded_evidence import EvidenceCommands, EvidenceError
from offline_schema import regular_bytes, sha


class Controller(EvidenceCommands):
    def __init__(self, root, output, schema, seconds):
        self.root = Path(root).resolve()
        raw = Path(output).absolute()
        if raw.is_symlink():
            raise EvidenceError("unsafe evidence output")
        self.out = raw.parent.resolve() / raw.name
        if self.out.is_relative_to(self.root):
            raise EvidenceError("evidence must remain outside source checkout")
        self.out.mkdir(parents=True, mode=0o700)
        self.deadline = time.monotonic() + seconds
        self.receipt_path = self.out / "evidence.json"
        self.receipt = {"schema": schema, "status": "incomplete", "commands": [],
                        "python": sys.version.split()[0], "containers": []}
        self.save()


def scanner(controller, name, argv, pin, payload=None):
    owner = uuid.uuid4().hex
    record = {"stage": name, "owner": owner, "image": pin["image"], "cleanup": "unknown", "id": None}
    controller.receipt["containers"].append(record)
    controller.save()
    create = ["docker", "create", "--name", "chanvoy-sbom-" + owner, "--network", "none", "--read-only",
              "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--label", "chanvoy.sbom-owner=" + owner,
              "--env", "SYFT_CHECK_FOR_APP_UPDATE=false"]
    if payload:
        payload = Path(payload)
        source = controller.out / (name + "-source")
        source.mkdir(mode=0o700)
        copy = source / payload.name
        shutil.copyfile(payload, copy)
        copy.chmod(0o444)
        if sha(regular_bytes(copy)) != sha(regular_bytes(payload)):
            raise EvidenceError("scanner source copy mismatch")
        create += ["--mount", "type=bind,src=" + str(source) + ",dst=/payload,readonly", "--workdir", "/payload"]
    create += [pin["image"], *argv]

    def inspect(stage):
        value = json.loads(controller.command(name + "-" + stage, ["docker", "inspect", "--type", "container", record["id"]], 10))
        if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
            raise EvidenceError("ambiguous owned scanner identity")
        value = value[0]
        host = value["HostConfig"]
        if not isinstance(host, dict) or not isinstance(value.get("Config"), dict):
            raise EvidenceError("malformed owned scanner identity")
        labels = value["Config"].get("Labels")
        options = host.get("SecurityOpt")
        if (not isinstance(labels, dict) or not isinstance(options, list) or not options
                or any(not isinstance(x, str)
                       or x not in {"no-new-privileges", "no-new-privileges=true"} for x in options)):
            raise EvidenceError("malformed owned scanner containment")
        if (value["Id"] != record["id"] or value["Config"]["Image"] != pin["image"]
                or labels.get("chanvoy.sbom-owner") != owner
                or host.get("NetworkMode") != "none" or host.get("ReadonlyRootfs") is not True
                or host.get("CapDrop") != ["ALL"]):
            raise EvidenceError("owned scanner identity/containment changed")
        if payload:
            mounts = value["Mounts"]
            if (len(mounts) != 1 or mounts[0]["Source"] != str(source) or mounts[0]["Destination"] != "/payload"
                    or mounts[0].get("RW") is not False):
                raise EvidenceError("scanner payload-only mount changed")
        return value

    try:
        identity = controller.command(name + "-create", create, 120).strip()
        if len(identity) != 64 or any(x not in "0123456789abcdef" for x in identity):
            raise EvidenceError("unknown scanner create outcome")
        record["id"] = identity
        controller.save()
        inspect("before-start")
        result = controller.command(name, ["docker", "start", "--attach", identity], 60)
        terminal = inspect("after-start")["State"]
        if terminal.get("Status") != "exited" or terminal.get("ExitCode") != 0:
            raise EvidenceError("scanner did not successfully complete")
        record["completed"] = True
    except (EvidenceError, OSError, ValueError, KeyError, TypeError) as error:
        controller.fail(str(error))
        raise
    finally:
        if record["id"]:
            try:
                inspect("before-remove")  # Full immutable ID + fresh owner/containment.
                controller.command(name + "-remove", ["docker", "rm", "--force", record["id"]], 15)
                remaining = controller.command(name + "-absence", ["docker", "ps", "--all", "--no-trunc",
                                                                        "--filter", "id=" + record["id"], "--format", "json"], 10)
                if remaining.strip():
                    raise EvidenceError("owned scanner survivor after removal")
                record["cleanup"] = "confirmed-absent"
                controller.save()
            except (EvidenceError, OSError, ValueError, KeyError, TypeError) as error:
                record["cleanup"] = "unknown"
                controller.fail("scanner cleanup unconfirmed: " + str(error))
                raise
    if payload and sha(regular_bytes(copy)) != sha(regular_bytes(payload)):
        raise EvidenceError("scanner payload changed")
    return result
