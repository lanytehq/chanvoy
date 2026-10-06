#!/usr/bin/env python3
"""Prepare a complete public tag message outside the repository, without clobber."""
import os
import re
import sys
from pathlib import Path


def fail(reason: str) -> None:
    raise ValueError(reason)


def validate_message(data: bytes) -> str:
    try:
        message = data.decode("utf-8")
    except UnicodeError:
        fail("message.txt must be UTF-8")
    if (not data or b"\x00" in data or b"\r" in data or not data.endswith(b"\n")
            or data.endswith(b"\n\n")
            or any(line.rstrip(b" \t") != line for line in data.splitlines())):
        fail("message.txt needs one final newline and no CR, NUL or trailing whitespace")
    if re.search(r"<(?:model|interface/tooling[^<>]*|TODO[^<>]*|REPLACE[^<>]*)>", message, re.I):
        fail("message.txt contains an unresolved example placeholder")
    return message


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    tag = os.environ.get("CHANVOY_RELEASE_TAG", "")
    if not re.fullmatch(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", tag):
        fail("canonical release tag required")
    if tag != f"v{(root / 'VERSION').read_text().strip()}":
        fail("release tag must match VERSION")
    raw = os.environ.get("CHANVOY_TAG_MESSAGE_DIR", "")
    directory = Path(raw)
    if not raw or not directory.is_absolute() or ".." in directory.parts or directory.name != tag:
        fail("external absolute per-cut message directory ending in release tag required")
    if any(p.is_symlink() for p in (directory, *directory.parents)):
        fail("message directory may not contain symlink ancestors")
    resolved = directory.resolve(strict=False)
    if resolved == root or root in resolved.parents:
        fail("message directory must be outside the repository")
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not directory.is_dir():
        fail("message directory must be a real directory")
    file = directory / "message.txt"
    generated = f"chanvoy {tag}\n".encode("utf-8")
    created = False
    try:
        descriptor = os.open(file, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        pass
    else:
        created = True
        try:
            with os.fdopen(descriptor, "wb") as output:
                output.write(generated)
        except OSError:
            file.unlink(missing_ok=True)
            raise
    if file.is_symlink() or not file.is_file():
        fail("message.txt must be a regular non-symlink file")
    message = validate_message(file.read_bytes())
    print(f"[ok] {'created' if created else 'preserved'} public tag message for review:")
    print(message, end="")


if __name__ == "__main__":
    try:
        if len(sys.argv) == 3 and sys.argv[1] == "--validate-message":
            validate_message(Path(sys.argv[2]).read_bytes())
        elif len(sys.argv) == 1:
            main()
        else:
            fail("expected prepare or validate-message mode")
    except (OSError, ValueError) as error:
        detail = str(error) if isinstance(error, ValueError) else "unable to prepare external tag message"
        print(f"error: {detail}", file=sys.stderr)
        sys.exit(1)
