#!/usr/bin/env python3
"""Refuse Rust sources that spawn process-inspection or signalling tools.

Spawning `ps`, `lsof`, `pgrep` and similar tools is denied inside sandboxed
agent seats and differs across platforms. Process state is read through
sysprims or the kernel, and signals go through libc or sysprims.

This is a hygiene check against accidental regressions in ordinary code. It
is lexical: it does not follow Rust data flow, macro expansion or full shell
grammar, and it is not a boundary against deliberately disguised spawns.

Two rules, both fail closed:

1. Spawn allowlist. The program passed to every `Command::new(...)` (std or
   tokio) must be one of ALLOWED_PROGRAMS (string literals) or an
   ALLOWED_EXPRESSIONS entry for that file (the exact argument source). A new
   spawn site with any other program, including a const, `concat!` or
   computed name, fails until it is added here deliberately.
2. Shell command words. No Rust string literal (normal, raw or byte, on one
   line or many) may use a banned tool as a shell command word: at the start,
   or after `|`, `;`, `&`, `(`, `)`, `{`, `!`, `` ` ``, `$(`, a newline or a
   shell keyword following one of those (`then`, `do`, `else`, `if`, `while`,
   `until`, `time`), after leading `VAR=value` assignments and an `env`,
   `exec`, `command`, `nohup`, `nice` or `sudo` wrapper (any path), ignoring
   quotes, accepting an absolute path and ending at whitespace, a separator or
   a redirection. This covers `sh -c` scripts and script constants.

A Rust line comment `// process-tool-guard: allow <reason>` exempts findings
that start on its own source line; the reason is required, and the same text
inside a string literal exempts nothing.

Usage: process-tool-guard.py [ROOT ...]   (default: the repository root)
"""
import os
import re
import sys

TOOLS = {"ps", "lsof", "pgrep", "pkill", "pidof", "kill", "killall", "fuser"}
ALLOWED_PROGRAMS = {
    "sleep", "/bin/sleep", "sh", "/bin/sh", "bash", "git", "cc", "rustc", "date",
}
# Exact argument source per file, relative to the repository root. Local
# bindings are pinned to the file whose binding was reviewed.
ALLOWED_EXPRESSIONS = {
    "crates/chanvoy-cli/src/lib.rs": {"exe"},
    "crates/chanvoy-cli/src/lifecycle.rs": {"executable", "std::env::current_exe().unwrap()"},
    # Managed launch constructs argv[0] from the revalidated current native
    # executable through RuntimeLaunchMarker::arguments, not caller input.
    "crates/chanvoy-cli/src/managed_launch.rs": {"&arguments[0]"},
    "*": {
        "CHANVOY_BIN",
        "common::CHANVOY_BIN",
        "std::env::current_exe().unwrap()",
        "env::current_exe().unwrap()",
    },
    "tests/fingerprint_contract.rs": {'override_bin.as_deref().unwrap_or(Path::new("decernor"))'},
}
ALLOW_MARKER = re.compile(r"process-tool-guard:\s*allow\s+\S")
COMMAND_WORD = re.compile(
    r"(?:^|[\n|;&()`{!]|\$\()\s*"
    r"(?:(?:then|do|else|elif|if|while|until|time|!|\{)\s+)*"
    r"(?:\w+=(?:(?!\$\()[^\s;|&])*\s+)*"
    r"(?:(?:/[\w./-]*/)?(?:exec|sudo|command|nohup|nice|env)\s+(?:-\S+\s+|\w+=\S*\s+)*)?"
    r"(/[\w./-]*/)?([\w-]+)(?=\s|$|[;|&)`}<>])"
)


def unescape(text):
    escapes = {"n": "\n", "t": "\t", "r": "\r", "0": "\0"}

    def repl(match):
        code = match.group(1) or match.group(2)
        if code:
            return chr(int(code, 16))
        return escapes.get(match.group(3), match.group(3))

    return re.sub(r"\\(?:x([0-9a-fA-F]{2})|u\{([0-9a-fA-F]{1,6})\}|(.))", repl, text, flags=re.S)


def skip_quoted(source, j):
    """Index just past the closing quote of a normal string starting at j."""
    j += 1
    while j < len(source) and source[j] != '"':
        j += 2 if source[j] == "\\" else 1
    return j + 1


def lex(source):
    """Yield (kind, text, start_line, end_line). kind is "string" (decoded
    literal text) or "spawn" (the source of a Command::new argument)."""
    i, line, n = 0, 1, len(source)
    while i < n:
        c = source[i]
        if source.startswith("//", i):
            end = source.find("\n", i)
            end = n if end < 0 else end
            yield "comment", source[i:end], line, line
            i = end
            continue
        if source.startswith("/*", i):
            end = source.find("*/", i + 2)
            end = n if end < 0 else end + 2
            line += source.count("\n", i, end)
            i = end
            continue
        boundary = i == 0 or not (source[i - 1].isalnum() or source[i - 1] == "_")
        raw = re.match(r'b?r(#*)"', source[i:]) if boundary else None
        if raw:
            start = i + raw.end()
            close = '"' + raw.group(1)
            end = source.find(close, start)
            end = n if end < 0 else end
            text = source[start:end]
            yield "string", text, line, line + text.count("\n")
            line += source.count("\n", i, end + len(close))
            i = end + len(close)
            continue
        if c == '"' or (boundary and source.startswith('b"', i)):
            opening = i + (1 if c == "b" else 0)
            j = skip_quoted(source, opening)
            text = source[opening + 1 : j - 1]
            yield "string", unescape(text), line, line + text.count("\n")
            line += source.count("\n", i, j)
            i = j
            continue
        if c == "'":
            # A char literal such as '"' must not open a string.
            char = re.match(r"'(\\.|\\u\{[0-9a-fA-F]+\}|[^\\'])'", source[i:])
            if char:
                i += char.end()
                continue
        if source.startswith("Command::new(", i):
            start = i + len("Command::new(")
            depth, j = 1, start
            while j < n and depth:
                if source[j] == "(":
                    depth += 1
                elif source[j] == ")":
                    depth -= 1
                elif source[j] == '"':
                    j = skip_quoted(source, j)
                    continue
                j += 1
            argument = " ".join(source[start : j - 1].split()).rstrip(",").strip()
            yield "spawn", argument, line, line + source.count("\n", start, j)
            # The allowlist judges the whole argument, literals included.
            line += source.count("\n", i, j)
            i = j
            continue
        if c == "\n":
            line += 1
        i += 1


def literal(argument):
    match = re.fullmatch(r'b?"((?:[^"\\]|\\.)*)"', argument)
    return unescape(match.group(1)) if match else None


KEYWORD = re.compile(r"\s*(?:then|do|else|elif|if|while|until|time)\s")
# After a keyword at the very start of a string, a command is followed by an
# option, a separator, a redirection or the end; prose ("if kill is needed")
# continues with an ordinary word.
COMMAND_TAIL = re.compile(r"\s*(?:$|-|[;|&)`}<>\n])")


def banned_command(text):
    normalized = text.replace("'", " ").replace('"', " ").replace("\t", " ").replace("\r", "\n")
    for match in COMMAND_WORD.finditer(normalized):
        if match.group(2) not in TOOLS:
            continue
        if match.start() == 0 and KEYWORD.match(normalized) and not COMMAND_TAIL.match(
            normalized, match.end()
        ):
            continue
        return match.group(2)
    return None


def check(source, path="*"):
    tokens = list(lex(source))
    allowed = {start for kind, text, start, _ in tokens
               if kind == "comment" and ALLOW_MARKER.search(text)}
    expressions = ALLOWED_EXPRESSIONS["*"] | ALLOWED_EXPRESSIONS.get(path, set())
    findings = []
    for kind, text, start, _ in tokens:
        if kind == "comment" or start in allowed:
            continue
        if kind == "spawn":
            program = literal(text)
            if program is not None:
                if program not in ALLOWED_PROGRAMS:
                    findings.append((start, f"spawns `{program}`, not an allowed program"))
            elif text not in expressions:
                findings.append((start, f"spawns `{text}`, not an allowed program expression"))
        else:
            tool = banned_command(text)
            if tool:
                findings.append((start, f"string runs `{tool}` as a command"))
    return findings


def scan(roots):
    findings = []
    for root in roots:
        for directory, dirs, files in os.walk(root):
            dirs[:] = sorted(d for d in dirs if d not in {"target", ".git"} and not d.startswith("."))
            for name in sorted(files):
                if name.endswith(".rs"):
                    path = os.path.join(directory, name)
                    relative = os.path.relpath(path, root).replace(os.sep, "/")
                    with open(path, encoding="utf-8") as source:
                        for number, message in check(source.read(), relative):
                            findings.append((path, number, message))
    return findings


def main(argv):
    roots = argv[1:] or [os.path.dirname(os.path.dirname(os.path.abspath(__file__)))]
    findings = scan([root for root in roots if os.path.isdir(root)])
    for path, number, message in findings:
        print(f"{path}:{number}: {message}; use sysprims or libc, or allow it in "
              "scripts/process-tool-guard.py with a reason", file=sys.stderr)
    if findings:
        return 1
    print("[ok] no process-tool spawns in Rust sources")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
