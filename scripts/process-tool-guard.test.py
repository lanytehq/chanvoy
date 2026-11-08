#!/usr/bin/env python3
"""Regression cases for process-tool-guard.py."""
import importlib.util
import os
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("guard", os.path.join(HERE, "process-tool-guard.py"))
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


def lines(source, path="*"):
    return [number for number, _ in guard.check(source, path)]


class SpawnAllowlist(unittest.TestCase):
    def test_banned_and_unknown_programs_fail(self):
        for source in [
            'Command::new("ps").arg("-ax");',
            'tokio::process::Command::new("/usr/sbin/lsof");',
            'Command::new(b"pgrep");',
            'Command::new(concat!("p", "s"));',
            'Command::new(PS_TOOL);',
            'Command::new(format!("{}", name));',
            'Command::new("/usr/bin/env").arg("ps");',
            'Command::new("python3");',
            'Command::new(\n    "ps"\n);',
        ]:
            self.assertTrue(lines(source), source)

    def test_allowed_programs_and_expressions_pass(self):
        for source in [
            'Command::new("bash").arg("scripts/x.sh");',
            'Command::new("/bin/sleep").arg("30");',
            "Command::new(common::CHANVOY_BIN);",
            "std::process::Command::new(std::env::current_exe().unwrap())",
        ]:
            self.assertEqual(lines(source), [], source)

    def test_local_bindings_are_pinned_to_their_reviewed_file(self):
        source = "let mut cmd = Command::new(exe);"
        self.assertEqual(lines(source, "crates/chanvoy-cli/src/lib.rs"), [])
        self.assertTrue(lines(source, "crates/other/src/lib.rs"))
        self.assertTrue(lines(source))

    def test_native_self_launch_allows_only_its_program_argument_in_its_file(self):
        path = "crates/chanvoy-cli/src/managed_launch.rs"
        self.assertEqual(lines("Command::new(&arguments[0]);", path), [])
        self.assertTrue(lines("Command::new(&arguments[1]);", path))
        self.assertTrue(lines("Command::new(&arguments[0]);", "crates/other/src/lib.rs"))


class ShellCommandWords(unittest.TestCase):
    def test_shell_scripts_naming_tools_fail(self):
        for text in [
            "ps aux",
            "ps -ax",
            "ps | grep chanvoy",
            "'ps' -ax",
            "ps\t-ax",
            "sleep 1; kill -9 $pid",
            "true && /bin/ps -o pid",
            "echo x | pkill -f y",
            "x=$(pgrep chanvoy)",
            "exec kill $PID",
            "env -i lsof -nP",
            "kill",
            "if ps -ax; then :; fi",
            "while true; do ps; done",
            "if true; then ps aux; fi",
            "{ ps aux; }",
            "! pgrep chanvoy",
            "case x in x) ps -ax;; esac",
            "FOO=1 ps aux",
            "A=1 B=2 /usr/bin/env lsof -nP",
            "/usr/bin/env ps",
            "nohup pkill -f x",
            "ps>/tmp/out",
            "x; kill<&0",
        ]:
            self.assertIsNotNone(guard.banned_command(text), text)

    def test_prose_and_other_programs_pass(self):
        for text in [
            "kill(pid, 0) checks a pid without signalling",
            "force kill escalation",
            "process stats",
            "skill",
            "kills nothing",
            "/opt/ps-tools/bin/run",
            "pid file",
            "sleep 30; :",
            "exit 0",
            "printf synthetic-stdout; exec /bin/sleep 30",
            "if kill is needed, escalate",
            "while ps output is cached",
            "VAR=value",
            "",
        ]:
            self.assertIsNone(guard.banned_command(text), text)

    def test_multiline_raw_and_escaped_strings_are_decoded(self):
        source = (
            'const SCRIPT: &str = r#"\n'
            "set -eu\n"
            "ps -ax | grep chanvoy\n"
            '"#;\n'
            'let s = "echo \\x70s ok; \\u{70}s -o pid";\n'
        )
        self.assertEqual(lines(source), [1, 5])

    def test_line_numbers_survive_multiline_spawn_arguments(self):
        source = 'Command::new(\n    "bash"\n)\n.arg("-c")\n.arg("ps -ax");\n'
        self.assertEqual(lines(source), [5])

    def test_comments_and_char_literals_do_not_confuse_the_lexer(self):
        source = (
            '// Command::new("ps") is forbidden\n'
            "/* kill -9 1 */\n"
            "let quote = '\"';\n"
            'let ok = "sleep 1";\n'
        )
        self.assertEqual(lines(source), [])


class Allowance(unittest.TestCase):
    def test_marker_needs_a_reason(self):
        self.assertEqual(lines('Command::new("ps"); // process-tool-guard: allow fixture for X'), [])
        self.assertTrue(lines('Command::new("ps"); // process-tool-guard: allow'))

    def test_marker_text_inside_a_string_exempts_nothing(self):
        source = 'let s = r#"// process-tool-guard: allow x\nps -ax"#;\n'
        self.assertEqual(lines(source), [1])

    def test_marker_applies_only_to_its_own_line(self):
        source = '// process-tool-guard: allow next line\nCommand::new("ps");\n'
        self.assertEqual(lines(source), [2])


class Scan(unittest.TestCase):
    def test_default_scan_covers_every_rust_root(self):
        with tempfile.TemporaryDirectory() as root:
            for directory in ["examples", "benches", "build-tools"]:
                os.makedirs(os.path.join(root, directory))
                with open(os.path.join(root, directory, "x.rs"), "w") as f:
                    f.write('Command::new("ps");\n')
            self.assertEqual(len(guard.scan([root])), 3)

    def test_default_root_is_the_repository(self):
        seen = []
        original = guard.scan
        guard.scan = lambda roots: seen.extend(roots) or []
        try:
            guard.main(["guard"])
        finally:
            guard.scan = original
        self.assertEqual(seen, [os.path.dirname(HERE)])

    def test_ignores_target_and_reports_exit_status(self):
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "target"))
            with open(os.path.join(root, "target", "x.rs"), "w") as f:
                f.write('Command::new("ps");\n')
            self.assertEqual(guard.main(["guard", root]), 0)
            with open(os.path.join(root, "a.rs"), "w") as f:
                f.write('Command::new("kill");\n')
            self.assertEqual(guard.main(["guard", root]), 1)


if __name__ == "__main__":
    unittest.main()
