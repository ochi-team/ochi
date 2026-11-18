"""Build command contract regressions. Run with python3 src/build/test_build.py."""

import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


class BuildCommandsTest(unittest.TestCase):
    def build(self, *args, env=None, run_args=None, cwd=ROOT):
        command = ["zig", "build", *args, "--color", "off", "--summary", "all"]
        if run_args is not None:
            command.extend(["--", *run_args])
        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        return result.stdout

    def test_documented_steps(self):
        output = self.build("--help")
        for name in ("install", "run", "scooby", "btest", "test", "test-main", "test-app", "test-encoding", "test-cli", "test-sack", "test-logging", "sack", "kcov", "cover", "check"):
            with self.subTest(step=name):
                self.assertRegex(output, rf"(?m)^  {name} +")

    def test_default_installation(self):
        for steps in ((), ("install",)):
            with self.subTest(steps=steps), tempfile.TemporaryDirectory() as prefix:
                output = self.build(*steps, "--prefix", prefix)
                self.assertEqual(
                    sorted(path.name for path in (pathlib.Path(prefix) / "bin").iterdir()),
                    ["ochi"],
                )
                self.assertNotIn("run exe", output)
                self.assertNotIn("run test", output)

    def test_main_suite_alias(self):
        for step in ("test-main", "test-app"):
            with self.subTest(step=step):
                output = self.build(step, "-Dtest-filter=options require explicit data")
                self.assertIn("2 pass", output)
                self.assertIn("2 total", output)
                self.assertEqual(output.count("run test"), 1)

    def test_run_steps_forward_arguments(self):
        # Keep the real build graph, but replace entry points to avoid server/data side effects.
        probe = (
            'const std = @import("std");\n'
            'pub fn main(init: std.process.Init) !void {\n'
            '    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);\n'
            '    defer args.deinit();\n'
            '    _ = args.skip();\n'
            '    while (args.next()) |arg| std.debug.print("forwarded: [{s}]\\n", .{arg});\n'
            '}\n'
        )
        arguments = ["--build-contract=two words", "", "--second"]
        with tempfile.TemporaryDirectory() as temporary:
            workspace = pathlib.Path(temporary)
            for name in ("build.zig", "build.zig.zon"):
                shutil.copy2(ROOT / name, workspace / name)
            shutil.copytree(ROOT / "src", workspace / "src")
            for entry in ("src/main.zig", "src/scooby.zig", "src/apps/sack/main.zig"):
                (workspace / entry).write_text(probe)
            for step, artifact in (("run", "ochi"), ("scooby", "scooby"), ("sack", "sack")):
                with self.subTest(step=step):
                    output = self.build(step, run_args=arguments, cwd=workspace)
                    self.assertIn(f"run exe {artifact}", output)
                    self.assertEqual(
                        re.findall(r"^forwarded: \[(.*)\]$", output, re.MULTILINE), arguments,
                    )
                    self.assertFalse((workspace / "zig-out" / "bin").exists())

    def test_check_membership(self):
        with tempfile.TemporaryDirectory() as prefix:
            output = self.build(
                "check", "-Dtest-filter=options require explicit data", "--prefix", prefix,
            )
            # Only direct children count: libraries are transitive compile dependencies.
            members = re.findall(r"^\+- compile (exe \S+|test) ", output, re.MULTILINE)
            self.assertCountEqual(members, ["exe ochi", "exe scooby", "exe sack", *(["test"] * 5)])
            self.assertNotIn("run exe", output)
            self.assertNotIn("run test", output)
            self.assertNotIn("install ", output)
            self.assertFalse((pathlib.Path(prefix) / "bin").exists())

    def test_btest_installs_without_running(self):
        with tempfile.TemporaryDirectory() as prefix:
            output = self.build("btest", "-Dtest-filter=options require explicit data", "--prefix", prefix)
            self.assertNotIn("run test", output)
            self.assertEqual(
                sorted(path.name for path in (pathlib.Path(prefix) / "bin").iterdir()),
                ["test"],
            )

    def test_combined_binary_contains_package_tests(self):
        for test_filter in ("options require explicit data", "OchiClient tenant headers", "Encoder.writeIntBytes"):
            with self.subTest(test_filter=test_filter), tempfile.TemporaryDirectory() as prefix:
                self.build("btest", f"-Dtest-filter={test_filter}", "--prefix", prefix)
                result = subprocess.run(
                    [str(pathlib.Path(prefix) / "bin" / "test")],
                    cwd=ROOT,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                self.assertEqual(result.returncode, 0, result.stdout)
                self.assertIn(test_filter.encode(), (pathlib.Path(prefix) / "bin" / "test").read_bytes())
                # Six unnamed discovery tests plus the selected package test.
                self.assertIn("|tests ran| 7", result.stdout)
                self.assertIn("|passed   | 7", result.stdout)

    def test_all_suites_use_custom_runner(self):
        with tempfile.TemporaryDirectory() as prefix:
            self.build("btest", "-Dtest-filter=options require explicit data", "--prefix", prefix)
            for binary in (pathlib.Path(prefix) / "bin").iterdir():
                with self.subTest(suite=binary.name):
                    self.assertTrue(b"|tests ran|" in binary.read_bytes(), f"{binary.name} does not use the custom runner")

    def test_kcov_merges_suites_with_fresh_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            kcov = root / "kcov"
            # Isolate build wiring from platform-specific coverage instrumentation.
            # Like kcov, this stub creates only the final output directory.
            kcov.write_text(
                "#!/usr/bin/env python3\n"
                "import pathlib, sys\n"
                "if '--merge' in sys.argv:\n"
                "    output = pathlib.Path(sys.argv[2])\n"
                "    inputs = [pathlib.Path(arg) for arg in sys.argv[3:]]\n"
                "    assert len(inputs) == 5\n"
                "    assert all((path / 'invoked').is_file() for path in inputs)\n"
                "    output.mkdir(exist_ok=True)\n"
                "    (output / 'index.html').write_text(','.join(sorted(path.name for path in inputs)))\n"
                "else:\n"
                "    output = pathlib.Path(sys.argv[-2])\n"
                "    output.mkdir(exist_ok=True)\n"
                "    (output / 'invoked').touch()\n"
            )
            kcov.chmod(0o755)
            env = dict(os.environ, PATH=f"{root}{os.pathsep}{os.environ['PATH']}")
            prefix = root / "fresh-output"
            for step in ("kcov", "cover"):
                with self.subTest(step=step):
                    self.build(step, "-Dtest-filter=options require explicit data", "--prefix", str(prefix), env=env)
                    self.assertEqual(
                        (prefix / "cov" / "index.html").read_text(),
                        "cli,encoding,logging,main,sack",
                    )
                    self.assertEqual([path.name for path in (prefix / "cov").iterdir()], ["index.html"])
                    shutil.rmtree(prefix)

    def test_package_discovery_and_filters(self):
        # Zig always includes unnamed discovery tests, even with a name filter.
        for step, test_filter, count in (("test-cli", "options require explicit data", 1), ("test-sack", "OchiClient tenant headers", 2), ("test-encoding", "Encoder.writeIntBytes", 3)):
            with self.subTest(step=step):
                output = self.build(step, f"-Dtest-filter={test_filter}")
                self.assertIn(f"{count} pass", output)
                self.assertIn(f"{count} total", output)


if __name__ == "__main__":
    unittest.main()
