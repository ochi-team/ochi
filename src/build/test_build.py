"""Build command contract regressions. Run with python3 src/build/test_build.py."""

import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]


class BuildCommandsTest(unittest.TestCase):
    def build(self, *args, env=None):
        result = subprocess.run(
            ["zig", "build", *args, "--color", "off", "--summary", "all"],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        return result.stdout

    def test_documented_steps(self):
        output = self.build("--help")
        for name in ("run", "btest", "test", "test-main", "test-encoding", "test-cli", "test-sack", "test-logging", "sack", "kcov", "check"):
            with self.subTest(step=name):
                self.assertRegex(output, rf"(?m)^  {name} +")

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
            for attempt in range(2):
                with self.subTest(attempt=attempt):
                    self.build("kcov", "-Dtest-filter=options require explicit data", "--prefix", str(prefix), env=env)
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
