"""Map per-test coverage to surface over- and under-covered lines.

Helper for spotting tests that exercise suspiciously few lines (likely
over-mocked) and lines exercised by many tests (potential redundancy). Not
part of CI; run ad-hoc:

    uv run python -m scripts.map_test_coverage

Writes ``coverage_map.json`` at the repo root (gitignored).
"""

from __future__ import annotations

import json
import os
import subprocess
from collections import defaultdict

import coverage

PACKAGE_PATH = "src"
TEST_PATH = "tests/"


def run_and_map():
    env = os.environ.copy()

    subprocess.run(
        [
            "python",
            "-m",
            "pytest",
            TEST_PATH,
            f"--cov={PACKAGE_PATH}",
            "--cov-context=test",
            "--cov-report=",
            "--no-cov-on-fail",
            "-q",
        ],
        env=env,
        check=False,
    )

    cov = coverage.Coverage(source=[PACKAGE_PATH])
    cov.load()
    data = cov.get_data()

    test_to_lines = defaultdict(list)
    line_to_tests = defaultdict(list)

    for filename in data.measured_files():
        rel = os.path.relpath(filename)
        contexts_by_line = data.contexts_by_lineno(filename)
        for lineno, contexts in contexts_by_line.items():
            for ctx in contexts:
                if not ctx:
                    continue
                key = f"{rel}:{lineno}"
                test_to_lines[ctx].append(key)
                line_to_tests[key].append(ctx)

    return test_to_lines, line_to_tests


if __name__ == "__main__":
    t2l, l2t = run_and_map()

    print(f"\nTotal tests with coverage data: {len(t2l)}")
    print(f"Total lines covered: {len(l2t)}")

    print("\n=== Tests hitting suspiciously few lines (likely over-mocked) ===")
    for test, lines in sorted(t2l.items(), key=lambda x: len(x[1])):
        if len(lines) < 5:
            print(f"  {test}: {len(lines)} lines -> {lines}")

    SKIP_PATHS = ["src/utils/logger.py"]

    print("\n=== Lines covered by the most tests (potential redundancy) ===")
    overcovered = sorted(
        (
            (line, tests)
            for line, tests in l2t.items()
            if not any(line.startswith(skip) for skip in SKIP_PATHS)
        ),
        key=lambda x: len(x[1]),
        reverse=True,
    )[:30]
    for line, tests in overcovered:
        print(f"  {line}  <-  {len(tests)} tests")
        for t in tests[:5]:
            print(f"      {t}")
        if len(tests) > 5:
            print(f"      ... and {len(tests) - 5} more")

    with open("coverage_map.json", "w") as f:
        json.dump(
            {
                "test_to_lines": {k: v for k, v in sorted(t2l.items())},
                "line_to_tests": {k: v for k, v in sorted(l2t.items())},
            },
            f,
            indent=2,
        )
    print("\nFull map saved to coverage_map.json")
