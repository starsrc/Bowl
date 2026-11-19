"""An agent loop that tries several fixes in parallel, one fork each, and keeps the one that passes.

Stands in for an LLM agent: the candidate fixes are hard-coded so the run is reproducible.
Run against `bowl-sandbox serve` with a python template:

    bowl-sandbox template create python:3.12-slim
    bowl-sandbox serve &
    python3 examples/try_fixes_in_forks.py      # BOWL_TEMPLATE picks another template
"""

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bowl_sandbox import Sandbox  # noqa: E402

BUGGY = '''\
def median(xs):
    xs = sorted(xs)
    return xs[len(xs) // 2]
'''

TESTS = '''\
import unittest
from stats import median

class MedianTest(unittest.TestCase):
    def test_odd(self):
        self.assertEqual(median([3, 1, 2]), 2)

    def test_even(self):
        self.assertEqual(median([4, 1, 3, 2]), 2.5)

    def test_empty(self):
        with self.assertRaises(ValueError):
            median([])

if __name__ == "__main__":
    unittest.main()
'''

CANDIDATES = {
    "average the middle pair": '''\
def median(xs):
    xs = sorted(xs)
    mid = len(xs) // 2
    return (xs[mid - 1] + xs[mid]) / 2 if len(xs) % 2 == 0 else xs[mid]
''',
    "average the middle pair, reject empty": '''\
def median(xs):
    if not xs:
        raise ValueError("median of empty list")
    xs = sorted(xs)
    mid = len(xs) // 2
    return (xs[mid - 1] + xs[mid]) / 2 if len(xs) % 2 == 0 else xs[mid]
''',
    "use the mean": '''\
def median(xs):
    return sum(xs) / len(xs)
''',
}

def run_tests(sbx: Sandbox) -> bool:
    r = sbx.commands.run("python3 -m unittest -q test_stats", cwd="/work")
    return r.exit_code == 0


def try_candidate(base: Sandbox, name: str, code: str):
    t = time.time()
    fork = base.fork()
    forked_in = time.time() - t
    fork.files.write("/work/stats.py", code)
    passed = run_tests(fork)
    return name, code, passed, forked_in, fork


def main() -> None:
    with Sandbox.create(template=os.environ.get("BOWL_TEMPLATE", "python:3.12-slim"), timeout=600) as sbx:
        print(f"sandbox {sbx.sandbox_id}")
        sbx.files.write("/work/stats.py", BUGGY)
        sbx.files.write("/work/test_stats.py", TESTS)
        print("tests before:", "pass" if run_tests(sbx) else "fail")

        with ThreadPoolExecutor(len(CANDIDATES)) as pool:
            results = list(pool.map(lambda kv: try_candidate(sbx, *kv), CANDIDATES.items()))

        winner = None
        for name, code, passed, forked_in, fork in results:
            print(f"  fork {fork.sandbox_id[-12:]} ({forked_in:.2f}s to a ready fork): {name}: {'pass' if passed else 'fail'}")
            if passed and winner is None:
                winner = (name, code)
            fork.kill()
        if winner is None:
            sys.exit("no candidate passed")

        sbx.files.write("/work/stats.py", winner[1])
        print(f"applied '{winner[0]}'; tests after:", "pass" if run_tests(sbx) else "fail")


if __name__ == "__main__":
    main()
