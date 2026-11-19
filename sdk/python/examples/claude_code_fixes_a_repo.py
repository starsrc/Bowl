"""A real agent (Claude Code, headless) fixes a failing test suite in a sandbox, touching it only
through the SDK.

The driver clones pallets/itsdangerous into a fresh sandbox, installs it, and breaks one function
so its tests fail. Claude Code then runs in an empty local directory with a single allowed tool,
`python3 sbx.py ...`, and is asked to make the tests pass without editing them. The driver checks
the result itself. Needs the `claude` CLI and `bowl-sandbox serve`.

    BOWL_TEMPLATE=python:3.12-slim python3 examples/claude_code_fixes_a_repo.py
"""

import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from bowl_sandbox import Sandbox  # noqa: E402

REPO = "https://github.com/pallets/itsdangerous"
SRC = "/work/itsdangerous/src/itsdangerous/encoding.py"
TESTS = "cd /work/itsdangerous && python3 -m pytest -q tests 2>&1"


def main() -> None:
    sbx = Sandbox.create(template=os.environ.get("BOWL_TEMPLATE", "python:3.12-slim"), timeout=3600)
    print("sandbox", sbx.sandbox_id)
    try:
        setup = (
            "apt-get update -qq >/dev/null && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq git >/dev/null && "
            f"git clone -q --depth 1 {REPO} /work/itsdangerous && cd /work/itsdangerous && "
            "pip install -q -e . pytest freezegun >/dev/null 2>&1"
        )
        sbx.commands.run(setup, timeout=900, check=True)
        # The bug: base64 padding is stripped one character short.
        sbx.commands.run(f"sed -i 's/return base64.urlsafe_b64encode(string).rstrip(b\"=\")/"
                         f"return base64.urlsafe_b64encode(string).rstrip(b\"=\")[:-1]/' {SRC}", check=True)
        before = sbx.commands.run(TESTS)
        print("tests before:", before.stdout.strip().splitlines()[-1])
        if before.exit_code == 0:
            sys.exit("the injected bug did not break the tests")

        work = Path(tempfile.mkdtemp(prefix="agent-"))
        shutil.copy(HERE / "sbx.py", work / "sbx.py")
        shutil.copy(HERE.parent / "bowl_sandbox.py", work / "bowl_sandbox.py")
        prompt = (
            f"A Python project is checked out at /work/itsdangerous inside a remote sandbox {sbx.sandbox_id}. "
            "Its test suite fails. Find the bug in the library code and fix it so that "
            "`cd /work/itsdangerous && python3 -m pytest -q tests` passes. Do not edit anything under tests/. "
            f"You can reach the sandbox only with: python3 sbx.py {sbx.sandbox_id} run \"<shell command>\" "
            f"(also `read <path>` and `write <path>` with content on stdin). Stop when the tests pass."
        )
        t = time.time()
        agent = subprocess.run(
            ["claude", "-p", prompt, "--max-turns", "40", "--allowedTools", "Bash(python3 sbx.py:*)"],
            cwd=work, capture_output=True, text=True, timeout=1800,
        )
        print(f"agent finished in {time.time() - t:.0f}s, exit {agent.returncode}")
        print("agent said:", agent.stdout.strip()[-600:])

        after = sbx.commands.run(TESTS)
        print("tests after:", after.stdout.strip().splitlines()[-1])
        changed = sbx.commands.run("cd /work/itsdangerous && git status --short && git diff --stat").stdout
        print("changed files:\n" + changed)
        if after.exit_code != 0 or "tests/" in changed:
            sys.exit("the agent did not make the tests pass without touching tests/")
    finally:
        sbx.kill()


if __name__ == "__main__":
    main()
