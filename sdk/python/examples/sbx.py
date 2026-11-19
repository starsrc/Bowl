"""A tiny command line over the SDK, for an agent that may only touch the sandbox through it.

    python3 sbx.py <sandbox-id> run "<shell command>" [--cwd DIR] [--timeout SECONDS]
    python3 sbx.py <sandbox-id> read <path>
    python3 sbx.py <sandbox-id> write <path>      # file content on stdin

`run` prints stdout and stderr and exits with the command's exit code.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bowl_sandbox import Sandbox  # noqa: E402


def main(argv):
    if len(argv) < 3:
        sys.exit(__doc__)
    sbx, op, arg = Sandbox.connect(argv[0]), argv[1], argv[2]
    if op == "run":
        opts = dict(zip(argv[3::2], argv[4::2]))
        r = sbx.commands.run(arg, cwd=opts.get("--cwd"), timeout=float(opts.get("--timeout", 600)))
        sys.stdout.write(r.stdout)
        sys.stderr.write(r.stderr)
        sys.exit(r.exit_code)
    if op == "read":
        sys.stdout.write(sbx.files.read(arg))
        return
    if op == "write":
        sbx.files.write(arg, sys.stdin.read())
        return
    sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
