# bowl-sandbox Python SDK

Talks to `bowl-sandbox serve`. Standard library only; copy `bowl_sandbox.py` or add this directory to `sys.path`.

```python
from bowl_sandbox import Sandbox

with Sandbox.create(template="python:3.12-slim", timeout=600) as sbx:
    sbx.files.write("/work/hello.py", "print('hi')")
    print(sbx.commands.run("python3 /work/hello.py").stdout)

    child = sbx.fork()          # copies the disk; sbx keeps running
    child.commands.run("rm -rf /work")
    child.kill()
```

`BOWL_SANDBOX_URL` (default `http://127.0.0.1:7070`) and `BOWL_SANDBOX_API_KEY` configure the client.

`examples/try_fixes_in_forks.py` tries three fixes for a failing test in three forks at once and keeps the one that passes.

`examples/claude_code_fixes_a_repo.py` gives a real agent (Claude Code, headless) a broken checkout of pallets/itsdangerous in a sandbox; the agent reaches the sandbox only through `examples/sbx.py`, a small command line over the SDK, and makes the tests pass.
