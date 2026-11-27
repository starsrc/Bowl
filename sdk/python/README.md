# bowl-sandbox Python SDK

Talks to `bowl-sandbox serve`. Standard library only; copy `bowl_sandbox.py` or add this directory to `sys.path`.

```python
from bowl_sandbox import Sandbox

with Sandbox.create(template="python:3.12-slim", timeout=600) as sbx:
    sbx.files.write("/work/hello.py", "print('hi')")
    print(sbx.commands.run("python3 /work/hello.py").stdout)

    job = sbx.commands.run("sleep 1; echo done", background=True)
    print(job.wait().stdout)
```

The template must exist on the host running `serve` (`bowl-sandbox template create python:3.12-slim`).

`BOWL_SANDBOX_URL` (default `http://127.0.0.1:7070`) and `BOWL_SANDBOX_API_KEY` configure the client.

`examples/sbx.py` is a small command line over the SDK (`run`, `read`, `write` against one sandbox ID), for an agent that should reach the sandbox only through it.
