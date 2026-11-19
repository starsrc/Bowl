"""Python SDK for Bowl sandboxes.

Talks to the HTTP API served by ``bowl-sandbox serve``. The shape follows E2B's SDK
(``Sandbox.create``, ``sbx.commands.run``, ``sbx.files.read/write``, ``sbx.kill``,
``sbx.set_timeout``); the wire protocol is Bowl's own. Standard library only.

    from bowl_sandbox import Sandbox

    with Sandbox.create(template="python:3.12-slim", timeout=600) as sbx:
        print(sbx.commands.run("python3 --version").stdout)
        child = sbx.fork()            # same disk contents, independent from here on
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Optional

__all__ = ["Sandbox", "CommandResult", "CommandHandle", "SandboxError", "CommandExitError"]

DEFAULT_URL = "http://127.0.0.1:7070"


class SandboxError(Exception):
    """The API returned an error."""

    def __init__(self, status: int, message: str):
        super().__init__(f"{status}: {message}")
        self.status = status
        self.message = message


@dataclass
class CommandResult:
    exit_code: int
    stdout: str
    stderr: str


class CommandExitError(SandboxError):
    """A command run with check=True exited non-zero."""

    def __init__(self, result: CommandResult):
        super().__init__(0, f"command exited with {result.exit_code}: {result.stderr.strip()}")
        self.result = result


class _Client:
    def __init__(self, url: Optional[str], api_key: Optional[str], request_timeout: float):
        self.url = (url or os.environ.get("BOWL_SANDBOX_URL") or DEFAULT_URL).rstrip("/")
        self.api_key = api_key or os.environ.get("BOWL_SANDBOX_API_KEY")
        self.request_timeout = request_timeout

    def call(self, method: str, path: str, body=None, raw: Optional[bytes] = None, timeout: Optional[float] = None):
        data, headers = None, {}
        if raw is not None:
            data, headers["Content-Type"] = raw, "application/octet-stream"
        elif body is not None:
            data, headers["Content-Type"] = json.dumps(body).encode(), "application/json"
        if self.api_key:
            headers["Authorization"] = "Bearer " + self.api_key
        req = urllib.request.Request(self.url + path, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.request_timeout) as resp:
                payload = resp.read()
                if resp.headers.get("Content-Type", "").startswith("application/json"):
                    return json.loads(payload) if payload else None
                return payload
        except urllib.error.HTTPError as e:
            text = e.read().decode(errors="replace")
            try:
                text = json.loads(text).get("error", text)
            except ValueError:
                pass
            raise SandboxError(e.code, text) from None


class CommandHandle:
    """A background command; ``wait()`` blocks until it exits."""

    def __init__(self, sbx: "Sandbox", run_id: str):
        self._sbx, self.run_id = sbx, run_id

    def wait(self, timeout: float = 3600) -> CommandResult:
        r = self._sbx._client.call("GET", f"{self._sbx._path}/commands/{self.run_id}", timeout=timeout)
        return CommandResult(r["exit_code"], r["stdout"], r["stderr"])


class _Commands:
    def __init__(self, sbx: "Sandbox"):
        self._sbx = sbx

    def run(self, cmd, cwd: Optional[str] = None, envs: Optional[dict] = None, stdin: str = "",
            background: bool = False, timeout: Optional[float] = None, check: bool = False):
        """Run ``cmd`` in the sandbox: a string goes through ``sh -c``, a list runs as argv.

        Returns a CommandResult, or a CommandHandle when ``background`` is set. ``timeout``
        kills the command after that many seconds. ``check`` raises on a non-zero exit.
        """
        body = {"cwd": cwd or "", "envs": envs or {}, "stdin": stdin, "background": background}
        if isinstance(cmd, str):
            body["cmd"] = cmd
        else:
            body["argv"] = list(cmd)
        if timeout:
            body["timeout_seconds"] = int(timeout)
        wait = None if background else (timeout or 3600) + 30
        r = self._sbx._client.call("POST", f"{self._sbx._path}/commands", body, timeout=wait)
        if background:
            return CommandHandle(self._sbx, r["run_id"])
        result = CommandResult(r["exit_code"], r["stdout"], r["stderr"])
        if check and result.exit_code != 0:
            raise CommandExitError(result)
        return result


class _Files:
    def __init__(self, sbx: "Sandbox"):
        self._sbx = sbx

    def _path(self, path: str) -> str:
        return f"{self._sbx._path}/files?" + urllib.parse.urlencode({"path": path})

    def read(self, path: str) -> str:
        return self.read_bytes(path).decode()

    def read_bytes(self, path: str) -> bytes:
        return self._sbx._client.call("GET", self._path(path))

    def write(self, path: str, data) -> None:
        """Write a file, creating parent directories."""
        raw = data.encode() if isinstance(data, str) else bytes(data)
        self._sbx._client.call("PUT", self._path(path), raw=raw)


class Sandbox:
    """A running sandbox. Its disk is a Bowl disk, so ``fork()`` copies it in milliseconds."""

    def __init__(self, info: dict, client: _Client):
        self._client = client
        self._info = info
        self.sandbox_id: str = info["sandbox_id"]
        self._path = "/sandboxes/" + urllib.parse.quote(self.sandbox_id)
        self.commands = _Commands(self)
        self.files = _Files(self)

    @classmethod
    def create(cls, template: str = "", timeout: Optional[int] = 300, cpus: int = 0, memory_mib: int = 0,
               url: Optional[str] = None, api_key: Optional[str] = None, request_timeout: float = 120) -> "Sandbox":
        """Start a sandbox from a template (see ``bowl-sandbox template list``).

        ``timeout`` is seconds until the sandbox and its disk are removed; None keeps it until kill().
        """
        client = _Client(url, api_key, request_timeout)
        body = {"template": template, "timeout_seconds": int(timeout or 0), "cpus": cpus, "memory_mib": memory_mib}
        return cls(client.call("POST", "/sandboxes", body), client)

    @classmethod
    def connect(cls, sandbox_id: str, url: Optional[str] = None, api_key: Optional[str] = None,
                request_timeout: float = 120) -> "Sandbox":
        client = _Client(url, api_key, request_timeout)
        return cls(client.call("GET", "/sandboxes/" + urllib.parse.quote(sandbox_id)), client)

    @classmethod
    def list(cls, url: Optional[str] = None, api_key: Optional[str] = None) -> list:
        return _Client(url, api_key, 30).call("GET", "/sandboxes")

    def fork(self) -> "Sandbox":
        """Copy this sandbox's disk at this moment and start a new sandbox on it.

        This sandbox is paused for the instant of the copy and then keeps running. Processes are
        not copied: the new sandbox boots fresh on the copied disk.
        """
        return Sandbox(self._client.call("POST", self._path + "/fork"), self._client)

    def set_timeout(self, timeout: Optional[int]) -> None:
        """Remove the sandbox ``timeout`` seconds from now; None or 0 cancels the deadline."""
        self._info = self._client.call("POST", self._path + "/timeout", {"timeout_seconds": int(timeout or 0)})

    def get_info(self) -> dict:
        self._info = self._client.call("GET", self._path)
        return self._info

    @property
    def address(self) -> str:
        """IP to reach ports inside the sandbox from the host running bowl-sandbox serve."""
        return self._info.get("address", "")

    def kill(self) -> None:
        """Remove the sandbox and its disk."""
        try:
            self._client.call("DELETE", self._path)
        except SandboxError as e:
            if e.status != 404:
                raise

    def __enter__(self) -> "Sandbox":
        return self

    def __exit__(self, *exc) -> None:
        self.kill()

    def __repr__(self) -> str:
        return f"Sandbox({self.sandbox_id!r})"
