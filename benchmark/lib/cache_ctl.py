"""Cache control (C0/C1/C2/C3) and server cgroup io.stat reading."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from pathlib import Path
from typing import Callable

from benchmark.lib.metrics_audit import parse_cgroup_io_stat

DEFAULT_DEPLOY_SCRIPT = "/opt/bowl-bench/benchmark/infra/deploy_target.sh"
DEFAULT_CGROUP_IO_STAT = "/sys/fs/cgroup/bench-target.slice/io.stat"


def _ssh_cmd(server_ssh: str, remote_cmd: str) -> list[str]:
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "ConnectTimeout=10",
        server_ssh,
        remote_cmd,
    ]


def drop_local_page_cache(
    *,
    remount_fn: Callable[[], None] | None = None,
) -> dict:
    """Flush dirty buffers and drop Linux client page cache, dentries, and inodes."""
    try:
        os.sync()
    except AttributeError:
        pass

    drop_path = Path("/proc/sys/vm/drop_caches")
    if drop_path.exists():
        try:
            drop_path.write_text("3\n", encoding="utf-8")
            return {"dropped": True, "method": "proc_drop_caches"}
        except OSError:
            pass

        if shutil.which("sudo"):
            proc = subprocess.run(
                ["sudo", "-n", "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode == 0:
                return {"dropped": True, "method": "sudo_drop_caches"}

    if remount_fn is not None:
        remount_fn()
        return {"dropped": True, "method": "remount"}

    return {"dropped": False, "method": "unavailable"}


def clear_directory_contents(directory: Path | None) -> bool:
    """Remove all files and subdirectories inside directory without removing it."""
    if directory is None or not directory.exists():
        return False
    for child in directory.iterdir():
        try:
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)
        except OSError:
            pass
    return True


def read_server_cgroup_rbytes(
    *,
    server_ssh: str | None = None,
    deploy_script: str = DEFAULT_DEPLOY_SCRIPT,
    cgroup_path: str = DEFAULT_CGROUP_IO_STAT,
) -> int | None:
    """Read cumulative block-device rbytes for /bench-target.slice on the server."""
    if server_ssh:
        cmd = _ssh_cmd(
            server_ssh,
            f"if [ -x {shlex.quote(deploy_script)} ]; then "
            f"{shlex.quote(deploy_script)} cgroup-rbytes; "
            f"elif [ -r {shlex.quote(cgroup_path)} ]; then "
            f"cat {shlex.quote(cgroup_path)}; fi",
        )
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15, check=False)
        if proc.returncode != 0 or not proc.stdout.strip():
            return None
        parsed = parse_cgroup_io_stat(proc.stdout)
        return parsed["rbytes"]

    local_path = Path(cgroup_path)
    if local_path.exists():
        try:
            parsed = parse_cgroup_io_stat(local_path.read_text(encoding="utf-8"))
            return parsed["rbytes"]
        except OSError:
            return None
    return None


def drop_remote_caches_and_restart(
    *,
    server_ssh: str,
    target: str,
    deploy_script: str = DEFAULT_DEPLOY_SCRIPT,
    extra_env: dict[str, str] | None = None,
) -> dict:
    """Drop server page cache and cold-restart the active target on Node B."""
    env_prefix = ""
    if extra_env:
        parts = [f"{k}={shlex.quote(str(v))}" for k, v in sorted(extra_env.items())]
        env_prefix = " ".join(parts) + " "

    remote = (
        "sync && (echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || "
        "sudo -n sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches' || true) && "
        f"{env_prefix}{shlex.quote(deploy_script)} restart-cold {shlex.quote(target)}"
    )
    proc = subprocess.run(_ssh_cmd(server_ssh, remote), capture_output=True, text=True, timeout=120, check=False)
    return {
        "dropped": proc.returncode == 0,
        "returncode": proc.returncode,
        "stderr": proc.stderr.strip(),
    }


def drop_caches(
    level: str,
    *,
    target: str | None = None,
    server_ssh: str | None = None,
    deploy_script: str = DEFAULT_DEPLOY_SCRIPT,
    jfs_cache_dir: Path | None = None,
    remount_fn: Callable[[], None] | None = None,
    extra_env: dict[str, str] | None = None,
) -> dict:
    """Execute cache control for C0, C1, C2, C3, C3-WarmServer, or C3-ColdServer."""
    norm = level.strip()
    if norm == "C0":
        return {"level": "C0", "client_dropped": False, "server_dropped": False}

    jfs_cleared = False
    if target and target.startswith("jfs_") and jfs_cache_dir is not None:
        jfs_cleared = clear_directory_contents(jfs_cache_dir)

    client_res = drop_local_page_cache(remount_fn=remount_fn)
    result: dict = {
        "level": norm,
        "client_dropped": client_res["dropped"],
        "client_method": client_res["method"],
        "jfs_cache_cleared": jfs_cleared,
        "server_dropped": False,
    }

    if norm in ("C2", "C3-ColdServer"):
        if server_ssh and target and target not in ("ext4", "passthrough_ll"):
            srv_res = drop_remote_caches_and_restart(
                server_ssh=server_ssh,
                target=target,
                deploy_script=deploy_script,
                extra_env=extra_env,
            )
            result["server_dropped"] = srv_res["dropped"]
            result["server_restart"] = srv_res
        elif not server_ssh:
            result["server_dropped"] = client_res["dropped"]

    return result
