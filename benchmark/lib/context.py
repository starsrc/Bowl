"""Execution context and target lifecycle management for Bowl Benchmark v2."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import socket
import subprocess
import threading
import time
import urllib.parse
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from benchmark.lib import cache_ctl, metrics_audit


@dataclass
class MountHandle:
    """Active mount handle returned by BenchContext.prepare_target."""

    target: str
    mount_dir: Path
    stage_dir: Path | None = None
    admin_url: str | None = None
    disk_id: str | None = None
    branch_name: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    _unmount_fn: Callable[[], float] | None = field(default=None, repr=False, compare=False)
    _unmounted: bool = field(default=False, repr=False, compare=False)

    def unmount(self) -> float:
        """Unmount and drain the mountpoint idempotently. Returns wall seconds."""
        if self._unmounted:
            return 0.0
        self._unmounted = True
        if self._unmount_fn is None:
            return 0.0
        return float(self._unmount_fn())

    def __enter__(self) -> MountHandle:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.unmount()


_PORT_LOCK = threading.Lock()
_PORT_RANGE_START = 19090
_PORT_RANGE_END = 19999
_NEXT_PORT = _PORT_RANGE_START
_RESERVED_PORTS: set[int] = set()


def _release_local_port(port: int) -> None:
    with _PORT_LOCK:
        _RESERVED_PORTS.discard(port)


def _find_free_local_port(start: int = _PORT_RANGE_START, end: int = _PORT_RANGE_END) -> int:
    global _NEXT_PORT
    span = end - start + 1
    with _PORT_LOCK:
        cursor = _NEXT_PORT if start <= _NEXT_PORT <= end else start
        for offset in range(span):
            port = start + ((cursor - start + offset) % span)
            if port in _RESERVED_PORTS:
                continue
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    sock.bind(("127.0.0.1", port))
                except OSError:
                    continue
            _RESERVED_PORTS.add(port)
            _NEXT_PORT = start + ((port - start + 1) % span)
            return port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            port = int(sock.getsockname()[1])
            _RESERVED_PORTS.add(port)
            return port


def _wait_readyz(base_url: str, timeout_s: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        ok, _ = metrics_audit.check_readyz(base_url, timeout_s=2.0)
        if ok:
            return True
        time.sleep(0.15)
    return False


def _wait_ismount(mount_dir: Path, timeout_s: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if os.path.ismount(str(mount_dir)):
            return True
        time.sleep(0.1)
    return False


class BenchContext:
    """Shared execution context passed to every benchmark suite."""

    def __init__(
        self,
        *,
        profile: str,
        profile_cfg: dict[str, Any],
        targets: list[str],
        run_dir: Path,
        suite_dir: Path,
        nvme_root: Path = Path("/mnt/nvme"),
        bowl_bin: str = "bowl",
        server_endpoint: str = "http://127.0.0.1:9000",
        server_metrics_url: str = "http://127.0.0.1:9090",
        server_ssh: str | None = None,
        tools_bin: Path = Path("/usr/local/bench/bin"),
        corpus_dir: Path = Path("/usr/local/bench/corpus"),
        deploy_script: str | None = None,
    ) -> None:
        self.profile = profile
        self.profile_cfg = profile_cfg
        self.targets = list(targets)
        self.run_dir = Path(run_dir)
        self.suite_dir = Path(suite_dir)
        self.nvme_root = Path(nvme_root)
        self.bowl_bin = bowl_bin
        self.server_endpoint = server_endpoint.rstrip("/")
        self.server_metrics_url = server_metrics_url.rstrip("/")
        self.server_ssh = server_ssh
        self.tools_bin = Path(tools_bin)
        self.corpus_dir = Path(corpus_dir)
        self.deploy_script = deploy_script or os.environ.get(
            "BOWL_DEPLOY_SCRIPT", cache_ctl.DEFAULT_DEPLOY_SCRIPT
        )
        self.active_handles: list[MountHandle] = []
        self._handles_lock = threading.Lock()
        self._deploy_lock = threading.Lock()
        self._current_remote_target: str | None = None
        self._current_remote_env: dict[str, str] = {}
        self.suite_dir.mkdir(parents=True, exist_ok=True)

    @property
    def server_host(self) -> str:
        parsed = urllib.parse.urlparse(self.server_endpoint)
        if parsed.hostname:
            return parsed.hostname
        if self.server_ssh:
            return self.server_ssh.split("@")[-1]
        return "127.0.0.1"

    def run_cmd(
        self,
        cmd: list[str] | str,
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout: int | None = None,
        check: bool = True,
        shell: bool = False,
    ) -> subprocess.CompletedProcess:
        """Execute a subprocess command and return CompletedProcess."""
        merged_env = os.environ.copy()
        if env:
            merged_env.update({k: str(v) for k, v in env.items()})
        return subprocess.run(
            cmd,
            cwd=str(cwd) if cwd is not None else None,
            env=merged_env,
            timeout=timeout,
            check=check,
            shell=shell,
            capture_output=True,
            text=True,
        )

    def bowl_rpc_cli(
        self,
        args: list[str],
        *,
        check: bool = True,
    ) -> dict[str, Any] | list[Any] | str:
        """Run the bowl CLI against self.server_endpoint."""
        cmd = [self.bowl_bin, "--endpoint", self.server_endpoint, *args]
        proc = self.run_cmd(cmd, check=check)
        out = proc.stdout.strip()
        if "--json" in args and out:
            return json.loads(out)
        return out

    def _deploy_remote_target(
        self,
        target: str,
        *,
        ttl_override: float | None = None,
        gc_interval: str | None = None,
        profiling: bool = False,
    ) -> None:
        if not self.server_ssh or target in ("ext4", "passthrough_ll"):
            return
        env_map: dict[str, str] = {
            "LOG_LEVEL": "info",
            "LOG_FORMAT": "json",
            "LOG_ADD_SOURCE": "false",
            "OTEL_ENABLED": "false",
            "PPROF_ENABLED": "true" if profiling else "false",
            "PPROF_MUTEX_FRACTION": "5" if profiling else "0",
            "PPROF_BLOCK_RATE": "10000" if profiling else "0",
            "METRICS_ENABLED": "true",
        }
        if profiling:
            env_map["PROFILING"] = "1"
        if ttl_override is not None:
            ttl_s = f"{ttl_override:g}"
            env_map["STORAGE_ENTRY_TIMEOUT_SECONDS"] = ttl_s
            env_map["STORAGE_ATTR_TIMEOUT_SECONDS"] = ttl_s
            env_map["STORAGE_DIR_ENTRY_TIMEOUT_SECONDS"] = ttl_s
        if gc_interval is not None:
            env_map["STORAGE_GC_INTERVAL"] = str(gc_interval)

        with self._deploy_lock:
            if self._current_remote_target == target and self._current_remote_env == env_map:
                return

            prefix = " ".join(f"{k}={shlex.quote(v)}" for k, v in sorted(env_map.items()))
            remote_cmd = f"{prefix} {shlex.quote(self.deploy_script)} start {shlex.quote(target)}"
            proc = self.run_cmd(cache_ctl._ssh_cmd(self.server_ssh, remote_cmd), timeout=180, check=True)
            _ = proc
            self._current_remote_target = target
            self._current_remote_env = env_map

    def prepare_target(
        self,
        target: str,
        *,
        ttl_override: float | None = None,
        gc_interval: str | None = None,
        jfs_cache_mode: str = "zero",
        direct_io: bool = False,
        allow_other: bool = False,
        read_only: bool = False,
        disk_name: str | None = None,
        branch_name: str | None = None,
        stage_size: str | None = None,
        disk_id: str | None = None,
        disk_size: str | None = None,
    ) -> MountHandle:
        """Prepare and mount the requested target, returning an idempotent MountHandle."""
        tag = uuid.uuid4().hex[:8]
        mounts_root = self.nvme_root / "mounts"
        mounts_root.mkdir(parents=True, exist_ok=True)
        mount_dir = mounts_root / f"{target}-{tag}"
        mount_dir.mkdir(parents=True, exist_ok=True)

        if target == "ext4":
            def _unmount_ext4() -> float:
                t0 = time.monotonic()
                try:
                    os.sync()
                except AttributeError:
                    pass
                shutil.rmtree(mount_dir, ignore_errors=True)
                return time.monotonic() - t0

            handle = MountHandle(
                target=target,
                mount_dir=mount_dir,
                extra={"ttl_override": ttl_override},
                _unmount_fn=_unmount_ext4,
            )
            self.active_handles.append(handle)
            return handle

        if target == "passthrough_ll":
            backing_dir = self.nvme_root / "backing" / f"pt-{tag}"
            backing_dir.mkdir(parents=True, exist_ok=True)
            pt_bin = self.tools_bin / "passthrough_ll"
            if not pt_bin.exists():
                found = shutil.which("passthrough_ll")
                if found:
                    pt_bin = Path(found)
                else:
                    raise FileNotFoundError(f"passthrough_ll binary not found at {pt_bin}")

            opts = [f"source={backing_dir}"]
            if allow_other:
                opts.append("allow_other")
            if read_only:
                opts.append("ro")
            cmd = [str(pt_bin), "-f", "-o", ",".join(opts), str(mount_dir)]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if not _wait_ismount(mount_dir, timeout_s=15.0):
                proc.terminate()
                _, err = proc.communicate(timeout=5)
                raise RuntimeError(f"passthrough_ll failed to mount {mount_dir}: {err}")

            def _unmount_pt() -> float:
                t0 = time.monotonic()
                unmounter = shutil.which("fusermount3") or shutil.which("fusermount")
                if unmounter:
                    subprocess.run([unmounter, "-u", str(mount_dir)], check=False, capture_output=True)
                else:
                    subprocess.run(["umount", str(mount_dir)], check=False, capture_output=True)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.terminate()
                    proc.wait(timeout=5)
                shutil.rmtree(mount_dir, ignore_errors=True)
                shutil.rmtree(backing_dir, ignore_errors=True)
                return time.monotonic() - t0

            handle = MountHandle(
                target=target,
                mount_dir=mount_dir,
                extra={"backing_dir": str(backing_dir), "pid": proc.pid},
                _unmount_fn=_unmount_pt,
            )
            self.active_handles.append(handle)
            return handle

        if target in ("bowl_staged", "bowl_sync"):
            self._deploy_remote_target(target, ttl_override=ttl_override, gc_interval=gc_interval)
            if not _wait_readyz(self.server_metrics_url, timeout_s=30.0):
                raise RuntimeError(f"bowl storage-service not ready at {self.server_metrics_url}/readyz")

            effective_disk_id = disk_id
            effective_branch = branch_name or "main"
            if effective_disk_id is None:
                size_str = disk_size or str(self.profile_cfg.get("disk_size", "64GiB"))
                dname = disk_name or f"bench-{self.suite_dir.name}-{tag}"
                created = self.bowl_rpc_cli(
                    ["disk", "create", "--size", size_str, "--name", dname, "--json"]
                )
                if not isinstance(created, dict) or not created.get("disk_id"):
                    raise RuntimeError(f"bowl disk create returned unexpected output: {created!r}")
                effective_disk_id = str(created["disk_id"])

            mode = "ro" if read_only else ("sync" if target == "bowl_sync" else "staged")
            port = _find_free_local_port()
            admin_addr = f"127.0.0.1:{port}"
            admin_url = f"http://{admin_addr}"

            effective_stage_size = stage_size or str(
                self.profile_cfg.get("topology", {}).get("stage_size", "512MiB")
            )
            stage_dir: Path | None = None
            mount_cmd = [
                self.bowl_bin,
                "--endpoint",
                self.server_endpoint,
                "mount",
                effective_disk_id,
                str(mount_dir),
                "--branch",
                effective_branch,
                "--mode",
                mode,
                "--trace-sample-ratio=0",
                f"--admin-addr={admin_addr}",
                "--log-level=info",
                "--log-format=json",
            ]
            if mode == "staged":
                stage_dir = self.nvme_root / "bowl-stage" / f"{tag}"
                stage_dir.mkdir(parents=True, exist_ok=True)
                mount_cmd.extend([f"--stage-dir={stage_dir}", f"--stage-size={effective_stage_size}"])
            if direct_io:
                mount_cmd.append("--direct-io")
            if allow_other:
                mount_cmd.append("--allow-other")

            try:
                self.run_cmd(mount_cmd, timeout=60, check=True)
                if not _wait_readyz(admin_url, timeout_s=30.0):
                    raise RuntimeError(f"bowl mount admin endpoint not ready at {admin_url}/readyz")
            except Exception:
                _release_local_port(port)
                raise

            def _unmount_bowl() -> float:
                t0 = time.monotonic()
                try:
                    proc = self.run_cmd([self.bowl_bin, "umount", str(mount_dir)], timeout=320, check=False)
                    if proc.returncode != 0:
                        self.run_cmd([self.bowl_bin, "umount", "-f", str(mount_dir)], timeout=30, check=False)
                    elapsed = time.monotonic() - t0
                    if stage_dir is not None:
                        shutil.rmtree(stage_dir, ignore_errors=True)
                    shutil.rmtree(mount_dir, ignore_errors=True)
                    return elapsed
                finally:
                    _release_local_port(port)

            handle = MountHandle(
                target=target,
                mount_dir=mount_dir,
                stage_dir=stage_dir,
                admin_url=admin_url,
                disk_id=effective_disk_id,
                branch_name=effective_branch,
                extra={
                    "mode": mode,
                    "direct_io": direct_io,
                    "allow_other": allow_other,
                    "ttl_override": ttl_override,
                    "gc_interval": gc_interval,
                    "stage_size": effective_stage_size if mode == "staged" else None,
                },
                _unmount_fn=_unmount_bowl,
            )
            self.active_handles.append(handle)
            return handle

        if target in ("jfs_pg_minio", "jfs_redis_minio"):
            self._deploy_remote_target(target, ttl_override=ttl_override)
            jfs_bin = shutil.which("juicefs") or "/usr/local/bin/juicefs"
            host = self.server_host
            minio_port = os.environ.get("JFS_MINIO_PORT", "9000")
            bucket_url = os.environ.get("JFS_MINIO_BUCKET", f"http://{host}:{minio_port}/juicefs")
            access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
            secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

            if target == "jfs_pg_minio":
                meta_url = os.environ.get(
                    "JFS_PG_URL", f"postgres://juicefs:juicefs@{host}:5432/juicefs?sslmode=disable"
                )
            else:
                meta_url = os.environ.get("JFS_REDIS_URL", f"redis://{host}:6379/1")

            vol_name = f"bench-{tag}"
            fmt_cmd = [
                jfs_bin,
                "format",
                "--storage",
                "minio",
                "--bucket",
                bucket_url,
                "--access-key",
                access_key,
                "--secret-key",
                secret_key,
                meta_url,
                vol_name,
            ]
            self.run_cmd(fmt_cmd, timeout=60, check=False)

            jfs_cache_dir = self.nvme_root / "jfs-cache" / tag
            jfs_cache_dir.mkdir(parents=True, exist_ok=True)
            mount_cmd = [jfs_bin, "mount", "-d", meta_url, str(mount_dir)]
            if jfs_cache_mode in ("tier0", "writeback", "512"):
                mount_cmd.extend(
                    [
                        "--writeback",
                        f"--cache-dir={jfs_cache_dir}",
                        "--cache-size=512",
                        "--buffer-size=300",
                    ]
                )
            else:
                mount_cmd.extend(["--cache-size=0", f"--cache-dir={jfs_cache_dir}"])

            if ttl_override is not None:
                ttl_s = f"{ttl_override:g}"
                mount_cmd.extend(
                    [f"--attr-cache={ttl_s}", f"--entry-cache={ttl_s}", f"--dir-entry-cache={ttl_s}"]
                )
            fuse_opts: list[str] = []
            if allow_other:
                fuse_opts.append("allow_other")
            if read_only:
                fuse_opts.append("ro")
            if fuse_opts:
                mount_cmd.extend(["-o", ",".join(fuse_opts)])

            self.run_cmd(mount_cmd, timeout=60, check=True)
            _wait_ismount(mount_dir, timeout_s=20.0)

            def _unmount_jfs() -> float:
                t0 = time.monotonic()
                self.run_cmd([jfs_bin, "umount", str(mount_dir)], timeout=180, check=False)
                elapsed = time.monotonic() - t0
                shutil.rmtree(jfs_cache_dir, ignore_errors=True)
                shutil.rmtree(mount_dir, ignore_errors=True)
                return elapsed

            handle = MountHandle(
                target=target,
                mount_dir=mount_dir,
                stage_dir=jfs_cache_dir,
                extra={
                    "meta_url": meta_url,
                    "jfs_cache_mode": jfs_cache_mode,
                    "ttl_override": ttl_override,
                },
                _unmount_fn=_unmount_jfs,
            )
            self.active_handles.append(handle)
            return handle

        if target == "drive9":
            if not os.environ.get("DRIVE9_REPO"):
                raise RuntimeError("drive9 target requested without DRIVE9_REPO set")
            self._deploy_remote_target(target, ttl_override=ttl_override)
            drive9_bin = shutil.which("drive9") or "/usr/local/bin/drive9"
            mount_cmd = [drive9_bin, "mount", "--endpoint", self.server_endpoint, str(mount_dir)]
            self.run_cmd(mount_cmd, timeout=60, check=True)

            def _unmount_drive9() -> float:
                t0 = time.monotonic()
                unmounter = shutil.which("fusermount3") or shutil.which("fusermount") or "umount"
                args = [unmounter, "-u", str(mount_dir)] if "fusermount" in unmounter else [unmounter, str(mount_dir)]
                subprocess.run(args, check=False, capture_output=True)
                shutil.rmtree(mount_dir, ignore_errors=True)
                return time.monotonic() - t0

            handle = MountHandle(target=target, mount_dir=mount_dir, _unmount_fn=_unmount_drive9)
            self.active_handles.append(handle)
            return handle

        raise ValueError(f"unsupported target: {target!r}")

    def drop_caches(
        self,
        level: str,
        *,
        target: str | None = None,
        handle: MountHandle | None = None,
    ) -> dict[str, Any]:
        """Drop client and optionally server caches per C0/C1/C2/C3 rules."""
        eff_target = target or (handle.target if handle is not None else None)
        jfs_dir = handle.stage_dir if (handle is not None and eff_target and eff_target.startswith("jfs_")) else None
        return cache_ctl.drop_caches(
            level,
            target=eff_target,
            server_ssh=self.server_ssh,
            deploy_script=self.deploy_script,
            jfs_cache_dir=jfs_dir,
            extra_env=self._current_remote_env if self._current_remote_env else None,
        )

    def snapshot_counters(
        self,
        target: str,
        handle: MountHandle | None = None,
    ) -> dict[str, Any]:
        """Capture pre/post metrics snapshot from server, cgroup io.stat, and client admin."""
        snap: dict[str, Any] = {
            "ts": time.time(),
            "target": target,
            "server_metrics": {},
            "client_metrics": {},
            "client_debug_stats": None,
            "server_cgroup_rbytes": None,
            "server_readyz_ok": None,
            "client_readyz_ok": None,
        }

        if target.startswith("bowl"):
            status, body = metrics_audit.scrape_http_text(f"{self.server_metrics_url}/metrics")
            if status == 200 and body:
                snap["server_metrics"] = metrics_audit.parse_prometheus_text(
                    body, allowed_names=metrics_audit.ALLOWED_SERVER_METRICS
                )
            ok, _ = metrics_audit.check_readyz(self.server_metrics_url)
            snap["server_readyz_ok"] = ok

        if target not in ("ext4", "passthrough_ll"):
            snap["server_cgroup_rbytes"] = cache_ctl.read_server_cgroup_rbytes(
                server_ssh=self.server_ssh,
                deploy_script=self.deploy_script,
            )

        if handle is not None and handle.admin_url:
            status, body = metrics_audit.scrape_http_text(f"{handle.admin_url}/metrics")
            if status == 200 and body:
                snap["client_metrics"] = metrics_audit.parse_prometheus_text(
                    body, allowed_names=metrics_audit.ALLOWED_CLIENT_METRICS
                )
            _, dbg = metrics_audit.scrape_http_json(f"{handle.admin_url}/debug/stats?format=json")
            snap["client_debug_stats"] = dbg
            ok, _ = metrics_audit.check_readyz(handle.admin_url)
            snap["client_readyz_ok"] = ok

        return snap

    def audit_case(
        self,
        before: dict[str, Any],
        after: dict[str, Any],
        *,
        target: str,
        cache_state: str = "C0",
        expected_read_bytes: int = 0,
        expected_stat_ops: int = 0,
        min_fuse_ratio: float = 0.95,
        min_nvme_ratio: float = 0.90,
    ) -> dict[str, Any]:
        """Compute counter deltas and enforce validity and cache-bypass gates."""
        require_nvme = self.profile == "full-vpc" and target not in ("ext4", "passthrough_ll")
        return metrics_audit.audit_case(
            before,
            after,
            target=target,
            cache_state=cache_state,
            expected_read_bytes=expected_read_bytes,
            expected_stat_ops=expected_stat_ops,
            min_fuse_ratio=min_fuse_ratio,
            min_nvme_ratio=min_nvme_ratio,
            require_server_nvme=require_nvme,
        )

    def sample_metrics_background(
        self,
        interval_s: float,
        stop_event: threading.Event,
        out_list: list[dict[str, Any]],
        *,
        handle: MountHandle | None = None,
    ) -> threading.Thread:
        """Background sampler for online-pack-gc 10s curves and Profiling Pass 1s peaks."""
        t_start = time.monotonic()

        def _loop() -> None:
            while True:
                sample: dict[str, Any] = {
                    "ts": time.time(),
                    "elapsed_s": round(time.monotonic() - t_start, 2),
                }
                status, body = metrics_audit.scrape_http_text(f"{self.server_metrics_url}/metrics")
                if status == 200 and body:
                    parsed = metrics_audit.parse_prometheus_text(
                        body, allowed_names=metrics_audit.ALLOWED_SERVER_METRICS
                    )
                    phys = metrics_audit.extract_metric_sum(parsed, "bowl_storage_pack_physical_bytes")
                    logical = metrics_audit._server_logical_bytes(parsed)
                    sample.update(
                        {
                            "pack_physical_bytes": int(round(phys)),
                            "logical_bytes": int(round(logical)),
                            "space_amp": round(phys / logical, 4) if logical > 0 else None,
                            "l0_sublevels": int(
                                round(metrics_audit.extract_metric_sum(parsed, "bowl_storage_pebble_l0_sublevels"))
                            ),
                            "compaction_debt_bytes": int(
                                round(
                                    metrics_audit.extract_metric_sum(
                                        parsed, "bowl_storage_pebble_compaction_debt_bytes"
                                    )
                                )
                            ),
                            "read_amp": int(
                                round(metrics_audit.extract_metric_sum(parsed, "bowl_storage_pebble_read_amp"))
                            ),
                        }
                    )
                if handle is not None and handle.admin_url:
                    c_status, c_body = metrics_audit.scrape_http_text(f"{handle.admin_url}/metrics")
                    if c_status == 200 and c_body:
                        c_parsed = metrics_audit.parse_prometheus_text(
                            c_body, allowed_names=metrics_audit.ALLOWED_CLIENT_METRICS
                        )
                        sample["fuse_conn_waiting"] = int(
                            round(
                                metrics_audit.extract_metric_sum(c_parsed, "bowl_client_fuse_connection_waiting")
                            )
                        )
                out_list.append(sample)
                if stop_event.wait(interval_s):
                    break

        thread = threading.Thread(target=_loop, name="bench-metrics-sampler", daemon=True)
        thread.start()
        return thread

    def cleanup_handles(self) -> None:
        """Unmount any handles left open by a suite."""
        with self._handles_lock:
            handles = list(self.active_handles)
            self.active_handles.clear()
        for handle in handles:
            try:
                handle.unmount()
            except Exception:
                pass
