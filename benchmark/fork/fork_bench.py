#!/usr/bin/env python3
"""Bowl Disk Fork 阿里云双机内网基准测试执行器（Suite 1 矩阵对比 / Suite 2 并发在线分叉 / Suite 3 多分支扇出与血统回收）。"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time
from typing import Any
import urllib.parse

MIB = 1024 * 1024
GIB = 1024 * MIB
KIB = 1024
LATENCY_CEILING_MS = 60000.0
SMALL_FILE_MAGIC = b"BOWLFRK1"

PRESET_SPECS: list[dict[str, Any]] = [
    {
        "preset": "small",
        "label": "ComputeSDK small (10 × 1 MiB)",
        "file_count": 10,
        "subdir_count": 0,
        "file_size_bytes": 1 * MIB,
        "fork_iters": 20,
        "copy_iters": 5,
    },
    {
        "preset": "wide",
        "label": "ComputeSDK wide (100 × 1 MiB)",
        "file_count": 100,
        "subdir_count": 0,
        "file_size_bytes": 1 * MIB,
        "fork_iters": 20,
        "copy_iters": 5,
    },
    {
        "preset": "deep",
        "label": "ComputeSDK deep (10 × 16 MiB)",
        "file_count": 10,
        "subdir_count": 0,
        "file_size_bytes": 16 * MIB,
        "fork_iters": 20,
        "copy_iters": 5,
    },
    {
        "preset": "large_1gib",
        "label": "多封包 NVMe 规模 (16 × 64 MiB = 1 GiB)",
        "file_count": 16,
        "subdir_count": 0,
        "file_size_bytes": 64 * MIB,
        "fork_iters": 20,
        "copy_iters": 3,
    },
    {
        "preset": "tree_5k",
        "label": "POSIX 多目录树规模 (50 子目录 × 100 × 4 KiB)",
        "file_count": 5000,
        "subdir_count": 50,
        "file_size_bytes": 4 * KIB,
        "fork_iters": 20,
        "copy_iters": 3,
    },
]

COMPUTESDK_REFERENCE_2026_11_06: dict[str, dict[str, Any]] = {
    "tensorlake": {
        "provider": "Tensorlake",
        "preset": "small",
        "fork_from_live_ms": 267.37,
        "first_read_ms": 89.20,
        "composite_score": 99.76,
        "boundary_note": "托管云沙箱与对象存储快照接口（不含客户端内核 FUSE 挂载握手）",
    },
    "tigris": {
        "provider": "Tigris",
        "preset": "small",
        "fork_from_live_ms": 340.75,
        "first_read_ms": 231.63,
        "composite_score": 99.67,
        "boundary_note": "对象存储分支接口（HTTP API 读写，不含 POSIX FUSE 挂载层）",
    },
    "azure_blob": {
        "provider": "Azure Blob",
        "preset": "small",
        "fork_from_live_ms": 3550.97,
        "first_read_ms": 31.73,
        "composite_score": 96.84,
        "boundary_note": "云对象存储复制快照接口（HTTP API 读写，不含 POSIX FUSE 挂载层）",
    },
}

REMOTE_AUDIT_SCRIPT = r"""#!/usr/bin/env python3
import json
import os
import subprocess
import sys

data_dir = sys.argv[1]
target_disk = sys.argv[2] if len(sys.argv) > 2 else ""

def run_du_sb(path: str) -> int:
    if not os.path.exists(path):
        return 0
    out = subprocess.check_output(["du", "-sb", path], text=True).strip().split()[0]
    return int(out)

du_total = run_du_sb(data_dir)
commits_root = os.path.join(data_dir, "commits")
disks_root = os.path.join(data_dir, "disks")

commit_dirs = []
if os.path.isdir(commits_root):
    commit_dirs = [e.name for e in os.scandir(commits_root) if e.is_dir(follow_symlinks=False)]

disk_dirs = []
if os.path.isdir(disks_root):
    disk_dirs = [e.name for e in os.scandir(disks_root) if e.is_dir(follow_symlinks=False)]

target_pack_nlinks = []
if target_disk:
    pdir = os.path.join(disks_root, target_disk, "packs")
    if os.path.isdir(pdir):
        for e in os.scandir(pdir):
            if e.is_file(follow_symlinks=False) and e.name.endswith(".pack"):
                st = e.stat(follow_symlinks=False)
                target_pack_nlinks.append({"name": e.name, "size": st.st_size, "nlink": st.st_nlink, "ino": st.st_ino})

target_pack_nlinks.sort(key=lambda x: x["name"])
nlinks_only = [x["nlink"] for x in target_pack_nlinks]

print(json.dumps({
    "du_sb_bytes": du_total,
    "commits_dir_count": len(commit_dirs),
    "disks_dir_count": len(disk_dirs),
    "target_pack_count": len(target_pack_nlinks),
    "target_pack_min_nlink": min(nlinks_only) if nlinks_only else 0,
    "target_pack_max_nlink": max(nlinks_only) if nlinks_only else 0,
    "target_packs": target_pack_nlinks,
}))
"""


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print(f"[{ts}] {msg}", flush=True)


def quantile_sorted(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = (len(sorted_vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    frac = pos - lo
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac


def summarize_samples(samples: list[float], trim_fraction: float = 0.05) -> dict[str, Any]:
    """按 ComputeSDK 5% 双侧截尾规则计算 median / p95 / p99 / mean / min / max。"""
    if not samples:
        return {
            "count": 0,
            "trimmed_count": 0,
            "median": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "mean": 0.0,
            "min": 0.0,
            "max": 0.0,
            "raw_samples": [],
        }
    ordered = sorted(float(x) for x in samples)
    trim_each = int(len(ordered) * trim_fraction) if len(ordered) >= 20 else 0
    trimmed = ordered[trim_each : len(ordered) - trim_each] if trim_each > 0 else ordered
    mean_val = sum(trimmed) / len(trimmed)
    return {
        "count": len(ordered),
        "trimmed_count": len(trimmed),
        "median": round(quantile_sorted(trimmed, 0.50), 3),
        "p95": round(quantile_sorted(trimmed, 0.95), 3),
        "p99": round(quantile_sorted(trimmed, 0.99), 3),
        "mean": round(mean_val, 3),
        "min": round(ordered[0], 3),
        "max": round(ordered[-1], 3),
        "raw_samples": [round(x, 3) for x in samples],
    }


def metric_score(latency_ms: float, ceiling_ms: float = LATENCY_CEILING_MS) -> float:
    if latency_ms < 0:
        latency_ms = 0.0
    return max(0.0, 1.0 - (latency_ms / ceiling_ms)) * 100.0


def compute_composite_score(fork_ms: float, first_read_ms: float, verified: bool) -> float:
    """对齐 ComputeSDK 评分上界（60000ms）与校验门槛；单步在线分叉合并权重 0.90 + 首读 0.10。"""
    if not verified:
        return 0.0
    raw = 0.90 * metric_score(fork_ms) + 0.10 * metric_score(first_read_ms)
    return round(raw, 2)


def deterministic_bytes(seed_tag: str, total_bytes: int) -> bytes:
    """生成高熵确定性字节序列，避免全零稀疏块干扰物理落盘统计。"""
    block = bytearray()
    counter = 0
    while len(block) < min(total_bytes, 64 * KIB):
        h = hashlib.blake2b(f"{seed_tag}:{counter}".encode("utf-8"), digest_size=64).digest()
        block.extend(h)
        counter += 1
    unit = bytes(block[: min(total_bytes, 64 * KIB)])
    if total_bytes <= len(unit):
        return unit[:total_bytes]
    repeats, rem = divmod(total_bytes, len(unit))
    # 首尾混入文件唯一摘要确保跨大文件块内容不完全同构
    head_patch = hashlib.sha256(f"head:{seed_tag}".encode("utf-8")).digest()
    tail_patch = hashlib.sha256(f"tail:{seed_tag}".encode("utf-8")).digest()
    buf = bytearray(unit * repeats + unit[:rem])
    buf[: len(head_patch)] = head_patch
    buf[-len(tail_patch) :] = tail_patch
    return bytes(buf)


def fsync_directory(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class ConnectStorageClient:
    """轻量持久连接 ConnectRPC JSON 客户端，用于精确测量服务端控制面 RPC 往返延迟。"""

    def __init__(self, endpoint: str, timeout_sec: float = 60.0) -> None:
        parsed = urllib.parse.urlparse(endpoint)
        self.host = parsed.hostname or "127.0.0.1"
        self.port = parsed.port or 9000
        self.timeout_sec = timeout_sec
        self._local = threading.local()

    def _conn(self) -> http.client.HTTPConnection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout_sec)
            self._local.conn = conn
        return conn

    def _reset_conn(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._local.conn = None

    def call(self, method: str, payload: dict[str, Any]) -> tuple[dict[str, Any], float]:
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Connect-Protocol-Version": "1",
            "Connection": "keep-alive",
        }
        path = f"/storage.v1.StorageService/{method}"
        for attempt in range(2):
            conn = self._conn()
            t0 = time.perf_counter()
            try:
                conn.request("POST", path, body=body, headers=headers)
                resp = conn.getresponse()
                raw = resp.read()
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                if resp.status != 200:
                    raise RuntimeError(
                        f"ConnectRPC {method} HTTP {resp.status}: {raw.decode('utf-8', errors='replace')}"
                    )
                data = json.loads(raw.decode("utf-8")) if raw else {}
                return data, elapsed_ms
            except (http.client.HTTPException, ConnectionError, OSError):
                self._reset_conn()
                if attempt == 1:
                    raise
        raise RuntimeError(f"ConnectRPC {method} retry exhausted")

    def create_disk(self, name: str, max_bytes: int = 2 * (1 << 50)) -> tuple[dict[str, Any], float]:
        resp, ms = self.call("CreateDisk", {"name": name, "maxBytes": str(max_bytes)})
        return resp["disk"], ms

    def fork_disk(self, source_disk_id: str, name: str) -> tuple[dict[str, Any], float]:
        resp, ms = self.call("ForkDisk", {"sourceDiskId": source_disk_id, "name": name})
        return resp["disk"], ms

    def get_disk(self, disk_id: str) -> tuple[dict[str, Any], float]:
        resp, ms = self.call("GetDisk", {"diskId": disk_id})
        return resp["disk"], ms

    def delete_disk(self, disk_id: str) -> float:
        _, ms = self.call("DeleteDisk", {"diskId": disk_id})
        return ms


class BenchEnv:
    """封装双端 SSH ControlMaster、缓存清理、临时挂载生命周期与遥测快照助手。"""

    def __init__(
        self,
        endpoint: str,
        server_priv_ip: str,
        ssh_key: str,
        bowl_bin: str,
        primary_mount: Path,
        primary_disk_id: str,
        work_mount_root: Path,
        ephemeral_log: Path,
        trace_sample_ratio: str,
        telemetry_helper: Path | None,
    ) -> None:
        self.endpoint = endpoint
        self.server_priv_ip = server_priv_ip
        self.ssh_key = ssh_key
        self.bowl_bin = bowl_bin
        self.primary_mount = primary_mount
        self.primary_disk_id = primary_disk_id
        self.work_mount_root = work_mount_root
        self.ephemeral_log = ephemeral_log
        self.trace_sample_ratio = trace_sample_ratio
        self.telemetry_helper = telemetry_helper
        self.rpc = ConnectStorageClient(endpoint)
        self.ssh_ctl_sock = "/tmp/bowl-forkbench-ssh.sock"
        self.ssh_base = [
            "ssh",
            "-n",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "LogLevel=ERROR",
            "-o",
            "ControlMaster=auto",
            "-o",
            f"ControlPath={self.ssh_ctl_sock}",
            "-o",
            "ControlPersist=600",
            "-i",
            self.ssh_key,
            f"root@{self.server_priv_ip}",
        ]
        self.work_mount_root.mkdir(parents=True, exist_ok=True)
        self.ephemeral_log.parent.mkdir(parents=True, exist_ok=True)
        self.server_data_dir = self._init_remote_audit()

    def _init_remote_audit(self) -> str:
        # 预热持久 SSH 连接并在 Server 端安装轻量物理字节与硬链接审计脚本（过滤 -n 以允许标准输入传输脚本内容）
        vol_mp = self.ssh_run(
            "docker volume inspect bowl_storage_data -f '{{.Mountpoint}}' 2>/dev/null || echo /mnt/nvme/docker/volumes/bowl_storage_data/_data"
        ).strip()
        ssh_with_stdin = [arg for arg in self.ssh_base if arg != "-n"]
        subprocess.run(
            ssh_with_stdin
            + ["cat >/usr/local/bin/bowl-storage-audit.py && chmod +x /usr/local/bin/bowl-storage-audit.py"],
            input=REMOTE_AUDIT_SCRIPT,
            text=True,
            check=True,
        )
        # 立即自检远端审计脚本输出有效 JSON，并预热一次 ConnectRPC 连接避免首跳建连计入基准样本
        probe_out = self.ssh_run(f"python3 /usr/local/bin/bowl-storage-audit.py '{vol_mp}' ''")
        probe_json = json.loads(probe_out)
        if "du_sb_bytes" not in probe_json:
            raise RuntimeError(f"Remote storage audit self-check failed: {probe_out}")
        self.rpc.get_disk(self.primary_disk_id)
        return vol_mp

    def close(self) -> None:
        subprocess.run(
            [
                "ssh",
                "-O",
                "exit",
                "-o",
                f"ControlPath={self.ssh_ctl_sock}",
                f"root@{self.server_priv_ip}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

    def ssh_run(self, remote_cmd: str) -> str:
        res = subprocess.run(self.ssh_base + [remote_cmd], capture_output=True, text=True, check=True)
        return res.stdout

    def drop_caches_dual_node(self) -> None:
        os.sync()
        Path("/proc/sys/vm/drop_caches").write_text("3\n", encoding="utf-8")
        self.ssh_run("sync && echo 3 > /proc/sys/vm/drop_caches")

    def audit_server_storage(self, target_disk_id: str = "") -> dict[str, Any]:
        cmd = f"python3 /usr/local/bin/bowl-storage-audit.py '{self.server_data_dir}' '{target_disk_id}'"
        out = self.ssh_run(cmd)
        return json.loads(out)

    def telemetry_cmd(self, helper_call: str) -> None:
        if self.telemetry_helper is None or not self.telemetry_helper.exists():
            return
        script = (
            "set -euo pipefail\n"
            f"SSH_SRV=({' '.join(repr(x) for x in self.ssh_base)})\n"
            f"source {str(self.telemetry_helper)!r}\n"
            f"{helper_call}\n"
        )
        subprocess.run(["bash", "-c", script], check=False)

    def mount_ephemeral(self, disk_id: str, mountpoint: Path, read_only: bool = False) -> float:
        mountpoint.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env["OTEL_SERVICE_NAME"] = "bowl-client"
        env["OTEL_EXPORTER_OTLP_ENDPOINT"] = f"http://{self.server_priv_ip}:4317"
        env["OTEL_EXPORTER_OTLP_INSECURE"] = "true"
        cmd = [
            self.bowl_bin,
            "mount",
            disk_id,
            str(mountpoint),
            "--endpoint",
            self.endpoint,
            "--admin-addr",
            "off",
            "--log-level",
            "warn",
            "--log-format",
            "json",
            "--trace-sample-ratio",
            self.trace_sample_ratio,
            "--log-file",
            str(self.ephemeral_log),
        ]
        if read_only:
            cmd += ["--mode", "ro"]
        t0 = time.perf_counter()
        subprocess.run(cmd, env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        return (time.perf_counter() - t0) * 1000.0

    def _wait_mount_daemon_exit(self, disk_id: str, mountpoint: Path, timeout_sec: float = 10.0) -> None:
        mp_str = str(mountpoint)
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            still_alive = False
            for entry in os.scandir("/proc"):
                if not entry.name.isdigit():
                    continue
                try:
                    cmdline = Path(f"/proc/{entry.name}/cmdline").read_bytes().split(b"\x00")
                    args_str = [x.decode("utf-8", errors="ignore") for x in cmdline if x]
                    if len(args_str) >= 4 and "mount" in args_str and disk_id in args_str and mp_str in args_str:
                        still_alive = True
                        break
                except OSError:
                    continue
            if not still_alive:
                return
            time.sleep(0.005)

    def umount_ephemeral(self, disk_id: str, mountpoint: Path) -> float:
        t0 = time.perf_counter()
        res = subprocess.run(
            [self.bowl_bin, "umount", str(mountpoint)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=False,
        )
        if res.returncode != 0 and os.path.ismount(mountpoint):
            subprocess.run(["fusermount3", "-u", str(mountpoint)], check=False)
        self._wait_mount_daemon_exit(disk_id, mountpoint)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        try:
            mountpoint.rmdir()
        except OSError:
            pass
        return elapsed_ms

    def umount_and_delete(self, disk_id: str, mountpoint: Path) -> float:
        umount_ms = self.umount_ephemeral(disk_id, mountpoint)
        del_ms = self.rpc.delete_disk(disk_id)
        return umount_ms + del_ms


def build_preset_file_list(spec: dict[str, Any]) -> list[str]:
    count = int(spec["file_count"])
    subdirs = int(spec["subdir_count"])
    if subdirs <= 0:
        return [f"f_{idx:04d}.bin" for idx in range(count)]
    per_dir = count // subdirs
    rels: list[str] = []
    for d_idx in range(subdirs):
        for f_idx in range(per_dir):
            global_idx = d_idx * per_dir + f_idx
            rels.append(f"d{d_idx:02d}/f_{global_idx:04d}.bin")
    return rels


def seed_dataset(root: Path, preset_name: str, rel_paths: list[str], file_size: int) -> tuple[dict[str, str], float]:
    """并发写入预设数据集并对所有文件与目录执行显式 fsync，返回相对路径到 SHA-256 映射。"""
    t0 = time.perf_counter()
    subdirs = sorted({str((root / rel).parent) for rel in rel_paths})
    for d in subdirs:
        Path(d).mkdir(parents=True, exist_ok=True)

    def _write_one(rel: str) -> tuple[str, str]:
        data = deterministic_bytes(f"{preset_name}:{rel}", file_size)
        digest = hashlib.sha256(data).hexdigest()
        fpath = root / rel
        fd = os.open(str(fpath), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
        try:
            view = memoryview(data)
            written = 0
            chunk_sz = 4 * MIB
            while written < len(data):
                n = os.write(fd, view[written : written + chunk_sz])
                written += n
            os.fsync(fd)
        finally:
            os.close(fd)
        return rel, digest

    manifest: dict[str, str] = {}
    workers = 8 if len(rel_paths) > 32 else 4
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_write_one, rel) for rel in rel_paths]
        for fut in as_completed(futures):
            rel, dig = fut.result()
            manifest[rel] = dig

    for d in reversed(subdirs):
        fsync_directory(Path(d))
    if str(root) not in subdirs:
        fsync_directory(root)

    seed_ms = (time.perf_counter() - t0) * 1000.0
    return manifest, seed_ms


def verify_single_file_sha256(fpath: Path, expected_sha256: str) -> tuple[bool, float]:
    t0 = time.perf_counter()
    h = hashlib.sha256()
    fd = os.open(str(fpath), os.O_RDONLY)
    try:
        while True:
            buf = os.read(fd, 4 * MIB)
            if not buf:
                break
            h.update(buf)
    finally:
        os.close(fd)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return (h.hexdigest() == expected_sha256), elapsed_ms


def verify_manifest_sha256(root: Path, manifest: dict[str, str], skip_rel: str | None = None) -> tuple[bool, float]:
    t0 = time.perf_counter()
    items = [(rel, exp) for rel, exp in manifest.items() if rel != skip_rel]
    if not items:
        return True, 0.0

    def _check(item: tuple[str, str]) -> bool:
        rel, exp = item
        ok, _ = verify_single_file_sha256(root / rel, exp)
        return ok

    workers = 8 if len(items) > 16 else 4
    all_ok = True
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for ok in pool.map(_check, items):
            if not ok:
                all_ok = False
    return all_ok, (time.perf_counter() - t0) * 1000.0


def copy_dataset_with_fsync(src_root: Path, dst_root: Path, rel_paths: list[str]) -> float:
    """从已挂载源盘多线程全量拷贝到目标新盘并执行每个文件与目录 fsync。"""
    t0 = time.perf_counter()
    dst_subdirs = sorted({str((dst_root / rel).parent) for rel in rel_paths})
    for d in dst_subdirs:
        Path(d).mkdir(parents=True, exist_ok=True)

    def _copy_one(rel: str) -> None:
        s_fd = os.open(str(src_root / rel), os.O_RDONLY)
        d_fd = os.open(str(dst_root / rel), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
        try:
            while True:
                buf = os.read(s_fd, 4 * MIB)
                if not buf:
                    break
                mv = memoryview(buf)
                off = 0
                while off < len(buf):
                    off += os.write(d_fd, mv[off:])
            os.fsync(d_fd)
        finally:
            os.close(d_fd)
            os.close(s_fd)

    workers = 8 if len(rel_paths) > 16 else 4
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(_copy_one, rel_paths))

    for d in reversed(dst_subdirs):
        fsync_directory(Path(d))
    if str(dst_root) not in dst_subdirs:
        fsync_directory(dst_root)

    return (time.perf_counter() - t0) * 1000.0


def run_suite1_matrix(env: BenchEnv) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """执行套件一：5 组数据集预设下 bowl_fork (N=20) vs bowl_copy 全量拷贝基线对比。"""
    log("=== Starting Suite 1: ComputeSDK Matrix + POSIX Extensions (bowl_fork vs bowl_copy) ===")
    presets_out: dict[str, Any] = {}
    stage_rows: list[dict[str, Any]] = []

    # 先准备全部预设的源盘与校验清单，再分段运行 bowl_fork 与 bowl_copy 以便双端 phase_deltas 精确归因
    prepared: dict[str, dict[str, Any]] = {}
    for spec in PRESET_SPECS:
        pname = spec["preset"]
        rel_paths = build_preset_file_list(spec)
        before_seed_audit = env.audit_server_storage()
        src_disk, _ = env.rpc.create_disk(f"fork-s1-src-{pname}")
        src_id = src_disk["diskId"]
        src_mp = env.work_mount_root / f"s1_src_{pname}"
        env.mount_ephemeral(src_id, src_mp)
        manifest, seed_ms = seed_dataset(src_mp, pname, rel_paths, int(spec["file_size_bytes"]))
        src_info, _ = env.rpc.get_disk(src_id)
        after_seed_audit = env.audit_server_storage(src_id)
        prepared[pname] = {
            "spec": spec,
            "rel_paths": rel_paths,
            "src_id": src_id,
            "src_mp": src_mp,
            "manifest": manifest,
            "seed_ms": round(seed_ms, 3),
            "source_used_bytes": int(src_info.get("usedBytes", 0)),
            "source_used_inodes": int(src_info.get("usedInodes", 0)),
            "source_physical_delta_bytes": max(0, after_seed_audit["du_sb_bytes"] - before_seed_audit["du_sb_bytes"]),
        }
        log(
            f"[Suite 1 Seed] preset={pname} files={len(rel_paths)} bytes={spec['file_count'] * spec['file_size_bytes']} "
            f"seed_ms={seed_ms:.2f} used_bytes={src_info.get('usedBytes')}"
        )

    # 阶段 1A：测量全部预设的 bowl_fork (ConnectRPC N=20 + CLI N=5)
    t_fork_stage = time.perf_counter()
    env.telemetry_cmd("capture_bowl_snapshot 'suite1_fork_matrix_pre'")
    env.telemetry_cmd("start_bowl_cpu_profile 'suite1_fork_matrix' 10 1")
    fork_results_by_preset: dict[str, dict[str, Any]] = {}

    for spec in PRESET_SPECS:
        pname = spec["preset"]
        ctx = prepared[pname]
        src_id = ctx["src_id"]
        rel_paths = ctx["rel_paths"]
        manifest = ctx["manifest"]
        target_first_rel = rel_paths[0]
        fork_iters = int(spec["fork_iters"])

        fork_rpc_samples: list[float] = []
        mount_samples: list[float] = []
        fork_plus_mount_samples: list[float] = []
        cold_first_read_samples: list[float] = []
        full_verify_samples: list[float] = []
        tti_samples: list[float] = []
        delete_samples: list[float] = []
        verified_cnt = 0
        phys_delta_fork = 0

        for idx in range(fork_iters):
            if idx == 0:
                pre_fork_audit = env.audit_server_storage()
            child_disk, fork_ms = env.rpc.fork_disk(src_id, f"fork-s1-{pname}-{idx:02d}")
            child_id = child_disk["diskId"]
            if idx == 0:
                post_fork_audit = env.audit_server_storage(child_id)
                phys_delta_fork = max(0, post_fork_audit["du_sb_bytes"] - pre_fork_audit["du_sb_bytes"])
                if int(child_disk.get("usedBytes", -1)) != ctx["source_used_bytes"]:
                    raise RuntimeError(
                        f"Fork usedBytes mismatch on {pname}: got {child_disk.get('usedBytes')}, want {ctx['source_used_bytes']}"
                    )

            child_mp = env.work_mount_root / f"s1_fork_{pname}_{idx:02d}"
            mount_ms = env.mount_ephemeral(child_id, child_mp)

            env.drop_caches_dual_node()
            first_ok, first_read_ms = verify_single_file_sha256(
                child_mp / target_first_rel, manifest[target_first_rel]
            )
            rest_ok, rest_verify_ms = verify_manifest_sha256(
                child_mp, manifest, skip_rel=target_first_rel
            )
            iter_ok = first_ok and rest_ok
            if not iter_ok:
                raise RuntimeError(f"SHA-256 verification failed in bowl_fork preset={pname} iter={idx}")
            verified_cnt += 1

            del_ms = env.umount_and_delete(child_id, child_mp)

            fork_rpc_samples.append(fork_ms)
            mount_samples.append(mount_ms)
            fork_plus_mount_samples.append(fork_ms + mount_ms)
            cold_first_read_samples.append(first_read_ms)
            full_verify_samples.append(first_read_ms + rest_verify_ms)
            tti_samples.append(fork_ms + mount_ms + first_read_ms)
            delete_samples.append(del_ms)

        # 额外测量 5 次纯 CLI `bowl disk fork --json` 进程端到端开销
        cli_samples: list[float] = []
        for c_idx in range(5):
            t_cli = time.perf_counter()
            out = subprocess.check_output(
                [
                    env.bowl_bin,
                    "disk",
                    "fork",
                    src_id,
                    "--name",
                    f"fork-s1-cli-{pname}-{c_idx}",
                    "--endpoint",
                    env.endpoint,
                    "--json",
                ],
                text=True,
            )
            cli_ms = (time.perf_counter() - t_cli) * 1000.0
            cli_json = json.loads(out)
            env.rpc.delete_disk(cli_json["disk_id"])
            cli_samples.append(cli_ms)

        s_fork_rpc = summarize_samples(fork_rpc_samples)
        s_fork_cli = summarize_samples(cli_samples, trim_fraction=0.0)
        s_mount = summarize_samples(mount_samples)
        s_fork_mount = summarize_samples(fork_plus_mount_samples)
        s_first_read = summarize_samples(cold_first_read_samples)
        s_full_ver = summarize_samples(full_verify_samples)
        s_tti = summarize_samples(tti_samples)
        s_del = summarize_samples(delete_samples)
        all_ok = verified_cnt == fork_iters

        fork_results_by_preset[pname] = {
            "iterations": fork_iters,
            "verified_count": verified_cnt,
            "all_verified": all_ok,
            "physical_delta_bytes_on_fork": phys_delta_fork,
            "fork_rpc_ms": s_fork_rpc,
            "fork_cli_ms": s_fork_cli,
            "mount_ready_ms": s_mount,
            "fork_plus_mount_ms": s_fork_mount,
            "cold_first_read_ms": s_first_read,
            "full_verify_ms": s_full_ver,
            "end_to_end_tti_ms": s_tti,
            "delete_ms": s_del,
            "computesdk_score_rpc": compute_composite_score(s_fork_rpc["median"], s_first_read["median"], all_ok),
            "computesdk_score_with_mount": compute_composite_score(
                s_fork_mount["median"], s_first_read["median"], all_ok
            ),
        }
        log(
            f"[Suite 1 bowl_fork] preset={pname} fork_rpc_p50={s_fork_rpc['median']:.2f}ms "
            f"mount_p50={s_mount['median']:.2f}ms cold_read_p50={s_first_read['median']:.2f}ms "
            f"tti_p50={s_tti['median']:.2f}ms phys_delta={phys_delta_fork}B"
        )

    env.telemetry_cmd("finish_bowl_stage_profiles 'suite1_fork_matrix'")
    env.telemetry_cmd("capture_bowl_snapshot 'suite1_fork_matrix_post'")
    stage_rows.append({"stage": "suite1_fork_matrix", "duration_sec": round(time.perf_counter() - t_fork_stage, 3)})

    # 阶段 1B：测量全部预设的同机整盘全量拷贝基线 bowl_copy
    t_copy_stage = time.perf_counter()
    env.telemetry_cmd("capture_bowl_snapshot 'suite1_copy_baseline_pre'")

    for spec in PRESET_SPECS:
        pname = spec["preset"]
        ctx = prepared[pname]
        src_mp = ctx["src_mp"]
        rel_paths = ctx["rel_paths"]
        manifest = ctx["manifest"]
        target_first_rel = rel_paths[0]
        copy_iters = int(spec["copy_iters"])

        create_samples: list[float] = []
        mount_samples: list[float] = []
        copy_sync_samples: list[float] = []
        prov_copy_samples: list[float] = []
        cold_first_read_samples: list[float] = []
        full_verify_samples: list[float] = []
        tti_samples: list[float] = []
        delete_samples: list[float] = []
        verified_cnt = 0
        phys_delta_copy = 0

        for idx in range(copy_iters):
            if idx == 0:
                pre_copy_audit = env.audit_server_storage()
            dst_disk, create_ms = env.rpc.create_disk(f"copy-s1-{pname}-{idx:02d}")
            dst_id = dst_disk["diskId"]
            dst_mp = env.work_mount_root / f"s1_copy_{pname}_{idx:02d}"
            mount_ms = env.mount_ephemeral(dst_id, dst_mp)
            copy_ms = copy_dataset_with_fsync(src_mp, dst_mp, rel_paths)

            if idx == 0:
                post_copy_audit = env.audit_server_storage(dst_id)
                phys_delta_copy = max(0, post_copy_audit["du_sb_bytes"] - pre_copy_audit["du_sb_bytes"])

            env.drop_caches_dual_node()
            first_ok, first_read_ms = verify_single_file_sha256(
                dst_mp / target_first_rel, manifest[target_first_rel]
            )
            rest_ok, rest_verify_ms = verify_manifest_sha256(
                dst_mp, manifest, skip_rel=target_first_rel
            )
            if not (first_ok and rest_ok):
                raise RuntimeError(f"SHA-256 verification failed in bowl_copy preset={pname} iter={idx}")
            verified_cnt += 1

            del_ms = env.umount_and_delete(dst_id, dst_mp)

            prov_copy = create_ms + mount_ms + copy_ms
            create_samples.append(create_ms)
            mount_samples.append(mount_ms)
            copy_sync_samples.append(copy_ms)
            prov_copy_samples.append(prov_copy)
            cold_first_read_samples.append(first_read_ms)
            full_verify_samples.append(first_read_ms + rest_verify_ms)
            tti_samples.append(prov_copy + first_read_ms)
            delete_samples.append(del_ms)

        # 清理该预设的源盘
        env.umount_and_delete(ctx["src_id"], src_mp)

        s_create = summarize_samples(create_samples, trim_fraction=0.0)
        s_mount = summarize_samples(mount_samples, trim_fraction=0.0)
        s_copy = summarize_samples(copy_sync_samples, trim_fraction=0.0)
        s_prov_copy = summarize_samples(prov_copy_samples, trim_fraction=0.0)
        s_first_read = summarize_samples(cold_first_read_samples, trim_fraction=0.0)
        s_full_ver = summarize_samples(full_verify_samples, trim_fraction=0.0)
        s_tti = summarize_samples(tti_samples, trim_fraction=0.0)
        s_del = summarize_samples(delete_samples, trim_fraction=0.0)
        all_ok = verified_cnt == copy_iters

        fork_res = fork_results_by_preset[pname]
        copy_res = {
            "iterations": copy_iters,
            "verified_count": verified_cnt,
            "all_verified": all_ok,
            "physical_delta_bytes_on_copy": phys_delta_copy,
            "create_ms": s_create,
            "mount_ready_ms": s_mount,
            "copy_sync_ms": s_copy,
            "provision_plus_copy_ms": s_prov_copy,
            "cold_first_read_ms": s_first_read,
            "full_verify_ms": s_full_ver,
            "end_to_end_tti_ms": s_tti,
            "delete_ms": s_del,
            "computesdk_score": compute_composite_score(s_prov_copy["median"], s_first_read["median"], all_ok),
        }

        dataset_bytes = int(spec["file_count"]) * int(spec["file_size_bytes"])
        fork_rpc_med = max(0.001, fork_res["fork_rpc_ms"]["median"])
        fork_mount_med = max(0.001, fork_res["fork_plus_mount_ms"]["median"])
        fork_tti_med = max(0.001, fork_res["end_to_end_tti_ms"]["median"])

        presets_out[pname] = {
            "preset": pname,
            "label": spec["label"],
            "file_count": int(spec["file_count"]),
            "subdir_count": int(spec["subdir_count"]),
            "file_size_bytes": int(spec["file_size_bytes"]),
            "dataset_bytes": dataset_bytes,
            "dataset_mib": round(dataset_bytes / MIB, 3),
            "seed_ms": ctx["seed_ms"],
            "source_used_bytes": ctx["source_used_bytes"],
            "source_used_inodes": ctx["source_used_inodes"],
            "source_physical_delta_bytes": ctx["source_physical_delta_bytes"],
            "bowl_fork": fork_res,
            "bowl_copy": copy_res,
            "comparison": {
                "fork_rpc_vs_copy_sync_speedup": round(s_copy["median"] / fork_rpc_med, 2),
                "fork_mount_vs_provision_copy_speedup": round(s_prov_copy["median"] / fork_mount_med, 2),
                "end_to_end_tti_speedup": round(s_tti["median"] / fork_tti_med, 2),
                "physical_bytes_saved_bytes": max(0, phys_delta_copy - fork_res["physical_delta_bytes_on_fork"]),
                "physical_bytes_ratio_copy_over_fork": round(
                    phys_delta_copy / max(1, fork_res["physical_delta_bytes_on_fork"]), 2
                ),
            },
        }
        log(
            f"[Suite 1 bowl_copy] preset={pname} copy_sync_p50={s_copy['median']:.2f}ms "
            f"tti_p50={s_tti['median']:.2f}ms tti_speedup={presets_out[pname]['comparison']['end_to_end_tti_speedup']:.2f}x"
        )

    env.telemetry_cmd("capture_bowl_snapshot 'suite1_copy_baseline_post'")
    stage_rows.append({"stage": "suite1_copy_baseline", "duration_sec": round(time.perf_counter() - t_copy_stage, 3)})

    suite1_payload = {
        "methodology": {
            "fork_iterations": 20,
            "trim_fraction": 0.05,
            "latency_ceiling_ms": LATENCY_CEILING_MS,
            "composite_weights": {"fork_ms": 0.90, "cold_first_read_ms": 0.10},
            "computesdk_reference_2026_11_06": COMPUTESDK_REFERENCE_2026_11_06,
        },
        "preset_order": [s["preset"] for s in PRESET_SPECS],
        "presets": presets_out,
    }
    return suite1_payload, stage_rows


def make_self_verifying_4k_block(tag: str) -> bytes:
    payload = deterministic_bytes(tag, 4 * KIB - 40)
    digest = hashlib.sha256(payload).digest()
    return SMALL_FILE_MAGIC + digest + payload


def verify_self_verifying_4k_file(fpath: Path) -> bool:
    data = fpath.read_bytes()
    if len(data) == 0:
        # 刚 Create 尚未写入首个字节的飞行中空文件合法
        return True
    if len(data) != 4 * KIB:
        return False
    if data[:8] != SMALL_FILE_MAGIC:
        return False
    expected = data[8:40]
    actual = hashlib.sha256(data[40:]).digest()
    return actual == expected


def run_suite2_online_fork(env: BenchEnv) -> tuple[dict[str, Any], dict[str, Any]]:
    """执行套件二：持续并发写负载下连续 10 次无停顿在线 ForkDisk 与 16 MiB 未关闭孤儿文件清洗验证。"""
    log("=== Starting Suite 2: Online Fork Under Concurrent Write Load & Orphan Sanitization ===")
    t_stage = time.perf_counter()
    mp = env.primary_mount
    for item in mp.iterdir():
        if item.name == ".stats":
            continue
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()
    os.sync()

    env.telemetry_cmd("capture_bowl_snapshot 'suite2_online_fork_pre'")
    env.telemetry_cmd("start_bowl_cpu_profile 'suite2_online_fork' 8 0")

    committed_dir = mp / "committed"
    orphans_dir = mp / "orphans"
    streams_dir = mp / "streams"
    small_dir = mp / "small"
    for d in (committed_dir, orphans_dir, streams_dir, small_dir):
        d.mkdir(parents=True, exist_ok=True)

    # 1. 写入 32 × 1 MiB 已提交基线文件并 fsync
    committed_rels = [f"c_{i:03d}.bin" for i in range(32)]
    committed_manifest, committed_seed_ms = seed_dataset(committed_dir, "suite2_committed", committed_rels, 1 * MIB)

    # 2. 创建 16 × 1 MiB 打开后立即 unlink 的孤儿文件，保持全部 16 个文件描述符处于打开态
    orphan_fds: list[int] = []
    orphan_file_bytes = 1 * MIB
    orphan_count = 16
    orphan_total_bytes = orphan_count * orphan_file_bytes
    orphan_payload = deterministic_bytes("suite2_orphan_1m", orphan_file_bytes)
    for idx in range(orphan_count):
        opath = orphans_dir / f"orphan_{idx:02d}.bin"
        fd = os.open(str(opath), os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
        os.write(fd, orphan_payload)
        os.fsync(fd)
        os.unlink(str(opath))
        orphan_fds.append(fd)
    fsync_directory(orphans_dir)
    fsync_directory(mp)

    parent_pre_writer, _ = env.rpc.get_disk(env.primary_disk_id)
    parent_used_with_orphans_initial = int(parent_pre_writer.get("usedBytes", 0))
    expected_initial_parent_bytes = 32 * MIB + orphan_total_bytes
    if parent_used_with_orphans_initial != expected_initial_parent_bytes:
        raise RuntimeError(
            f"Unexpected parent usedBytes before concurrent writers: got {parent_used_with_orphans_initial}, "
            f"want {expected_initial_parent_bytes}"
        )

    # 3. 启动持续并发写入线程（2 个 4 MiB 顺序写+fsync 线程 + 2 个 4 KiB 小文件创建+fsync 线程），全程零停顿
    stop_ev = threading.Event()
    rec_lock = threading.Lock()
    stream_records: list[tuple[float, float]] = []  # (finish_mono, latency_ms)
    small_records: list[tuple[float, float]] = []
    writer_errors: list[str] = []
    total_bytes_written = 0

    def _stream_writer(tid: int) -> None:
        nonlocal total_bytes_written
        sub = streams_dir / f"w{tid}"
        sub.mkdir(parents=True, exist_ok=True)
        buf = deterministic_bytes(f"stream_4m_{tid}", 4 * MIB)
        seq = 0
        while not stop_ev.is_set():
            fpath = sub / f"seg_{seq:05d}.bin"
            seq += 1
            t0 = time.perf_counter()
            try:
                fd = os.open(str(fpath), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
                try:
                    mv = memoryview(buf)
                    off = 0
                    while off < len(buf):
                        off += os.write(fd, mv[off : off + 1 * MIB])
                    os.fsync(fd)
                finally:
                    os.close(fd)
                lat_ms = (time.perf_counter() - t0) * 1000.0
                now_m = time.monotonic()
                with rec_lock:
                    stream_records.append((now_m, lat_ms))
                    total_bytes_written += len(buf)
            except Exception as exc:
                with rec_lock:
                    writer_errors.append(f"stream_writer[{tid}]: {exc}")
                return

    def _small_writer(tid: int) -> None:
        nonlocal total_bytes_written
        sub = small_dir / f"s{tid}"
        sub.mkdir(parents=True, exist_ok=True)
        seq = 0
        while not stop_ev.is_set():
            fpath = sub / f"sm_{seq:05d}.bin"
            blk = make_self_verifying_4k_block(f"sm_{tid}_{seq}")
            seq += 1
            t0 = time.perf_counter()
            try:
                fd = os.open(str(fpath), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
                try:
                    os.write(fd, blk)
                    os.fsync(fd)
                finally:
                    os.close(fd)
                lat_ms = (time.perf_counter() - t0) * 1000.0
                now_m = time.monotonic()
                with rec_lock:
                    small_records.append((now_m, lat_ms))
                    total_bytes_written += len(blk)
            except Exception as exc:
                with rec_lock:
                    writer_errors.append(f"small_writer[{tid}]: {exc}")
                return

    writers = [
        threading.Thread(target=_stream_writer, args=(0,), daemon=True),
        threading.Thread(target=_stream_writer, args=(1,), daemon=True),
        threading.Thread(target=_small_writer, args=(0,), daemon=True),
        threading.Thread(target=_small_writer, args=(1,), daemon=True),
    ]
    t_writers_start = time.monotonic()
    for th in writers:
        th.start()

    # 分叉前稳态并发写采样窗口（2.2s）
    time.sleep(2.2)

    # 4. 在写入持续飞行中且 16 个孤儿 FD 保持打开下，连续触发 10 次在线 ForkDisk
    online_forks_raw: list[dict[str, Any]] = []
    t_fork_window_start = time.monotonic()
    for f_idx in range(10):
        child_disk, fork_ms = env.rpc.fork_disk(env.primary_disk_id, f"fork-s2-online-{f_idx:02d}")
        online_forks_raw.append(
            {
                "fork_index": f_idx,
                "child_disk_id": child_disk["diskId"],
                "fork_rpc_ms": round(fork_ms, 3),
                "child_used_bytes": int(child_disk.get("usedBytes", 0)),
                "child_used_inodes": int(child_disk.get("usedInodes", 0)),
            }
        )
        if f_idx < 9:
            time.sleep(0.18)
    t_fork_window_end = time.monotonic()

    # 让并发写继续短暂运行以确保尾随写入完整采样，随后通知线程安全收尾
    time.sleep(0.4)
    stop_ev.set()
    for th in writers:
        th.join(timeout=30.0)
    t_writers_end = time.monotonic()

    if writer_errors:
        raise RuntimeError(f"Concurrent writer errors detected: {writer_errors[:5]}")

    # 校验父盘在关闭 16 个孤儿 FD 前后的精确回收差值（16 MiB）
    os.sync()
    parent_post_with_orphans, _ = env.rpc.get_disk(env.primary_disk_id)
    bytes_post_with_orphans = int(parent_post_with_orphans.get("usedBytes", 0))
    for fd in orphan_fds:
        os.close(fd)
    orphan_fds.clear()

    # 等待 FUSE Release 异步送达并完成 ReclaimOrphan
    bytes_post_after_close = bytes_post_with_orphans
    for _ in range(50):
        p_info, _ = env.rpc.get_disk(env.primary_disk_id)
        bytes_post_after_close = int(p_info.get("usedBytes", 0))
        if bytes_post_with_orphans - bytes_post_after_close == orphan_total_bytes:
            break
        time.sleep(0.05)

    reclaimed_on_parent = bytes_post_with_orphans - bytes_post_after_close
    if reclaimed_on_parent != orphan_total_bytes:
        raise RuntimeError(
            f"Parent orphan reclaim mismatch: reclaimed={reclaimed_on_parent}, expected={orphan_total_bytes}"
        )

    # 5. 逐个挂载全部 10 个在线分叉子盘，核验已提交基线 100% SHA-256、小文件无撕裂、孤儿节点 100% 净化
    verified_forks: list[dict[str, Any]] = []
    for item in online_forks_raw:
        cid = item["child_disk_id"]
        cmp_dir = env.work_mount_root / f"s2_child_{item['fork_index']:02d}"
        env.mount_ephemeral(cid, cmp_dir, read_only=True)

        # (a) orphans 目录必须为空（0 个孤儿残留路径）
        orphan_entries = list((cmp_dir / "orphans").iterdir()) if (cmp_dir / "orphans").exists() else []

        # (b) 32 个已提交 1 MiB 文件必须 100% 通过 SHA-256 校验
        c_ok, _ = verify_manifest_sha256(cmp_dir / "committed", committed_manifest)

        # (c) 扫描子盘全部可见常规文件：累计精确 st_size 并校验全部 4 KiB 小文件完整性
        live_visible_bytes = 0
        small_verified = 0
        corrupted_files = 0
        for root_dir, _, files in os.walk(cmp_dir):
            for fname in files:
                if fname == ".stats":
                    continue
                p = Path(root_dir) / fname
                st = p.lstat()
                live_visible_bytes += st.st_size
                if "/small/" in str(p):
                    if verify_self_verifying_4k_file(p):
                        if st.st_size == 4 * KIB:
                            small_verified += 1
                    else:
                        corrupted_files += 1

        env.umount_and_delete(cid, cmp_dir)

        orphan_remaining = item["child_used_bytes"] - live_visible_bytes
        child_ok = (len(orphan_entries) == 0) and c_ok and (corrupted_files == 0) and (orphan_remaining == 0)
        if not child_ok:
            raise RuntimeError(
                f"Online fork child {item['fork_index']} verification failed: "
                f"c_ok={c_ok} corrupted={corrupted_files} orphan_remaining={orphan_remaining}"
            )

        verified_forks.append(
            {
                **item,
                "child_live_visible_bytes": live_visible_bytes,
                "orphan_bytes_in_child": orphan_remaining,
                "sanitized_orphan_bytes": orphan_total_bytes,
                "committed_files_verified": len(committed_manifest),
                "small_files_verified": small_verified,
                "corrupted_files": corrupted_files,
                "verified": True,
            }
        )

    # 清理主锚点挂载盘测试目录，保持后续 Suite 3 物理空间审计纯净
    for d in (committed_dir, orphans_dir, streams_dir, small_dir):
        shutil.rmtree(d, ignore_errors=True)
    fsync_directory(mp)
    os.sync()

    env.telemetry_cmd("finish_bowl_stage_profiles 'suite2_online_fork'")
    env.telemetry_cmd("capture_bowl_snapshot 'suite2_online_fork_post'")

    def _split_before_during(records: list[tuple[float, float]]) -> tuple[list[float], list[float]]:
        before = [lat for ts_m, lat in records if ts_m < t_fork_window_start]
        during = [lat for ts_m, lat in records if t_fork_window_start <= ts_m <= t_fork_window_end]
        return before, during

    stream_before, stream_during = _split_before_during(stream_records)
    small_before, small_during = _split_before_during(small_records)
    writer_wall_sec = max(0.001, t_writers_end - t_writers_start)

    fork_lat_stats = summarize_samples([x["fork_rpc_ms"] for x in verified_forks], trim_fraction=0.0)
    log(
        f"[Suite 2 Done] throughput={total_bytes_written / MIB / writer_wall_sec:.2f} MiB/s "
        f"writer_errors=0 online_fork_p50={fork_lat_stats['median']:.2f}ms "
        f"sanitized_orphans=16MiB across 10/10 children"
    )

    suite2_payload = {
        "committed_baseline_files": len(committed_manifest),
        "committed_baseline_bytes": 32 * MIB,
        "committed_seed_ms": round(committed_seed_ms, 3),
        "open_orphan_fds": orphan_count,
        "open_orphan_file_bytes": orphan_file_bytes,
        "open_orphan_total_bytes": orphan_total_bytes,
        "parent_used_bytes_after_seed_and_orphans": parent_used_with_orphans_initial,
        "online_fork_count": len(verified_forks),
        "writer_threads": {"stream_4mib_threads": 2, "small_4kib_threads": 2},
        "writer_errors": len(writer_errors),
        "writer_summary": {
            "total_duration_sec": round(writer_wall_sec, 3),
            "total_bytes_written": total_bytes_written,
            "aggregate_throughput_mib_s": round((total_bytes_written / MIB) / writer_wall_sec, 2),
            "stream_4mib_ops": len(stream_records),
            "stream_4mib_before_fork_ms": summarize_samples(stream_before, trim_fraction=0.0),
            "stream_4mib_during_fork_ms": summarize_samples(stream_during, trim_fraction=0.0),
            "small_4kib_ops": len(small_records),
            "small_4kib_ops_per_sec": round(len(small_records) / writer_wall_sec, 2),
            "small_4kib_before_fork_ms": summarize_samples(small_before, trim_fraction=0.0),
            "small_4kib_during_fork_ms": summarize_samples(small_during, trim_fraction=0.0),
        },
        "online_fork_rpc_ms": fork_lat_stats,
        "online_forks": verified_forks,
        "parent_used_bytes_post_write_with_orphans": bytes_post_with_orphans,
        "parent_used_bytes_post_write_after_orphan_close": bytes_post_after_close,
        "parent_reclaimed_orphan_bytes_on_close": reclaimed_on_parent,
        "all_children_verified": True,
        "all_orphans_sanitized": True,
    }
    stage_row = {"stage": "suite2_online_fork", "duration_sec": round(time.perf_counter() - t_stage, 3)}
    return suite2_payload, stage_row


def run_suite3_fanout_cow(env: BenchEnv) -> tuple[dict[str, Any], dict[str, Any]]:
    """执行套件三：1 GiB 父盘 -> 16 子分支扇出、并发读写 CoW 隔离与父/兄删除后血统提交零残留验证。"""
    log("=== Starting Suite 3: 1 -> 16 Multi-Branch Fan-Out, CoW Isolation & LCA Commit GC ===")
    t_stage = time.perf_counter()
    env.telemetry_cmd("capture_bowl_snapshot 'suite3_fanout_cow_pre'")
    env.telemetry_cmd("start_bowl_cpu_profile 'suite3_fanout_cow' 8 0")

    # 等待 Suite 2 的死包异步清理稳定，采集基准物理字节
    time.sleep(0.3)
    audit_before_seed = env.audit_server_storage()
    base_overhead_bytes = int(audit_before_seed["du_sb_bytes"])

    # 1. 创建并填充 1 GiB (16 × 64 MiB) 父盘
    parent_disk, _ = env.rpc.create_disk("fork-s3-parent-1gib")
    parent_id = parent_disk["diskId"]
    parent_mp = env.work_mount_root / "s3_parent"
    env.mount_ephemeral(parent_id, parent_mp)

    parent_rels = [f"pack_{idx:02d}.bin" for idx in range(16)]
    parent_manifest, parent_seed_ms = seed_dataset(parent_mp, "suite3_1gib", parent_rels, 64 * MIB)
    audit_after_seed = env.audit_server_storage(parent_id)
    lineage_seed_phys = max(0, int(audit_after_seed["du_sb_bytes"]) - base_overhead_bytes)

    # 2. 扇出分叉 16 个子盘（连同父盘逻辑容量共 17 GiB）
    fanout_count = 16
    children: list[dict[str, Any]] = []
    fork_samples: list[float] = []
    t_fanout0 = time.perf_counter()
    for idx in range(fanout_count):
        cdisk, f_ms = env.rpc.fork_disk(parent_id, f"fork-s3-child-{idx:02d}")
        children.append(
            {
                "index": idx,
                "disk_id": cdisk["diskId"],
                "mountpoint": env.work_mount_root / f"s3_c_{idx:02d}",
                "delta_name": f"private_delta_{idx:02d}.bin",
            }
        )
        fork_samples.append(f_ms)
    fanout_total_ms = (time.perf_counter() - t_fanout0) * 1000.0
    audit_after_16_forks = env.audit_server_storage(parent_id)
    parent_pack_names = {p["name"] for p in audit_after_16_forks.get("target_packs", [])}
    lineage_after_forks_phys = max(0, int(audit_after_16_forks["du_sb_bytes"]) - base_overhead_bytes)
    logical_after_forks = (1 + fanout_count) * GIB

    if audit_after_16_forks["commits_dir_count"] != fanout_count:
        raise RuntimeError(
            f"Expected {fanout_count} commit dirs after 16 forks, got {audit_after_16_forks['commits_dir_count']}"
        )

    # 3. 在客户端同时并发挂载全部 16 个子盘
    t_mount0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=8) as pool:
        futs = [pool.submit(env.mount_ephemeral, c["disk_id"], c["mountpoint"]) for c in children]
        for f in futs:
            f.result()
    concurrent_mount_ms = (time.perf_counter() - t_mount0) * 1000.0

    # 4. 16 并发线程：读取共享父文件校验 SHA-256 + 写入并 fsync 各自独有 4 MiB 增量数据（合计 64 MiB）
    delta_bytes_per_child = 4 * MIB
    delta_manifests: dict[int, str] = {}

    def _child_cow_worker(c: dict[str, Any]) -> tuple[int, str]:
        idx = int(c["index"])
        cmp_p = c["mountpoint"]
        # 校验两份共享 64 MiB 父文件
        for r_idx in (idx % 16, (idx + 8) % 16):
            rel = parent_rels[r_idx]
            ok, _ = verify_single_file_sha256(cmp_p / rel, parent_manifest[rel])
            if not ok:
                raise RuntimeError(f"Child {idx} failed shared read SHA-256 on {rel}")
        # 写入独有 4 MiB 私有增量并 fsync
        d_data = deterministic_bytes(f"s3_private_delta_{idx:02d}", delta_bytes_per_child)
        d_sha = hashlib.sha256(d_data).hexdigest()
        d_path = cmp_p / c["delta_name"]
        fd = os.open(str(d_path), os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
        try:
            os.write(fd, d_data)
            os.fsync(fd)
        finally:
            os.close(fd)
        fsync_directory(cmp_p)
        return idx, d_sha

    t_io0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=16) as pool:
        for idx, d_sha in pool.map(_child_cow_worker, children):
            delta_manifests[idx] = d_sha
    concurrent_io_ms = (time.perf_counter() - t_io0) * 1000.0

    # 验证写时复制命名空间隔离：父盘看不到任何子盘增量，各子盘互看不到兄弟增量
    for c in children:
        if (parent_mp / c["delta_name"]).exists():
            raise RuntimeError(f"CoW isolation violated on parent: {c['delta_name']} visible in parent")
        other = children[(c["index"] + 1) % fanout_count]
        if (c["mountpoint"] / other["delta_name"]).exists():
            raise RuntimeError(f"CoW isolation violated between siblings {c['index']} and {other['index']}")

    audit_after_cow = env.audit_server_storage(children[0]["disk_id"])
    c0_packs = audit_after_cow.get("target_packs", [])
    shared_nlinks_cow = [p["nlink"] for p in c0_packs if p["name"] in parent_pack_names]
    delta_nlinks_cow = [p["nlink"] for p in c0_packs if p["name"] not in parent_pack_names]

    unique_data_bytes = GIB + fanout_count * delta_bytes_per_child  # 1,088 MiB
    logical_after_cow = (1 + fanout_count) * GIB + fanout_count * delta_bytes_per_child  # 17,472 MiB
    lineage_after_cow_phys = max(0, int(audit_after_cow["du_sb_bytes"]) - base_overhead_bytes)
    space_amp_cow = round(int(audit_after_cow["du_sb_bytes"]) / unique_data_bytes, 4)
    logical_to_phys_cow = round(logical_after_cow / max(1, int(audit_after_cow["du_sb_bytes"])), 2)
    phys_savings_pct = round((1.0 - int(audit_after_cow["du_sb_bytes"]) / logical_after_cow) * 100.0, 2)

    # 5. 先卸载并删除父盘，随后清空双端页缓存，核验全部 16 个子盘仍 100% 通过共享父文件与私有增量冷读校验
    env.umount_and_delete(parent_id, parent_mp)
    env.drop_caches_dual_node()

    def _verify_child_after_parent_del(c: dict[str, Any]) -> bool:
        idx = int(c["index"])
        cmp_p = c["mountpoint"]
        rel = parent_rels[idx]
        ok_shared, _ = verify_single_file_sha256(cmp_p / rel, parent_manifest[rel])
        ok_delta, _ = verify_single_file_sha256(cmp_p / c["delta_name"], delta_manifests[idx])
        return ok_shared and ok_delta

    with ThreadPoolExecutor(max_workers=16) as pool:
        results_after_pdel = list(pool.map(_verify_child_after_parent_del, children))
    if not all(results_after_pdel):
        raise RuntimeError("Child SHA-256 read verification failed after deleting parent disk!")

    audit_after_parent_del = env.audit_server_storage(children[0]["disk_id"])
    c0_packs_pdel = audit_after_parent_del.get("target_packs", [])
    shared_nlinks_pdel = [p["nlink"] for p in c0_packs_pdel if p["name"] in parent_pack_names]

    # 6. 卸载并删除 15 个子盘（c_01 .. c_15），仅保留唯一幸存子盘 c_00
    for c in children[1:]:
        env.umount_ephemeral(c["disk_id"], c["mountpoint"])
        env.rpc.delete_disk(c["disk_id"])

    survivor = children[0]
    audit_sole_survivor = env.audit_server_storage(survivor["disk_id"])
    commits_remaining = int(audit_sole_survivor["commits_dir_count"])
    surv_min_nlink = int(audit_sole_survivor["target_pack_min_nlink"])
    surv_max_nlink = int(audit_sole_survivor["target_pack_max_nlink"])
    surv_pack_cnt = int(audit_sole_survivor["target_pack_count"])

    if commits_remaining != 0:
        raise RuntimeError(f"Expected 0 commits remaining on sole survivor, got {commits_remaining}")
    if surv_min_nlink != 1 or surv_max_nlink != 1:
        raise RuntimeError(
            f"Expected survivor .pack st_nlink == 1, got min={surv_min_nlink}, max={surv_max_nlink}"
        )

    # 双端清缓存后对唯一幸存盘执行全量 16 × 64 MiB + 4 MiB SHA-256 终态校验
    env.drop_caches_dual_node()
    surv_shared_ok, _ = verify_manifest_sha256(survivor["mountpoint"], parent_manifest)
    surv_delta_ok, _ = verify_single_file_sha256(
        survivor["mountpoint"] / survivor["delta_name"], delta_manifests[0]
    )
    if not (surv_shared_ok and surv_delta_ok):
        raise RuntimeError("Sole survivor full SHA-256 verification failed!")

    # 清理唯一幸存盘
    env.umount_and_delete(survivor["disk_id"], survivor["mountpoint"])

    env.telemetry_cmd("finish_bowl_stage_profiles 'suite3_fanout_cow'")
    env.telemetry_cmd("capture_bowl_snapshot 'suite3_fanout_cow_post'")

    fork_stats = summarize_samples(fork_samples, trim_fraction=0.0)
    log(
        f"[Suite 3 Done] 1->16 fanout_total={fanout_total_ms:.2f}ms "
        f"du_sb_cow={audit_after_cow['du_sb_bytes'] / GIB:.3f}GiB vs logical={logical_after_cow / GIB:.3f}GiB "
        f"space_amp={space_amp_cow:.3f}x commits_after_gc={commits_remaining} survivor_nlink={surv_max_nlink}"
    )

    suite3_payload = {
        "parent_seed_files": len(parent_rels),
        "parent_seed_file_bytes": 64 * MIB,
        "parent_seed_bytes": GIB,
        "parent_seed_ms": round(parent_seed_ms, 3),
        "fanout_children": fanout_count,
        "private_delta_per_child_bytes": delta_bytes_per_child,
        "private_delta_total_bytes": fanout_count * delta_bytes_per_child,
        "fanout_fork_total_ms": round(fanout_total_ms, 3),
        "fanout_fork_rpc_ms": fork_stats,
        "concurrent_mount_total_ms": round(concurrent_mount_ms, 3),
        "concurrent_io_verify_ms": round(concurrent_io_ms, 3),
        "checkpoints": {
            "before_seed": {
                "du_sb_bytes": base_overhead_bytes,
                "commits_dir_count": int(audit_before_seed["commits_dir_count"]),
            },
            "after_parent_seed": {
                "du_sb_bytes": int(audit_after_seed["du_sb_bytes"]),
                "lineage_du_sb_bytes": lineage_seed_phys,
                "logical_bytes": GIB,
                "commits_dir_count": int(audit_after_seed["commits_dir_count"]),
                "parent_pack_count": int(audit_after_seed["target_pack_count"]),
                "parent_pack_max_nlink": int(audit_after_seed["target_pack_max_nlink"]),
            },
            "after_16_forks": {
                "du_sb_bytes": int(audit_after_16_forks["du_sb_bytes"]),
                "lineage_du_sb_bytes": lineage_after_forks_phys,
                "logical_bytes": logical_after_forks,
                "commits_dir_count": int(audit_after_16_forks["commits_dir_count"]),
                "shared_pack_min_nlink": int(audit_after_16_forks["target_pack_min_nlink"]),
                "shared_pack_max_nlink": int(audit_after_16_forks["target_pack_max_nlink"]),
                "space_amplification_vs_unique": round(int(audit_after_16_forks["du_sb_bytes"]) / GIB, 4),
                "logical_to_physical_ratio": round(
                    logical_after_forks / max(1, int(audit_after_16_forks["du_sb_bytes"])), 2
                ),
            },
            "after_concurrent_cow_writes": {
                "du_sb_bytes": int(audit_after_cow["du_sb_bytes"]),
                "lineage_du_sb_bytes": lineage_after_cow_phys,
                "unique_data_bytes": unique_data_bytes,
                "logical_bytes": logical_after_cow,
                "commits_dir_count": int(audit_after_cow["commits_dir_count"]),
                "shared_pack_max_nlink": max(shared_nlinks_cow) if shared_nlinks_cow else 0,
                "delta_pack_nlink": max(delta_nlinks_cow) if delta_nlinks_cow else 1,
                "space_amplification_vs_unique": space_amp_cow,
                "logical_to_physical_ratio": logical_to_phys_cow,
                "physical_savings_pct": phys_savings_pct,
            },
            "after_parent_delete": {
                "du_sb_bytes": int(audit_after_parent_del["du_sb_bytes"]),
                "lineage_du_sb_bytes": max(0, int(audit_after_parent_del["du_sb_bytes"]) - base_overhead_bytes),
                "logical_bytes": fanout_count * GIB + fanout_count * delta_bytes_per_child,
                "commits_dir_count": int(audit_after_parent_del["commits_dir_count"]),
                "shared_pack_min_nlink": min(shared_nlinks_pdel) if shared_nlinks_pdel else 0,
                "shared_pack_max_nlink": max(shared_nlinks_pdel) if shared_nlinks_pdel else 0,
                "children_verified": fanout_count,
                "all_16_children_sha256_ok": True,
            },
            "after_15_children_delete_sole_survivor": {
                "du_sb_bytes": int(audit_sole_survivor["du_sb_bytes"]),
                "lineage_du_sb_bytes": max(0, int(audit_sole_survivor["du_sb_bytes"]) - base_overhead_bytes),
                "survivor_disk_id": survivor["disk_id"],
                "logical_bytes": GIB + delta_bytes_per_child,
                "commits_dir_count": commits_remaining,
                "survivor_pack_count": surv_pack_cnt,
                "survivor_pack_min_nlink": surv_min_nlink,
                "survivor_pack_max_nlink": surv_max_nlink,
                "survivor_sha256_ok": True,
            },
        },
        "cow_isolation_verified": True,
        "lca_gc_verified": True,
    }
    stage_row = {"stage": "suite3_fanout_cow", "duration_sec": round(time.perf_counter() - t_stage, 3)}
    return suite3_payload, stage_row


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True, help="Bowl StorageService HTTP endpoint")
    parser.add_argument("--server-priv-ip", required=True, help="Server node VPC private IP")
    parser.add_argument("--ssh-key", default="/root/.ssh/id_rsa", help="SSH private key path to Server")
    parser.add_argument("--bowl-bin", default="/usr/local/bin/bowl", help="bowl CLI binary path")
    parser.add_argument("--primary-mount", type=Path, default=Path("/mnt/bowl"), help="Primary telemetry anchor mount")
    parser.add_argument("--primary-disk-id", required=True, help="Disk ID mounted at --primary-mount")
    parser.add_argument("--work-mount-root", type=Path, default=Path("/mnt/fork_work"), help="Ephemeral mount root")
    parser.add_argument(
        "--ephemeral-log",
        type=Path,
        default=Path("/var/log/bowl-mount-ephemeral.log"),
        help="Structured log file for ephemeral mounts",
    )
    parser.add_argument("--trace-sample-ratio", default="0.10", help="OTLP trace sample ratio")
    parser.add_argument(
        "--telemetry-helper",
        type=Path,
        default=Path("/opt/bowl/client_telemetry.sh"),
        help="Optional client telemetry helper script",
    )
    parser.add_argument("--out-dir", type=Path, required=True, help="Output directory for raw JSON results")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    env = BenchEnv(
        endpoint=args.endpoint,
        server_priv_ip=args.server_priv_ip,
        ssh_key=args.ssh_key,
        bowl_bin=args.bowl_bin,
        primary_mount=args.primary_mount,
        primary_disk_id=args.primary_disk_id,
        work_mount_root=args.work_mount_root,
        ephemeral_log=args.ephemeral_log,
        trace_sample_ratio=args.trace_sample_ratio,
        telemetry_helper=args.telemetry_helper if args.telemetry_helper.exists() else None,
    )
    try:
        s1, s1_stages = run_suite1_matrix(env)
        (args.out_dir / "suite1_matrix.json").write_text(
            json.dumps(s1, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

        s2, s2_stage = run_suite2_online_fork(env)
        (args.out_dir / "suite2_online_fork.json").write_text(
            json.dumps(s2, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

        s3, s3_stage = run_suite3_fanout_cow(env)
        (args.out_dir / "suite3_fanout_cow.json").write_text(
            json.dumps(s3, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

        all_stages = [*s1_stages, s2_stage, s3_stage]
        with (args.out_dir / "fork_stages.jsonl").open("w", encoding="utf-8") as f:
            for row in all_stages:
                row_with_ts = {
                    "stage": row["stage"],
                    "duration_sec": row["duration_sec"],
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
                f.write(json.dumps(row_with_ts, ensure_ascii=False) + "\n")
    finally:
        env.close()


if __name__ == "__main__":
    main()
