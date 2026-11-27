from __future__ import annotations

import hashlib
import http.client
import json
import os
import shutil
import threading
import time
import urllib.parse
from pathlib import Path
from typing import Any

from benchmark.lib import metrics_audit
from benchmark.lib.context import BenchContext, MountHandle
from benchmark.lib.stats import (
    computesdk_fork_score,
    computesdk_storage_score,
    summarize_latency,
    summarize_steady,
)

SUITE_ID = "computesdk-bench"
SUPPORTED_TARGETS = ["bowl_staged", "bowl_sync", "ext4", "jfs_pg_minio"]

KIB = 1024
MIB = 1024 * 1024

STORAGE_SIZES_FULL_MB = [1, 4, 10, 16]
STORAGE_SIZES_SMOKE_MB = [1, 4]

SNAPSHOT_FORK_PRESETS: list[dict[str, Any]] = [
    {"preset": "small", "label": "ComputeSDK small (10 x 1 MiB)", "file_count": 10, "subdir_count": 0, "file_size_bytes": 1 * MIB, "smoke": True},
    {"preset": "wide", "label": "ComputeSDK wide (100 x 1 MiB)", "file_count": 100, "subdir_count": 0, "file_size_bytes": 1 * MIB, "smoke": False},
    {"preset": "deep", "label": "ComputeSDK deep (10 x 16 MiB)", "file_count": 10, "subdir_count": 0, "file_size_bytes": 16 * MIB, "smoke": False},
    {"preset": "large_1gib", "label": "POSIX ext large_1gib (64 x 16 MiB)", "file_count": 64, "subdir_count": 0, "file_size_bytes": 16 * MIB, "smoke": False},
    {"preset": "tree_10k", "label": "POSIX ext tree_10k (10,000 x 4 KiB)", "file_count": 10000, "subdir_count": 100, "file_size_bytes": 4 * KIB, "smoke": False},
]


def _deterministic_bytes(seed_tag: str, total_bytes: int) -> bytes:
    block = bytearray()
    counter = 0
    unit_cap = min(total_bytes, 64 * KIB)
    while len(block) < unit_cap:
        block.extend(hashlib.blake2b(f"{seed_tag}:{counter}".encode("utf-8"), digest_size=64).digest())
        counter += 1
    unit = bytes(block[:unit_cap])
    if total_bytes <= len(unit):
        return unit[:total_bytes]
    reps, rem = divmod(total_bytes, len(unit))
    buf = bytearray(unit * reps + unit[:rem])
    head = hashlib.sha256(f"h:{seed_tag}".encode("utf-8")).digest()
    tail = hashlib.sha256(f"t:{seed_tag}".encode("utf-8")).digest()
    buf[: len(head)] = head
    buf[-len(tail) :] = tail
    return bytes(buf)


def _fsync_dir(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class ConnectStorageClient:
    """Keep-alive HTTP/1.1 ConnectRPC JSON client for pure control-plane RPC timing."""

    def __init__(self, endpoint: str, timeout_sec: float = 60.0) -> None:
        parsed = urllib.parse.urlparse(endpoint)
        self.host = parsed.hostname or "127.0.0.1"
        self.port = parsed.port or (443 if parsed.scheme == "https" else 9000)
        self.timeout_sec = timeout_sec
        self._local = threading.local()

    def _conn(self) -> http.client.HTTPConnection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout_sec)
            self._local.conn = conn
        return conn

    def _reset(self) -> None:
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
                return (json.loads(raw.decode("utf-8")) if raw else {}), elapsed_ms
            except (http.client.HTTPException, ConnectionError, OSError):
                self._reset()
                if attempt == 1:
                    raise
        raise RuntimeError(f"ConnectRPC {method} retry exhausted")

    def create_commit(self, disk_id: str, branch: str, message: str = "") -> tuple[dict[str, Any], float]:
        return self.call("CreateCommit", {"diskId": disk_id, "branch": branch, "message": message})

    def fork_branch(
        self, disk_id: str, source_branch: str, new_branch: str, message: str = ""
    ) -> tuple[dict[str, Any], float]:
        return self.call(
            "ForkBranch",
            {
                "diskId": disk_id,
                "sourceBranch": source_branch,
                "newBranch": new_branch,
                "message": message,
            },
        )

    def delete_branch(self, disk_id: str, branch: str) -> float:
        _, ms = self.call("DeleteBranch", {"diskId": disk_id, "branch": branch})
        return ms

    def get_disk(self, disk_id: str) -> dict[str, Any]:
        resp, _ = self.call("GetDisk", {"diskId": disk_id})
        return resp.get("disk", {})

    def close(self) -> None:
        self._reset()


def _run_storage_lifecycle(ctx: BenchContext, target: str) -> dict[str, Any]:
    smoke = ctx.profile == "smoke-local"
    sizes_mb = STORAGE_SIZES_SMOKE_MB if smoke else STORAGE_SIZES_FULL_MB
    warmup_n = 1
    runs_n = 5 if smoke else 20

    size_results: dict[str, Any] = {}
    all_bytes_verified = True
    audit_failed = False
    high_variance = False
    audit_reasons: list[str] = []
    agg_deltas: dict[str, float] = {}

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(target, jfs_cache_mode="zero")
        work_dir = handle.mount_dir / "computesdk_storage"
        work_dir.mkdir(parents=True, exist_ok=True)

        for size_mb in sizes_mb:
            size_label = f"{size_mb}MB"
            size_bytes = size_mb * MIB
            payload = _deterministic_bytes(f"csdk-storage-{size_label}", size_bytes)

            upload_ms: list[float] = []
            download_ms: list[float] = []
            unlink_ms: list[float] = []
            throughput_mbps: list[float] = []

            # Warmup iterations (excluded from stats & audit)
            for w in range(warmup_n):
                fp = work_dir / f"warmup_{size_label}_{w}.bin"
                fd = os.open(str(fp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                try:
                    view = memoryview(payload)
                    pos = 0
                    while pos < size_bytes:
                        pos += os.write(fd, view[pos:])
                    os.fsync(fd)
                finally:
                    os.close(fd)
                ctx.drop_caches("C1", target=target, handle=handle)
                data = fp.read_bytes()
                if data != payload:
                    all_bytes_verified = False
                os.unlink(str(fp))

            before = ctx.snapshot_counters(target, handle=handle)
            for i in range(runs_n):
                fp = work_dir / f"iter_{size_label}_{i}.bin"

                # 1. Tier 1 mandatory create + write + fsync
                t_up0 = time.perf_counter()
                fd = os.open(str(fp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                try:
                    view = memoryview(payload)
                    pos = 0
                    while pos < size_bytes:
                        pos += os.write(fd, view[pos:])
                    os.fsync(fd)
                finally:
                    os.close(fd)
                up_ms = (time.perf_counter() - t_up0) * 1000.0
                upload_ms.append(up_ms)

                # 2. C1 cold read + full byte verification
                ctx.drop_caches("C1", target=target, handle=handle)
                t_dl0 = time.perf_counter()
                data = fp.read_bytes()
                dl_ms = (time.perf_counter() - t_dl0) * 1000.0
                download_ms.append(dl_ms)

                if data != payload:
                    all_bytes_verified = False

                total_s = (up_ms + dl_ms) / 1000.0
                if total_s > 0:
                    throughput_mbps.append((2.0 * size_bytes * 8.0) / (total_s * 1_000_000.0))

                # 3. Unlink
                t_rm0 = time.perf_counter()
                os.unlink(str(fp))
                rm_ms = (time.perf_counter() - t_rm0) * 1000.0
                unlink_ms.append(rm_ms)

            after = ctx.snapshot_counters(target, handle=handle)
            audit = ctx.audit_case(
                before,
                after,
                target=target,
                cache_state="C1",
                expected_read_bytes=runs_n * size_bytes,
                min_fuse_ratio=0.95,
            )
            if audit.get("validity_status") != "PASS":
                audit_failed = True
                audit_reasons.extend(f"{size_label}: {r}" for r in audit.get("reasons", []))
            for k, v in audit.get("deltas", {}).items():
                if isinstance(v, (int, float)):
                    agg_deltas[k] = round(agg_deltas.get(k, 0.0) + float(v), 4)

            up_stat = summarize_latency(upload_ms, kind="discrete")
            dl_stat = summarize_latency(download_ms, kind="discrete")
            rm_stat = summarize_latency(unlink_ms, kind="discrete")
            tp_stat = summarize_latency(throughput_mbps, kind="discrete")
            if up_stat["high_variance"] or dl_stat["high_variance"]:
                high_variance = True

            up_p50 = up_stat["p50"] or 0.0
            dl_p50 = dl_stat["p50"] or 0.0
            ref_score = (
                round(0.5 * computesdk_storage_score(up_p50) + 0.5 * computesdk_storage_score(dl_p50), 2)
                if all_bytes_verified
                else 0.0
            )
            size_results[size_label] = {
                "size_bytes": size_bytes,
                "upload_ms": up_stat,
                "download_ms": dl_stat,
                "unlink_ms": rm_stat,
                "throughput_mbps": tp_stat,
                "computesdk_ref_score_30s": ref_score,
                "audit_status": audit.get("validity_status"),
            }

        drain_s = round(handle.unmount(), 3)
        handle = None
    finally:
        if handle is not None:
            handle.unmount()

    if not all_bytes_verified or audit_failed:
        validity = "INVALID"
    elif high_variance:
        validity = "HIGH_VARIANCE"
    else:
        validity = "PASS"

    notes = f"sizes={','.join(f'{s}MB' for s in sizes_mb)}, N={runs_n}, Tier 1 fsync + C1 cold read byte-verify"
    if audit_reasons:
        notes += "; issues=" + "; ".join(audit_reasons[:5])

    return {
        "case_id": "computesdk-storage-lifecycle",
        "target": target,
        "durability_tier": "Tier 1",
        "cache_state": "C1",
        "runs_n": runs_n,
        "metrics": {
            "sizes": size_results,
            "drain_time_s": drain_s,
        },
        "validity_status": validity,
        "integrity_verified": all_bytes_verified,
        "counter_deltas": agg_deltas,
        "notes": notes,
    }


def _seed_preset_tree(
    root: Path,
    preset: dict[str, Any],
) -> tuple[list[Path], dict[str, tuple[int, str]]]:
    file_count = int(preset["file_count"])
    subdir_count = int(preset["subdir_count"])
    file_size = int(preset["file_size_bytes"])
    p_name = preset["preset"]

    base_unit = _deterministic_bytes(f"preset-{p_name}-base", file_size)
    rel_paths: list[Path] = []
    manifest: dict[str, tuple[int, str]] = {}

    if subdir_count > 0:
        for d in range(subdir_count):
            (root / f"d{d:03d}").mkdir(parents=True, exist_ok=True)

    for idx in range(file_count):
        if subdir_count > 0:
            rel = Path(f"d{idx % subdir_count:03d}") / f"f{idx:05d}.bin"
        else:
            rel = Path(f"f{idx:05d}.bin")
        full = root / rel

        # Patch first 32 bytes per file so every file has unique content & SHA-256
        hdr = hashlib.sha256(f"{p_name}:{idx}".encode("utf-8")).digest()
        data = hdr + base_unit[len(hdr) :] if len(base_unit) >= len(hdr) else hdr[: len(base_unit)]

        fd = os.open(str(full), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            view = memoryview(data)
            pos = 0
            while pos < len(data):
                pos += os.write(fd, view[pos:])
            os.fsync(fd)
        finally:
            os.close(fd)

        rel_paths.append(rel)
        if idx < 16 or idx == file_count - 1:
            manifest[str(rel)] = (len(data), hashlib.sha256(data).hexdigest())

    _fsync_dir(root)
    return rel_paths, manifest


def _verify_files_sha256(root: Path, manifest: dict[str, tuple[int, str]]) -> tuple[bool, int]:
    total_read = 0
    for rel, (exp_size, exp_sha) in manifest.items():
        fp = root / rel
        if not fp.is_file():
            return False, total_read
        data = fp.read_bytes()
        total_read += len(data)
        if len(data) != exp_size or hashlib.sha256(data).hexdigest() != exp_sha:
            return False, total_read
    return True, total_read


def _measure_bowl_copy_baseline(
    ctx: BenchContext,
    target: str,
    src_disk_id: str,
    src_branch: str,
    preset_name: str,
    manifest: dict[str, tuple[int, str]],
    copy_n: int,
) -> tuple[dict[str, Any], bool]:
    copy_ms_samples: list[float] = []
    verified = True
    for i in range(copy_n):
        src_handle: MountHandle | None = None
        dst_handle: MountHandle | None = None
        try:
            src_handle = ctx.prepare_target(target, disk_id=src_disk_id, branch_name=src_branch, read_only=True)
            dst_handle = ctx.prepare_target(target, disk_name=f"csdk-copy-{preset_name}-{i}")

            t0 = time.perf_counter()
            src_data = src_handle.mount_dir / "dataset"
            dst_data = dst_handle.mount_dir / "dataset"
            shutil.copytree(src_data, dst_data)
            os.sync()
            dst_handle.unmount()
            dst_ms = (time.perf_counter() - t0) * 1000.0
            copy_ms_samples.append(dst_ms)

            # Verify copied disk on fresh ro mount
            if dst_handle.disk_id:
                verify_h = ctx.prepare_target(
                    target, disk_id=dst_handle.disk_id, branch_name=dst_handle.branch_name or "main", read_only=True
                )
                try:
                    ok, _ = _verify_files_sha256(verify_h.mount_dir / "dataset", manifest)
                    if not ok:
                        verified = False
                finally:
                    verify_h.unmount()
                    ctx.bowl_rpc_cli(["disk", "delete", dst_handle.disk_id, "--yes"], check=False)
            dst_handle = None
        finally:
            if dst_handle is not None:
                dst_handle.unmount()
            if src_handle is not None:
                src_handle.unmount()
    return summarize_steady(copy_ms_samples), verified


def _run_snapshot_fork(ctx: BenchContext, target: str) -> dict[str, Any]:
    smoke = ctx.profile == "smoke-local"
    presets = [p for p in SNAPSHOT_FORK_PRESETS if (p["smoke"] if smoke else True)]
    rpc_n = 3 if smoke else 100
    mount_n = 3 if smoke else 20
    copy_n = 1 if smoke else 3

    rpc_client = ConnectStorageClient(ctx.server_endpoint)
    preset_results: dict[str, Any] = {}
    all_verified = True
    audit_failed = False
    high_variance = False
    audit_reasons: list[str] = []
    agg_deltas: dict[str, float] = {}

    try:
        for preset in presets:
            p_name = preset["preset"]
            seed_handle: MountHandle | None = None
            live_handle: MountHandle | None = None
            try:
                seed_handle = ctx.prepare_target(target, disk_name=f"csdk-sf-{target}-{p_name}")
                disk_id = seed_handle.disk_id or ""
                src_branch = seed_handle.branch_name or "main"

                dataset_dir = seed_handle.mount_dir / "dataset"
                dataset_dir.mkdir(parents=True, exist_ok=True)
                rel_paths, manifest = _seed_preset_tree(dataset_dir, preset)
                seed_handle.unmount()
                seed_handle = None

                snap_before_rpc = ctx.snapshot_counters(target, handle=None)
                phys_before = int(
                    round(
                        metrics_audit.extract_metric_sum(
                            snap_before_rpc.get("server_metrics", {}),
                            "bowl_storage_pack_physical_bytes",
                        )
                    )
                )

                # Step 1: CreateCommit (snapshotCreate) pure RPC N=rpc_n (+1 warmup)
                rpc_client.create_commit(disk_id, src_branch, f"warmup-{p_name}")
                snapshot_create_ms: list[float] = []
                for i in range(rpc_n):
                    _, ms = rpc_client.create_commit(disk_id, src_branch, f"snap-{p_name}-{i}")
                    snapshot_create_ms.append(ms)

                # Step 2: ForkBranch from clean committed branch (forkFromSnapshot) pure RPC N=rpc_n
                fork_from_snap_ms: list[float] = []
                rpc_client.fork_branch(disk_id, src_branch, f"warmup-fcs-{p_name}")
                rpc_client.delete_branch(disk_id, f"warmup-fcs-{p_name}")
                for i in range(rpc_n):
                    bname = f"fcs-{p_name}-{i}"
                    _, ms = rpc_client.fork_branch(disk_id, src_branch, bname, f"from-snap-{i}")
                    fork_from_snap_ms.append(ms)
                    rpc_client.delete_branch(disk_id, bname)

                # Step 3: ForkBranch from live mounted RW branch with dirty layer (forkFromLive)
                live_handle = ctx.prepare_target(target, disk_id=disk_id, branch_name=src_branch, read_only=False)
                marker = live_handle.mount_dir / ".live_marker"
                fork_from_live_ms: list[float] = []
                for i in range(rpc_n):
                    mfd = os.open(str(marker), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                    try:
                        os.write(mfd, f"live-{i}\n".encode("utf-8"))
                        os.fsync(mfd)
                    finally:
                        os.close(mfd)
                    bname = f"flive-{p_name}-{i}"
                    _, ms = rpc_client.fork_branch(disk_id, src_branch, bname, f"from-live-{i}")
                    fork_from_live_ms.append(ms)
                    rpc_client.delete_branch(disk_id, bname)
                live_handle.unmount()
                live_handle = None

                snap_after_rpc = ctx.snapshot_counters(target, handle=None)
                phys_after = int(
                    round(
                        metrics_audit.extract_metric_sum(
                            snap_after_rpc.get("server_metrics", {}),
                            "bowl_storage_pack_physical_bytes",
                        )
                    )
                )
                added_phys_bytes = max(0, phys_after - phys_before)

                # Step 4: fresh bowl mount + C2 cold first read + byte/SHA-256 verification (N=mount_n)
                mount_ready_ms: list[float] = []
                first_read_ms: list[float] = []
                first_read_file = rel_paths[0]
                first_read_exp_size, first_read_exp_sha = manifest[str(first_read_file)]

                for i in range(mount_n):
                    bname = f"ftti-{p_name}-{i}"
                    rpc_client.fork_branch(disk_id, src_branch, bname, f"tti-{i}")
                    fork_handle: MountHandle | None = None
                    try:
                        t_m0 = time.perf_counter()
                        fork_handle = ctx.prepare_target(
                            target, disk_id=disk_id, branch_name=bname, read_only=True
                        )
                        mount_ready_ms.append((time.perf_counter() - t_m0) * 1000.0)

                        ctx.drop_caches("C2", target=target, handle=fork_handle)
                        before_rd = ctx.snapshot_counters(target, handle=fork_handle)

                        target_fp = fork_handle.mount_dir / "dataset" / first_read_file
                        t_r0 = time.perf_counter()
                        raw_bytes = target_fp.read_bytes()
                        first_read_ms.append((time.perf_counter() - t_r0) * 1000.0)

                        after_rd = ctx.snapshot_counters(target, handle=fork_handle)
                        if (
                            len(raw_bytes) != first_read_exp_size
                            or hashlib.sha256(raw_bytes).hexdigest() != first_read_exp_sha
                        ):
                            all_verified = False

                        if i == 0:
                            ok_full, _ = _verify_files_sha256(fork_handle.mount_dir / "dataset", manifest)
                            if not ok_full:
                                all_verified = False

                        rd_audit = ctx.audit_case(
                            before_rd,
                            after_rd,
                            target=target,
                            cache_state="C2",
                            expected_read_bytes=first_read_exp_size,
                        )
                        if rd_audit.get("validity_status") != "PASS":
                            audit_failed = True
                            audit_reasons.extend(f"{p_name}/r{i}: {r}" for r in rd_audit.get("reasons", []))
                        for k, v in rd_audit.get("deltas", {}).items():
                            if isinstance(v, (int, float)):
                                agg_deltas[k] = round(agg_deltas.get(k, 0.0) + float(v), 4)

                        fork_handle.unmount()
                        fork_handle = None
                    finally:
                        if fork_handle is not None:
                            fork_handle.unmount()
                        rpc_client.delete_branch(disk_id, bname)

                # Same-host bowl_copy baseline
                copy_stat, copy_verified = _measure_bowl_copy_baseline(
                    ctx, target, disk_id, src_branch, p_name, manifest, copy_n
                )
                if not copy_verified:
                    all_verified = False

                snap_stat = summarize_latency(snapshot_create_ms, kind="discrete")
                fcs_stat = summarize_latency(fork_from_snap_ms, kind="discrete")
                flive_stat = summarize_latency(fork_from_live_ms, kind="discrete")
                mnt_stat = summarize_latency(mount_ready_ms, kind="discrete")
                rd_stat = summarize_latency(first_read_ms, kind="discrete")

                for s in (snap_stat, fcs_stat, flive_stat, mnt_stat, rd_stat):
                    if s.get("high_variance"):
                        high_variance = True

                flive_p50 = flive_stat["p50"] or 0.0
                mnt_p50 = mnt_stat["p50"] or 0.0
                rd_p50 = rd_stat["p50"] or 0.0
                tti_total_ms = flive_p50 + mnt_p50 + rd_p50
                copy_med_ms = copy_stat.get("median") or 0.0
                speedup = round(copy_med_ms / tti_total_ms, 2) if (copy_med_ms > 0 and tti_total_ms > 0) else None

                score_rpc = (
                    round(0.90 * computesdk_fork_score(flive_p50) + 0.10 * computesdk_fork_score(rd_p50), 2)
                    if all_verified
                    else 0.0
                )
                score_mount = (
                    round(
                        0.90 * computesdk_fork_score(flive_p50 + mnt_p50)
                        + 0.10 * computesdk_fork_score(rd_p50),
                        2,
                    )
                    if all_verified
                    else 0.0
                )

                preset_results[p_name] = {
                    "label": preset["label"],
                    "file_count": preset["file_count"],
                    "file_size_bytes": preset["file_size_bytes"],
                    "snapshot_create_rpc_ms": snap_stat,
                    "fork_from_snapshot_rpc_ms": fcs_stat,
                    "fork_from_live_rpc_ms": flive_stat,
                    "mount_ready_ms": mnt_stat,
                    "fork_first_read_ms": rd_stat,
                    "bowl_copy_ms": copy_stat,
                    "speedup_vs_bowl_copy": speedup,
                    "added_physical_bytes": added_phys_bytes,
                    "computesdk_ref_score_60s_rpc": score_rpc,
                    "computesdk_ref_score_60s_with_mount": score_mount,
                }
            finally:
                if live_handle is not None:
                    live_handle.unmount()
                if seed_handle is not None:
                    seed_handle.unmount()
    finally:
        rpc_client.close()

    if not all_verified or audit_failed:
        validity = "INVALID"
    elif high_variance:
        validity = "HIGH_VARIANCE"
    else:
        validity = "PASS"

    notes = f"presets={','.join(p['preset'] for p in presets)}, rpc_n={rpc_n}, mount_n={mount_n}, copy_n={copy_n}"
    if audit_reasons:
        notes += "; issues=" + "; ".join(audit_reasons[:5])

    return {
        "case_id": "computesdk-snapshot-fork",
        "target": target,
        "durability_tier": "Tier 1",
        "cache_state": "C2",
        "runs_n": mount_n,
        "metrics": {
            "rpc_runs_n": rpc_n,
            "mount_runs_n": mount_n,
            "presets": preset_results,
        },
        "validity_status": validity,
        "integrity_verified": all_verified,
        "counter_deltas": agg_deltas,
        "notes": notes,
    }


def _fmt_pct(stat_dict: dict[str, Any] | None, keys: tuple[str, ...]) -> str:
    if not stat_dict:
        return "N/A"
    parts = []
    for k in keys:
        v = stat_dict.get(k)
        parts.append(f"{v:.2f}" if isinstance(v, (int, float)) else "N/A")
    return " / ".join(parts)


def _render_summary_markdown(cases: list[dict[str, Any]]) -> str:
    lines = [
        "架构边界说明：ComputeSDK 公开参考值运行于对象存储 HTTP SDK，不含客户端 Linux 内核 FUSE 挂载；下表并列报告纯控制面 RPC 口径与含 `bowl mount` POSIX 口径。主指标为原始毫秒延迟，ComputeSDK 参考分仅作外部对表附注，不作回归判定依据。",
        "",
        "### `computesdk-snapshot-fork`（对齐 `snapshot-fork.bench.ts`）",
        "",
        "| 目标 | 预设 | `CreateCommit` (`P50/P95/P99` ms) | `ForkBranch(snap)` (`P50/P95/P99` ms) | `ForkBranch(live)` (`P50/P95/P99` ms) | `bowl mount` (`P50/P95` ms) | C2 首读校验 (`P50/P95` ms) | 相对 `bowl_copy` 加速 | ComputeSDK 60s 参考分 (RPC / 含挂载) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for c in cases:
        if c["case_id"] != "computesdk-snapshot-fork":
            continue
        for p_name, pm in c.get("metrics", {}).get("presets", {}).items():
            sp = pm.get("speedup_vs_bowl_copy")
            sp_txt = f"{sp:.2f}x" if isinstance(sp, (int, float)) else "N/A"
            lines.append(
                f"| `{c['target']}` | `{p_name}` | "
                f"{_fmt_pct(pm.get('snapshot_create_rpc_ms'), ('p50', 'p95', 'p99'))} | "
                f"{_fmt_pct(pm.get('fork_from_snapshot_rpc_ms'), ('p50', 'p95', 'p99'))} | "
                f"{_fmt_pct(pm.get('fork_from_live_rpc_ms'), ('p50', 'p95', 'p99'))} | "
                f"{_fmt_pct(pm.get('mount_ready_ms'), ('p50', 'p95'))} | "
                f"{_fmt_pct(pm.get('fork_first_read_ms'), ('p50', 'p95'))} | "
                f"{sp_txt} | "
                f"{pm.get('computesdk_ref_score_60s_rpc', 0):.2f} / {pm.get('computesdk_ref_score_60s_with_mount', 0):.2f} |"
            )

    lines.extend(
        [
            "",
            "### `computesdk-storage-lifecycle`（对齐 `storage.bench.ts`）",
            "",
            "| 目标 | 文件大小 | 同步落盘写 (`upload`, Tier 1 `fsync`, `P50/P95` ms) | C1 冷读校验 (`download`, `P50/P95` ms) | 吞吐 (`P50` Mbps) | 删除 (`unlink`, `P50` ms) | ComputeSDK 30s 参考分 | Gate |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        ]
    )
    for c in cases:
        if c["case_id"] != "computesdk-storage-lifecycle":
            continue
        for sz, sm in c.get("metrics", {}).get("sizes", {}).items():
            tp = sm.get("throughput_mbps", {}).get("p50")
            rm = sm.get("unlink_ms", {}).get("p50")
            tp_txt = f"{tp:.2f}" if isinstance(tp, (int, float)) else "N/A"
            rm_txt = f"{rm:.2f}" if isinstance(rm, (int, float)) else "N/A"
            lines.append(
                f"| `{c['target']}` | `{sz}` | "
                f"{_fmt_pct(sm.get('upload_ms'), ('p50', 'p95'))} | "
                f"{_fmt_pct(sm.get('download_ms'), ('p50', 'p95'))} | "
                f"{tp_txt} | {rm_txt} | "
                f"{sm.get('computesdk_ref_score_30s', 0):.2f} | {c['validity_status']} |"
            )

    return "\n".join(lines) + "\n"


def run_suite(ctx: BenchContext) -> dict[str, Any]:
    active = [t for t in ctx.targets if t in SUPPORTED_TARGETS]
    cases: list[dict[str, Any]] = []

    for target in active:
        cases.append(_run_storage_lifecycle(ctx, target))
        if target in ("bowl_staged", "bowl_sync"):
            cases.append(_run_snapshot_fork(ctx, target))

    return {
        "suite": SUITE_ID,
        "cases": cases,
        "summary_markdown": _render_summary_markdown(cases),
    }
