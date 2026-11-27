"""fio-io suite: 9 deep engine I/O, durability, syncGate, ReadAhead, and staging cases."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable

from benchmark.lib.context import BenchContext, MountHandle
from benchmark.lib.stats import (
    format_stat,
    percentile,
    summarize_latency,
    summarize_steady,
)

SUITE_ID = "fio-io"
SUPPORTED_TARGETS = [
    "ext4",
    "passthrough_ll",
    "bowl_staged",
    "bowl_sync",
    "jfs_pg_minio",
    "jfs_redis_minio",
    "drive9",
]

_SMOKE_CORE_CASES = {
    "seq-write-buffered-8j",
    "seq-write-fdatasync-8j",
    "rand-write-4k-fsync-8j",
    "seq-read-cold-e2e-16j",
}


def _is_smoke(ctx: BenchContext) -> bool:
    return ctx.profile == "smoke-local"


def _is_bowl(target: str) -> bool:
    return target.startswith("bowl")


def _extract_json_blob(raw: str) -> dict[str, Any]:
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end <= start:
        raise ValueError(f"fio output did not contain JSON object: {raw[:400]!r}")
    return json.loads(raw[start : end + 1])


def _parse_fio_json(raw: str, direction: str) -> dict[str, Any]:
    """Parse fio --output-format=json output into throughput, IOPS, and op latency percentiles."""
    data = _extract_json_blob(raw)
    jobs = data.get("jobs", [])
    if not jobs:
        raise ValueError("fio JSON contains no jobs")

    total_bw_bytes = 0.0
    total_iops = 0.0
    total_io_bytes = 0
    total_ios = 0
    max_runtime_ms = 0.0
    p50_list: list[float] = []
    p95_list: list[float] = []
    p99_list: list[float] = []

    for job in jobs:
        sec = job.get(direction, {})
        bw_bytes = float(sec.get("bw_bytes", 0.0))
        if bw_bytes == 0.0 and sec.get("bw"):
            bw_bytes = float(sec["bw"]) * 1024.0
        total_bw_bytes += bw_bytes
        total_iops += float(sec.get("iops", 0.0))
        total_io_bytes += int(sec.get("io_bytes", 0))
        total_ios += int(sec.get("total_ios", 0))
        max_runtime_ms = max(max_runtime_ms, float(sec.get("runtime", 0.0)))

        lat_block = sec.get("clat_ns") or sec.get("lat_ns") or {}
        pcts = lat_block.get("percentile") or {}
        if "50.000000" in pcts:
            p50_list.append(float(pcts["50.000000"]) / 1e6)
        if "95.000000" in pcts:
            p95_list.append(float(pcts["95.000000"]) / 1e6)
        if "99.000000" in pcts:
            p99_list.append(float(pcts["99.000000"]) / 1e6)

    runtime_s = max_runtime_ms / 1000.0 if max_runtime_ms > 0 else 0.0
    mib_s = total_bw_bytes / (1024.0 * 1024.0)
    if mib_s == 0.0 and runtime_s > 0 and total_io_bytes > 0:
        mib_s = total_io_bytes / (runtime_s * 1024.0 * 1024.0)

    return {
        "mib_s": round(mib_s, 4),
        "iops": round(total_iops, 2),
        "io_bytes": total_io_bytes,
        "total_ios": total_ios,
        "runtime_s": round(runtime_s, 4),
        "p50_ms": round(percentile(p50_list, 50.0, enforce_min_samples=False), 4) if p50_list else None,
        "p95_ms": (
            round(percentile(p95_list, 50.0, enforce_min_samples=False), 4)
            if p95_list and total_ios >= 20
            else None
        ),
        "p99_ms": (
            round(percentile(p99_list, 50.0, enforce_min_samples=False), 4)
            if p99_list and total_ios >= 100
            else None
        ),
    }


def _aggregate_op_latency(parsed_runs: list[dict[str, Any]]) -> dict[str, Any]:
    total_ios = sum(int(r.get("total_ios", 0)) for r in parsed_runs)
    p50s = [r["p50_ms"] for r in parsed_runs if r.get("p50_ms") is not None]
    p95s = [r["p95_ms"] for r in parsed_runs if r.get("p95_ms") is not None]
    p99s = [r["p99_ms"] for r in parsed_runs if r.get("p99_ms") is not None]
    return {
        "total_ios": total_ios,
        "p50_ms": round(percentile(p50s, 50.0, enforce_min_samples=False), 4) if p50s else None,
        "p95_ms": round(percentile(p95s, 50.0, enforce_min_samples=False), 4) if p95s and total_ios >= 20 else None,
        "p99_ms": round(percentile(p99s, 50.0, enforce_min_samples=False), 4) if p99s and total_ios >= 100 else None,
    }


def _run_fio(ctx: BenchContext, args: list[str], *, timeout: int = 900) -> str:
    cmd = ["fio", "--output-format=json", "--group_reporting", *args]
    proc = ctx.run_cmd(cmd, timeout=timeout, check=True)
    return proc.stdout


def _run_xxhash_verify_pass(
    ctx: BenchContext,
    verify_dir: Path,
    *,
    rw: str,
    bs: str,
    size_str: str,
    numjobs: int = 2,
    ioengine: str = "psync",
    extra_args: list[str] | None = None,
) -> bool:
    """Run a separate post-write fio --verify=xxhash --do_verify=1 integrity pass."""
    verify_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "fio",
        "--name=xxhash_verify",
        f"--directory={verify_dir}",
        f"--rw={rw}",
        f"--ioengine={ioengine}",
        f"--bs={bs}",
        "--iodepth=1",
        f"--numjobs={numjobs}",
        f"--size={size_str}",
        "--verify=xxhash",
        "--do_verify=1",
        "--verify_fatal=1",
        "--output-format=json",
    ]
    if extra_args:
        cmd.extend(extra_args)
    proc = ctx.run_cmd(cmd, timeout=600, check=False)
    return proc.returncode == 0


def _write_sha256_sentinel(path: Path, size_bytes: int) -> str:
    h = hashlib.sha256()
    chunk = os.urandom(min(size_bytes, 1024 * 1024))
    remaining = size_bytes
    with open(path, "wb") as fh:
        while remaining > 0:
            part = chunk[:remaining]
            fh.write(part)
            h.update(part)
            remaining -= len(part)
        fh.flush()
        os.fsync(fh.fileno())
    return h.hexdigest()


def _verify_sha256_sentinel(path: Path, expected: str) -> bool:
    if not path.is_file():
        return False
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            buf = fh.read(1024 * 1024)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest() == expected


def _delete_disk_quiet(ctx: BenchContext, disk_id: str | None) -> None:
    if not disk_id:
        return
    ctx.bowl_rpc_cli(["disk", "delete", disk_id, "-y", "--json"], check=False)


def _combine_validity(
    audit_status: str,
    integrity_ok: bool,
    primary_stat: dict[str, Any],
) -> str:
    if not integrity_ok or audit_status == "INVALID":
        return "INVALID"
    if primary_stat.get("high_variance"):
        return "HIGH_VARIANCE"
    return "PASS"


def _skipped_case(case_id: str, target: str, tier: str, cache_state: str, reason: str) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": tier,
        "cache_state": cache_state,
        "runs_n": 0,
        "metrics": {},
        "validity_status": "SKIPPED",
        "integrity_verified": True,
        "counter_deltas": {},
        "notes": reason,
    }


def _cfg_params(ctx: BenchContext) -> dict[str, Any]:
    if _is_smoke(ctx):
        return {
            "runs_n": 2,
            "ramp_s": 2,
            "steady_s": 8,
            "write_jobs": 4,
            "read_jobs": 4,
            "stage_size": "64MiB",
            "write_total_mib": 64,
            "cold_seq_read_mib": 128,
            "rand_read_ws_mib": 128,
            "verify_size": "8m",
            "warmup_mib": 16,
        }
    return {
        "runs_n": 3,
        "ramp_s": 10,
        "steady_s": 60,
        "write_jobs": 8,
        "read_jobs": 16,
        "stage_size": "512MiB",
        "write_total_mib": 4096,
        "cold_seq_read_mib": 8192,
        "rand_read_ws_mib": 32768,
        "verify_size": "64m",
        "warmup_mib": 64,
    }


def _case_seq_write_buffered(ctx: BenchContext, target: str, cfg: dict[str, Any]) -> dict[str, Any]:
    case_id = "seq-write-buffered-8j"
    runs_n = cfg["runs_n"]
    numjobs = int(cfg["write_jobs"])
    per_job_mib = max(4, cfg["write_total_mib"] // numjobs)
    visible_vals: list[float] = []
    drain_vals: list[float] = []
    effective_vals: list[float] = []
    parsed_runs: list[dict[str, Any]] = []
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []
    integrity_ok = True

    for run_idx in range(runs_n):
        handle: MountHandle | None = None
        ro_handle: MountHandle | None = None
        disk_id: str | None = None
        try:
            handle = ctx.prepare_target(
                target,
                jfs_cache_mode="tier0",
                stage_size=cfg["stage_size"],
                disk_name=f"fio-seqw-buf-{run_idx}",
            )
            disk_id = handle.disk_id
            work_dir = handle.mount_dir / f"seqw_buf_{run_idx}"
            work_dir.mkdir(parents=True, exist_ok=True)
            before = ctx.snapshot_counters(target, handle)
            t0 = time.perf_counter()
            raw = _run_fio(
                ctx,
                [
                    "--name=seq_write_buf",
                    f"--directory={work_dir}",
                    "--rw=write",
                    "--ioengine=psync",
                    "--bs=1m",
                    "--iodepth=1",
                    "--buffered=1",
                    f"--numjobs={numjobs}",
                    f"--size={per_job_mib}m",
                    f"--ramp_time={cfg['ramp_s']}",
                    f"--runtime={cfg['steady_s']}",
                    "--time_based",
                ],
            )
            write_wall_s = time.perf_counter() - t0
            parsed = _parse_fio_json(raw, "write")
            parsed_runs.append(parsed)

            sentinel_path = work_dir / "tier2_sentinel.bin"
            sentinel_sha = _write_sha256_sentinel(sentinel_path, 4 * 1024 * 1024)

            if run_idx == runs_n - 1:
                v_ok = _run_xxhash_verify_pass(
                    ctx,
                    handle.mount_dir / "verify_pass",
                    rw="write",
                    bs="256k",
                    size_str=cfg["verify_size"],
                    numjobs=2,
                    extra_args=["--buffered=1"],
                )
                integrity_ok = integrity_ok and v_ok

            after = ctx.snapshot_counters(target, handle)
            audit = ctx.audit_case(before, after, target=target, cache_state="C0")
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])

            disk_id = handle.disk_id
            branch = handle.branch_name or "main"
            drain_s = handle.unmount()
            handle = None
            total_bytes = max(parsed["io_bytes"], cfg["write_total_mib"] * 1024 * 1024)
            eff_mib_s = total_bytes / (max(1e-6, write_wall_s + drain_s) * 1024.0 * 1024.0)
            visible_vals.append(parsed["mib_s"])
            drain_vals.append(round(drain_s, 4))
            effective_vals.append(round(eff_mib_s, 4))

            if _is_bowl(target) and disk_id and run_idx == runs_n - 1:
                ro_handle = ctx.prepare_target(target, disk_id=disk_id, branch_name=branch, read_only=True)
                integrity_ok = integrity_ok and _verify_sha256_sentinel(
                    ro_handle.mount_dir / f"seqw_buf_{run_idx}" / "tier2_sentinel.bin",
                    sentinel_sha,
                )
        finally:
            if ro_handle is not None:
                ro_handle.unmount()
            if handle is not None:
                handle.unmount()
            _delete_disk_quiet(ctx, disk_id)

    eff_stat = summarize_steady(effective_vals)
    vis_stat = summarize_steady(visible_vals)
    drain_stat = summarize_steady(drain_vals)
    op_lat = _aggregate_op_latency(parsed_runs)
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok, eff_stat)
    notes = "; ".join(audit_reasons) if audit_reasons else f"stage_size={cfg['stage_size']}, xxhash+tier2_verify={'PASS' if integrity_ok else 'FAIL'}"
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 0 + Tier 2 drain",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "effective_throughput_mib_s": eff_stat,
            "visible_throughput_mib_s": vis_stat,
            "drain_time_s": drain_stat,
            "op_latency_ms": op_lat,
        },
        "validity_status": status,
        "integrity_verified": integrity_ok,
        "counter_deltas": last_deltas,
        "notes": notes,
    }


def _case_seq_write_fdatasync(ctx: BenchContext, target: str, cfg: dict[str, Any]) -> dict[str, Any]:
    case_id = "seq-write-fdatasync-8j"
    if target == "jfs_redis_minio":
        return _skipped_case(
            case_id,
            target,
            "Tier 1",
            "C0",
            "Tier 0.5 target excluded from Tier 1 fdatasync comparison",
        )

    runs_n = cfg["runs_n"]
    numjobs = int(cfg["write_jobs"])
    per_job_mib = max(4, cfg["write_total_mib"] // numjobs)
    bw_vals: list[float] = []
    parsed_runs: list[dict[str, Any]] = []
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []
    integrity_ok = True

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(
            target,
            jfs_cache_mode="zero",
            stage_size=cfg["stage_size"],
            disk_name="fio-seqw-fdatasync",
        )
        for run_idx in range(runs_n):
            work_dir = handle.mount_dir / f"seqw_fds_{run_idx}"
            work_dir.mkdir(parents=True, exist_ok=True)
            before = ctx.snapshot_counters(target, handle)
            raw = _run_fio(
                ctx,
                [
                    "--name=seq_write_fds",
                    f"--directory={work_dir}",
                    "--rw=write",
                    "--ioengine=psync",
                    "--bs=256k",
                    "--iodepth=1",
                    "--fdatasync=1",
                    f"--numjobs={numjobs}",
                    f"--size={per_job_mib}m",
                    f"--ramp_time={cfg['ramp_s']}",
                    f"--runtime={cfg['steady_s']}",
                    "--time_based",
                ],
            )
            after = ctx.snapshot_counters(target, handle)
            parsed = _parse_fio_json(raw, "write")
            parsed_runs.append(parsed)
            bw_vals.append(parsed["mib_s"])
            audit = ctx.audit_case(before, after, target=target, cache_state="C0")
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])
            shutil.rmtree(work_dir, ignore_errors=True)

        integrity_ok = _run_xxhash_verify_pass(
            ctx,
            handle.mount_dir / "verify_fds",
            rw="write",
            bs="256k",
            size_str=cfg["verify_size"],
            numjobs=2,
            extra_args=["--fdatasync=1"],
        )
    finally:
        disk_id = handle.disk_id if handle is not None else None
        if handle is not None:
            handle.unmount()
        _delete_disk_quiet(ctx, disk_id)

    bw_stat = summarize_steady(bw_vals)
    op_lat = _aggregate_op_latency(parsed_runs)
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok, bw_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 1",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "throughput_mib_s": bw_stat,
            "op_latency_ms": op_lat,
        },
        "validity_status": status,
        "integrity_verified": integrity_ok,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons) if audit_reasons else f"xxhash_verify={'PASS' if integrity_ok else 'FAIL'}",
    }


def _case_rand_write_4k_fsync(ctx: BenchContext, target: str, cfg: dict[str, Any]) -> dict[str, Any]:
    case_id = "rand-write-4k-fsync-8j"
    if target == "jfs_redis_minio":
        return _skipped_case(
            case_id,
            target,
            "Tier 1",
            "C0",
            "Tier 0.5 target excluded from Tier 1 fsync comparison",
        )

    runs_n = cfg["runs_n"]
    numjobs = int(cfg["write_jobs"])
    per_job_mib = max(4, cfg["write_total_mib"] // numjobs)
    iops_vals: list[float] = []
    parsed_runs: list[dict[str, Any]] = []
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []
    integrity_ok = True

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(
            target,
            jfs_cache_mode="zero",
            stage_size=cfg["stage_size"],
            disk_name="fio-randw-4k-fsync",
        )
        for run_idx in range(runs_n):
            work_dir = handle.mount_dir / f"randw_4k_{run_idx}"
            work_dir.mkdir(parents=True, exist_ok=True)
            before = ctx.snapshot_counters(target, handle)
            raw = _run_fio(
                ctx,
                [
                    "--name=rand_write_4k",
                    f"--directory={work_dir}",
                    "--rw=randwrite",
                    "--ioengine=psync",
                    "--bs=4k",
                    "--iodepth=1",
                    "--fsync=1",
                    f"--numjobs={numjobs}",
                    f"--size={per_job_mib}m",
                    f"--ramp_time={cfg['ramp_s']}",
                    f"--runtime={cfg['steady_s']}",
                    "--time_based",
                ],
            )
            after = ctx.snapshot_counters(target, handle)
            parsed = _parse_fio_json(raw, "write")
            parsed_runs.append(parsed)
            iops_vals.append(parsed["iops"])
            audit = ctx.audit_case(before, after, target=target, cache_state="C0")
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])
            shutil.rmtree(work_dir, ignore_errors=True)

        integrity_ok = _run_xxhash_verify_pass(
            ctx,
            handle.mount_dir / "verify_rand4k",
            rw="randwrite",
            bs="4k",
            size_str="4m" if _is_smoke(ctx) else "16m",
            numjobs=2,
            extra_args=["--fsync=1"],
        )
    finally:
        disk_id = handle.disk_id if handle is not None else None
        if handle is not None:
            handle.unmount()
        _delete_disk_quiet(ctx, disk_id)

    iops_stat = summarize_steady(iops_vals)
    op_lat = _aggregate_op_latency(parsed_runs)
    space_amp = last_deltas.get("server_space_amplification")
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok, iops_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 1",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "iops": iops_stat,
            "op_latency_ms": op_lat,
            "space_amplification_ratio": space_amp,
        },
        "validity_status": status,
        "integrity_verified": integrity_ok,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"space_amp={space_amp}, xxhash_verify={'PASS' if integrity_ok else 'FAIL'}",
    }


def _case_seq_read_cold_e2e(ctx: BenchContext, target: str, cfg: dict[str, Any]) -> dict[str, Any]:
    case_id = "seq-read-cold-e2e-16j"
    runs_n = cfg["runs_n"]
    read_jobs = int(cfg["read_jobs"])
    per_job_mib = max(4, cfg["cold_seq_read_mib"] // read_jobs)
    expected_bytes = per_job_mib * read_jobs * 1024 * 1024
    bw_vals: list[float] = []
    parsed_runs: list[dict[str, Any]] = []
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(
            target,
            jfs_cache_mode="zero",
            stage_size=cfg["stage_size"],
            disk_name="fio-seqr-cold-c2",
        )
        dataset_dir = handle.mount_dir / "dataset_8g"
        warmup_dir = handle.mount_dir / "warmup_throwaway"
        dataset_dir.mkdir(parents=True, exist_ok=True)
        warmup_dir.mkdir(parents=True, exist_ok=True)

        _run_fio(
            ctx,
            [
                "--name=seq_read_c2",
                f"--directory={dataset_dir}",
                "--rw=write",
                "--ioengine=psync",
                "--bs=1m",
                "--iodepth=1",
                f"--numjobs={read_jobs}",
                f"--size={per_job_mib}m",
                "--end_fsync=1",
            ],
            timeout=1200,
        )
        _run_fio(
            ctx,
            [
                "--name=warmup_file",
                f"--directory={warmup_dir}",
                "--rw=write",
                "--ioengine=psync",
                "--bs=1m",
                "--iodepth=1",
                "--numjobs=1",
                f"--size={cfg['warmup_mib']}m",
                "--end_fsync=1",
            ],
            timeout=300,
        )

        for _run_idx in range(runs_n):
            _run_fio(
                ctx,
                [
                    "--name=warmup_file",
                    f"--directory={warmup_dir}",
                    "--rw=read",
                    "--ioengine=psync",
                    "--bs=1m",
                    "--iodepth=1",
                    "--numjobs=1",
                    f"--size={cfg['warmup_mib']}m",
                ],
                timeout=300,
            )
            ctx.drop_caches("C2", target=target, handle=handle)
            before = ctx.snapshot_counters(target, handle)
            raw = _run_fio(
                ctx,
                [
                    "--name=seq_read_c2",
                    f"--directory={dataset_dir}",
                    "--rw=read",
                    "--ioengine=psync",
                    "--bs=1m",
                    "--iodepth=1",
                    f"--numjobs={read_jobs}",
                    f"--size={per_job_mib}m",
                ],
                timeout=1200,
            )
            after = ctx.snapshot_counters(target, handle)
            parsed = _parse_fio_json(raw, "read")
            parsed_runs.append(parsed)
            bw_vals.append(parsed["mib_s"])
            audit = ctx.audit_case(
                before,
                after,
                target=target,
                cache_state="C2",
                expected_read_bytes=expected_bytes,
                min_fuse_ratio=0.95,
                min_nvme_ratio=0.90,
            )
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])
    finally:
        disk_id = handle.disk_id if handle is not None else None
        if handle is not None:
            handle.unmount()
        _delete_disk_quiet(ctx, disk_id)

    bw_stat = summarize_steady(bw_vals)
    op_lat = _aggregate_op_latency(parsed_runs)
    status = _combine_validity("PASS" if audit_ok else "INVALID", True, bw_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Read-Only",
        "cache_state": "C2",
        "runs_n": runs_n,
        "metrics": {
            "throughput_mib_s": bw_stat,
            "op_latency_ms": op_lat,
            "single_pass_bytes": expected_bytes,
        },
        "validity_status": status,
        "integrity_verified": True,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons) if audit_reasons else "single-pass C2 cold read verified",
    }


def _case_rand_read_4k_direct(ctx: BenchContext, target: str, cfg: dict[str, Any]) -> dict[str, Any]:
    case_id = "rand-read-4k-direct-32qd"
    runs_n = cfg["runs_n"]
    per_job_mib = max(64, cfg["rand_read_ws_mib"] // 4)
    iops_vals: list[float] = []
    parsed_runs: list[dict[str, Any]] = []
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(
            target,
            jfs_cache_mode="zero",
            direct_io=_is_bowl(target),
            stage_size=cfg["stage_size"],
            disk_name="fio-randr-4k-dio",
        )
        ws_dir = handle.mount_dir / "ws_32g"
        ws_dir.mkdir(parents=True, exist_ok=True)
        _run_fio(
            ctx,
            [
                "--name=rand_read_dio",
                f"--directory={ws_dir}",
                "--rw=write",
                "--ioengine=psync",
                "--bs=1m",
                "--iodepth=1",
                "--numjobs=4",
                f"--size={per_job_mib}m",
                "--end_fsync=1",
            ],
            timeout=2400,
        )

        for _run_idx in range(runs_n):
            ctx.drop_caches("C2", target=target, handle=handle)
            before = ctx.snapshot_counters(target, handle)
            raw = _run_fio(
                ctx,
                [
                    "--name=rand_read_dio",
                    f"--directory={ws_dir}",
                    "--rw=randread",
                    "--ioengine=libaio",
                    "--direct=1",
                    "--bs=4k",
                    "--iodepth=32",
                    "--numjobs=4",
                    f"--size={per_job_mib}m",
                    f"--ramp_time={cfg['ramp_s']}",
                    f"--runtime={cfg['steady_s']}",
                    "--time_based",
                ],
                timeout=900,
            )
            after = ctx.snapshot_counters(target, handle)
            parsed = _parse_fio_json(raw, "read")
            parsed_runs.append(parsed)
            iops_vals.append(parsed["iops"])
            audit = ctx.audit_case(
                before,
                after,
                target=target,
                cache_state="C3-ColdServer",
                expected_read_bytes=parsed["io_bytes"],
                min_fuse_ratio=0.95,
                min_nvme_ratio=0.90,
            )
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])
    finally:
        disk_id = handle.disk_id if handle is not None else None
        if handle is not None:
            handle.unmount()
        _delete_disk_quiet(ctx, disk_id)

    iops_stat = summarize_steady(iops_vals)
    op_lat = _aggregate_op_latency(parsed_runs)
    status = _combine_validity("PASS" if audit_ok else "INVALID", True, iops_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Read-Only",
        "cache_state": "C3-ColdServer",
        "runs_n": runs_n,
        "metrics": {
            "iops": iops_stat,
            "op_latency_ms": op_lat,
            "working_set_mib": per_job_mib * 4,
        },
        "validity_status": status,
        "integrity_verified": True,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons) if audit_reasons else "direct_io=True on Bowl; 32 GiB C3-ColdServer working set",
    }


def _case_rand_write_batched_syncwal(
    ctx: BenchContext,
    target: str,
    cfg: dict[str, Any],
    fsync1_iops_by_target: dict[str, float],
) -> dict[str, Any]:
    case_id = "rand-write-4k-batched-syncwal"
    runs_n = cfg["runs_n"]
    iops_vals: list[float] = []
    parsed_runs: list[dict[str, Any]] = []
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []
    integrity_ok = True

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(
            target,
            direct_io=False,
            stage_size=cfg["stage_size"],
            disk_name="fio-batched-syncwal",
        )
        for run_idx in range(runs_n):
            work_dir = handle.mount_dir / f"batched_{run_idx}"
            work_dir.mkdir(parents=True, exist_ok=True)
            before = ctx.snapshot_counters(target, handle)
            raw = _run_fio(
                ctx,
                [
                    "--name=rand_write_batched",
                    f"--directory={work_dir}",
                    "--rw=randwrite",
                    "--ioengine=libaio",
                    "--direct=1",
                    "--bs=4k",
                    "--iodepth=16",
                    "--fdatasync=16",
                    "--numjobs=4",
                    "--size=512m",
                    f"--ramp_time={cfg['ramp_s']}",
                    f"--runtime={cfg['steady_s']}",
                    "--time_based",
                ],
            )
            after = ctx.snapshot_counters(target, handle)
            parsed = _parse_fio_json(raw, "write")
            parsed_runs.append(parsed)
            iops_vals.append(parsed["iops"])
            audit = ctx.audit_case(before, after, target=target, cache_state="C0")
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])
            shutil.rmtree(work_dir, ignore_errors=True)

        integrity_ok = _run_xxhash_verify_pass(
            ctx,
            handle.mount_dir / "verify_batched",
            rw="randwrite",
            bs="4k",
            size_str="16m",
            numjobs=2,
            ioengine="libaio",
            extra_args=["--direct=1", "--fdatasync=16"],
        )
    finally:
        disk_id = handle.disk_id if handle is not None else None
        if handle is not None:
            handle.unmount()
        _delete_disk_quiet(ctx, disk_id)

    iops_stat = summarize_steady(iops_vals)
    op_lat = _aggregate_op_latency(parsed_runs)
    base_iops = fsync1_iops_by_target.get(target)
    gain = round(iops_stat["median"] / base_iops, 3) if base_iops and iops_stat["median"] else None
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok, iops_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 1 (batched 16)",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "iops": iops_stat,
            "op_latency_ms": op_lat,
            "coalescing_gain_vs_fsync1": gain,
        },
        "validity_status": status,
        "integrity_verified": integrity_ok,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"direct_io=False (O_DIRECT still uses staging in staged mode); gain={gain}x",
    }


def _case_cross_disk_syncgate(ctx: BenchContext, cfg: dict[str, Any]) -> dict[str, Any]:
    case_id = "cross-disk-syncgate-interference"
    target = "bowl_staged"
    runs_n = cfg["runs_n"]
    clean_bw: list[float] = []
    clean_runs: list[dict[str, Any]] = []
    interf_b_bw: list[float] = []
    interf_b_runs: list[dict[str, Any]] = []
    interf_a_iops: list[float] = []
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []

    handle_a: MountHandle | None = None
    handle_b: MountHandle | None = None
    try:
        handle_a = ctx.prepare_target(target, stage_size=cfg["stage_size"], disk_name="syncgate-disk-a")
        handle_b = ctx.prepare_target(target, stage_size=cfg["stage_size"], disk_name="syncgate-disk-b")

        for run_idx in range(runs_n):
            dir_b = handle_b.mount_dir / f"clean_b_{run_idx}"
            dir_b.mkdir(parents=True, exist_ok=True)
            raw_clean = _run_fio(
                ctx,
                [
                    "--name=disk_b_clean",
                    f"--directory={dir_b}",
                    "--rw=write",
                    "--ioengine=psync",
                    "--bs=256k",
                    "--iodepth=1",
                    "--buffered=1",
                    "--numjobs=4",
                    "--size=512m",
                    f"--ramp_time={cfg['ramp_s']}",
                    f"--runtime={cfg['steady_s']}",
                    "--time_based",
                ],
            )
            parsed_c = _parse_fio_json(raw_clean, "write")
            clean_runs.append(parsed_c)
            clean_bw.append(parsed_c["mib_s"])
            shutil.rmtree(dir_b, ignore_errors=True)

        for run_idx in range(runs_n):
            dir_a = handle_a.mount_dir / f"interf_a_{run_idx}"
            dir_b = handle_b.mount_dir / f"interf_b_{run_idx}"
            dir_a.mkdir(parents=True, exist_ok=True)
            dir_b.mkdir(parents=True, exist_ok=True)
            before = ctx.snapshot_counters(target, handle_b)

            a_result: dict[str, Any] = {}
            a_err: list[Exception] = []

            def _run_disk_a() -> None:
                try:
                    raw_a = _run_fio(
                        ctx,
                        [
                            "--name=disk_a_fsync16",
                            f"--directory={dir_a}",
                            "--rw=randwrite",
                            "--ioengine=psync",
                            "--bs=4k",
                            "--iodepth=1",
                            "--fsync=1",
                            "--numjobs=16",
                            "--size=128m",
                            f"--ramp_time={cfg['ramp_s']}",
                            f"--runtime={cfg['steady_s']}",
                            "--time_based",
                        ],
                    )
                    a_result["parsed"] = _parse_fio_json(raw_a, "write")
                except Exception as exc:
                    a_err.append(exc)

            t_a = threading.Thread(target=_run_disk_a, daemon=True)
            t_a.start()
            raw_b = _run_fio(
                ctx,
                [
                    "--name=disk_b_interf",
                    f"--directory={dir_b}",
                    "--rw=write",
                    "--ioengine=psync",
                    "--bs=256k",
                    "--iodepth=1",
                    "--buffered=1",
                    "--numjobs=4",
                    "--size=512m",
                    f"--ramp_time={cfg['ramp_s']}",
                    f"--runtime={cfg['steady_s']}",
                    "--time_based",
                ],
            )
            t_a.join(timeout=900)
            after = ctx.snapshot_counters(target, handle_b)
            if a_err:
                raise a_err[0]

            parsed_b = _parse_fio_json(raw_b, "write")
            interf_b_runs.append(parsed_b)
            interf_b_bw.append(parsed_b["mib_s"])
            if "parsed" in a_result:
                interf_a_iops.append(a_result["parsed"]["iops"])
            audit = ctx.audit_case(before, after, target=target, cache_state="C0")
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])
            shutil.rmtree(dir_a, ignore_errors=True)
            shutil.rmtree(dir_b, ignore_errors=True)
    finally:
        disk_a_id = handle_a.disk_id if handle_a is not None else None
        disk_b_id = handle_b.disk_id if handle_b is not None else None
        if handle_a is not None:
            handle_a.unmount()
        if handle_b is not None:
            handle_b.unmount()
        _delete_disk_quiet(ctx, disk_a_id)
        _delete_disk_quiet(ctx, disk_b_id)

    clean_lat = _aggregate_op_latency(clean_runs)
    interf_lat = _aggregate_op_latency(interf_b_runs)
    p99_ratio = (
        round(interf_lat["p99_ms"] / clean_lat["p99_ms"], 3)
        if clean_lat.get("p99_ms") and interf_lat.get("p99_ms") and clean_lat["p99_ms"] > 0
        else None
    )
    interf_b_stat = summarize_steady(interf_b_bw)
    status = _combine_validity("PASS" if audit_ok else "INVALID", True, interf_b_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 1 interfering Tier 0",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "disk_b_clean_mib_s": summarize_steady(clean_bw),
            "disk_b_interfered_mib_s": interf_b_stat,
            "disk_a_iops": summarize_steady(interf_a_iops),
            "disk_b_clean_op_latency_ms": clean_lat,
            "disk_b_interfered_op_latency_ms": interf_lat,
            "disk_b_p99_amplification": p99_ratio,
        },
        "validity_status": status,
        "integrity_verified": True,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons) if audit_reasons else f"syncGate cross-disk P99 amplification={p99_ratio}x",
    }


def _case_seq_read_64streams_ra(ctx: BenchContext, cfg: dict[str, Any]) -> dict[str, Any]:
    case_id = "seq-read-64streams-readahead-cap"
    target = "bowl_staged"
    runs_n = cfg["runs_n"]
    rw_bw: list[float] = []
    rw_runs: list[dict[str, Any]] = []
    ro_bw: list[float] = []
    ro_runs: list[dict[str, Any]] = []
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []

    handle: MountHandle | None = None
    ro_handle: MountHandle | None = None
    disk_id: str | None = None
    try:
        handle = ctx.prepare_target(target, stage_size=cfg["stage_size"], disk_name="fio-ra-64streams")
        disk_id = handle.disk_id
        stream_dir = handle.mount_dir / "streams64"
        stream_dir.mkdir(parents=True, exist_ok=True)
        _run_fio(
            ctx,
            [
                "--name=stream64",
                f"--directory={stream_dir}",
                "--rw=write",
                "--ioengine=psync",
                "--bs=1m",
                "--iodepth=1",
                "--numjobs=64",
                "--size=64m",
                "--end_fsync=1",
            ],
            timeout=1200,
        )

        for _run_idx in range(runs_n):
            ctx.drop_caches("C1", target=target, handle=handle)
            before = ctx.snapshot_counters(target, handle)
            raw = _run_fio(
                ctx,
                [
                    "--name=stream64",
                    f"--directory={stream_dir}",
                    "--rw=read",
                    "--ioengine=psync",
                    "--bs=256k",
                    "--iodepth=1",
                    "--numjobs=64",
                    "--size=64m",
                    f"--ramp_time={cfg['ramp_s']}",
                    f"--runtime={cfg['steady_s']}",
                    "--time_based",
                ],
                timeout=900,
            )
            after = ctx.snapshot_counters(target, handle)
            parsed = _parse_fio_json(raw, "read")
            rw_runs.append(parsed)
            rw_bw.append(parsed["mib_s"])
            audit = ctx.audit_case(
                before,
                after,
                target=target,
                cache_state="C1",
                expected_read_bytes=parsed["io_bytes"],
                min_fuse_ratio=0.95,
            )
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])

        disk_id = handle.disk_id
        branch = handle.branch_name or "main"
        handle.unmount()
        handle = None

        if disk_id:
            ro_handle = ctx.prepare_target(target, disk_id=disk_id, branch_name=branch, read_only=True)
            ro_stream_dir = ro_handle.mount_dir / "streams64"
            for _run_idx in range(runs_n):
                ctx.drop_caches("C1", target=target, handle=ro_handle)
                raw_ro = _run_fio(
                    ctx,
                    [
                        "--name=stream64",
                        f"--directory={ro_stream_dir}",
                        "--rw=read",
                        "--ioengine=psync",
                        "--bs=256k",
                        "--iodepth=1",
                        "--numjobs=16",
                        "--size=64m",
                        f"--ramp_time={cfg['ramp_s']}",
                        f"--runtime={cfg['steady_s']}",
                        "--time_based",
                    ],
                    timeout=900,
                )
                parsed_ro = _parse_fio_json(raw_ro, "read")
                ro_runs.append(parsed_ro)
                ro_bw.append(parsed_ro["mib_s"])
    finally:
        if ro_handle is not None:
            ro_handle.unmount()
        if handle is not None:
            handle.unmount()
        _delete_disk_quiet(ctx, disk_id)

    rw_stat = summarize_steady(rw_bw)
    status = _combine_validity("PASS" if audit_ok else "INVALID", True, rw_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Read-Only",
        "cache_state": "C1",
        "runs_n": runs_n,
        "metrics": {
            "rw_64streams_mib_s": rw_stat,
            "rw_64streams_op_latency_ms": _aggregate_op_latency(rw_runs),
            "ro_16streams_mib_s": summarize_steady(ro_bw) if ro_bw else {},
            "ro_16streams_op_latency_ms": _aggregate_op_latency(ro_runs) if ro_runs else {},
            "readahead_hit_pct": last_deltas.get("client_readahead_hit_pct"),
            "readahead_discard_pct": last_deltas.get("client_readahead_discard_pct"),
            "readahead_wait": last_deltas.get("client_readahead_wait"),
        },
        "validity_status": status,
        "integrity_verified": True,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"RA hit={last_deltas.get('client_readahead_hit_pct')}%, discard={last_deltas.get('client_readahead_discard_pct')}%",
    }


def _run_o_append_loop(path: Path, duration_s: float) -> tuple[float, list[float]]:
    payload = b"A" * (64 * 1024)
    lat_ms: list[float] = []
    total_bytes = 0
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o644)
    try:
        t_start = time.perf_counter()
        deadline = t_start + duration_s
        while time.perf_counter() < deadline:
            w0 = time.perf_counter_ns()
            n = os.write(fd, payload)
            lat_ms.append((time.perf_counter_ns() - w0) / 1e6)
            total_bytes += n
        elapsed = max(1e-6, time.perf_counter() - t_start)
    finally:
        os.close(fd)
    return round(total_bytes / (elapsed * 1024.0 * 1024.0), 4), lat_ms


def _case_staged_bypass_unaligned(ctx: BenchContext, cfg: dict[str, Any]) -> dict[str, Any]:
    case_id = "staged-bypass-and-unaligned-write"
    target = "bowl_staged"
    runs_n = cfg["runs_n"]
    osync_bw: list[float] = []
    osync_runs: list[dict[str, Any]] = []
    append_bw: list[float] = []
    append_lats: list[float] = []
    unaligned_bw: list[float] = []
    unaligned_runs: list[dict[str, Any]] = []
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []
    integrity_ok = True

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(target, stage_size=cfg["stage_size"], disk_name="fio-staged-bypass")
        for run_idx in range(runs_n):
            before = ctx.snapshot_counters(target, handle)

            osync_dir = handle.mount_dir / f"osync_{run_idx}"
            osync_dir.mkdir(parents=True, exist_ok=True)
            raw_osync = _run_fio(
                ctx,
                [
                    "--name=osync_bypass",
                    f"--directory={osync_dir}",
                    "--rw=write",
                    "--ioengine=psync",
                    "--bs=64k",
                    "--iodepth=1",
                    "--sync=1",
                    "--numjobs=4",
                    "--size=64m",
                    f"--ramp_time={cfg['ramp_s']}",
                    f"--runtime={cfg['steady_s']}",
                    "--time_based",
                ],
            )
            p_osync = _parse_fio_json(raw_osync, "write")
            osync_runs.append(p_osync)
            osync_bw.append(p_osync["mib_s"])

            app_mib, app_lat = _run_o_append_loop(
                handle.mount_dir / f"oappend_{run_idx}.dat",
                min(float(cfg["steady_s"]), 20.0),
            )
            append_bw.append(app_mib)
            append_lats.extend(app_lat)

            single_file = handle.mount_dir / f"unaligned_single_{run_idx}.dat"
            raw_unaligned = _run_fio(
                ctx,
                [
                    "--name=unaligned_16k",
                    f"--filename={single_file}",
                    "--rw=randwrite",
                    "--ioengine=psync",
                    "--bs=16k",
                    "--blockalign=4k",
                    "--iodepth=1",
                    "--numjobs=1",
                    "--size=256m",
                    f"--ramp_time={cfg['ramp_s']}",
                    f"--runtime={cfg['steady_s']}",
                    "--time_based",
                ],
            )
            p_unal = _parse_fio_json(raw_unaligned, "write")
            unaligned_runs.append(p_unal)
            unaligned_bw.append(p_unal["mib_s"])

            after = ctx.snapshot_counters(target, handle)
            audit = ctx.audit_case(before, after, target=target, cache_state="C0")
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])

            shutil.rmtree(osync_dir, ignore_errors=True)
            single_file.unlink(missing_ok=True)
            (handle.mount_dir / f"oappend_{run_idx}.dat").unlink(missing_ok=True)

        integrity_ok = _run_xxhash_verify_pass(
            ctx,
            handle.mount_dir / "verify_unaligned",
            rw="randwrite",
            bs="16k",
            size_str="32m",
            numjobs=1,
            extra_args=["--blockalign=4k"],
        )
    finally:
        disk_id = handle.disk_id if handle is not None else None
        if handle is not None:
            handle.unmount()
        _delete_disk_quiet(ctx, disk_id)

    unal_stat = summarize_steady(unaligned_bw)
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok, unal_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 1 (O_SYNC) / Tier 0 (unaligned)",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "osync_bypass_mib_s": summarize_steady(osync_bw),
            "osync_op_latency_ms": _aggregate_op_latency(osync_runs),
            "oappend_bypass_mib_s": summarize_steady(append_bw),
            "oappend_op_latency_ms": summarize_latency(append_lats),
            "unaligned_16k_mib_s": unal_stat,
            "unaligned_16k_op_latency_ms": _aggregate_op_latency(unaligned_runs),
            "stage_backpressure_wait": last_deltas.get("client_stage_backpressure_wait"),
        },
        "validity_status": status,
        "integrity_verified": integrity_ok,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"xxhash_verify={'PASS' if integrity_ok else 'FAIL'}",
    }


def _render_summary_markdown(cases: list[dict[str, Any]]) -> str:
    lines = [
        "### `fio-io` Detail Summary",
        "",
        "| Case | Target | Tier / Cache | Primary Throughput or IOPS | Op Latency P50 / P95 / P99 (ms) | Gate |",
        "| :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for c in cases:
        m = c.get("metrics", {})
        primary = "N/A"
        if "effective_throughput_mib_s" in m:
            primary = format_stat(m["effective_throughput_mib_s"], unit="MiB/s (eff)", show_range=True)
        elif "throughput_mib_s" in m:
            primary = format_stat(m["throughput_mib_s"], unit="MiB/s", show_range=True)
        elif "iops" in m:
            primary = format_stat(m["iops"], unit="IOPS", show_range=True)
        elif "disk_b_interfered_mib_s" in m:
            primary = format_stat(m["disk_b_interfered_mib_s"], unit="MiB/s (Disk B)", show_range=True)
        elif "rw_64streams_mib_s" in m:
            primary = format_stat(m["rw_64streams_mib_s"], unit="MiB/s (64s)", show_range=True)
        elif "unaligned_16k_mib_s" in m:
            primary = format_stat(m["unaligned_16k_mib_s"], unit="MiB/s (16k)", show_range=True)

        lat = (
            m.get("op_latency_ms")
            or m.get("disk_b_interfered_op_latency_ms")
            or m.get("rw_64streams_op_latency_ms")
            or m.get("unaligned_16k_op_latency_ms")
            or {}
        )
        lat_str = (
            f"{lat.get('p50_ms') if lat.get('p50_ms') is not None else 'N/A'} / "
            f"{lat.get('p95_ms') if lat.get('p95_ms') is not None else 'N/A'} / "
            f"{lat.get('p99_ms') if lat.get('p99_ms') is not None else 'N/A'}"
        )
        lines.append(
            f"| `{c['case_id']}` | `{c['target']}` | {c['durability_tier']} / {c['cache_state']} | "
            f"{primary} | {lat_str} | `{c['validity_status']}` |"
        )
    return "\n".join(lines) + "\n"


def _format_exc_note(exc: Exception) -> str:
    if isinstance(exc, subprocess.CalledProcessError):
        tail = (exc.stderr or exc.stdout or "").strip().splitlines()
        detail = " | ".join(tail[-5:]) if tail else str(exc)
        return f"CalledProcessError(rc={exc.returncode}): {detail}"[:500]
    return f"{type(exc).__name__}: {exc}"[:500]


def _safe_run_case(
    case_id: str,
    target: str,
    tier: str,
    cache_state: str,
    fn: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    try:
        return fn()
    except Exception as exc:
        return {
            "case_id": case_id,
            "target": target,
            "durability_tier": tier,
            "cache_state": cache_state,
            "runs_n": 0,
            "metrics": {},
            "validity_status": "INVALID",
            "integrity_verified": False,
            "counter_deltas": {},
            "notes": _format_exc_note(exc),
        }


def run_suite(ctx: BenchContext) -> dict[str, Any]:
    cfg = _cfg_params(ctx)
    smoke = _is_smoke(ctx)
    active_targets = [t for t in ctx.targets if t in SUPPORTED_TARGETS]
    cases: list[dict[str, Any]] = []
    fsync1_iops_by_target: dict[str, float] = {}

    for target in active_targets:
        cases.append(
            _safe_run_case(
                "seq-write-buffered-8j",
                target,
                "Tier 0 + Tier 2 drain",
                "C0",
                lambda t=target: _case_seq_write_buffered(ctx, t, cfg),
            )
        )
        cases.append(
            _safe_run_case(
                "seq-write-fdatasync-8j",
                target,
                "Tier 1",
                "C0",
                lambda t=target: _case_seq_write_fdatasync(ctx, t, cfg),
            )
        )
        rand_case = _safe_run_case(
            "rand-write-4k-fsync-8j",
            target,
            "Tier 1",
            "C0",
            lambda t=target: _case_rand_write_4k_fsync(ctx, t, cfg),
        )
        cases.append(rand_case)
        med_iops = rand_case.get("metrics", {}).get("iops", {}).get("median")
        if med_iops:
            fsync1_iops_by_target[target] = float(med_iops)
        cases.append(
            _safe_run_case(
                "seq-read-cold-e2e-16j",
                target,
                "Read-Only",
                "C2",
                lambda t=target: _case_seq_read_cold_e2e(ctx, t, cfg),
            )
        )
        if not smoke:
            cases.append(
                _safe_run_case(
                    "rand-read-4k-direct-32qd",
                    target,
                    "Read-Only",
                    "C3-ColdServer",
                    lambda t=target: _case_rand_read_4k_direct(ctx, t, cfg),
                )
            )

    if not smoke:
        for bowl_target in ("bowl_staged", "bowl_sync"):
            if bowl_target in active_targets:
                cases.append(
                    _safe_run_case(
                        "rand-write-4k-batched-syncwal",
                        bowl_target,
                        "Tier 1 (batched 16)",
                        "C0",
                        lambda bt=bowl_target: _case_rand_write_batched_syncwal(
                            ctx, bt, cfg, fsync1_iops_by_target
                        ),
                    )
                )
        if "bowl_staged" in active_targets:
            cases.append(
                _safe_run_case(
                    "cross-disk-syncgate-interference",
                    "bowl_staged",
                    "Tier 1 interfering Tier 0",
                    "C0",
                    lambda: _case_cross_disk_syncgate(ctx, cfg),
                )
            )
            cases.append(
                _safe_run_case(
                    "seq-read-64streams-readahead-cap",
                    "bowl_staged",
                    "Read-Only",
                    "C1",
                    lambda: _case_seq_read_64streams_ra(ctx, cfg),
                )
            )
            cases.append(
                _safe_run_case(
                    "staged-bypass-and-unaligned-write",
                    "bowl_staged",
                    "Tier 1 (O_SYNC) / Tier 0 (unaligned)",
                    "C0",
                    lambda: _case_staged_bypass_unaligned(ctx, cfg),
                )
            )

    return {
        "suite": SUITE_ID,
        "cases": cases,
        "summary_markdown": _render_summary_markdown(cases),
    }
