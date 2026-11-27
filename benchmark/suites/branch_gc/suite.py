"""branch-gc suite: 5 Bowl-specific Git branch, commit depth, CoW merge, Pack GC, and multi-disk cases."""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
import random
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable

from benchmark.lib.context import BenchContext, MountHandle
from benchmark.lib.stats import (
    format_stat,
    linear_slope,
    summarize_latency,
    summarize_steady,
)

SUITE_ID = "branch-gc"
SUPPORTED_TARGETS = ["bowl_staged", "bowl_sync"]


def _is_smoke(ctx: BenchContext) -> bool:
    return ctx.profile == "smoke-local"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            buf = fh.read(1024 * 1024)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def _write_deterministic_file(path: Path, size_bytes: int, seed: str, *, do_fsync: bool = True) -> str:
    h = hashlib.sha256()
    block = bytearray()
    ctr = 0
    while len(block) < min(size_bytes, 1024 * 1024):
        block.extend(hashlib.sha256(f"{seed}:{ctr}".encode("utf-8")).digest())
        ctr += 1
    chunk = bytes(block)
    remaining = size_bytes
    with open(path, "wb") as fh:
        while remaining > 0:
            part = chunk[:remaining]
            fh.write(part)
            h.update(part)
            remaining -= len(part)
        fh.flush()
        if do_fsync:
            os.fsync(fh.fileno())
    return h.hexdigest()


def _delete_disk_quiet(ctx: BenchContext, disk_id: str | None) -> None:
    if not disk_id:
        return
    ctx.bowl_rpc_cli(["disk", "delete", disk_id, "-y", "--json"], check=False)


def _combine_validity(audit_status: str, integrity_ok: bool, primary_stat: dict[str, Any] | None = None) -> str:
    if not integrity_ok or audit_status == "INVALID":
        return "INVALID"
    if primary_stat and primary_stat.get("high_variance"):
        return "HIGH_VARIANCE"
    return "PASS"


def _case_online_fork_under_active_write(ctx: BenchContext, target: str) -> dict[str, Any]:
    case_id = "online-fork-under-active-write"
    runs_n = 30
    fork_interval_s = 3.0
    orphan_size = 16 * 1024 * 1024

    fork_lats_ms: list[float] = []
    writer_lats_ms: list[float] = []
    created_branches: list[str] = []
    integrity_ok = True
    orphan_bytes_leaked = 0
    audit_ok = True
    audit_reasons: list[str] = []
    last_deltas: dict[str, Any] = {}

    handle: MountHandle | None = None
    disk_id: str | None = None
    orphan_fd: int | None = None
    try:
        handle = ctx.prepare_target(target, disk_name="bgc-online-fork")
        disk_id = handle.disk_id
        assert disk_id is not None

        base_dir = handle.mount_dir / "committed_base"
        base_dir.mkdir(parents=True, exist_ok=True)
        expected_hashes: dict[str, str] = {}
        expected_base_bytes = 0
        for idx in range(8):
            rel = f"committed_base/base_{idx:02d}.bin"
            size = 256 * 1024
            expected_hashes[rel] = _write_deterministic_file(handle.mount_dir / rel, size, f"base-{idx}")
            expected_base_bytes += size

        ctx.bowl_rpc_cli(["commit", "create", "-d", disk_id, "--branch", "main", "-m", "base", "--json"])

        orphan_path = handle.mount_dir / "active_orphan_16m.bin"
        orphan_fd = os.open(orphan_path, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
        chunk_1m = b"O" * (1024 * 1024)
        for _ in range(orphan_size // len(chunk_1m)):
            os.write(orphan_fd, chunk_1m)
        os.fsync(orphan_fd)
        os.unlink(orphan_path)

        active_dir = handle.mount_dir / "active_writers"
        active_dir.mkdir(parents=True, exist_ok=True)
        stop_writers = threading.Event()
        per_thread_lats: list[list[float]] = [[] for _ in range(4)]

        def _writer(tid: int) -> None:
            buf = os.urandom(64 * 1024)
            fpath = active_dir / f"w_{tid}.dat"
            fd = os.open(fpath, os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o644)
            offset = 0
            local = per_thread_lats[tid]
            try:
                while not stop_writers.is_set():
                    t0 = time.perf_counter_ns()
                    os.pwrite(fd, buf, offset)
                    dt_ms = (time.perf_counter_ns() - t0) / 1e6
                    if len(local) < 25000:
                        local.append(dt_ms)
                    offset = (offset + len(buf)) % (4 * 1024 * 1024)
            finally:
                os.close(fd)

        before = ctx.snapshot_counters(target, handle)
        threads = [threading.Thread(target=_writer, args=(i,), daemon=True) for i in range(4)]
        for t in threads:
            t.start()
        try:
            for idx in range(runs_n):
                time.sleep(fork_interval_s)
                br = f"online_fork_{idx:03d}"
                t0 = time.perf_counter_ns()
                ctx.bowl_rpc_cli(
                    ["branch", "create", br, "-d", disk_id, "--from", "main", "-m", f"fork {idx}", "--json"]
                )
                fork_lats_ms.append((time.perf_counter_ns() - t0) / 1e6)
                created_branches.append(br)
        finally:
            stop_writers.set()
            for t in threads:
                t.join(timeout=30)

        after = ctx.snapshot_counters(target, handle)
        audit = ctx.audit_case(before, after, target=target, cache_state="C0")
        last_deltas = audit["deltas"]
        if audit["validity_status"] != "PASS":
            audit_ok = False
            audit_reasons.extend(audit["reasons"])

        for lst in per_thread_lats:
            writer_lats_ms.extend(lst)

        if orphan_fd is not None:
            os.close(orphan_fd)
            orphan_fd = None

        verify_branches = [created_branches[0], created_branches[len(created_branches) // 2], created_branches[-1]]
        for br in verify_branches:
            child_handle: MountHandle | None = None
            try:
                child_handle = ctx.prepare_target(target, disk_id=disk_id, branch_name=br, read_only=True)
                for rel, exp_sha in expected_hashes.items():
                    if not (child_handle.mount_dir / rel).is_file() or _sha256_file(child_handle.mount_dir / rel) != exp_sha:
                        integrity_ok = False
                if (child_handle.mount_dir / "active_orphan_16m.bin").exists():
                    integrity_ok = False
                    orphan_bytes_leaked += orphan_size
                child_disk_info = ctx.bowl_rpc_cli(["disk", "get", disk_id, "--json"])
                _ = child_disk_info
            finally:
                if child_handle is not None:
                    child_handle.unmount()

        for br in created_branches:
            ctx.bowl_rpc_cli(["branch", "delete", br, "-d", disk_id, "-y", "--json"], check=False)
    finally:
        if orphan_fd is not None:
            try:
                os.close(orphan_fd)
            except OSError:
                pass
        if handle is not None:
            handle.unmount()
        _delete_disk_quiet(ctx, disk_id)

    fork_stat = summarize_latency(fork_lats_ms, kind="discrete")
    writer_stat = summarize_latency(writer_lats_ms, kind="discrete")
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok and orphan_bytes_leaked == 0, fork_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 1 (control plane)",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "fork_latency_ms": fork_stat,
            "concurrent_write_latency_ms": writer_stat,
            "orphan_bytes_leaked": orphan_bytes_leaked,
        },
        "validity_status": status,
        "integrity_verified": integrity_ok and orphan_bytes_leaked == 0,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"4 writers + 16MiB orphan FD; child SHA-256={'PASS' if integrity_ok else 'FAIL'}; orphan_leaked={orphan_bytes_leaked}",
    }


def _case_commit_depth_scaling(ctx: BenchContext) -> dict[str, Any]:
    case_id = "commit-depth-scaling-1-to-200"
    target = "bowl_staged"
    smoke = _is_smoke(ctx)
    depths = [1, 16] if smoke else [1, 16, 64, 200]
    runs_n = 5 if smoke else 20

    op_names = ["hot_read", "cold_base_read", "enoent", "readdirplus", "unlink", "statfs", "diff"]
    per_depth: dict[str, dict[str, Any]] = {}
    p50_curves: dict[str, list[float]] = {op: [] for op in op_names}
    audit_ok = True
    audit_reasons: list[str] = []
    last_deltas: dict[str, Any] = {}

    handle: MountHandle | None = None
    disk_id: str | None = None
    try:
        handle = ctx.prepare_target(target, disk_name="bgc-depth-scaling")
        disk_id = handle.disk_id
        assert disk_id is not None

        base_file = handle.mount_dir / "base_cold.bin"
        _write_deterministic_file(base_file, 64 * 1024, "cold-base")

        dir_walk = handle.mount_dir / "walk_dir"
        dir_walk.mkdir(parents=True, exist_ok=True)
        for i in range(64):
            fd = os.open(dir_walk / f"e_{i:03d}.txt", os.O_CREAT | os.O_WRONLY, 0o644)
            os.close(fd)

        unlink_dir = handle.mount_dir / "unlink_pool"
        unlink_dir.mkdir(parents=True, exist_ok=True)
        total_unlinks = len(depths) * runs_n + 4
        for i in range(total_unlinks):
            _write_deterministic_file(unlink_dir / f"u_{i:04d}.dat", 4096, f"u-{i}", do_fsync=False)
        _ = os.statvfs(handle.mount_dir)

        c1_res = ctx.bowl_rpc_cli(["commit", "create", "-d", disk_id, "--branch", "main", "-m", "depth-1", "--json"])
        first_commit_id = int(c1_res["commit_id"]) if isinstance(c1_res, dict) else 1
        current_commit_id = first_commit_id
        current_depth = 1
        unlink_cursor = 0

        layers_dir = handle.mount_dir / "layers"
        layers_dir.mkdir(parents=True, exist_ok=True)

        for target_depth in depths:
            while current_depth < target_depth:
                current_depth += 1
                lf = layers_dir / f"layer_{current_depth:04d}.txt"
                fd = os.open(lf, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
                try:
                    os.write(fd, f"depth={current_depth}\n".encode("utf-8"))
                    os.fsync(fd)
                finally:
                    os.close(fd)
                cres = ctx.bowl_rpc_cli(
                    ["commit", "create", "-d", disk_id, "--branch", "main", "-m", f"d-{current_depth}", "--json"]
                )
                if isinstance(cres, dict) and cres.get("commit_id"):
                    current_commit_id = int(cres["commit_id"])

            hot_file = handle.mount_dir / f"hot_d{target_depth}.bin"
            _write_deterministic_file(hot_file, 64 * 1024, f"hot-{target_depth}")

            samples: dict[str, list[float]] = {op: [] for op in op_names}
            before = ctx.snapshot_counters(target, handle)

            for i in range(runs_n):
                t0 = time.perf_counter_ns()
                with open(hot_file, "rb") as fh:
                    _ = fh.read()
                samples["hot_read"].append((time.perf_counter_ns() - t0) / 1e6)

                ctx.drop_caches("C1", target=target, handle=handle)
                t0 = time.perf_counter_ns()
                with open(base_file, "rb") as fh:
                    _ = fh.read()
                samples["cold_base_read"].append((time.perf_counter_ns() - t0) / 1e6)

                t0 = time.perf_counter_ns()
                try:
                    os.stat(handle.mount_dir / f"no_such_d{target_depth}_{i}")
                except FileNotFoundError:
                    pass
                samples["enoent"].append((time.perf_counter_ns() - t0) / 1e6)

                ctx.drop_caches("C1", target=target, handle=handle)
                t0 = time.perf_counter_ns()
                with os.scandir(dir_walk) as it:
                    for entry in it:
                        _ = entry.stat(follow_symlinks=False)
                samples["readdirplus"].append((time.perf_counter_ns() - t0) / 1e6)

                upath = unlink_dir / f"u_{unlink_cursor:04d}.dat"
                unlink_cursor += 1
                t0 = time.perf_counter_ns()
                os.unlink(upath)
                samples["unlink"].append((time.perf_counter_ns() - t0) / 1e6)

                t0 = time.perf_counter_ns()
                _ = os.statvfs(handle.mount_dir)
                samples["statfs"].append((time.perf_counter_ns() - t0) / 1e6)

                if current_commit_id != first_commit_id:
                    diff_args = ["diff", "--commits", f"{first_commit_id}..{current_commit_id}", "-d", disk_id, "--json"]
                else:
                    diff_args = ["diff", "--work", "--branch", "main", "-d", disk_id, "--json"]
                t0 = time.perf_counter_ns()
                ctx.bowl_rpc_cli(diff_args)
                samples["diff"].append((time.perf_counter_ns() - t0) / 1e6)

            after = ctx.snapshot_counters(target, handle)
            audit = ctx.audit_case(before, after, target=target, cache_state="C1")
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])

            depth_summary: dict[str, Any] = {}
            for op in op_names:
                st = summarize_latency(samples[op], kind="discrete")
                depth_summary[f"{op}_ms"] = st
                p50_curves[op].append(float(st["p50"] or 0.0))
            per_depth[f"D{target_depth}"] = depth_summary
    finally:
        if handle is not None:
            handle.unmount()
        _delete_disk_quiet(ctx, disk_id)

    slopes = {f"{op}_ms_per_layer": linear_slope([float(d) for d in depths], p50_curves[op]) for op in op_names}
    status = "PASS" if audit_ok else "INVALID"
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 1 / C1",
        "cache_state": "C1",
        "runs_n": runs_n,
        "metrics": {
            "depths": depths,
            "per_depth": per_depth,
            "slopes_ms_per_layer": slopes,
        },
        "validity_status": status,
        "integrity_verified": True,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"depths={depths}; cold_read_slope={slopes['cold_base_read_ms_per_layer']}ms/layer; statfs_slope={slopes['statfs_ms_per_layer']}ms/layer",
    }


def _case_fanout_cow_merge(ctx: BenchContext) -> dict[str, Any]:
    case_id = "fanout-16-cow-and-3way-merge"
    target = "bowl_staged"
    smoke = _is_smoke(ctx)
    runs_n = 1 if smoke else 5
    branch_count = 4 if smoke else 16
    parent_file_count = 4 if smoke else 64
    parent_file_mib = 4 if smoke else 16
    cow_mib = 4 if smoke else 64
    stage_size = "64MiB" if smoke else "512MiB"

    ff_merge_ms: list[float] = []
    threeway_merge_ms: list[float] = []
    space_amps: list[float] = []
    merge_ok = True
    sha_ok = True
    audit_ok = True
    audit_reasons: list[str] = []
    last_deltas: dict[str, Any] = {}

    for run_idx in range(runs_n):
        main_handle: MountHandle | None = None
        ro_handle: MountHandle | None = None
        child_handles: list[MountHandle] = []
        disk_id: str | None = None
        try:
            main_handle = ctx.prepare_target(
                target,
                stage_size=stage_size,
                disk_name=f"bgc-fanout-{run_idx}",
                disk_size="16GiB" if smoke else "64GiB",
            )
            disk_id = main_handle.disk_id
            assert disk_id is not None

            expected_hashes: dict[str, str] = {}
            parent_dir = main_handle.mount_dir / "parent"
            parent_dir.mkdir(parents=True, exist_ok=True)
            for idx in range(parent_file_count):
                rel = f"parent/p_{idx:03d}.bin"
                expected_hashes[rel] = _write_deterministic_file(
                    main_handle.mount_dir / rel,
                    parent_file_mib * 1024 * 1024,
                    f"parent-{run_idx}-{idx}",
                )

            main_handle.unmount()
            main_handle = None
            ctx.bowl_rpc_cli(["commit", "create", "-d", disk_id, "--branch", "main", "-m", "parent-base", "--json"])

            branches = [f"cow_b{i:02d}" for i in range(branch_count)]
            for br in branches:
                ctx.bowl_rpc_cli(["branch", "create", br, "-d", disk_id, "--from", "main", "--json"])

            before = ctx.snapshot_counters(target)

            def _mount_and_write_branch(br_idx: int, br_name: str) -> tuple[MountHandle, str, str]:
                bh = ctx.prepare_target(
                    target,
                    disk_id=disk_id,
                    branch_name=br_name,
                    stage_size="32MiB" if smoke else "128MiB",
                )
                cow_dir = bh.mount_dir / "cow"
                cow_dir.mkdir(parents=True, exist_ok=True)
                rel = f"cow/{br_name}.bin"
                digest = _write_deterministic_file(
                    bh.mount_dir / rel,
                    cow_mib * 1024 * 1024,
                    f"cow-{run_idx}-{br_idx}",
                )
                return bh, rel, digest

            with concurrent.futures.ThreadPoolExecutor(max_workers=min(branch_count, 16)) as pool:
                futs = [pool.submit(_mount_and_write_branch, i, br) for i, br in enumerate(branches)]
                for fut in futs:
                    bh, rel, digest = fut.result()
                    child_handles.append(bh)
                    expected_hashes[rel] = digest

            for bh in child_handles:
                bh.unmount()
            child_handles.clear()

            for br in branches:
                ctx.bowl_rpc_cli(["commit", "create", "-d", disk_id, "--branch", br, "-m", f"commit {br}", "--json"])

            t_ff = time.perf_counter_ns()
            ff_res = ctx.bowl_rpc_cli(
                ["branch", "merge", branches[-1], "--into", "main", "-d", disk_id, "--json"]
            )
            ff_merge_ms.append((time.perf_counter_ns() - t_ff) / 1e6)
            if not isinstance(ff_res, dict) or not ff_res.get("fast_forward"):
                merge_ok = False

            for br in branches[:-1]:
                t_m = time.perf_counter_ns()
                m_res = ctx.bowl_rpc_cli(
                    ["branch", "merge", br, "--into", "main", "-d", disk_id, "--json"]
                )
                threeway_merge_ms.append((time.perf_counter_ns() - t_m) / 1e6)
                if not isinstance(m_res, dict) or not m_res.get("merged"):
                    merge_ok = False

            after = ctx.snapshot_counters(target)
            audit = ctx.audit_case(before, after, target=target, cache_state="C0")
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])
            if last_deltas.get("server_space_amplification") is not None:
                space_amps.append(float(last_deltas["server_space_amplification"]))

            ro_handle = ctx.prepare_target(target, disk_id=disk_id, branch_name="main", read_only=True)
            for rel, exp_sha in expected_hashes.items():
                p = ro_handle.mount_dir / rel
                if not p.is_file() or _sha256_file(p) != exp_sha:
                    sha_ok = False
        finally:
            for bh in child_handles:
                bh.unmount()
            if ro_handle is not None:
                ro_handle.unmount()
            if main_handle is not None:
                main_handle.unmount()
            _delete_disk_quiet(ctx, disk_id)

    integrity_ok = merge_ok and sha_ok
    threeway_stat = summarize_steady(threeway_merge_ms)
    ff_stat = summarize_steady(ff_merge_ms)
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok, threeway_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 1 / Tier 2",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "branch_count": branch_count,
            "cow_mib_per_branch": cow_mib,
            "ff_merge_ms": ff_stat,
            "threeway_merge_ms": threeway_stat,
            "space_amplification_ratio": summarize_steady(space_amps) if space_amps else None,
        },
        "validity_status": status,
        "integrity_verified": integrity_ok,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"{branch_count}x{cow_mib}MiB CoW + FF & 3-way merge; merge={'PASS' if merge_ok else 'FAIL'}; SHA-256={'PASS' if sha_ok else 'FAIL'}",
    }


def _run_single_gc_mode(
    ctx: BenchContext,
    *,
    gc_interval: str,
    duration_s: float,
    sample_interval_s: float,
    file_count: int,
    file_size_mib: int,
    stage_size: str,
) -> dict[str, Any]:
    target = "bowl_staged"
    samples: list[dict[str, Any]] = []
    write_lats_ms: list[float] = []
    ctrl_lats_ms: list[float] = []
    audit_ok = True
    audit_reasons: list[str] = []
    deltas: dict[str, Any] = {}

    handle: MountHandle | None = None
    disk_id: str | None = None
    try:
        handle = ctx.prepare_target(
            target,
            gc_interval=gc_interval,
            stage_size=stage_size,
            disk_name=f"bgc-gc-{gc_interval}",
            disk_size="16GiB" if _is_smoke(ctx) else "64GiB",
        )
        disk_id = handle.disk_id
        assert disk_id is not None

        churn_dir = handle.mount_dir / "churn"
        churn_dir.mkdir(parents=True, exist_ok=True)
        payload = os.urandom(file_size_mib * 1024 * 1024)
        for i in range(file_count):
            fpath = churn_dir / f"c_{i:04d}.bin"
            with open(fpath, "wb") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
        ctx.bowl_rpc_cli(["commit", "create", "-d", disk_id, "--branch", "main", "-m", "gc-init", "--json"])

        before = ctx.snapshot_counters(target, handle)
        stop_evt = threading.Event()
        sampler_t = ctx.sample_metrics_background(sample_interval_s, stop_evt, samples, handle=handle)

        stop_workers = threading.Event()
        per_t_lats: list[list[float]] = [[] for _ in range(4)]

        def _churn_writer(tid: int) -> None:
            rng = random.Random(1000 + tid)
            chunk = os.urandom(256 * 1024)
            local = per_t_lats[tid]
            while not stop_workers.is_set():
                idx = rng.randrange(file_count)
                fpath = churn_dir / f"c_{idx:04d}.bin"
                t0 = time.perf_counter_ns()
                try:
                    if rng.random() < 0.25:
                        fpath.unlink(missing_ok=True)
                        with open(fpath, "wb") as fh:
                            fh.write(chunk)
                    else:
                        with open(fpath, "r+b" if fpath.exists() else "wb") as fh:
                            fh.seek(0)
                            fh.write(chunk)
                except OSError:
                    continue
                dt_ms = (time.perf_counter_ns() - t0) / 1e6
                if len(local) < 25000:
                    local.append(dt_ms)

        def _ctrl_loop() -> None:
            step = 0
            while not stop_workers.wait(5.0):
                step += 1
                br = f"gc_tmp_{step}"
                t0 = time.perf_counter_ns()
                ctx.bowl_rpc_cli(
                    ["commit", "create", "-d", disk_id, "--branch", "main", "-m", f"gc-c-{step}", "--json"],
                    check=False,
                )
                ctrl_lats_ms.append((time.perf_counter_ns() - t0) / 1e6)

                t1 = time.perf_counter_ns()
                res = ctx.bowl_rpc_cli(
                    ["branch", "create", br, "-d", disk_id, "--from", "main", "--json"],
                    check=False,
                )
                ctrl_lats_ms.append((time.perf_counter_ns() - t1) / 1e6)
                if res:
                    ctx.bowl_rpc_cli(["branch", "delete", br, "-d", disk_id, "-y", "--json"], check=False)
                ctx.bowl_rpc_cli(["commit", "prune", "-d", disk_id, "-y", "--json"], check=False)

        writers = [threading.Thread(target=_churn_writer, args=(i,), daemon=True) for i in range(4)]
        ctrl_t = threading.Thread(target=_ctrl_loop, daemon=True)
        for t in writers:
            t.start()
        ctrl_t.start()
        try:
            time.sleep(duration_s)
        finally:
            stop_workers.set()
            for t in writers:
                t.join(timeout=30)
            ctrl_t.join(timeout=30)
            stop_evt.set()
            sampler_t.join(timeout=15)

        after = ctx.snapshot_counters(target, handle)
        audit = ctx.audit_case(before, after, target=target, cache_state="C0")
        deltas = audit["deltas"]
        if audit["validity_status"] != "PASS":
            audit_ok = False
            audit_reasons.extend(audit["reasons"])

        for lst in per_t_lats:
            write_lats_ms.extend(lst)
    finally:
        if handle is not None:
            handle.unmount()
        _delete_disk_quiet(ctx, disk_id)

    ratios = [s["space_amp"] for s in samples if s.get("space_amp") is not None]
    return {
        "gc_interval": gc_interval,
        "write_latency_ms": summarize_latency(write_lats_ms, kind="discrete"),
        "control_op_latency_ms": summarize_latency(ctrl_lats_ms, kind="discrete"),
        "max_control_stall_ms": round(max(ctrl_lats_ms), 4) if ctrl_lats_ms else None,
        "initial_space_amp": ratios[0] if ratios else None,
        "peak_space_amp": max(ratios) if ratios else None,
        "final_space_amp": ratios[-1] if ratios else None,
        "curve_samples": samples,
        "audit_ok": audit_ok,
        "audit_reasons": audit_reasons,
        "deltas": deltas,
    }


def _case_online_pack_gc_churn(ctx: BenchContext) -> dict[str, Any]:
    case_id = "online-pack-gc-240s-churn"
    target = "bowl_staged"
    smoke = _is_smoke(ctx)
    duration_s = 35.0 if smoke else 240.0
    sample_interval_s = 5.0 if smoke else 10.0
    gc_on_interval = "10s" if smoke else "30s"
    file_count = 16 if smoke else 256
    file_size_mib = 2 if smoke else 4
    stage_size = "64MiB" if smoke else "512MiB"

    gc_on = _run_single_gc_mode(
        ctx,
        gc_interval=gc_on_interval,
        duration_s=duration_s,
        sample_interval_s=sample_interval_s,
        file_count=file_count,
        file_size_mib=file_size_mib,
        stage_size=stage_size,
    )
    gc_off = _run_single_gc_mode(
        ctx,
        gc_interval="0",
        duration_s=duration_s,
        sample_interval_s=sample_interval_s,
        file_count=file_count,
        file_size_mib=file_size_mib,
        stage_size=stage_size,
    )

    audit_ok = gc_on["audit_ok"] and gc_off["audit_ok"]
    reasons = gc_on["audit_reasons"] + gc_off["audit_reasons"]
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 0 / Tier 1",
        "cache_state": "C0",
        "runs_n": 1,
        "metrics": {
            "duration_s": duration_s,
            "gc_enabled": {k: v for k, v in gc_on.items() if k not in ("audit_ok", "audit_reasons", "deltas")},
            "gc_disabled": {k: v for k, v in gc_off.items() if k not in ("audit_ok", "audit_reasons", "deltas")},
        },
        "validity_status": "PASS" if audit_ok else "INVALID",
        "integrity_verified": audit_ok,
        "counter_deltas": {"gc_enabled": gc_on["deltas"], "gc_disabled": gc_off["deltas"]},
        "notes": "; ".join(reasons)
        if reasons
        else f"gc={gc_on_interval} peak={gc_on['peak_space_amp']}->final={gc_on['final_space_amp']} vs gc=0 final={gc_off['final_space_amp']}; max_stall={gc_on['max_control_stall_ms']}ms",
    }


def _case_multi_disk_pebble_scaling(ctx: BenchContext) -> dict[str, Any]:
    case_id = "multi-disk-shared-pebble-scaling"
    target = "bowl_staged"
    disk_counts = [1, 8, 32]
    runs_n = 3
    ramp_s = 10
    steady_s = 60

    per_scale: dict[str, Any] = {}
    base_med_mib_s: float | None = None
    audit_ok = True
    audit_reasons: list[str] = []
    last_deltas: dict[str, Any] = {}

    for k in disk_counts:
        handles: list[MountHandle] = []
        disk_ids: list[str] = []
        agg_mibs: list[float] = []
        try:
            for i in range(k):
                h = ctx.prepare_target(
                    target,
                    stage_size="64MiB",
                    disk_name=f"bgc-scale-{k}-{i}",
                    disk_size="4GiB",
                )
                handles.append(h)
                if h.disk_id:
                    disk_ids.append(h.disk_id)

            for run_idx in range(runs_n):
                before = ctx.snapshot_counters(target, handles[0])
                fio_cmd = [
                    "fio",
                    "--output-format=json",
                    "--group_reporting",
                    "--ioengine=psync",
                    "--rw=randrw",
                    "--rwmixread=50",
                    "--bs=64k",
                    "--iodepth=1",
                    "--size=64m",
                    f"--ramp_time={ramp_s}",
                    f"--runtime={steady_s}",
                    "--time_based",
                ]
                for i, h in enumerate(handles):
                    d = h.mount_dir / f"r{run_idx}"
                    d.mkdir(parents=True, exist_ok=True)
                    fio_cmd.extend([f"--name=disk_{i}", f"--directory={d}"])

                proc = ctx.run_cmd(fio_cmd, timeout=900, check=True)
                after = ctx.snapshot_counters(target, handles[0])
                start = proc.stdout.find("{")
                end = proc.stdout.rfind("}")
                data = json.loads(proc.stdout[start : end + 1])
                total_bw = 0.0
                for job in data.get("jobs", []):
                    total_bw += float(job.get("read", {}).get("bw_bytes", 0.0))
                    total_bw += float(job.get("write", {}).get("bw_bytes", 0.0))
                agg_mibs.append(round(total_bw / (1024.0 * 1024.0), 4))

                audit = ctx.audit_case(before, after, target=target, cache_state="C0")
                last_deltas = audit["deltas"]
                if audit["validity_status"] != "PASS":
                    audit_ok = False
                    audit_reasons.extend(audit["reasons"])
        finally:
            for h in handles:
                h.unmount()
            for did in disk_ids:
                _delete_disk_quiet(ctx, did)

        stat = summarize_steady(agg_mibs)
        if k == 1 and stat.get("median"):
            base_med_mib_s = float(stat["median"])
        eff_pct = (
            round((float(stat["median"]) / (k * base_med_mib_s)) * 100.0, 2)
            if base_med_mib_s and stat.get("median") and base_med_mib_s > 0
            else None
        )
        per_scale[f"disks_{k}"] = {
            "disk_count": k,
            "aggregate_mib_s": stat,
            "scaling_efficiency_pct": eff_pct,
        }

    top_stat = per_scale[f"disks_{disk_counts[-1]}"]["aggregate_mib_s"]
    status = _combine_validity("PASS" if audit_ok else "INVALID", True, top_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 0",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "disk_counts": disk_counts,
            "per_scale": per_scale,
        },
        "validity_status": status,
        "integrity_verified": True,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"32-disk efficiency={per_scale['disks_32']['scaling_efficiency_pct']}%",
    }


def _render_summary_markdown(cases: list[dict[str, Any]]) -> str:
    lines = [
        "### `branch-gc` Detail Summary",
        "",
        "| Case | Target | Runs | Primary Metric | Notes | Gate |",
        "| :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for c in cases:
        m = c.get("metrics", {})
        primary = "N/A"
        if "fork_latency_ms" in m:
            fl = m["fork_latency_ms"]
            primary = f"Fork P50={fl.get('p50')} / P95={fl.get('p95')} / Max={fl.get('max')} ms"
        elif "slopes_ms_per_layer" in m:
            sl = m["slopes_ms_per_layer"]
            primary = f"cold_read {sl.get('cold_base_read_ms_per_layer')} ms/layer, statfs {sl.get('statfs_ms_per_layer')} ms/layer"
        elif "threeway_merge_ms" in m:
            primary = f"3-way merge {format_stat(m['threeway_merge_ms'], unit='ms')}"
        elif "gc_enabled" in m:
            ge = m["gc_enabled"]
            primary = f"GC on final={ge.get('final_space_amp')}x, write P99={ge.get('write_latency_ms', {}).get('p99')}ms"
        elif "per_scale" in m:
            ps = m["per_scale"]
            last_k = f"disks_{m['disk_counts'][-1]}"
            primary = f"{last_k}: {format_stat(ps[last_k]['aggregate_mib_s'], unit='MiB/s')} ({ps[last_k]['scaling_efficiency_pct']}%)"
        lines.append(
            f"| `{c['case_id']}` | `{c['target']}` | {c['runs_n']} | {primary} | {c['notes']} | `{c['validity_status']}` |"
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
    smoke = _is_smoke(ctx)
    active_targets = [t for t in ctx.targets if t in SUPPORTED_TARGETS]
    cases: list[dict[str, Any]] = []

    if not active_targets:
        return {"suite": SUITE_ID, "cases": [], "summary_markdown": _render_summary_markdown([])}

    if smoke:
        if "bowl_staged" in active_targets:
            cases.append(
                _safe_run_case(
                    "commit-depth-scaling-1-to-200",
                    "bowl_staged",
                    "Tier 1 / C1",
                    "C1",
                    lambda: _case_commit_depth_scaling(ctx),
                )
            )
            cases.append(
                _safe_run_case(
                    "fanout-16-cow-and-3way-merge",
                    "bowl_staged",
                    "Tier 1 / Tier 2",
                    "C0",
                    lambda: _case_fanout_cow_merge(ctx),
                )
            )
            cases.append(
                _safe_run_case(
                    "online-pack-gc-240s-churn",
                    "bowl_staged",
                    "Tier 0 / Tier 1",
                    "C0",
                    lambda: _case_online_pack_gc_churn(ctx),
                )
            )
    else:
        for bowl_target in ("bowl_staged", "bowl_sync"):
            if bowl_target in active_targets:
                cases.append(
                    _safe_run_case(
                        "online-fork-under-active-write",
                        bowl_target,
                        "Tier 1 (control plane)",
                        "C0",
                        lambda bt=bowl_target: _case_online_fork_under_active_write(ctx, bt),
                    )
                )
        if "bowl_staged" in active_targets:
            cases.append(
                _safe_run_case(
                    "commit-depth-scaling-1-to-200",
                    "bowl_staged",
                    "Tier 1 / C1",
                    "C1",
                    lambda: _case_commit_depth_scaling(ctx),
                )
            )
            cases.append(
                _safe_run_case(
                    "fanout-16-cow-and-3way-merge",
                    "bowl_staged",
                    "Tier 1 / Tier 2",
                    "C0",
                    lambda: _case_fanout_cow_merge(ctx),
                )
            )
            cases.append(
                _safe_run_case(
                    "online-pack-gc-240s-churn",
                    "bowl_staged",
                    "Tier 0 / Tier 1",
                    "C0",
                    lambda: _case_online_pack_gc_churn(ctx),
                )
            )
            cases.append(
                _safe_run_case(
                    "multi-disk-shared-pebble-scaling",
                    "bowl_staged",
                    "Tier 0",
                    "C0",
                    lambda: _case_multi_disk_pebble_scaling(ctx),
                )
            )

    return {
        "suite": SUITE_ID,
        "cases": cases,
        "summary_markdown": _render_summary_markdown(cases),
    }
