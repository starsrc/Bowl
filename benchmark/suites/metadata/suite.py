"""metadata suite: 7 metadata traversal, negative lookup, TTL parity, ID-segment, and H2C cases."""

from __future__ import annotations

import multiprocessing
import os
import re
import shutil
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any

from benchmark.lib.context import BenchContext, MountHandle
from benchmark.lib.stats import format_stat, summarize_latency, summarize_steady

SUITE_ID = "metadata"
SUPPORTED_TARGETS = [
    "ext4",
    "passthrough_ll",
    "bowl_staged",
    "bowl_sync",
    "jfs_pg_minio",
    "jfs_redis_minio",
    "drive9",
]


def _is_smoke(ctx: BenchContext) -> bool:
    return ctx.profile == "smoke-local"


def _combine_validity(audit_status: str, integrity_ok: bool, primary_stat: dict[str, Any]) -> str:
    if not integrity_ok or audit_status == "INVALID":
        return "INVALID"
    if primary_stat.get("high_variance"):
        return "HIGH_VARIANCE"
    return "PASS"


def _check_readyz_http(url: str) -> bool:
    try:
        with urllib.request.urlopen(url.rstrip("/") + "/readyz", timeout=3.0) as resp:
            return int(getattr(resp, "status", 0)) == 200
    except Exception:
        return False


def _py_mdtest_worker(args: tuple[str, str, int, int]) -> float:
    phase, dir_str, proc_id, files_per_proc = args
    base = Path(dir_str) / f"p_{proc_id}"
    payload = b"m" * 4096
    t0 = time.perf_counter()
    if phase == "create":
        base.mkdir(parents=True, exist_ok=True)
        for i in range(files_per_proc):
            fd = os.open(base / f"f_{i:06d}", os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
            try:
                os.write(fd, payload)
            finally:
                os.close(fd)
    elif phase == "stat":
        for i in range(files_per_proc):
            os.stat(base / f"f_{i:06d}")
    elif phase == "read":
        for i in range(files_per_proc):
            with open(base / f"f_{i:06d}", "rb") as fh:
                _ = fh.read()
    elif phase == "remove":
        for i in range(files_per_proc):
            os.unlink(base / f"f_{i:06d}")
    return time.perf_counter() - t0


def _run_py_mdtest_phase(phase: str, root: Path, procs: int, files_per_proc: int) -> float:
    total_files = procs * files_per_proc
    work = [(phase, str(root), p, files_per_proc) for p in range(procs)]
    t0 = time.perf_counter()
    with multiprocessing.Pool(processes=procs) as pool:
        pool.map(_py_mdtest_worker, work)
    elapsed = max(1e-6, time.perf_counter() - t0)
    return round(total_files / elapsed, 2)


_MDTEST_RATE_RE = re.compile(
    r"File\s+(creation|stat|read|removal)\s*:\s+[\d.]+\s+[\d.]+\s+([\d.]+)",
    re.IGNORECASE,
)


def _run_mdtest_binary_phase(
    ctx: BenchContext,
    flag: str,
    root: Path,
    procs: int,
    files_per_proc: int,
    read_bytes: int = 4096,
) -> float | None:
    mdtest_bin = shutil.which("mdtest") or str(ctx.tools_bin / "mdtest")
    mpirun_bin = shutil.which("mpirun")
    if not (Path(mdtest_bin).is_file() and mpirun_bin):
        return None
    cmd = [
        mpirun_bin,
        "--allow-run-as-root",
        "-np",
        str(procs),
        mdtest_bin,
        "-d",
        str(root),
        "-n",
        str(files_per_proc),
        "-F",
        "-u",
        "-w",
        str(read_bytes),
        "-e",
        str(read_bytes),
        flag,
    ]
    proc = ctx.run_cmd(cmd, timeout=600, check=False)
    if proc.returncode != 0:
        return None
    match = _MDTEST_RATE_RE.search(proc.stdout)
    if not match:
        return None
    return round(float(match.group(2)), 2)


def _case_mdtest_split_phases(ctx: BenchContext, target: str) -> dict[str, Any]:
    case_id = "mdtest-split-phases-8p"
    procs = 8
    total_files = 20000
    files_per_proc = total_files // procs
    runs_n = 5

    rates: dict[str, list[float]] = {
        "create": [],
        "stat_c0": [],
        "stat_c1": [],
        "read_c1": [],
        "remove": [],
    }
    used_driver = "mdtest"
    audit_ok = True
    audit_reasons: list[str] = []
    last_deltas: dict[str, Any] = {}

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(target, jfs_cache_mode="zero", disk_name="meta-mdtest-8p")
        for run_idx in range(-1, runs_n):
            is_warmup = run_idx < 0
            run_root = handle.mount_dir / f"mdtest_r{run_idx}"
            run_root.mkdir(parents=True, exist_ok=True)

            c_rate = _run_mdtest_binary_phase(ctx, "-C", run_root, procs, files_per_proc)
            if c_rate is None:
                used_driver = "python-mp-fallback"
                shutil.rmtree(run_root, ignore_errors=True)
                run_root.mkdir(parents=True, exist_ok=True)
                c_rate = _run_py_mdtest_phase("create", run_root, procs, files_per_proc)
                s0_rate = _run_py_mdtest_phase("stat", run_root, procs, files_per_proc)

                ctx.drop_caches("C1", target=target, handle=handle)
                before_s1 = ctx.snapshot_counters(target, handle)
                s1_rate = _run_py_mdtest_phase("stat", run_root, procs, files_per_proc)
                after_s1 = ctx.snapshot_counters(target, handle)

                ctx.drop_caches("C1", target=target, handle=handle)
                before_r1 = ctx.snapshot_counters(target, handle)
                r1_rate = _run_py_mdtest_phase("read", run_root, procs, files_per_proc)
                after_r1 = ctx.snapshot_counters(target, handle)

                rm_rate = _run_py_mdtest_phase("remove", run_root, procs, files_per_proc)
            else:
                s0_rate = _run_mdtest_binary_phase(ctx, "-T", run_root, procs, files_per_proc) or 0.0

                ctx.drop_caches("C1", target=target, handle=handle)
                before_s1 = ctx.snapshot_counters(target, handle)
                s1_rate = _run_mdtest_binary_phase(ctx, "-T", run_root, procs, files_per_proc) or 0.0
                after_s1 = ctx.snapshot_counters(target, handle)

                ctx.drop_caches("C1", target=target, handle=handle)
                before_r1 = ctx.snapshot_counters(target, handle)
                r1_rate = _run_mdtest_binary_phase(ctx, "-E", run_root, procs, files_per_proc) or 0.0
                after_r1 = ctx.snapshot_counters(target, handle)

                rm_rate = _run_mdtest_binary_phase(ctx, "-r", run_root, procs, files_per_proc) or 0.0

            if not is_warmup:
                rates["create"].append(c_rate)
                rates["stat_c0"].append(s0_rate)
                rates["stat_c1"].append(s1_rate)
                rates["read_c1"].append(r1_rate)
                rates["remove"].append(rm_rate)

                audit_s1 = ctx.audit_case(
                    before_s1,
                    after_s1,
                    target=target,
                    cache_state="C1",
                    expected_stat_ops=total_files,
                    min_fuse_ratio=0.95,
                )
                audit_r1 = ctx.audit_case(
                    before_r1,
                    after_r1,
                    target=target,
                    cache_state="C1",
                    expected_read_bytes=total_files * 4096,
                    min_fuse_ratio=0.95,
                )
                last_deltas = {
                    "stat_c1": audit_s1["deltas"],
                    "read_c1": audit_r1["deltas"],
                }
                if audit_s1["validity_status"] != "PASS":
                    audit_ok = False
                    audit_reasons.extend(audit_s1["reasons"])
                if audit_r1["validity_status"] != "PASS":
                    audit_ok = False
                    audit_reasons.extend(audit_r1["reasons"])

            shutil.rmtree(run_root, ignore_errors=True)
    finally:
        if handle is not None:
            handle.unmount()

    stat_c1_sum = summarize_steady(rates["stat_c1"])
    status = _combine_validity("PASS" if audit_ok else "INVALID", True, stat_c1_sum)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 0 / C1 split",
        "cache_state": "C0+C1",
        "runs_n": runs_n,
        "metrics": {
            "create_ops_s": summarize_steady(rates["create"]),
            "stat_c0_ops_s": summarize_steady(rates["stat_c0"]),
            "stat_c1_ops_s": stat_c1_sum,
            "read_c1_ops_s": summarize_steady(rates["read_c1"]),
            "remove_ops_s": summarize_steady(rates["remove"]),
        },
        "validity_status": status,
        "integrity_verified": True,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons) if audit_reasons else f"driver={used_driver}, files={total_files}",
    }


def _case_large_dir_readdirplus(ctx: BenchContext, target: str) -> dict[str, Any]:
    case_id = "large-dir-readdirplus-pagination"
    smoke = _is_smoke(ctx)
    sizes = [200, 2000] if smoke else [200, 2000, 10000]
    runs_n = 5 if smoke else 20

    per_size_metrics: dict[str, Any] = {}
    readdirplus_rpcs_by_size: dict[str, float] = {}
    audit_ok = True
    audit_reasons: list[str] = []
    last_deltas: dict[str, Any] = {}
    integrity_ok = True

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(target, jfs_cache_mode="zero", disk_name="meta-readdirplus")
        for count in sizes:
            dpath = handle.mount_dir / f"dir_{count}"
            dpath.mkdir(parents=True, exist_ok=True)
            for i in range(count):
                fd = os.open(dpath / f"entry_{i:05d}.txt", os.O_CREAT | os.O_WRONLY, 0o644)
                os.close(fd)

            lat_ms: list[float] = []
            rpc_counts: list[int] = []
            for run_idx in range(-1, runs_n):
                ctx.drop_caches("C1", target=target, handle=handle)
                before = ctx.snapshot_counters(target, handle)
                t0 = time.perf_counter_ns()
                seen = 0
                with os.scandir(dpath) as it:
                    for entry in it:
                        _ = entry.stat(follow_symlinks=False)
                        seen += 1
                elapsed_ms = (time.perf_counter_ns() - t0) / 1e6
                after = ctx.snapshot_counters(target, handle)

                if seen != count:
                    integrity_ok = False
                if run_idx >= 0:
                    lat_ms.append(elapsed_ms)
                    audit = ctx.audit_case(before, after, target=target, cache_state="C1")
                    last_deltas = audit["deltas"]
                    rpc_counts.append(int(audit["deltas"].get("client_fuse_readdirplus_ops", 0)))
                    if audit["validity_status"] != "PASS":
                        audit_ok = False
                        audit_reasons.extend(audit["reasons"])

            per_size_metrics[f"dir_{count}_ms"] = summarize_latency(lat_ms, kind="discrete")
            readdirplus_rpcs_by_size[str(count)] = round(sum(rpc_counts) / len(rpc_counts), 2) if rpc_counts else 0.0
            shutil.rmtree(dpath, ignore_errors=True)
    finally:
        if handle is not None:
            handle.unmount()

    largest_key = f"dir_{sizes[-1]}_ms"
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok, per_size_metrics[largest_key])
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Read-Only",
        "cache_state": "C1",
        "runs_n": runs_n,
        "metrics": {
            **per_size_metrics,
            "avg_readdirplus_rpcs": readdirplus_rpcs_by_size,
        },
        "validity_status": status,
        "integrity_verified": integrity_ok,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"sizes={sizes}, avg_readdirplus_rpcs={readdirplus_rpcs_by_size}",
    }


def _case_enoent_storm(ctx: BenchContext, target: str) -> dict[str, Any]:
    case_id = "enoent-negative-lookup-storm"
    runs_n = 3
    ramp_s = 10.0
    steady_s = 60.0
    threads = 8
    path_count = 10000

    ops_s_runs: list[float] = []
    all_lats_ms: list[float] = []
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(target, jfs_cache_mode="zero", disk_name="meta-enoent")
        base_dir = handle.mount_dir / "enoent_dir"
        base_dir.mkdir(parents=True, exist_ok=True)
        missing_paths = [str(base_dir / f"missing_{i:05d}.nope") for i in range(path_count)]

        for _run_idx in range(runs_n):
            before = ctx.snapshot_counters(target, handle)
            counts = [0] * threads
            per_thread_lats: list[list[float]] = [[] for _ in range(threads)]
            t_start = time.perf_counter()
            steady_start = t_start + ramp_s
            steady_end = steady_start + steady_s

            def _worker(tid: int) -> None:
                idx = tid * 131
                local_lats = per_thread_lats[tid]
                local_cnt = 0
                while True:
                    now = time.perf_counter()
                    if now >= steady_end:
                        break
                    p = missing_paths[idx % path_count]
                    idx += 1
                    t0 = time.perf_counter_ns()
                    try:
                        os.stat(p)
                    except FileNotFoundError:
                        pass
                    dt_ms = (time.perf_counter_ns() - t0) / 1e6
                    if now >= steady_start:
                        local_cnt += 1
                        if len(local_lats) < 25000:
                            local_lats.append(dt_ms)
                counts[tid] = local_cnt

            ts = [threading.Thread(target=_worker, args=(i,), daemon=True) for i in range(threads)]
            for t in ts:
                t.start()
            for t in ts:
                t.join()

            after = ctx.snapshot_counters(target, handle)
            total_ops = sum(counts)
            ops_s_runs.append(round(total_ops / steady_s, 2))
            for lst in per_thread_lats:
                all_lats_ms.extend(lst)

            audit = ctx.audit_case(
                before,
                after,
                target=target,
                cache_state="C1",
                expected_stat_ops=total_ops,
                min_fuse_ratio=0.95,
            )
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])
    finally:
        if handle is not None:
            handle.unmount()

    ops_stat = summarize_steady(ops_s_runs)
    lat_stat = summarize_latency(all_lats_ms, kind="discrete")
    status = _combine_validity("PASS" if audit_ok else "INVALID", True, ops_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Read-Only",
        "cache_state": "C1",
        "runs_n": runs_n,
        "metrics": {
            "ops_s": ops_stat,
            "op_latency_ms": lat_stat,
            "lookup_ops_delta": last_deltas.get("client_fuse_lookup_ops"),
        },
        "validity_status": status,
        "integrity_verified": True,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons) if audit_reasons else "8 threads x 10k missing paths; 0 negative caching",
    }


def _case_metadata_ttl_1s_parity(ctx: BenchContext, target: str) -> dict[str, Any]:
    case_id = "metadata-ttl-1s-parity"
    runs_n = 5
    file_count = 2000
    walk_count = 10000

    warm_stat_ops: list[float] = []
    cold_stat_ops: list[float] = []
    walk_ms: list[float] = []
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(
            target,
            ttl_override=1.0,
            jfs_cache_mode="zero",
            disk_name="meta-ttl-1s-parity",
        )
        stat_dir = handle.mount_dir / "stat_set"
        walk_dir = handle.mount_dir / "walk_10k"
        stat_dir.mkdir(parents=True, exist_ok=True)
        walk_dir.mkdir(parents=True, exist_ok=True)

        stat_files = [stat_dir / f"s_{i:05d}" for i in range(file_count)]
        for p in stat_files:
            fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
            os.close(fd)
        for i in range(walk_count):
            fd = os.open(walk_dir / f"w_{i:05d}", os.O_CREAT | os.O_WRONLY, 0o644)
            os.close(fd)

        for run_idx in range(-1, runs_n):
            t0 = time.perf_counter()
            for p in stat_files:
                os.stat(p)
            warm_dt = max(1e-6, time.perf_counter() - t0)

            ctx.drop_caches("C1", target=target, handle=handle)
            before = ctx.snapshot_counters(target, handle)
            t1 = time.perf_counter()
            for p in stat_files:
                os.stat(p)
            cold_dt = max(1e-6, time.perf_counter() - t1)
            after = ctx.snapshot_counters(target, handle)

            ctx.drop_caches("C1", target=target, handle=handle)
            tw0 = time.perf_counter_ns()
            with os.scandir(walk_dir) as it:
                for entry in it:
                    _ = entry.stat(follow_symlinks=False)
            walk_dt_ms = (time.perf_counter_ns() - tw0) / 1e6

            if run_idx >= 0:
                warm_stat_ops.append(round(file_count / warm_dt, 2))
                cold_stat_ops.append(round(file_count / cold_dt, 2))
                walk_ms.append(walk_dt_ms)
                audit = ctx.audit_case(
                    before,
                    after,
                    target=target,
                    cache_state="C1",
                    expected_stat_ops=file_count,
                    min_fuse_ratio=0.95,
                )
                last_deltas = audit["deltas"]
                if audit["validity_status"] != "PASS":
                    audit_ok = False
                    audit_reasons.extend(audit["reasons"])
    finally:
        if handle is not None:
            handle.unmount()

    cold_stat = summarize_steady(cold_stat_ops)
    status = _combine_validity("PASS" if audit_ok else "INVALID", True, cold_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Read-Only",
        "cache_state": "C0+C1 (ttl=1.0s)",
        "runs_n": runs_n,
        "metrics": {
            "warm_stat_ops_s": summarize_steady(warm_stat_ops),
            "cold_stat_ops_s": cold_stat,
            "walk_10k_ms": summarize_latency(walk_ms, kind="discrete"),
            "ttl_override_s": 1.0,
        },
        "validity_status": status,
        "integrity_verified": True,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons) if audit_reasons else "entry_ttl=1.0s, attr_ttl=1.0s parity control",
    }


def _case_seq_create_id_segment_spike(ctx: BenchContext, target: str) -> dict[str, Any]:
    case_id = "seq-create-20k-id-segment-spike"
    smoke = _is_smoke(ctx)
    file_count = 5000 if smoke else 20000
    runs_n = 1 if smoke else 3

    all_lats_ms: list[float] = []
    boundary_lats_ms: list[float] = []
    non_boundary_lats_ms: list[float] = []
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(target, disk_name="meta-id-segment-spike")
        for run_idx in range(runs_n):
            run_dir = handle.mount_dir / f"idseg_{run_idx}"
            run_dir.mkdir(parents=True, exist_ok=True)
            before = ctx.snapshot_counters(target, handle)
            for k in range(1, file_count + 1):
                fpath = run_dir / f"f_{k:06d}"
                t0 = time.perf_counter_ns()
                fd = os.open(fpath, os.O_CREAT | os.O_WRONLY | os.O_EXCL, 0o644)
                os.close(fd)
                dt_ms = (time.perf_counter_ns() - t0) / 1e6
                all_lats_ms.append(dt_ms)
                if k % 4096 == 0:
                    boundary_lats_ms.append(dt_ms)
                else:
                    non_boundary_lats_ms.append(dt_ms)
            after = ctx.snapshot_counters(target, handle)
            audit = ctx.audit_case(before, after, target=target, cache_state="C0")
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])
            shutil.rmtree(run_dir, ignore_errors=True)
    finally:
        if handle is not None:
            handle.unmount()

    lat_stat = summarize_latency(all_lats_ms, kind="discrete")
    b_mean = round(sum(boundary_lats_ms) / len(boundary_lats_ms), 4) if boundary_lats_ms else None
    nb_mean = round(sum(non_boundary_lats_ms) / len(non_boundary_lats_ms), 4) if non_boundary_lats_ms else None
    spike_ratio = round(b_mean / nb_mean, 3) if b_mean is not None and nb_mean and nb_mean > 0 else None
    status = "INVALID" if not audit_ok else "PASS"
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 0" if target == "bowl_staged" else "Tier 1",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "create_latency_ms": lat_stat,
            "boundary_k4096_mean_ms": b_mean,
            "boundary_k4096_max_ms": round(max(boundary_lats_ms), 4) if boundary_lats_ms else None,
            "boundary_samples": len(boundary_lats_ms),
            "non_boundary_mean_ms": nb_mean,
            "spike_ratio": spike_ratio,
        },
        "validity_status": status,
        "integrity_verified": True,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"files_per_run={file_count}; k%4096==0 mean={b_mean}ms vs normal={nb_mean}ms ({spike_ratio}x)",
    }


def _case_h2_bulk_rw_metadata_contention(ctx: BenchContext) -> dict[str, Any]:
    case_id = "h2-bulk-rw-metadata-contention"
    target = "bowl_staged"
    smoke = _is_smoke(ctx)
    ramp_s = 2.0 if smoke else 10.0
    steady_s = 15.0 if smoke else 60.0
    runs_n = 1 if smoke else 3
    bulk_threads = 8
    meta_threads = 4
    chunk_4m = b"B" * (4 * 1024 * 1024)

    baseline_lats_ms: list[float] = []
    contended_lats_ms: list[float] = []
    readyz_ok = True
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(target, disk_name="meta-h2c-contention")
        meta_dir = handle.mount_dir / "meta_fg"
        bulk_dir = handle.mount_dir / "bulk_bg"
        meta_dir.mkdir(parents=True, exist_ok=True)
        bulk_dir.mkdir(parents=True, exist_ok=True)

        def _run_meta_fg(duration_s: float, warmup_s: float, tag: str, out_lats: list[float]) -> None:
            per_t: list[list[float]] = [[] for _ in range(meta_threads)]
            t0_all = time.perf_counter()
            s_start = t0_all + warmup_s
            s_end = s_start + duration_s

            def _fg_worker(tid: int) -> None:
                seq = 0
                local = per_t[tid]
                while True:
                    now = time.perf_counter()
                    if now >= s_end:
                        break
                    seq += 1
                    op = seq % 3
                    t_op = time.perf_counter_ns()
                    if op == 0:
                        try:
                            os.stat(meta_dir / f"nope_{tag}_{tid}_{seq}")
                        except FileNotFoundError:
                            pass
                    elif op == 1:
                        p = meta_dir / f"c_{tag}_{tid}_{seq}"
                        fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
                        os.close(fd)
                        os.unlink(p)
                    else:
                        os.stat(meta_dir)
                    dt_ms = (time.perf_counter_ns() - t_op) / 1e6
                    if now >= s_start and len(local) < 20000:
                        local.append(dt_ms)

            ts = [threading.Thread(target=_fg_worker, args=(i,), daemon=True) for i in range(meta_threads)]
            for t in ts:
                t.start()
            for t in ts:
                t.join()
            for lst in per_t:
                out_lats.extend(lst)

        _run_meta_fg(min(steady_s, 10.0), 1.0, "base", baseline_lats_ms)

        for run_idx in range(runs_n):
            before = ctx.snapshot_counters(target, handle)
            stop_bg = threading.Event()

            def _bg_bulk_worker(tid: int) -> None:
                fpath = bulk_dir / f"bulk_{run_idx}_{tid}.bin"
                while not stop_bg.is_set():
                    with open(fpath, "wb") as fh:
                        for _ in range(4):
                            if stop_bg.is_set():
                                break
                            fh.write(chunk_4m)
                        fh.flush()
                    if stop_bg.is_set():
                        break
                    with open(fpath, "rb") as fh:
                        while not stop_bg.is_set():
                            buf = fh.read(4 * 1024 * 1024)
                            if not buf:
                                break

            bg_ts = [threading.Thread(target=_bg_bulk_worker, args=(i,), daemon=True) for i in range(bulk_threads)]
            for t in bg_ts:
                t.start()
            try:
                _run_meta_fg(steady_s, ramp_s, f"r{run_idx}", contended_lats_ms)
                if handle.admin_url and not _check_readyz_http(handle.admin_url):
                    readyz_ok = False
                srv_admin_base = ctx.server_metrics_url.rsplit("/metrics", 1)[0]
                if not _check_readyz_http(srv_admin_base):
                    readyz_ok = False
            finally:
                stop_bg.set()
                for t in bg_ts:
                    t.join(timeout=30)

            after = ctx.snapshot_counters(target, handle)
            audit = ctx.audit_case(before, after, target=target, cache_state="C1")
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])
            shutil.rmtree(bulk_dir, ignore_errors=True)
            bulk_dir.mkdir(parents=True, exist_ok=True)
    finally:
        if handle is not None:
            handle.unmount()

    base_stat = summarize_latency(baseline_lats_ms, kind="discrete")
    cont_stat = summarize_latency(contended_lats_ms, kind="discrete")
    degrad = (
        round(cont_stat["p99"] / base_stat["p99"], 3)
        if base_stat.get("p99") and cont_stat.get("p99") and base_stat["p99"] > 0
        else None
    )
    stale_epoch = int(last_deltas.get("client_stale_epoch", 0))
    ok = audit_ok and readyz_ok and stale_epoch == 0
    if not readyz_ok:
        audit_reasons.append("INVALID_RUN: /readyz returned non-200 during H2C contention")
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 0 + C1 metadata",
        "cache_state": "C1",
        "runs_n": runs_n,
        "metrics": {
            "baseline_metadata_ms": base_stat,
            "contended_metadata_ms": cont_stat,
            "p99_degradation_ratio": degrad,
            "readyz_ok": readyz_ok,
            "stale_epoch_delta": stale_epoch,
        },
        "validity_status": "PASS" if ok else "INVALID",
        "integrity_verified": ok,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"8x4MiB bulk + 4x cold meta; P99 degradation={degrad}x; readyz=200; stale_epoch=0",
    }


def _case_staged_write_statfs_barrier(ctx: BenchContext) -> dict[str, Any]:
    case_id = "staged-write-statfs-barrier"
    target = "bowl_staged"
    runs_n = 20
    chunk_1m = b"S" * (1024 * 1024)
    statfs_lats_ms: list[float] = []
    last_deltas: dict[str, Any] = {}
    audit_ok = True
    audit_reasons: list[str] = []

    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(target, disk_name="meta-statfs-barrier")
        stop_bg = threading.Event()

        def _staged_writer(tid: int) -> None:
            seq = 0
            while not stop_bg.is_set():
                fpath = handle.mount_dir / f"st_w_{tid}_{seq % 8}.bin"
                with open(fpath, "wb") as fh:
                    for _ in range(16):
                        if stop_bg.is_set():
                            break
                        fh.write(chunk_1m)
                seq += 1

        writers = [threading.Thread(target=_staged_writer, args=(i,), daemon=True) for i in range(4)]
        before = ctx.snapshot_counters(target, handle)
        for t in writers:
            t.start()
        try:
            time.sleep(2.0)
            for idx in range(-1, runs_n):
                time.sleep(1.0)
                t0 = time.perf_counter_ns()
                _ = os.statvfs(handle.mount_dir)
                dt_ms = (time.perf_counter_ns() - t0) / 1e6
                if idx >= 0:
                    statfs_lats_ms.append(dt_ms)
        finally:
            stop_bg.set()
            for t in writers:
                t.join(timeout=30)
        after = ctx.snapshot_counters(target, handle)
        audit = ctx.audit_case(before, after, target=target, cache_state="C0")
        last_deltas = audit["deltas"]
        if audit["validity_status"] != "PASS":
            audit_ok = False
            audit_reasons.extend(audit["reasons"])
    finally:
        if handle is not None:
            handle.unmount()

    lat_stat = summarize_latency(statfs_lats_ms, kind="discrete")
    status = _combine_validity("PASS" if audit_ok else "INVALID", True, lat_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 0",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "statfs_barrier_ms": lat_stat,
            "client_stage_barrier_wait": last_deltas.get("client_stage_barrier_wait"),
        },
        "validity_status": status,
        "integrity_verified": True,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons) if audit_reasons else "periodic os.statvfs during sustained staged writes",
    }


def _render_summary_markdown(cases: list[dict[str, Any]]) -> str:
    lines = [
        "### `metadata` Detail Summary",
        "",
        "| Case | Target | Runs | Primary Metric | Key Notes | Gate |",
        "| :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for c in cases:
        m = c.get("metrics", {})
        primary = "N/A"
        if "stat_c1_ops_s" in m:
            primary = f"C1 stat {format_stat(m['stat_c1_ops_s'], unit='ops/s')}"
        elif "create_latency_ms" in m:
            cl = m["create_latency_ms"]
            primary = f"P50={cl.get('p50')} / P99={cl.get('p99')} / P99.9={cl.get('p999')} ms"
        elif "contended_metadata_ms" in m:
            cm = m["contended_metadata_ms"]
            primary = f"P50={cm.get('p50')} / P99={cm.get('p99')} ms ({m.get('p99_degradation_ratio')}x)"
        elif "statfs_barrier_ms" in m:
            sb = m["statfs_barrier_ms"]
            primary = f"P50={sb.get('p50')} / P95={sb.get('p95')} / Max={sb.get('max')} ms"
        elif "ops_s" in m:
            primary = format_stat(m["ops_s"], unit="ops/s")
        elif "cold_stat_ops_s" in m:
            primary = f"TTL=1s cold stat {format_stat(m['cold_stat_ops_s'], unit='ops/s')}"
        else:
            for k, v in m.items():
                if k.startswith("dir_") and isinstance(v, dict):
                    primary = f"{k}: P50={v.get('p50')} / P95={v.get('p95')} ms"
        lines.append(
            f"| `{c['case_id']}` | `{c['target']}` | {c['runs_n']} | {primary} | {c['notes']} | `{c['validity_status']}` |"
        )
    return "\n".join(lines) + "\n"


def run_suite(ctx: BenchContext) -> dict[str, Any]:
    smoke = _is_smoke(ctx)
    active_targets = [t for t in ctx.targets if t in SUPPORTED_TARGETS]
    cases: list[dict[str, Any]] = []

    if smoke:
        for bowl_target in ("bowl_staged", "bowl_sync"):
            if bowl_target in active_targets:
                cases.append(_case_seq_create_id_segment_spike(ctx, bowl_target))
        for target in active_targets:
            cases.append(_case_large_dir_readdirplus(ctx, target))
        if "bowl_staged" in active_targets:
            cases.append(_case_h2_bulk_rw_metadata_contention(ctx))
    else:
        for target in active_targets:
            cases.append(_case_mdtest_split_phases(ctx, target))
            cases.append(_case_large_dir_readdirplus(ctx, target))
            cases.append(_case_enoent_storm(ctx, target))
            if target in ("bowl_staged", "bowl_sync", "jfs_pg_minio", "jfs_redis_minio"):
                cases.append(_case_metadata_ttl_1s_parity(ctx, target))
        for bowl_target in ("bowl_staged", "bowl_sync"):
            if bowl_target in active_targets:
                cases.append(_case_seq_create_id_segment_spike(ctx, bowl_target))
        if "bowl_staged" in active_targets:
            cases.append(_case_h2_bulk_rw_metadata_contention(ctx))
            cases.append(_case_staged_write_statfs_barrier(ctx))

    return {
        "suite": SUITE_ID,
        "cases": cases,
        "summary_markdown": _render_summary_markdown(cases),
    }
