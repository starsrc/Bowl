from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

from benchmark.lib.context import BenchContext, MountHandle
from benchmark.lib.stats import format_stat, summarize_steady

SUITE_ID = "smallfile"
SUPPORTED_TARGETS = [
    "ext4",
    "passthrough_ll",
    "bowl_staged",
    "bowl_sync",
    "jfs_pg_minio",
    "jfs_redis_minio",
]

FILE_OPS_SEQUENCE = [
    ("create", "C0"),
    ("read", "C0"),
    ("read", "C1"),
    ("append", "C0"),
    ("stat", "C0"),
    ("stat", "C1"),
    ("chmod", "C0"),
    ("rename", "C0"),
    ("delete-renamed", "C0"),
]

DIR_OPS_SEQUENCE = [
    ("mkdir", "C0"),
    ("rmdir", "C0"),
]


def _find_smallfile_cli(ctx: BenchContext) -> Path | None:
    candidates = [
        Path("/usr/local/bench/smallfile/smallfile_cli.py"),
        ctx.tools_bin.parent / "smallfile" / "smallfile_cli.py",
        ctx.tools_bin / "smallfile_cli.py",
    ]
    for path in candidates:
        if path.is_file():
            return path
    return None


def parse_smallfile_output(
    stdout: str,
    json_path: Path | None,
    expected_files: int,
    wall_s: float,
) -> dict[str, Any]:
    files_per_sec: float | None = None
    files_completed: int | None = None
    elapsed_s: float | None = None

    if json_path is not None and json_path.is_file():
        try:
            raw = json.loads(json_path.read_text(encoding="utf-8"))
            res = raw.get("results", raw) if isinstance(raw, dict) else {}
            if isinstance(res, dict):
                for key in ("files-per-sec", "files_per_sec", "filesPerSec"):
                    if key in res and res[key] is not None:
                        files_per_sec = float(res[key])
                        break
                for key in ("total-files", "files-created", "total_files", "files"):
                    if key in res and res[key] is not None:
                        files_completed = int(res[key])
                        break
                for key in ("elapsed-time", "elapsed_time", "elapsed"):
                    if key in res and res[key] is not None:
                        elapsed_s = float(res[key])
                        break
        except Exception:
            pass

    if files_per_sec is None:
        m = re.search(r"files/sec\s*=\s*([0-9]+(?:\.[0-9]+)?)", stdout, re.IGNORECASE)
        if not m:
            m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s+files/sec", stdout, re.IGNORECASE)
        if m:
            files_per_sec = float(m.group(1))

    if files_completed is None:
        m = re.search(r"total\s+files\s*=\s*([0-9]+)", stdout, re.IGNORECASE)
        if m:
            files_completed = int(m.group(1))

    if elapsed_s is None:
        m = re.search(r"elapsed\s+time\s*=\s*([0-9]+(?:\.[0-9]+)?)", stdout, re.IGNORECASE)
        if m:
            elapsed_s = float(m.group(1))

    eff_elapsed = elapsed_s if (elapsed_s and elapsed_s > 0) else max(wall_s, 1e-6)
    if files_completed is None:
        files_completed = expected_files
    if files_per_sec is None:
        files_per_sec = files_completed / eff_elapsed

    return {
        "files_per_sec": round(files_per_sec, 2),
        "files_completed": files_completed,
        "elapsed_s": round(eff_elapsed, 4),
    }


def _op_metric_key(op: str, cache_mode: str) -> str:
    if op in ("read", "stat"):
        return f"{op}_c1_cold" if cache_mode == "C1" else f"{op}_continuous"
    return op


def _run_smallfile_op(
    ctx: BenchContext,
    smallfile_cli: Path,
    top_dir: Path,
    op: str,
    threads: int,
    files_per_thread: int,
    file_size_kb: int,
    json_out: Path,
) -> tuple[int, dict[str, Any], str]:
    if json_out.exists():
        json_out.unlink()
    cmd = [
        sys.executable,
        str(smallfile_cli),
        "--operation",
        op,
        "--threads",
        str(threads),
        "--files",
        str(files_per_thread),
        "--file-size",
        str(file_size_kb),
        "--top",
        str(top_dir),
        "--pause",
        "0",
        "--stonewall",
        "N",
        "--output-json",
        str(json_out),
    ]
    t0 = time.perf_counter()
    proc = ctx.run_cmd(cmd, timeout=1800, check=False)
    wall_s = time.perf_counter() - t0
    combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
    parsed = parse_smallfile_output(
        combined,
        json_out,
        expected_files=threads * files_per_thread,
        wall_s=wall_s,
    )
    return proc.returncode, parsed, combined


def _run_target_smallfile(
    ctx: BenchContext,
    target: str,
    smallfile_cli: Path,
) -> dict[str, Any]:
    smoke = ctx.profile == "smoke-local"
    threads = 4
    files_per_thread = 2500 if smoke else 25000
    file_size_kb = 4
    total_files = threads * files_per_thread
    runs_n = 1 if smoke else 3

    op_samples: dict[str, list[float]] = {
        _op_metric_key(op, mode): [] for op, mode in (FILE_OPS_SEQUENCE + DIR_OPS_SEQUENCE)
    }
    commit_s_samples: list[float] = []
    commit_us_samples: list[float] = []
    stage_barrier_samples: list[float] = []
    drain_samples: list[float] = []

    all_ops_complete = True
    audit_failed = False
    audit_reasons: list[str] = []
    agg_deltas: dict[str, float] = {}
    c1_audit_details: dict[str, list[dict[str, Any]]] = {"read": [], "stat": []}

    for run_idx in range(1, runs_n + 1):
        handle: MountHandle | None = None
        try:
            handle = ctx.prepare_target(target, jfs_cache_mode="zero")
            top_files = handle.mount_dir / f"sf_files_r{run_idx}"
            top_dirs = handle.mount_dir / f"sf_dirs_r{run_idx}"
            top_files.mkdir(parents=True, exist_ok=True)
            top_dirs.mkdir(parents=True, exist_ok=True)

            for op, cache_mode in FILE_OPS_SEQUENCE:
                m_key = _op_metric_key(op, cache_mode)
                json_out = ctx.suite_dir / f"smallfile_{target}_r{run_idx}_{m_key}.json"

                if cache_mode == "C1":
                    ctx.drop_caches("C1", target=target, handle=handle)

                before = ctx.snapshot_counters(target, handle=handle)
                rc, parsed, combined = _run_smallfile_op(
                    ctx,
                    smallfile_cli,
                    top_files,
                    op,
                    threads,
                    files_per_thread,
                    file_size_kb,
                    json_out,
                )
                after = ctx.snapshot_counters(target, handle=handle)

                if rc != 0 or parsed["files_completed"] < total_files:
                    all_ops_complete = False
                    audit_reasons.append(
                        f"run {run_idx} {m_key}: rc={rc}, completed={parsed['files_completed']}/{total_files}"
                    )
                    (ctx.suite_dir / f"smallfile_{target}_r{run_idx}_{m_key}.err.log").write_text(
                        combined, encoding="utf-8"
                    )

                op_samples[m_key].append(parsed["files_per_sec"])

                exp_read = (total_files * file_size_kb * 1024) if (op == "read" and cache_mode == "C1") else 0
                exp_stat = total_files if (op == "stat" and cache_mode == "C1") else 0
                audit = ctx.audit_case(
                    before,
                    after,
                    target=target,
                    cache_state=cache_mode,
                    expected_read_bytes=exp_read,
                    expected_stat_ops=exp_stat,
                    min_fuse_ratio=0.95,
                )
                if cache_mode == "C1":
                    c1_audit_details[op].append(
                        {
                            "run": run_idx,
                            "validity_status": audit.get("validity_status"),
                            "reasons": audit.get("reasons", []),
                            "deltas": audit.get("deltas", {}),
                        }
                    )
                if audit.get("validity_status") != "PASS":
                    audit_failed = True
                    audit_reasons.extend(f"r{run_idx}/{m_key}: {r}" for r in audit.get("reasons", []))
                for k, v in audit.get("deltas", {}).items():
                    if isinstance(v, (int, float)):
                        agg_deltas[k] = round(agg_deltas.get(k, 0.0) + float(v), 4)

                # For Bowl targets, measure full-tree CreateCommit right after create
                if op == "create" and target.startswith("bowl_") and handle.disk_id:
                    if target == "bowl_staged":
                        t_bar = time.perf_counter()
                        os.statvfs(str(handle.mount_dir))
                        stage_barrier_samples.append(round(time.perf_counter() - t_bar, 4))
                    branch = handle.branch_name or "main"
                    t_c0 = time.perf_counter()
                    ctx.bowl_rpc_cli(
                        [
                            "--json",
                            "commit",
                            "create",
                            "-d",
                            handle.disk_id,
                            "--branch",
                            branch,
                            "-m",
                            f"smallfile-{total_files}-r{run_idx}",
                        ],
                        check=True,
                    )
                    c_sec = time.perf_counter() - t_c0
                    commit_s_samples.append(round(c_sec, 4))
                    commit_us_samples.append(round((c_sec * 1e6) / total_files, 2))

            for op, cache_mode in DIR_OPS_SEQUENCE:
                m_key = _op_metric_key(op, cache_mode)
                json_out = ctx.suite_dir / f"smallfile_{target}_r{run_idx}_{m_key}.json"
                before = ctx.snapshot_counters(target, handle=handle)
                rc, parsed, combined = _run_smallfile_op(
                    ctx,
                    smallfile_cli,
                    top_dirs,
                    op,
                    threads,
                    files_per_thread,
                    file_size_kb,
                    json_out,
                )
                after = ctx.snapshot_counters(target, handle=handle)
                if rc != 0 or parsed["files_completed"] < total_files:
                    all_ops_complete = False
                    audit_reasons.append(
                        f"run {run_idx} {m_key}: rc={rc}, completed={parsed['files_completed']}/{total_files}"
                    )
                    (ctx.suite_dir / f"smallfile_{target}_r{run_idx}_{m_key}.err.log").write_text(
                        combined, encoding="utf-8"
                    )
                op_samples[m_key].append(parsed["files_per_sec"])
                audit = ctx.audit_case(before, after, target=target, cache_state="C0")
                if audit.get("validity_status") != "PASS":
                    audit_failed = True
                    audit_reasons.extend(f"r{run_idx}/{m_key}: {r}" for r in audit.get("reasons", []))

            drain_s = round(handle.unmount(), 3)
            drain_samples.append(drain_s)
            handle = None
        finally:
            if handle is not None:
                handle.unmount()

    ops_stats = {k: summarize_steady(vals) for k, vals in op_samples.items()}
    high_variance = any(s.get("high_variance") for s in ops_stats.values())

    metrics: dict[str, Any] = {
        "total_files": total_files,
        "threads": threads,
        "file_size_kb": file_size_kb,
        "ops_files_per_sec": ops_stats,
        "drain_time_s": summarize_steady(drain_samples),
        "c1_audit_details": c1_audit_details,
    }
    if commit_s_samples:
        metrics["commit_total_s"] = summarize_steady(commit_s_samples)
        metrics["commit_us_per_file"] = summarize_steady(commit_us_samples)
    if stage_barrier_samples:
        metrics["stage_barrier_before_commit_s"] = summarize_steady(stage_barrier_samples)

    if not all_ops_complete or audit_failed:
        validity = "INVALID"
    elif high_variance:
        validity = "HIGH_VARIANCE"
    else:
        validity = "PASS"

    tier = (
        "Tier 0"
        if target in ("bowl_staged", "ext4", "passthrough_ll")
        else ("Tier 0.5" if target == "jfs_redis_minio" else "Tier 1")
    )
    notes = f"{total_files:,} x {file_size_kb}KiB, threads={threads}, N={runs_n}; continuous + C1-cold read/stat"
    if audit_reasons:
        notes += "; issues=" + "; ".join(audit_reasons[:5])

    return {
        "case_id": "smallfile-100k-4k",
        "target": target,
        "durability_tier": tier,
        "cache_state": "C0/C1",
        "runs_n": runs_n,
        "metrics": metrics,
        "validity_status": validity,
        "integrity_verified": all_ops_complete,
        "counter_deltas": agg_deltas,
        "notes": notes,
    }


def _render_summary_markdown(cases: list[dict[str, Any]]) -> str:
    by_target = {c["target"]: c for c in cases}
    pt_ops = by_target.get("passthrough_ll", {}).get("metrics", {}).get("ops_files_per_sec", {})

    def _cell(target: str, key: str, with_ratio: bool = False) -> str:
        c = by_target.get(target)
        if not c or c["validity_status"] == "SKIPPED":
            return "N/A"
        st = c["metrics"].get("ops_files_per_sec", {}).get(key)
        if not st or st.get("median") is None:
            return "N/A"
        text = format_stat(st, precision=1)
        if with_ratio:
            pt_st = pt_ops.get(key)
            if pt_st and pt_st.get("median") and pt_st["median"] > 0:
                ratio = st["median"] / pt_st["median"]
                text += f" ({ratio:.2f}x)"
        return text

    def _pair_cell(target: str, c0_key: str, c1_key: str) -> str:
        return f"{_cell(target, c0_key)} / {_cell(target, c1_key)}"

    lines = [
        "| 操作 | `ext4` | `passthrough_ll` | `bowl_staged` (相对 `passthrough_ll`) | `bowl_sync` (相对 `passthrough_ll`) | `jfs_pg_minio` (`--cache-size=0`) | `jfs_redis_minio` (`--cache-size=0`) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    rows = [
        ("`create`", "create", None),
        ("`read` (连续 / C1 冷读)", "read_continuous", "read_c1_cold"),
        ("`append`", "append", None),
        ("`stat` (连续 / C1 冷 dcache)", "stat_continuous", "stat_c1_cold"),
        ("`chmod`", "chmod", None),
        ("`rename`", "rename", None),
        ("`delete-renamed`", "delete-renamed", None),
        ("`mkdir`", "mkdir", None),
        ("`rmdir`", "rmdir", None),
    ]
    for label, k1, k2 in rows:
        if k2 is None:
            lines.append(
                f"| {label} | {_cell('ext4', k1)} | {_cell('passthrough_ll', k1)} | "
                f"{_cell('bowl_staged', k1, True)} | {_cell('bowl_sync', k1, True)} | "
                f"{_cell('jfs_pg_minio', k1)} | {_cell('jfs_redis_minio', k1)} |"
            )
        else:
            lines.append(
                f"| {label} | {_pair_cell('ext4', k1, k2)} | {_pair_cell('passthrough_ll', k1, k2)} | "
                f"{_pair_cell('bowl_staged', k1, k2)} | {_pair_cell('bowl_sync', k1, k2)} | "
                f"{_pair_cell('jfs_pg_minio', k1, k2)} | {_pair_cell('jfs_redis_minio', k1, k2)} |"
            )

    def _commit_cell(target: str) -> str:
        c = by_target.get(target)
        if not c or "commit_total_s" not in c.get("metrics", {}):
            return "N/A"
        cs = c["metrics"]["commit_total_s"].get("median")
        us = c["metrics"]["commit_us_per_file"].get("median")
        if cs is None or us is None:
            return "N/A"
        return f"{cs:.3f} s ({us:.1f} us/file)"

    lines.append(
        f"| 整树 `CreateCommit` | N/A | N/A | {_commit_cell('bowl_staged')} | {_commit_cell('bowl_sync')} | N/A | N/A |"
    )
    return "\n".join(lines) + "\n"


def run_suite(ctx: BenchContext) -> dict[str, Any]:
    active = [t for t in ctx.targets if t in SUPPORTED_TARGETS]
    smallfile_cli = _find_smallfile_cli(ctx)
    cases: list[dict[str, Any]] = []

    if smallfile_cli is None:
        for target in active:
            cases.append(
                {
                    "case_id": "smallfile-100k-4k",
                    "target": target,
                    "durability_tier": "Tier 0" if target == "bowl_staged" else "Tier 1",
                    "cache_state": "C0/C1",
                    "runs_n": 0,
                    "metrics": {},
                    "validity_status": "SKIPPED",
                    "integrity_verified": False,
                    "counter_deltas": {},
                    "notes": "smallfile_cli.py not found at /usr/local/bench/smallfile/smallfile_cli.py",
                }
            )
        return {"suite": SUITE_ID, "cases": cases, "summary_markdown": "smallfile_cli.py not found; suite skipped.\n"}

    for target in active:
        cases.append(_run_target_smallfile(ctx, target, smallfile_cli))

    return {
        "suite": SUITE_ID,
        "cases": cases,
        "summary_markdown": _render_summary_markdown(cases),
    }
