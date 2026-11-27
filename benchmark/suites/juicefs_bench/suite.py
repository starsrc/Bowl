from __future__ import annotations

import re
import shutil
import time
from pathlib import Path
from typing import Any

from benchmark.lib.context import BenchContext, MountHandle
from benchmark.lib.stats import format_stat, summarize_steady

SUITE_ID = "juicefs-bench"
SUPPORTED_TARGETS = [
    "ext4",
    "passthrough_ll",
    "bowl_staged",
    "bowl_sync",
    "jfs_pg_minio",
    "jfs_redis_minio",
]

ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")

METRIC_SPECS = [
    ("Write big file", "write_big_mib_s", "MiB/s"),
    ("Read big file", "read_big_mib_s", "MiB/s"),
    ("Write small file", "write_small_files_s", "files/s"),
    ("Read small file", "read_small_files_s", "files/s"),
    ("Stat file", "stat_files_s", "files/s"),
    ("FUSE operation", "fuse_op_ms", "ms/op"),
]


def _find_juicefs_bin() -> str | None:
    found = shutil.which("juicefs")
    if found:
        return found
    for candidate in ("/usr/local/bin/juicefs", "/usr/local/bench/bin/juicefs"):
        if Path(candidate).is_file():
            return candidate
    return None


def parse_juicefs_bench_output(text: str) -> dict[str, dict[str, Any]]:
    clean = ANSI_RE.sub("", text)
    results: dict[str, dict[str, Any]] = {}

    row_re = re.compile(
        r"\|\s*(Write big file|Read big file|Write small file|Read small file|Stat file|FUSE operation)\s*\|\s*"
        r"([0-9]+(?:\.[0-9]+)?)\s*([^|]+?)\s*\|\s*"
        r"([0-9]+(?:\.[0-9]+)?)\s*([^|]+?)\s*\|",
        re.IGNORECASE,
    )
    single_col_re = re.compile(
        r"\|\s*(FUSE operation)\s*\|\s*([0-9]+(?:\.[0-9]+)?)\s*([^|]+?)\s*\|",
        re.IGNORECASE,
    )

    canonical_names = {label.lower(): label for label, _, _ in METRIC_SPECS}

    for line in clean.splitlines():
        m = row_re.search(line)
        if m:
            item = canonical_names[m.group(1).lower()]
            val = float(m.group(2))
            val_unit = m.group(3).strip()
            cost = float(m.group(4))
            cost_unit = m.group(5).strip()
            entry: dict[str, Any] = {
                "value": val,
                "value_unit": val_unit,
                "cost": cost,
                "cost_unit": cost_unit,
            }
            if item == "FUSE operation":
                cost_ms = cost
                if cost_unit.lower().startswith("us") or "µs" in cost_unit:
                    cost_ms = cost / 1000.0
                elif cost_unit.lower().startswith("s/"):
                    cost_ms = cost * 1000.0
                entry["latency_ms"] = round(cost_ms, 4)
            results[item] = entry
            continue

        m2 = single_col_re.search(line)
        if m2 and "FUSE operation" not in results:
            val = float(m2.group(2))
            unit = m2.group(3).strip()
            lat_ms = val
            if unit.lower().startswith("us") or "µs" in unit:
                lat_ms = val / 1000.0
            elif unit.lower().startswith("ops/"):
                lat_ms = (1000.0 / val) if val > 0 else 0.0
            results["FUSE operation"] = {
                "value": val,
                "value_unit": unit,
                "cost": lat_ms,
                "cost_unit": "ms/op",
                "latency_ms": round(lat_ms, 4),
            }

    return results


def _run_target_juicefs_bench(
    ctx: BenchContext,
    target: str,
    jfs_bin: str,
) -> dict[str, Any]:
    smoke = ctx.profile == "smoke-local"
    threads = 4
    warmup_runs = 1
    measured_runs = 2 if smoke else 5
    big_file_size_mib = 128 if smoke else 1024
    big_file_count = 1
    small_file_size_kib = 128
    small_file_count = 50 if smoke else 100

    expected_read_bytes = (
        threads * big_file_count * big_file_size_mib * 1024 * 1024
        + threads * small_file_count * small_file_size_kib * 1024
    )
    expected_stat_ops = threads * small_file_count

    samples: dict[str, list[float]] = {key: [] for _, key, _ in METRIC_SPECS}
    drain_samples: list[float] = []
    run_logs: list[dict[str, Any]] = []
    audit_failed = False
    audit_reasons: list[str] = []
    agg_deltas: dict[str, float] = {}
    all_runs_ok = True

    total_iters = warmup_runs + measured_runs
    for idx in range(total_iters):
        is_warmup = idx < warmup_runs
        run_tag = f"warmup{idx + 1}" if is_warmup else f"run{idx - warmup_runs + 1}"
        handle: MountHandle | None = None
        try:
            handle = ctx.prepare_target(target, jfs_cache_mode="zero")
            ctx.drop_caches("C2", target=target, handle=handle)

            bench_dir = handle.mount_dir / f"jfsbench_{run_tag}"
            bench_dir.mkdir(parents=True, exist_ok=True)

            cmd = [jfs_bin, "bench", "-p", str(threads)]
            if smoke:
                cmd.extend(
                    [
                        "--big-file-size",
                        str(big_file_size_mib),
                        "--small-file-count",
                        str(small_file_count),
                    ]
                )
            cmd.append(str(bench_dir))

            before = ctx.snapshot_counters(target, handle=handle)
            t0 = time.perf_counter()
            proc = ctx.run_cmd(cmd, timeout=1800, check=False)
            wall_s = round(time.perf_counter() - t0, 3)
            after = ctx.snapshot_counters(target, handle=handle)

            combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
            (ctx.suite_dir / f"juicefs_bench_{target}_{run_tag}.log").write_text(
                combined, encoding="utf-8"
            )
            parsed = parse_juicefs_bench_output(combined)
            drain_s = round(handle.unmount(), 3)
            handle = None

            if is_warmup:
                continue

            drain_samples.append(drain_s)
            if proc.returncode != 0 or len(parsed) < 5:
                all_runs_ok = False
                audit_reasons.append(f"{run_tag}: rc={proc.returncode}, parsed_items={len(parsed)}")

            for label, key, _ in METRIC_SPECS:
                if label in parsed:
                    if label == "FUSE operation":
                        samples[key].append(float(parsed[label].get("latency_ms", parsed[label]["cost"])))
                    else:
                        samples[key].append(float(parsed[label]["value"]))

            audit = ctx.audit_case(
                before,
                after,
                target=target,
                cache_state="C2",
                expected_read_bytes=expected_read_bytes,
                expected_stat_ops=expected_stat_ops,
            )
            if audit.get("validity_status") != "PASS":
                audit_failed = True
                audit_reasons.extend(f"{run_tag}: {r}" for r in audit.get("reasons", []))
            for k, v in audit.get("deltas", {}).items():
                if isinstance(v, (int, float)):
                    agg_deltas[k] = round(agg_deltas.get(k, 0.0) + float(v), 4)

            run_logs.append(
                {
                    "run": run_tag,
                    "returncode": proc.returncode,
                    "wall_s": wall_s,
                    "drain_time_s": drain_s,
                    "parsed": parsed,
                    "audit_status": audit.get("validity_status"),
                }
            )
        finally:
            if handle is not None:
                handle.unmount()

    metric_summaries = {
        key: summarize_steady(samples[key]) if samples[key] else None
        for _, key, _ in METRIC_SPECS
    }
    drain_summary = summarize_steady(drain_samples)
    high_variance = any(
        isinstance(s, dict) and s.get("high_variance")
        for s in metric_summaries.values()
    )

    if not all_runs_ok or audit_failed:
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
    notes = f"p={threads}, warmup={warmup_runs}, N={measured_runs}, C2 drop_caches before each run"
    if audit_reasons:
        notes += "; issues=" + "; ".join(audit_reasons[:5])

    return {
        "case_id": "juicefs-bench-cold-5runs",
        "target": target,
        "durability_tier": tier,
        "cache_state": "C2",
        "runs_n": measured_runs,
        "metrics": {
            **metric_summaries,
            "drain_time_s": drain_summary,
            "runs": run_logs,
        },
        "validity_status": validity,
        "integrity_verified": all_runs_ok,
        "counter_deltas": agg_deltas,
        "notes": notes,
    }


def _render_summary_markdown(cases: list[dict[str, Any]]) -> str:
    by_target = {c["target"]: c for c in cases}

    def _cell(target: str, key: str, add_drain: bool = False) -> str:
        c = by_target.get(target)
        if not c or c["validity_status"] == "SKIPPED":
            return "N/A"
        st = c["metrics"].get(key)
        if not st or st.get("median") is None:
            return "N/A"
        prec = 3 if key == "fuse_op_ms" else 2
        text = format_stat(st, precision=prec)
        if add_drain and target == "bowl_staged":
            dr = c["metrics"].get("drain_time_s", {})
            if isinstance(dr, dict) and dr.get("median") is not None:
                text += f" (drain {dr['median']:.2f}s)"
        return text

    row_labels = [
        ("Write big file (MiB/s)", "write_big_mib_s", True),
        ("Read big file (MiB/s, C2 冷读)", "read_big_mib_s", False),
        ("Write small file (files/s)", "write_small_files_s", False),
        ("Read small file (files/s, C2 冷读)", "read_small_files_s", False),
        ("Stat file (files/s, C1 冷 dcache)", "stat_files_s", False),
        ("FUSE operation (ms/op)", "fuse_op_ms", False),
    ]

    lines = [
        "| `juicefs bench` 子项 | `ext4` (上限) | `passthrough_ll` | `bowl_staged` (Tier 0) | `bowl_sync` (Tier 1) | `jfs_pg_minio` (Tier 1, `--cache-size=0`) | `jfs_redis_minio` (Tier 0.5, `--cache-size=0`) | Gate |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    all_statuses = [c["validity_status"] for c in cases]
    gate = (
        "PASS"
        if all_statuses and all(s == "PASS" for s in all_statuses)
        else ("HIGH_VARIANCE" if all_statuses and all(s in ("PASS", "HIGH_VARIANCE") for s in all_statuses) else "INVALID")
    )
    for label, key, add_drain in row_labels:
        ext4_cell = "N/A" if key == "fuse_op_ms" else _cell("ext4", key)
        lines.append(
            f"| {label} | {ext4_cell} | {_cell('passthrough_ll', key)} | "
            f"{_cell('bowl_staged', key, add_drain)} | {_cell('bowl_sync', key)} | "
            f"{_cell('jfs_pg_minio', key)} | {_cell('jfs_redis_minio', key)} | {gate} |"
        )
    return "\n".join(lines) + "\n"


def run_suite(ctx: BenchContext) -> dict[str, Any]:
    active = [t for t in ctx.targets if t in SUPPORTED_TARGETS]
    jfs_bin = _find_juicefs_bin()
    cases: list[dict[str, Any]] = []

    if jfs_bin is None:
        for target in active:
            cases.append(
                {
                    "case_id": "juicefs-bench-cold-5runs",
                    "target": target,
                    "durability_tier": "Tier 0" if target == "bowl_staged" else "Tier 1",
                    "cache_state": "C2",
                    "runs_n": 0,
                    "metrics": {},
                    "validity_status": "SKIPPED",
                    "integrity_verified": False,
                    "counter_deltas": {},
                    "notes": "juicefs binary not found in PATH or /usr/local/bin/juicefs",
                }
            )
        return {"suite": SUITE_ID, "cases": cases, "summary_markdown": "juicefs binary not found; suite skipped.\n"}

    for target in active:
        cases.append(_run_target_juicefs_bench(ctx, target, jfs_bin))

    return {
        "suite": SUITE_ID,
        "cases": cases,
        "summary_markdown": _render_summary_markdown(cases),
    }
