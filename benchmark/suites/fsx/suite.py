from __future__ import annotations

import hashlib
import re
import shutil
import time
from pathlib import Path
from typing import Any

from benchmark.lib.context import BenchContext, MountHandle

SUITE_ID = "fsx"
SUPPORTED_TARGETS = ["bowl_staged", "bowl_sync", "ext4"]

FSX_VARIANTS: list[dict[str, Any]] = [
    {"name": "default", "flags": [], "full_ops": 600000, "smoke_ops": 20000, "direct_io": False, "smoke": True},
    {"name": "no-mmap", "flags": ["-R", "-W"], "full_ops": 600000, "smoke_ops": 20000, "direct_io": False, "smoke": False},
    {"name": "aligned", "flags": ["-r", "4096", "-w", "4096", "-t", "4096"], "full_ops": 600000, "smoke_ops": 20000, "direct_io": False, "smoke": True},
    {"name": "medium", "flags": ["-l", "4194304"], "full_ops": 400000, "smoke_ops": 20000, "direct_io": False, "smoke": False},
    {"name": "large", "flags": ["-l", "67108864", "-o", "1048576"], "full_ops": 24000, "smoke_ops": 2000, "direct_io": False, "smoke": True},
    {"name": "large-no-mmap", "flags": ["-l", "67108864", "-o", "1048576", "-R", "-W"], "full_ops": 32000, "smoke_ops": 2000, "direct_io": False, "smoke": False},
    {"name": "direct", "flags": ["-Z", "-R", "-W", "-r", "4096", "-w", "4096"], "full_ops": 800000, "smoke_ops": 20000, "direct_io": True, "smoke": True},
]

PROBE_OPS: list[tuple[str, list[str]]] = [
    ("fallocate(0)", ["fallocate mode 0x0", "fallocate_calls", "fallocate(0)", "prealloc"]),
    ("KEEP_SIZE", ["keep_size", "keep size", "mode 0x1"]),
    ("PUNCH_HOLE", ["punch_hole", "punch hole", "mode 0x3"]),
    ("ZERO_RANGE", ["zero_range", "zero range", "mode 0x10"]),
    ("COLLAPSE_RANGE", ["collapse_range", "collapse range", "mode 0x8"]),
    ("INSERT_RANGE", ["insert_range", "insert range", "mode 0x20"]),
    ("copy_file_range", ["copy_file_range", "copy range"]),
    ("clone_range", ["clone_range", "clone range", "ficlonerange"]),
    ("dedupe_range", ["dedupe_range", "dedupe range", "fideduperange"]),
    ("exchange_range", ["exchange_range", "exchange range"]),
]


def _find_fsx_bin(ctx: BenchContext) -> Path | None:
    for candidate in (ctx.tools_bin / "fsx", Path("/usr/local/bench/bin/fsx")):
        if candidate.is_file():
            return candidate
    found = shutil.which("fsx")
    return Path(found) if found else None


def _sha256_file(path: Path) -> tuple[int, str]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while True:
            buf = f.read(1024 * 1024)
            if not buf:
                break
            size += len(buf)
            h.update(buf)
    return size, h.hexdigest()


def _remount_verify_sha256(
    ctx: BenchContext,
    target: str,
    disk_id: str,
    branch: str,
    rel_path: str,
    expected_size: int,
    expected_sha256: str,
) -> bool:
    ro_handle = ctx.prepare_target(target, disk_id=disk_id, branch_name=branch, read_only=True)
    try:
        target_file = ro_handle.mount_dir / rel_path
        if not target_file.is_file():
            return False
        got_size, got_sha = _sha256_file(target_file)
        return got_size == expected_size and got_sha == expected_sha256
    finally:
        ro_handle.unmount()


def parse_fsx_probe(output: str, *, returncode: int = 0) -> tuple[dict[str, str], list[str]]:
    probe_lines: list[str] = []
    disabled_tokens: list[str] = []
    for raw_line in output.splitlines():
        low = raw_line.lower()
        if any(
            kw in low
            for kw in (
                "does not support",
                "disabling",
                "disabled",
                "operation not supported",
                "eopnotsupp",
                "not supported",
            )
        ):
            line_clean = raw_line.strip()
            if line_clean:
                probe_lines.append(line_clean)
                disabled_tokens.append(low)

    if returncode != 0 and not probe_lines:
        return {op_label: "unknown" for op_label, _ in PROBE_OPS}, probe_lines

    matrix: dict[str, str] = {}
    for op_label, keywords in PROBE_OPS:
        is_disabled = any(any(kw in line for kw in keywords) for line in disabled_tokens)
        if is_disabled:
            matrix[op_label] = "disabled (EOPNOTSUPP)"
        elif returncode != 0:
            matrix[op_label] = "unknown"
        else:
            matrix[op_label] = "enabled"
    return matrix, probe_lines


def _count_miscompares(output: str) -> int:
    patterns = (
        re.compile(r"READ BAD DATA", re.IGNORECASE),
        re.compile(r"CORRUPTION", re.IGNORECASE),
        re.compile(r"miscompare", re.IGNORECASE),
        re.compile(r"bad length", re.IGNORECASE),
        re.compile(r"Size error", re.IGNORECASE),
    )
    count = 0
    for line in output.splitlines():
        if any(p.search(line) for p in patterns):
            count += 1
    return count


def _run_target_fsx(ctx: BenchContext, target: str, fsx_bin: Path) -> tuple[dict[str, Any], dict[str, str]]:
    smoke = ctx.profile == "smoke-local"
    variants = [v for v in FSX_VARIANTS if (v["smoke"] if smoke else True)]
    seeds = [1] if smoke else [1, 2]

    run_records: list[dict[str, Any]] = []
    probe_matrix: dict[str, str] = {}
    probe_lines_sample: list[str] = []
    total_ops = 0
    passed_runs = 0
    total_miscompares = 0
    remount_checks_passed = 0
    remount_checks_total = 0
    audit_failed = False
    audit_reasons: list[str] = []
    agg_deltas: dict[str, float] = {}

    for variant in variants:
        v_name = variant["name"]
        ops = variant["smoke_ops"] if smoke else variant["full_ops"]
        flags = list(variant["flags"])
        direct_io = bool(variant["direct_io"])

        for seed in seeds:
            handle: MountHandle | None = None
            try:
                handle = ctx.prepare_target(target, direct_io=direct_io)
                test_file = handle.mount_dir / f"fsx_{v_name}_s{seed}.dat"

                before = ctx.snapshot_counters(target, handle=handle)
                t0 = time.perf_counter()
                proc = ctx.run_cmd(
                    [str(fsx_bin), "-N", str(ops), "-S", str(seed), *flags, str(test_file)],
                    cwd=handle.mount_dir,
                    timeout=1800,
                    check=False,
                )
                duration_s = round(time.perf_counter() - t0, 3)
                after = ctx.snapshot_counters(target, handle=handle)

                combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
                log_path = ctx.suite_dir / f"fsx_{target}_{v_name}_s{seed}.log"
                log_path.write_text(combined, encoding="utf-8")

                if not direct_io and (not probe_matrix or all(v == "unknown" for v in probe_matrix.values())):
                    probe_matrix, probe_lines_sample = parse_fsx_probe(combined, returncode=proc.returncode)

                a_ok = bool(re.search(r"all\s+(?:\d+\s+)?operations\s+completed\s+a-ok", combined, re.IGNORECASE))
                miscompares = _count_miscompares(combined)
                total_miscompares += miscompares

                audit = ctx.audit_case(before, after, target=target, cache_state="C3" if direct_io else "C0")
                if audit.get("validity_status") != "PASS":
                    audit_failed = True
                    audit_reasons.extend(audit.get("reasons", []))
                for k, v in audit.get("deltas", {}).items():
                    if isinstance(v, (int, float)):
                        agg_deltas[k] = round(agg_deltas.get(k, 0.0) + float(v), 4)

                remount_ok = True
                drain_s = 0.0
                if target.startswith("bowl_") and test_file.is_file():
                    pre_size, pre_sha = _sha256_file(test_file)
                    disk_id = handle.disk_id or ""
                    branch = handle.branch_name or "main"
                    drain_s = round(handle.unmount(), 3)
                    handle = None
                    if disk_id:
                        remount_checks_total += 1
                        remount_ok = _remount_verify_sha256(
                            ctx, target, disk_id, branch, test_file.name, pre_size, pre_sha
                        )
                        if remount_ok:
                            remount_checks_passed += 1
                else:
                    drain_s = round(handle.unmount(), 3)
                    handle = None

                run_ok = proc.returncode == 0 and a_ok and miscompares == 0 and remount_ok
                if run_ok:
                    passed_runs += 1
                total_ops += ops
                run_records.append(
                    {
                        "variant": v_name,
                        "seed": seed,
                        "ops": ops,
                        "flags": " ".join(flags) if flags else "(none)",
                        "returncode": proc.returncode,
                        "a_ok": a_ok,
                        "miscompares": miscompares,
                        "remount_sha256_ok": remount_ok,
                        "duration_s": duration_s,
                        "drain_time_s": drain_s,
                        "status": "PASS" if run_ok else "FAIL",
                    }
                )
            finally:
                if handle is not None:
                    handle.unmount()

    total_runs = len(run_records)
    integrity_ok = (passed_runs == total_runs and total_runs > 0 and total_miscompares == 0)
    validity = "PASS" if (integrity_ok and not audit_failed) else "INVALID"

    notes = f"{passed_runs}/{total_runs} runs PASS, total_ops={total_ops}, miscompares={total_miscompares}"
    if remount_checks_total > 0:
        notes += f", remount_sha256={remount_checks_passed}/{remount_checks_total}"
    if audit_reasons:
        notes += "; audit=" + "; ".join(audit_reasons[:5])

    case = {
        "case_id": "fsx-14-variants",
        "target": target,
        "durability_tier": "Tier 2" if target.startswith("bowl_") else "Tier 0",
        "cache_state": "C0/C3",
        "runs_n": total_runs,
        "metrics": {
            "passed_runs": passed_runs,
            "total_runs": total_runs,
            "total_ops": total_ops,
            "miscompares": total_miscompares,
            "remount_checks_passed": remount_checks_passed,
            "remount_checks_total": remount_checks_total,
            "runs": run_records,
            "probe_matrix": probe_matrix,
            "probe_lines": probe_lines_sample[:20],
        },
        "validity_status": validity,
        "integrity_verified": integrity_ok,
        "counter_deltas": agg_deltas,
        "notes": notes,
    }
    return case, probe_matrix


def _render_summary_markdown(
    cases: list[dict[str, Any]],
    probe_by_target: dict[str, dict[str, str]],
    smoke: bool,
) -> str:
    by_target: dict[str, dict[tuple[str, int], dict[str, Any]]] = {}
    for c in cases:
        t_map: dict[tuple[str, int], dict[str, Any]] = {}
        for r in c.get("metrics", {}).get("runs", []):
            t_map[(r["variant"], int(r["seed"]))] = r
        by_target[c["target"]] = t_map

    variants = [v for v in FSX_VARIANTS if (v["smoke"] if smoke else True)]
    seeds = [1] if smoke else [1, 2]

    lines = [
        "| 变体 | `fsx` 参数 | 每臂操作数 | `bowl_staged` | `bowl_sync` | `ext4` 参考 | 数据/长度错配 |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for v in variants:
        v_name = v["name"]
        flags = " ".join(v["flags"]) if v["flags"] else "(none)"
        ops = v["smoke_ops"] if smoke else v["full_ops"]
        cells: list[str] = []
        miscompares = 0
        for target in SUPPORTED_TARGETS:
            t_map = by_target.get(target)
            if t_map is None:
                cells.append("N/A")
                continue
            seed_statuses = []
            for s in seeds:
                rec = t_map.get((v_name, s))
                if rec:
                    seed_statuses.append(rec["status"])
                    miscompares += int(rec.get("miscompares", 0))
                else:
                    seed_statuses.append("N/A")
            cells.append(" / ".join(seed_statuses))
        lines.append(
            f"| `{v_name}` | `{flags}` | {ops:,} | {cells[0]} | {cells[1]} | {cells[2]} | `{miscompares}` |"
        )

    if probe_by_target:
        active_probe_targets = [t for t in SUPPORTED_TARGETS if t in probe_by_target and probe_by_target[t]]
        if active_probe_targets:
            lines.append("")
            lines.append("### `fsx` 启动探测矩阵")
            lines.append("")
            hdr = "| 可选操作 | " + " | ".join(f"`{t}`" for t in active_probe_targets) + " |"
            sep = "| :--- | " + " | ".join(":---" for _ in active_probe_targets) + " |"
            lines.append(hdr)
            lines.append(sep)
            for op_label, _ in PROBE_OPS:
                row = f"| `{op_label}` | " + " | ".join(
                    probe_by_target[t].get(op_label, "N/A") for t in active_probe_targets
                ) + " |"
                lines.append(row)

    return "\n".join(lines) + "\n"


def run_suite(ctx: BenchContext) -> dict[str, Any]:
    active = [t for t in ctx.targets if t in SUPPORTED_TARGETS]
    fsx_bin = _find_fsx_bin(ctx)
    cases: list[dict[str, Any]] = []
    probe_by_target: dict[str, dict[str, str]] = {}

    if fsx_bin is None:
        for target in active:
            cases.append(
                {
                    "case_id": "fsx-14-variants",
                    "target": target,
                    "durability_tier": "Tier 2" if target.startswith("bowl_") else "Tier 0",
                    "cache_state": "C0/C3",
                    "runs_n": 0,
                    "metrics": {},
                    "validity_status": "SKIPPED",
                    "integrity_verified": False,
                    "counter_deltas": {},
                    "notes": "fsx binary not found at /usr/local/bench/bin/fsx",
                }
            )
        return {"suite": SUITE_ID, "cases": cases, "summary_markdown": "fsx binary not found; suite skipped.\n"}

    for target in active:
        case, probe_matrix = _run_target_fsx(ctx, target, fsx_bin)
        cases.append(case)
        probe_by_target[target] = probe_matrix

    return {
        "suite": SUITE_ID,
        "cases": cases,
        "summary_markdown": _render_summary_markdown(cases, probe_by_target, smoke=(ctx.profile == "smoke-local")),
    }
