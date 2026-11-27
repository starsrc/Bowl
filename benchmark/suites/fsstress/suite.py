from __future__ import annotations

import hashlib
import os
import re
import shutil
import stat
import time
from pathlib import Path
from typing import Any

from benchmark.lib import metrics_audit
from benchmark.lib.context import BenchContext, MountHandle

SUITE_ID = "fsstress"
SUPPORTED_TARGETS = ["bowl_staged", "bowl_sync", "ext4"]

REFUSED_ERRNOS = {1, 38, 95}  # EPERM, ENOSYS, EOPNOTSUPP


def _find_fsstress_bin(ctx: BenchContext) -> Path | None:
    for candidate in (ctx.tools_bin / "fsstress", Path("/usr/local/bench/bin/fsstress")):
        if candidate.is_file():
            return candidate
    found = shutil.which("fsstress")
    return Path(found) if found else None


def parse_fsstress_verbose(output: str) -> dict[str, Any]:
    op_re = re.compile(r"^\s*\d+/\d+:\s+([a-zA-Z0-9_]+)\b(.*)$")
    rc_re = re.compile(r"(?:^|\s)(-?\d+)\s*$")

    per_op: dict[str, dict[str, int]] = {}
    total_calls = 0
    refused_calls = 0

    for line in output.splitlines():
        m = op_re.match(line)
        if not m:
            continue
        op = m.group(1)
        rest = m.group(2).strip()
        rec = per_op.setdefault(op, {"total": 0, "ok": 0, "err": 0, "refused": 0})
        rec["total"] += 1
        total_calls += 1

        rc_match = rc_re.search(rest)
        if rc_match is None:
            rec["ok"] += 1
            continue
        rc = int(rc_match.group(1))
        if rc == 0:
            rec["ok"] += 1
        else:
            rec["err"] += 1
            if abs(rc) in REFUSED_ERRNOS:
                rec["refused"] += 1
                refused_calls += 1

    op_stats: dict[str, dict[str, Any]] = {}
    for op in sorted(per_op.keys()):
        r = per_op[op]
        rate = round((r["ok"] / r["total"]) * 100.0, 2) if r["total"] > 0 else 0.0
        op_stats[op] = {
            "total": r["total"],
            "ok": r["ok"],
            "err": r["err"],
            "refused": r["refused"],
            "success_rate_pct": rate,
        }

    return {
        "total_calls": total_calls,
        "refused_calls": refused_calls,
        "distinct_ops": len(op_stats),
        "ops": op_stats,
    }


def _file_type_label(mode: int) -> str:
    if stat.S_ISREG(mode):
        return "file"
    if stat.S_ISDIR(mode):
        return "dir"
    if stat.S_ISLNK(mode):
        return "symlink"
    if stat.S_ISFIFO(mode):
        return "fifo"
    if stat.S_ISCHR(mode):
        return "char"
    if stat.S_ISBLK(mode):
        return "block"
    if stat.S_ISSOCK(mode):
        return "sock"
    return "other"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            buf = f.read(1024 * 1024)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def collect_tree_manifest(root: Path) -> dict[str, dict[str, Any]]:
    manifest: dict[str, dict[str, Any]] = {}
    root_str = str(root)

    for dirpath, dirnames, filenames in os.walk(root_str, followlinks=False):
        dirnames.sort()
        filenames.sort()
        rel_dir = os.path.relpath(dirpath, root_str)
        if rel_dir != ".":
            st = os.lstat(dirpath)
            manifest[rel_dir] = {
                "type": _file_type_label(st.st_mode),
                "mode": stat.S_IMODE(st.st_mode),
                "size": 0,
                "sha256": "",
                "symlink_target": "",
                "nlink": st.st_nlink,
            }
        for name in filenames:
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, root_str)
            st = os.lstat(full)
            ftype = _file_type_label(st.st_mode)
            entry: dict[str, Any] = {
                "type": ftype,
                "mode": stat.S_IMODE(st.st_mode),
                "size": st.st_size if ftype == "file" else 0,
                "sha256": "",
                "symlink_target": "",
                "nlink": st.st_nlink,
            }
            if ftype == "file":
                try:
                    entry["sha256"] = _sha256_file(Path(full))
                except PermissionError:
                    entry["sha256"] = "UNREADABLE_PERM"
            elif ftype == "symlink":
                entry["symlink_target"] = os.readlink(full)
            manifest[rel] = entry

    return manifest


def compare_manifests(
    before: dict[str, dict[str, Any]],
    after: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    all_paths = sorted(set(before.keys()) | set(after.keys()))
    missing: list[str] = []
    extra: list[str] = []
    mismatched: list[dict[str, Any]] = []
    matched = 0

    for p in all_paths:
        b = before.get(p)
        a = after.get(p)
        if b is None:
            extra.append(p)
            continue
        if a is None:
            missing.append(p)
            continue
        diffs: dict[str, list[Any]] = {}
        for field in ("type", "mode", "size", "sha256", "symlink_target", "nlink"):
            if b.get(field) != a.get(field):
                diffs[field] = [b.get(field), a.get(field)]
        if diffs:
            mismatched.append({"path": p, "diffs": diffs})
        else:
            matched += 1

    return {
        "total": len(before),
        "matched": matched,
        "missing_count": len(missing),
        "extra_count": len(extra),
        "mismatched_count": len(mismatched),
        "missing_sample": missing[:20],
        "extra_sample": extra[:20],
        "mismatched_sample": mismatched[:20],
        "identical": len(missing) == 0 and len(extra) == 0 and len(mismatched) == 0 and len(before) > 0,
    }


def _extract_pack_physical_bytes(snapshot: dict[str, Any]) -> int:
    srv = snapshot.get("server_metrics")
    if isinstance(srv, dict):
        return int(round(metrics_audit.extract_metric_sum(srv, "bowl_storage_pack_physical_bytes")))
    return 0


def _get_disk_used_bytes(ctx: BenchContext, disk_id: str) -> int:
    try:
        res = ctx.bowl_rpc_cli(["--json", "disk", "get", disk_id], check=True)
        if isinstance(res, dict):
            return int(res.get("used_bytes", 0))
    except Exception:
        return 0
    return 0


def _run_target_fsstress(ctx: BenchContext, target: str, fsstress_bin: Path) -> list[dict[str, Any]]:
    smoke = ctx.profile == "smoke-local"
    ops_n = 2000 if smoke else 10000
    workloads = [
        {"id": "c1", "procs": 1, "ops": ops_n, "seed": 365001},
        {"id": "c2", "procs": 4, "ops": ops_n, "seed": 365002},
    ]

    results_out: list[dict[str, Any]] = []
    workload_metrics: dict[str, Any] = {}
    agg_deltas: dict[str, float] = {}
    audit_failed = False
    audit_reasons: list[str] = []
    all_rc_zero = True

    c2_handle: MountHandle | None = None
    c2_work_dir: Path | None = None

    try:
        for wl in workloads:
            wl_id = wl["id"]
            handle = ctx.prepare_target(target)
            work_dir = handle.mount_dir / f"fsstress_{wl_id}"
            work_dir.mkdir(parents=True, exist_ok=True)

            before = ctx.snapshot_counters(target, handle=handle)
            t0 = time.perf_counter()
            proc = ctx.run_cmd(
                [
                    str(fsstress_bin),
                    "-d",
                    str(work_dir),
                    "-p",
                    str(wl["procs"]),
                    "-n",
                    str(wl["ops"]),
                    "-s",
                    str(wl["seed"]),
                    "-v",
                ],
                timeout=1800,
                check=False,
            )
            wall_s = round(time.perf_counter() - t0, 3)
            after = ctx.snapshot_counters(target, handle=handle)

            combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
            (ctx.suite_dir / f"fsstress_{target}_{wl_id}.log").write_text(combined, encoding="utf-8")

            parsed = parse_fsstress_verbose(combined)
            audit = ctx.audit_case(before, after, target=target, cache_state="C0")
            if audit.get("validity_status") != "PASS":
                audit_failed = True
                audit_reasons.extend(audit.get("reasons", []))
            for k, v in audit.get("deltas", {}).items():
                if isinstance(v, (int, float)):
                    agg_deltas[k] = round(agg_deltas.get(k, 0.0) + float(v), 4)

            if proc.returncode != 0:
                all_rc_zero = False

            workload_metrics[wl_id] = {
                "procs": wl["procs"],
                "ops_per_proc": wl["ops"],
                "seed": wl["seed"],
                "returncode": proc.returncode,
                "wall_s": wall_s,
                "total_calls": parsed["total_calls"],
                "refused_calls": parsed["refused_calls"],
                "distinct_ops": parsed["distinct_ops"],
                "ops": parsed["ops"],
            }

            if wl_id == "c2" and target.startswith("bowl_"):
                c2_handle = handle
                c2_work_dir = work_dir
            else:
                handle.unmount()

        stress_validity = "PASS" if (all_rc_zero and not audit_failed) else "INVALID"
        results_out.append(
            {
                "case_id": "fsstress-single-and-concurrent",
                "target": target,
                "durability_tier": "Tier 0" if target == "bowl_staged" else "Tier 1",
                "cache_state": "C0",
                "runs_n": len(workloads),
                "metrics": workload_metrics,
                "validity_status": stress_validity,
                "integrity_verified": all_rc_zero,
                "counter_deltas": agg_deltas,
                "notes": (
                    f"c1={workload_metrics['c1']['wall_s']}s(rc={workload_metrics['c1']['returncode']}), "
                    f"c2={workload_metrics['c2']['wall_s']}s(rc={workload_metrics['c2']['returncode']})"
                    + ("; audit=" + "; ".join(audit_reasons[:5]) if audit_reasons else "")
                ),
            }
        )

        # Case 2: fsstress-commit-fork-compare (Bowl targets right after c2)
        if c2_handle is not None and c2_work_dir is not None:
            fork_branch = f"fsstress-verify-{int(time.time() * 1000)}"
            disk_id = c2_handle.disk_id or ""
            src_branch = c2_handle.branch_name or "main"
            pre_manifest = collect_tree_manifest(c2_work_dir)
            snap_before_commit = ctx.snapshot_counters(target, handle=c2_handle)
            phys_before = _extract_pack_physical_bytes(snap_before_commit)
            used_before = _get_disk_used_bytes(ctx, disk_id) if disk_id else 0

            drain_s = round(c2_handle.unmount(), 3)
            c2_handle = None

            t_commit0 = time.perf_counter()
            commit_res = ctx.bowl_rpc_cli(
                ["--json", "commit", "create", "-d", disk_id, "--branch", src_branch, "-m", "fsstress-c2-verify"],
                check=True,
            )
            create_commit_s = round(time.perf_counter() - t_commit0, 4)

            snap_after_commit = ctx.snapshot_counters(target, handle=None)
            phys_after = _extract_pack_physical_bytes(snap_after_commit)
            used_after = _get_disk_used_bytes(ctx, disk_id) if disk_id else 0
            added_phys_bytes = max(0, phys_after - phys_before) if phys_after >= phys_before else 0
            if added_phys_bytes == 0 and used_after > 0:
                added_phys_bytes = max(0, used_after - used_before)

            t_fork0 = time.perf_counter()
            fork_res = ctx.bowl_rpc_cli(
                ["--json", "branch", "create", fork_branch, "-d", disk_id, "--from", src_branch, "-m", "fsstress-c2-fork"],
                check=True,
            )
            fork_branch_s = round(time.perf_counter() - t_fork0, 4)

            ro_handle = ctx.prepare_target(target, disk_id=disk_id, branch_name=fork_branch, read_only=True)
            try:
                post_manifest = collect_tree_manifest(ro_handle.mount_dir / "fsstress_c2")
                diff_report = compare_manifests(pre_manifest, post_manifest)
            finally:
                ro_handle.unmount()
            ctx.bowl_rpc_cli(["branch", "delete", fork_branch, "-d", disk_id, "--yes"], check=False)

            audit_cf = ctx.audit_case(snap_before_commit, snap_after_commit, target=target, cache_state="C0")
            cf_ok = bool(diff_report["identical"])
            cf_validity = "PASS" if (cf_ok and audit_cf.get("validity_status") == "PASS") else "INVALID"

            commit_id = commit_res.get("commit_id") if isinstance(commit_res, dict) else None
            fork_commit_id = (
                fork_res.get("commit", {}).get("commit_id") if isinstance(fork_res, dict) else None
            )

            results_out.append(
                {
                    "case_id": "fsstress-commit-fork-compare",
                    "target": target,
                    "durability_tier": "Tier 2",
                    "cache_state": "C2",
                    "runs_n": 1,
                    "metrics": {
                        "drain_time_s": drain_s,
                        "create_commit_s": create_commit_s,
                        "fork_branch_s": fork_branch_s,
                        "commit_id": commit_id,
                        "fork_commit_id": fork_commit_id,
                        "added_physical_bytes": added_phys_bytes,
                        "added_physical_mib": round(added_phys_bytes / (1024.0 * 1024.0), 3),
                        "used_bytes_after_commit": used_after,
                        "tree_compare": diff_report,
                    },
                    "validity_status": cf_validity,
                    "integrity_verified": cf_ok,
                    "counter_deltas": audit_cf.get("deltas", {}),
                    "notes": (
                        f"{diff_report['matched']}/{diff_report['total']} entries IDENTICAL "
                        f"(missing={diff_report['missing_count']}, extra={diff_report['extra_count']}, "
                        f"mismatched={diff_report['mismatched_count']})"
                    ),
                }
            )
    finally:
        if c2_handle is not None:
            c2_handle.unmount()

    return results_out


def _render_summary_markdown(cases: list[dict[str, Any]], smoke: bool) -> str:
    stress_by_target: dict[str, dict[str, Any]] = {}
    cf_by_target: dict[str, dict[str, Any]] = {}
    for c in cases:
        if c["case_id"] == "fsstress-single-and-concurrent":
            stress_by_target[c["target"]] = c
        elif c["case_id"] == "fsstress-commit-fork-compare":
            cf_by_target[c["target"]] = c

    ops_n = 2000 if smoke else 10000
    lines = [
        "| 用例 | 并发 x 操作数 / Seed | `bowl_staged` (Exit / 耗时 / 拒绝数) | `bowl_sync` (Exit / 耗时 / 拒绝数) | `ext4` 参考 (Exit / 耗时 / 拒绝数) | `CreateCommit` 耗时与新增物理字节 | Fork 重挂全树比对 |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for wl_id, procs, seed in (("c1", 1, 365001), ("c2", 4, 365002)):
        cells: list[str] = []
        for t in SUPPORTED_TARGETS:
            sc = stress_by_target.get(t)
            if not sc or sc["validity_status"] == "SKIPPED":
                cells.append("N/A")
                continue
            wm = sc["metrics"].get(wl_id, {})
            cells.append(f"`{wm.get('returncode', '?')}` / {wm.get('wall_s', 0):.2f}s / {wm.get('refused_calls', 0)}")
        if wl_id == "c2":
            commit_parts = []
            cmp_parts = []
            for t in ("bowl_staged", "bowl_sync"):
                cf = cf_by_target.get(t)
                if cf:
                    cm = cf["metrics"]
                    tc = cm.get("tree_compare", {})
                    tag = "staged" if t == "bowl_staged" else "sync"
                    commit_parts.append(f"{tag}: {cm.get('create_commit_s', 0):.3f}s / {cm.get('added_physical_mib', 0):.2f} MiB")
                    status_lbl = "IDENTICAL" if tc.get("identical") else "DIFF"
                    cmp_parts.append(f"{tag}: {tc.get('matched', 0)}/{tc.get('total', 0)} {status_lbl}")
            commit_cell = "; ".join(commit_parts) if commit_parts else "N/A"
            cmp_cell = "; ".join(cmp_parts) if cmp_parts else "N/A"
        else:
            commit_cell = "N/A (see c2)"
            cmp_cell = "N/A (see c2)"

        lines.append(
            f"| `{wl_id}` | `{procs} x {ops_n:,}` / `{seed}` | {cells[0]} | {cells[1]} | {cells[2]} | {commit_cell} | {cmp_cell} |"
        )

    # Per-op comparison table (c2)
    ext4_ops = (
        stress_by_target.get("ext4", {}).get("metrics", {}).get("c2", {}).get("ops", {})
    )
    staged_ops = (
        stress_by_target.get("bowl_staged", {}).get("metrics", {}).get("c2", {}).get("ops", {})
    )
    sync_ops = (
        stress_by_target.get("bowl_sync", {}).get("metrics", {}).get("c2", {}).get("ops", {})
    )
    all_ops = sorted(set(ext4_ops.keys()) | set(staged_ops.keys()) | set(sync_ops.keys()))
    if all_ops:
        lines.append("")
        lines.append("### `fsstress` Syscall 成功率对照（`c2`）")
        lines.append("")
        lines.append("| Syscall | `bowl_staged` (OK/Total, %) | `bowl_sync` (OK/Total, %) | `ext4` (OK/Total, %) | 与 `ext4` 差值 >5% |")
        lines.append("| :--- | :--- | :--- | :--- | :--- |")
        for op in all_ops:
            s_rec = staged_ops.get(op)
            y_rec = sync_ops.get(op)
            e_rec = ext4_ops.get(op)

            def _fmt(r: dict[str, Any] | None) -> str:
                if not r:
                    return "0/0 (N/A)"
                return f"{r['ok']}/{r['total']} ({r['success_rate_pct']:.1f}%)"

            diff_flag = ""
            if e_rec:
                e_rate = e_rec["success_rate_pct"]
                deltas = []
                if s_rec and abs(s_rec["success_rate_pct"] - e_rate) > 5.0:
                    deltas.append(f"staged {s_rec['success_rate_pct'] - e_rate:+.1f}%")
                if y_rec and abs(y_rec["success_rate_pct"] - e_rate) > 5.0:
                    deltas.append(f"sync {y_rec['success_rate_pct'] - e_rate:+.1f}%")
                diff_flag = ", ".join(deltas) if deltas else "OK"
            else:
                diff_flag = "N/A"
            lines.append(f"| `{op}` | {_fmt(s_rec)} | {_fmt(y_rec)} | {_fmt(e_rec)} | {diff_flag} |")

    return "\n".join(lines) + "\n"


def run_suite(ctx: BenchContext) -> dict[str, Any]:
    active = [t for t in ctx.targets if t in SUPPORTED_TARGETS]
    fsstress_bin = _find_fsstress_bin(ctx)
    cases: list[dict[str, Any]] = []

    if fsstress_bin is None:
        for target in active:
            cases.append(
                {
                    "case_id": "fsstress-single-and-concurrent",
                    "target": target,
                    "durability_tier": "Tier 0" if target == "bowl_staged" else "Tier 1",
                    "cache_state": "C0",
                    "runs_n": 0,
                    "metrics": {},
                    "validity_status": "SKIPPED",
                    "integrity_verified": False,
                    "counter_deltas": {},
                    "notes": "fsstress binary not found at /usr/local/bench/bin/fsstress",
                }
            )
        return {"suite": SUITE_ID, "cases": cases, "summary_markdown": "fsstress binary not found; suite skipped.\n"}

    for target in active:
        cases.extend(_run_target_fsstress(ctx, target, fsstress_bin))

    return {
        "suite": SUITE_ID,
        "cases": cases,
        "summary_markdown": _render_summary_markdown(cases, smoke=(ctx.profile == "smoke-local")),
    }
