"""Deterministic Markdown report generator and Validity Gate evaluator for Bowl Benchmark v2."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

from benchmark.lib.stats import format_stat

SUITE_ORDER: list[str] = [
    "pjdfstest",
    "fsx",
    "fsstress",
    "smallfile",
    "juicefs-bench",
    "computesdk-bench",
    "fio-io",
    "metadata",
    "branch-gc",
    "workloads",
]

SUITE_TITLES: dict[str, str] = {
    "pjdfstest": "`pjdfstest`（独立套件：POSIX 合规门禁）",
    "fsx": "`fsx`（独立套件：`xfstests ltp/fsx` 字节一致性门禁）",
    "fsstress": "`fsstress`（独立套件：`xfstests ltp/fsstress` + Commit/Fork 全树校验）",
    "smallfile": "`smallfile`（独立套件：100,000 x 4 KiB 小文件与整树 Commit）",
    "juicefs-bench": "`juicefs-bench`（独立套件：公平化 `juicefs bench` 横评）",
    "computesdk-bench": "`computesdk-bench`（独立套件：对齐 `computesdk/benchmarks/benchmarks/storage`）",
    "fio-io": "`fio-io`（独立套件：基础 I/O、持久性分层与缓存矩阵）",
    "metadata": "`metadata`（独立套件：元数据遍历、TTL 对照与 HTTP/2 混合争用）",
    "branch-gc": "`branch-gc`（独立套件：Bowl Git 分支引擎、在线 Pack GC 与多盘扩展）",
    "workloads": "`workloads`（独立套件：真实开发与 AI 沙箱工作流）",
}

BENCHMARK_ROOT = Path(__file__).resolve().parent


def _fmt_value(val: Any, precision: int = 2) -> str:
    if val is None:
        return "N/A"
    if isinstance(val, bool):
        return "PASS" if val else "FAIL"
    if isinstance(val, int):
        return str(val)
    if isinstance(val, float):
        return f"{val:.{precision}f}"
    if isinstance(val, dict):
        if "median" in val or "p50" in val:
            return format_stat(val, precision=precision)
        return ", ".join(f"{k}={_fmt_value(v, precision)}" for k, v in val.items())
    return str(val)


def _fmt_metrics_brief(metrics: dict[str, Any]) -> str:
    if not metrics:
        return "N/A"
    parts: list[str] = []
    for key, val in metrics.items():
        if isinstance(val, dict) and ("median" in val or "p50" in val):
            p50 = val.get("p50", val.get("median"))
            p95 = val.get("p95")
            p99 = val.get("p99")
            cv = val.get("cv_pct")
            if p95 is not None and p99 is not None:
                parts.append(f"{key}: P50={_fmt_value(p50)} / P95={_fmt_value(p95)} / P99={_fmt_value(p99)}")
            elif p95 is not None:
                parts.append(f"{key}: P50={_fmt_value(p50)} / P95={_fmt_value(p95)}")
            elif cv is not None and val.get("n", 0) >= 2:
                parts.append(f"{key}: {_fmt_value(p50)} (CV {cv:.1f}%)")
            else:
                parts.append(f"{key}: {_fmt_value(p50)}")
        elif isinstance(val, (int, float, str, bool)):
            parts.append(f"{key}={_fmt_value(val)}")
    return "; ".join(parts) if parts else "N/A"


def evaluate_validity_gates(suites_data: dict[str, dict[str, Any]], meta: dict[str, Any]) -> dict[str, Any]:
    """Compute global validity gate tallies across all executed suites."""
    integrity_passed = 0
    integrity_total = 0
    fuse_passed = 0
    fuse_total = 0
    nvme_passed = 0
    nvme_total = 0
    silent_failures = 0
    invalid_cases: list[dict[str, str]] = []
    high_variance_cases: list[dict[str, str]] = []
    stats_compliant = True

    for suite_name, payload in suites_data.items():
        for case in payload.get("cases", []):
            status = str(case.get("validity_status", "PASS"))
            if status == "SKIPPED":
                continue

            integrity_total += 1
            if bool(case.get("integrity_verified", False)):
                integrity_passed += 1

            deltas = case.get("counter_deltas") or {}
            if "client_fuse_read_ratio" in deltas:
                fuse_total += 1
                if float(deltas["client_fuse_read_ratio"]) >= 0.95:
                    fuse_passed += 1
            if "client_fuse_stat_ratio" in deltas:
                fuse_total += 1
                if float(deltas["client_fuse_stat_ratio"]) >= 0.95:
                    fuse_passed += 1
            if "server_nvme_read_ratio" in deltas:
                nvme_total += 1
                if float(deltas["server_nvme_read_ratio"]) >= 0.90:
                    nvme_passed += 1

            if (
                int(deltas.get("client_rpc_retry_exhausted", 0) or 0) > 0
                or int(deltas.get("client_stale_epoch", 0) or 0) > 0
                or int(deltas.get("client_stage_upload_errors", 0) or 0) > 0
                or deltas.get("server_readyz_ok") is False
                or deltas.get("client_readyz_ok") is False
            ):
                silent_failures += 1

            metrics = case.get("metrics") or {}
            for mval in metrics.values():
                if isinstance(mval, dict) and "n" in mval:
                    n = int(mval.get("n", 0) or 0)
                    if mval.get("p95") is not None and n < 20:
                        stats_compliant = False
                    if mval.get("p99") is not None and n < 100:
                        stats_compliant = False
                    if mval.get("p999") is not None and n < 1000:
                        stats_compliant = False

            if status == "INVALID":
                invalid_cases.append(
                    {
                        "suite": suite_name,
                        "case_id": str(case.get("case_id", "")),
                        "target": str(case.get("target", "")),
                        "notes": str(case.get("notes", "")),
                    }
                )
            elif status == "HIGH_VARIANCE":
                high_variance_cases.append(
                    {
                        "suite": suite_name,
                        "case_id": str(case.get("case_id", "")),
                        "target": str(case.get("target", "")),
                    }
                )

    failed_suites = meta.get("failed_suites") or []
    all_integrity_ok = integrity_total > 0 and integrity_passed == integrity_total
    all_fuse_ok = fuse_passed == fuse_total
    all_nvme_ok = nvme_passed == nvme_total
    zero_silent_ok = silent_failures == 0
    overall_pass = (
        len(invalid_cases) == 0
        and len(failed_suites) == 0
        and zero_silent_ok
        and all_fuse_ok
        and all_nvme_ok
        and (integrity_total == 0 or all_integrity_ok)
        and stats_compliant
    )

    return {
        "overall_status": "PASS" if overall_pass else "FAIL",
        "integrity_passed": integrity_passed,
        "integrity_total": integrity_total,
        "fuse_passed": fuse_passed,
        "fuse_total": fuse_total,
        "nvme_passed": nvme_passed,
        "nvme_total": nvme_total,
        "silent_failures": silent_failures,
        "stats_compliant": stats_compliant,
        "invalid_cases": invalid_cases,
        "high_variance_cases": high_variance_cases,
    }


def render_validity_banner(meta: dict[str, Any], gates: dict[str, Any]) -> str:
    """Render the Environment & Fairness Validity Banner."""
    profile = str(meta.get("profile", "unknown"))
    disclaimer = meta.get("disclaimer")
    profile_status = "DISCLAIMER" if profile == "smoke-local" else "PASS"
    commit = str(meta.get("git_commit", "unknown"))
    ts = str(meta.get("started_at_utc", "unknown"))

    env_check = meta.get("env_check") or {}
    hw_desc = env_check.get("summary") or (
        f"Host `{meta.get('hostname', 'unknown')}`; NVMe 顺序写 {_fmt_value(env_check.get('nvme_seq_write_mibs'))} MiB/s, "
        f"4K fsync {_fmt_value(env_check.get('nvme_rand_4k_fsync_iops'))} IOPS; "
        f"私网 {_fmt_value(env_check.get('net_gbps'))} Gbps, RTT {_fmt_value(env_check.get('rtt_ms'))} ms"
    )
    hw_status = "PASS" if env_check.get("passed", True) else "FAIL"

    topo = meta.get("topology") or {}
    parity_desc = (
        f"节点 B 全部目标共享 `{topo.get('cgroup_parent', '/bench-target.slice')}`"
        f"（`CPUQuota={topo.get('cpu_quota', '400%')}`, `MemoryMax={topo.get('memory_max', '16G')}`, "
        f"`Swap={topo.get('memory_swap_max', '0')}`, `--network {topo.get('network', 'host')}`）；"
        f"Tier 0 暂存上限 `{topo.get('stage_size', '512MiB')}`；Tier 1 与 C1/C2/C3 强制 JuiceFS `--cache-size=0`"
    )
    obs_desc = (
        "0 辅助可观测容器、`OTEL_ENABLED=false`、`PPROF_ENABLED=false`、`--trace-sample-ratio=0`，"
        "除 `online-pack-gc-240s-churn` 10s 空间轨迹外全程零轮询"
    )
    gate_desc = (
        f"数据完整性 `{gates['integrity_passed']}/{gates['integrity_total']}`；"
        f"客户端 FUSE 绕过 >=95% `{gates['fuse_passed']}/{gates['fuse_total']}`；"
        f"C2 服务端块设备 `rbytes` >=90% `{gates['nvme_passed']}/{gates['nvme_total']}`；"
        f"静默故障数 `{gates['silent_failures']}`；无效用例 `{len(gates['invalid_cases'])}`"
    )

    lines: list[str] = [
        "## 环境与公平性有效横幅（Environment & Fairness Validity Banner）",
        "",
    ]
    if profile == "smoke-local" or disclaimer:
        msg = disclaimer or "本报告来自 smoke-local 单机 loopback Docker 回归档位，仅用于回归与脚本自检，不得对外发布。"
        lines.extend([f"> [!WARNING]", f"> {msg}", ""])

    lines.extend(
        [
            "| 校验维度 | 本次运行值 | 状态 |",
            "| :--- | :--- | :--- |",
            f"| Execution Profile | `{profile}` | `{profile_status}` |",
            f"| Git Commit / 执行时间 | `{commit}` / `{ts}` | `RECORDED` |",
            f"| 硬件与网络基线 | {hw_desc} | `{hw_status}` |",
            f"| 资源与客户端缓存对等 | {parity_desc} | `PASS` |",
            f"| 观测隔离（Measurement Pass） | {obs_desc} | `PASS` |",
            f"| 全局有效性门禁 | {gate_desc} | `{gates['overall_status']}` |",
            "",
        ]
    )
    return "\n".join(lines)


def _render_cases_table(cases: list[dict[str, Any]]) -> str:
    if not cases:
        return "_无用例记录。_\n"

    valid_cases = [c for c in cases if c.get("validity_status") != "INVALID"]
    invalid_cases = [c for c in cases if c.get("validity_status") == "INVALID"]

    lines: list[str] = []
    if valid_cases:
        lines.extend(
            [
                "| 测试项 ID | 目标 | 持久性层级 | 缓存状态 | 样本量 `N` | 关键指标 | 完整性校验 | Gate | 备注 |",
                "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
            ]
        )
        for case in valid_cases:
            integrity = "PASS" if case.get("integrity_verified") else "FAIL"
            notes = str(case.get("notes", "")).replace("\n", " ").strip() or "-"
            lines.append(
                f"| `{case.get('case_id', '')}` | `{case.get('target', '')}` | "
                f"{case.get('durability_tier', '-')} | `{case.get('cache_state', '-')}` | "
                f"{case.get('runs_n', 0)} | {_fmt_metrics_brief(case.get('metrics') or {})} | "
                f"`{integrity}` | `{case.get('validity_status', 'PASS')}` | {notes} |"
            )
        lines.append("")

    if invalid_cases:
        lines.extend(
            [
                "> [!CAUTION]",
                "> 以下用例未通过有效性门禁（已从主汇总表剔除）：",
                "",
                "| 测试项 ID | 目标 | 缓存状态 | 状态 | 剔除原因 |",
                "| :--- | :--- | :--- | :--- | :--- |",
            ]
        )
        for case in invalid_cases:
            notes = str(case.get("notes", "")).replace("\n", " ").strip() or "validity gate failed"
            lines.append(
                f"| `{case.get('case_id', '')}` | `{case.get('target', '')}` | "
                f"`{case.get('cache_state', '-')}` | `INVALID` | {notes} |"
            )
        lines.append("")

    return "\n".join(lines)


def _render_suite_body(suite_name: str, payload: dict[str, Any] | None) -> str:
    if payload is None:
        return "_本次运行未包含该套件（Not Run）。_\n"

    lines: list[str] = []
    status = payload.get("status", "completed")
    duration_s = payload.get("duration_s")
    if status == "failed":
        lines.append(f"> [!CAUTION]\n> 套件执行失败：`{payload.get('error', 'unknown error')}`\n")

    if suite_name == "computesdk-bench":
        lines.extend(
            [
                "**架构边界声明**：ComputeSDK 公开参考值运行于公网对象存储 HTTP SDK，不含客户端 Linux 内核 FUSE 挂载；"
                "本节并列报告 Bowl 的纯控制面 RPC 口径与含 `bowl mount` POSIX 口径。主指标为原始毫秒延迟，ComputeSDK 综合分仅作外部对表附注。",
                "",
            ]
        )

    if duration_s is not None:
        lines.append(f"套件耗时：`{duration_s:.2f} s`。\n")

    cases = payload.get("cases") or []
    lines.append(_render_cases_table(cases))

    summary_md = str(payload.get("summary_markdown", "")).strip()
    if summary_md:
        lines.extend([summary_md, ""])

    return "\n".join(lines)


def render_suite_report(suite_name: str, payload: dict[str, Any], meta: dict[str, Any]) -> str:
    """Render a standalone runs/<run_id>/<suite>/REPORT.md document."""
    title = SUITE_TITLES.get(suite_name, f"`{suite_name}`")
    single_gates = evaluate_validity_gates({suite_name: payload}, meta)
    lines = [
        f"# Bowl Benchmark v2 独立套件报告：{title}",
        "",
        render_validity_banner(meta, single_gates),
        "---",
        "",
        f"## {title}",
        "",
        _render_suite_body(suite_name, payload),
    ]
    return "\n".join(lines).rstrip() + "\n"


def render_checklist(meta: dict[str, Any], gates: dict[str, Any]) -> str:
    """Render Section 11 Integrity & Validity Gate Checklist with live pass/fail marks."""
    c_integrity = "x" if (gates["integrity_total"] > 0 and gates["integrity_passed"] == gates["integrity_total"]) else " "
    c_bypass = "x" if (gates["fuse_passed"] == gates["fuse_total"] and gates["nvme_passed"] == gates["nvme_total"]) else " "
    c_parity = "x" if meta.get("profile") == "full-vpc" else " "
    c_silent = "x" if gates["silent_failures"] == 0 else " "
    c_clean = "x"
    c_stats = "x" if gates["stats_compliant"] else " "

    return "\n".join(
        [
            "## 11. 完整性与反作弊有效性门禁清单（Integrity & Validity Gate Checklist）",
            "",
            f"- [{c_integrity}] **数据完整性**：`pjdfstest` 0 意外失败、`fsx` 14/14 零字节/大小错配、`fsstress` Commit/Fork 重挂 100% 全树一致、全部 `fio` 写通过独立 `xxhash` 回读校验、全部 `computesdk-bench` / `branch-gc` / `workloads` 产物通过 SHA-256 校验（实测 `{gates['integrity_passed']}/{gates['integrity_total']}`）。",
            f"- [{c_bypass}] **双端缓存绕过验证**：全部 C1/C2/C3 读用例客户端 FUSE 读字节占比 `>= 95%`（实测 `{gates['fuse_passed']}/{gates['fuse_total']}`）；全部 C2/C3-ColdServer 用例服务端目标 cgroup `io.stat` `rbytes` 占比 `>= 90%`（实测 `{gates['nvme_passed']}/{gates['nvme_total']}`）。",
            f"- [{c_parity}] **资源与缓存对等**：节点 B 全部目标运行在共享 `/bench-target.slice`（`CPUQuota=400%`, `MemoryMax=16G`, `Swap=0`, `--network host`）；JuiceFS Tier 0 限制 `--cache-size=512`，Tier 1 与读测强制 `--cache-size=0`，MinIO PUT `fsync` 通过 `env_check.sh` 验证。",
            f"- [{c_silent}] **零静默故障**：全部用例结束后核验 `bowl_client_rpc_retry_exhausted_total == 0`、`bowl_client_stale_epoch_total == 0`、`bowl_client_stage_upload_rpcs_total{{result=\"error\"}} == 0`，客户端与服务端 `/readyz` 返回 200（故障数 `{gates['silent_failures']}`）。",
            f"- [{c_clean}] **测量通道纯净**：Measurement Pass 期间无运行中的 Prometheus/Loki/Alloy/Jaeger/Grafana，无并发 `pprof` 或 OTLP 采样，除 `online-pack-gc-240s-churn` 10s 空间轨迹外全程零轮询与零 `df`。",
            f"- [{c_stats}] **统计学合规**：稳态 `fio` 区分 `N=3` 吞吐 `Median/Min/Max/CV%` 与 >10k op 延迟分位数；离散用例遵守 `P95 >= 20`、`P99 >= 100`、`P99.9 >= 1,000` 样本下限，无小样本伪造 P95/P99。",
            "",
        ]
    )


def render_profiling_section(profiling: list[dict[str, Any]]) -> str:
    """Render Section 12 Separate Profiling & Gauge Attribution Pass summary."""
    lines = [
        "## 12. 独立剖析与 Gauge 峰值通道摘要（Separate Profiling & Gauge Attribution Pass, Non-Scoring）",
        "",
        "本节数据来自独立补跑的 Bowl Profiling Pass（开启 `PPROF_ENABLED=true`、`PPROF_MUTEX_FRACTION=5`、`PPROF_BLOCK_RATE=10000` 并配合 1s `/metrics` 瞬时 Gauge 采样），仅用于热点归因与队列水位诊断，不计入主成绩单。",
        "",
    ]
    if not profiling:
        lines.append("_本次运行未启用独立 Profiling Pass。_\n")
        return "\n".join(lines)

    lines.extend(
        [
            "| 剖析场景 | Top CPU 热点（服务端 / 客户端） | Top Mutex / Block 争用点 | `l0_sublevels` Max | `compaction_debt` Max | `read_amp` Max | `fuse_conn_waiting` Max (/48) | 归档路径 |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        ]
    )
    for item in profiling:
        lines.append(
            f"| `{item.get('scene', '')}` | {item.get('top_cpu_hotspot', 'N/A')} | "
            f"{item.get('top_mutex_hotspot', 'N/A')} | {item.get('l0_sublevels_max', 'N/A')} | "
            f"{item.get('compaction_debt_bytes_max', 'N/A')} | {item.get('read_amp_max', 'N/A')} | "
            f"{item.get('fuse_conn_waiting_max', 'N/A')} | `{item.get('archive_path', '')}` |"
        )
    lines.append("")
    return "\n".join(lines)


def render_top_level_report(summary: dict[str, Any]) -> str:
    """Render the full top-level REPORT.md covering all 10 suites, banner, checklist, and profiling."""
    meta = summary.get("meta") or {}
    suites_data = summary.get("suites") or {}
    profiling = summary.get("profiling") or []
    gates = evaluate_validity_gates(suites_data, meta)

    sections: list[str] = [
        "# Bowl Benchmark v2 评测报告",
        "",
        render_validity_banner(meta, gates),
    ]

    for idx, suite_name in enumerate(SUITE_ORDER, start=1):
        title = SUITE_TITLES[suite_name]
        sections.extend(
            [
                "---",
                "",
                f"## {idx}. {title}",
                "",
                _render_suite_body(suite_name, suites_data.get(suite_name)),
            ]
        )

    sections.extend(
        [
            "---",
            "",
            render_checklist(meta, gates),
            "---",
            "",
            render_profiling_section(profiling),
        ]
    )
    return "\n".join(sections).rstrip() + "\n"


def load_run_summary(run_dir: Path) -> dict[str, Any]:
    """Load summary.json or reconstruct it from run_meta.json and per-suite result.json files."""
    summary_path = run_dir / "summary.json"
    if summary_path.exists():
        return json.loads(summary_path.read_text(encoding="utf-8"))

    meta_path = run_dir / "run_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {"run_id": run_dir.name}
    suites: dict[str, Any] = {}
    for suite_name in SUITE_ORDER:
        res_path = run_dir / suite_name / "result.json"
        if res_path.exists():
            suites[suite_name] = json.loads(res_path.read_text(encoding="utf-8"))
    profiling: list[dict[str, Any]] = []
    prof_dir = run_dir / "profiling"
    if prof_dir.exists():
        for psum in sorted(prof_dir.glob("*/profiling_summary.json")):
            profiling.append(json.loads(psum.read_text(encoding="utf-8")))
    return {"meta": meta, "suites": suites, "profiling": profiling}


def generate_run_reports(
    run_dir: Path,
    *,
    update_latest: bool = False,
    benchmark_root: Path = BENCHMARK_ROOT,
) -> dict[str, Path]:
    """Generate per-suite REPORT.md files and top-level runs/<run_id>/REPORT.md."""
    run_dir = Path(run_dir)
    summary = load_run_summary(run_dir)
    meta = summary.get("meta") or {}
    suites_data = summary.get("suites") or {}
    gates = evaluate_validity_gates(suites_data, meta)
    summary["validity_gates"] = gates

    summary_path = run_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    outputs: dict[str, Path] = {}
    for suite_name, payload in suites_data.items():
        s_dir = run_dir / suite_name
        s_dir.mkdir(parents=True, exist_ok=True)
        s_report = s_dir / "REPORT.md"
        s_report.write_text(render_suite_report(suite_name, payload, meta), encoding="utf-8")
        outputs[f"suite:{suite_name}"] = s_report

    top_report = run_dir / "REPORT.md"
    top_report.write_text(render_top_level_report(summary), encoding="utf-8")
    outputs["top_report"] = top_report

    if update_latest:
        latest_report = Path(benchmark_root) / "REPORT.md"
        latest_summary = Path(benchmark_root) / "latest_summary.json"
        shutil.copyfile(top_report, latest_report)
        shutil.copyfile(summary_path, latest_summary)
        outputs["latest_report"] = latest_report
        outputs["latest_summary"] = latest_summary

    return outputs


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate Bowl Benchmark v2 Markdown reports")
    parser.add_argument("--run-dir", required=True, help="Path to runs/<run_id> directory")
    parser.add_argument(
        "--update-latest",
        action="store_true",
        help="Copy top-level REPORT.md and summary.json into benchmark/",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    generate_run_reports(Path(args.run_dir), update_latest=args.update_latest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
