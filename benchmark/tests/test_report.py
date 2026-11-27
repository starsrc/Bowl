"""Unit tests for benchmark.report covering all 10 suites and Validity Gates."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from benchmark import report
from benchmark.lib.stats import summarize_latency, summarize_steady


def _build_full_synthetic_summary(profile: str = "full-vpc", inject_invalid: bool = False) -> dict:
    suites: dict = {}
    for suite_name in report.SUITE_ORDER:
        cases = [
            {
                "case_id": f"{suite_name}-case-1",
                "target": "bowl_staged",
                "durability_tier": "Tier 1",
                "cache_state": "C2",
                "runs_n": 20,
                "metrics": {
                    "throughput_mibs": summarize_steady([410.0, 420.0, 415.0]),
                    "latency_ms": summarize_latency([float(i) for i in range(1, 21)]),
                },
                "validity_status": "PASS",
                "integrity_verified": True,
                "counter_deltas": {
                    "client_fuse_read_ratio": 0.99,
                    "server_nvme_read_ratio": 0.96,
                    "client_rpc_retry_exhausted": 0,
                    "client_stale_epoch": 0,
                    "client_stage_upload_errors": 0,
                    "server_readyz_ok": True,
                    "client_readyz_ok": True,
                },
                "notes": "ok",
            }
        ]
        if inject_invalid and suite_name == "fio-io":
            cases.append(
                {
                    "case_id": "seq-read-cold-e2e-16j",
                    "target": "bowl_staged",
                    "durability_tier": "Tier 1",
                    "cache_state": "C2",
                    "runs_n": 3,
                    "metrics": {"throughput_mibs": summarize_steady([8000.0, 8100.0, 8050.0])},
                    "validity_status": "INVALID",
                    "integrity_verified": True,
                    "counter_deltas": {"client_fuse_read_ratio": 0.12, "server_nvme_read_ratio": 0.05},
                    "notes": "INVALID_CACHE_LEAK",
                }
            )
        extra_md = ""
        if suite_name == "fsx":
            extra_md = "### `fsx` 启动探测矩阵\n\n| 特性 | bowl_staged | ext4 |\n| :--- | :--- | :--- |\n| `PUNCH_HOLE` | enabled | enabled |"
        elif suite_name == "fsstress":
            extra_md = "### `fsstress` 61-op 成功率差异表\n\n无差异超过 5% 的系统调用。"

        suites[suite_name] = {
            "suite": suite_name,
            "status": "completed",
            "duration_s": 12.34,
            "cases": cases,
            "summary_markdown": extra_md,
        }

    return {
        "meta": {
            "run_id": "20261127T000000Z-test",
            "profile": profile,
            "disclaimer": "smoke-local disclaimer" if profile == "smoke-local" else None,
            "git_commit": "68d787b",
            "started_at_utc": "2026-11-27T00:00:00+00:00",
            "hostname": "bench-client-1",
            "targets": ["ext4", "passthrough_ll", "bowl_staged", "bowl_sync"],
            "topology": {
                "cgroup_parent": "/bench-target.slice",
                "cpu_quota": "400%",
                "memory_max": "16G",
                "memory_swap_max": "0",
                "network": "host",
                "stage_size": "512MiB",
            },
            "env_check": {"passed": True, "nvme_seq_write_mibs": 3100.0, "rtt_ms": 0.25},
        },
        "suites": suites,
        "profiling": [
            {
                "scene": "rand-write-4k-fsync-8j",
                "top_cpu_hotspot": "PackStore.AppendBlocks",
                "top_mutex_hotspot": "syncGate.Lock",
                "l0_sublevels_max": 3,
                "compaction_debt_bytes_max": 1048576,
                "read_amp_max": 4,
                "fuse_conn_waiting_max": 12,
                "archive_path": "runs/20261127T000000Z-test/profiling/rmw/",
            }
        ],
    }


class TestReport(unittest.TestCase):
    def test_generates_all_10_suite_and_top_level_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "runs" / "20261127T000000Z-test"
            run_dir.mkdir(parents=True)
            summary = _build_full_synthetic_summary(profile="full-vpc")
            (run_dir / "summary.json").write_text(json.dumps(summary), encoding="utf-8")

            outputs = report.generate_run_reports(run_dir, update_latest=True, benchmark_root=root)

            self.assertTrue((run_dir / "REPORT.md").exists())
            self.assertTrue((root / "REPORT.md").exists())
            self.assertTrue((root / "latest_summary.json").exists())

            top_md = (run_dir / "REPORT.md").read_text(encoding="utf-8")
            self.assertIn("# Bowl Benchmark v2 评测报告", top_md)
            self.assertIn("## 环境与公平性有效横幅", top_md)
            self.assertIn("## 11. 完整性与反作弊有效性门禁清单", top_md)
            self.assertIn("## 12. 独立剖析与 Gauge 峰值通道摘要", top_md)
            self.assertIn("`fsx` 启动探测矩阵", top_md)
            self.assertIn("`fsstress` 61-op 成功率差异表", top_md)
            self.assertIn("| 全局有效性门禁 |", top_md)
            self.assertIn("`PASS`", top_md)

            for idx, suite_name in enumerate(report.SUITE_ORDER, start=1):
                self.assertIn(f"## {idx}. ", top_md)
                suite_report = run_dir / suite_name / "REPORT.md"
                self.assertTrue(suite_report.exists(), f"missing standalone report for {suite_name}")
                self.assertIn(f"suite:{suite_name}", outputs)

    def test_invalid_case_quarantined_and_fails_global_gate(self) -> None:
        summary = _build_full_synthetic_summary(profile="smoke-local", inject_invalid=True)
        gates = report.evaluate_validity_gates(summary["suites"], summary["meta"])
        self.assertEqual(gates["overall_status"], "FAIL")
        self.assertEqual(len(gates["invalid_cases"]), 1)

        md = report.render_top_level_report(summary)
        self.assertIn("[!WARNING]", md)
        self.assertIn("smoke-local disclaimer", md)
        self.assertIn("以下用例未通过有效性门禁（已从主汇总表剔除）", md)
        self.assertIn("INVALID_CACHE_LEAK", md)

    def test_detects_forged_small_sample_percentile(self) -> None:
        summary = _build_full_synthetic_summary(profile="full-vpc")
        # Inject an illegal forged P95 on n=5
        summary["suites"]["workloads"]["cases"][0]["metrics"]["forged"] = {"n": 5, "p50": 10.0, "p95": 15.0}
        gates = report.evaluate_validity_gates(summary["suites"], summary["meta"])
        self.assertFalse(gates["stats_compliant"])
        self.assertEqual(gates["overall_status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
