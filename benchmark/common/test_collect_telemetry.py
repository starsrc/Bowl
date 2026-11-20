#!/usr/bin/env python3
"""验证 `benchmark/common/collect_telemetry.py` 公开接口契约与极值表现。"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

BENCH_ROOT = Path(__file__).resolve().parent.parent
if str(BENCH_ROOT) not in sys.path:
    sys.path.insert(0, str(BENCH_ROOT))

from common import collect_telemetry  # noqa: E402


class CollectTelemetryContractTest(unittest.TestCase):
    """覆盖 `collect_telemetry` 遥测解析、聚合与渲染契约。"""

    def test_given_26_buckets_with_heavy_tail_when_computing_quantiles_then_returns_expected_bounds(self) -> None:
        # Given: 100 次调用分布在第 10 桶 (<=1024us)、第 22 桶 (<=4194304us) 与第 25 桶 (<=33554432us)
        buckets = [0] * 10 + [90] + [0] * 11 + [9] + [0] * 2 + [1]

        # When: 计算 P50、P95 与 P100 分位数
        p50 = collect_telemetry.noncum_quantile_us(buckets, 100, 0.50)
        p95 = collect_telemetry.noncum_quantile_us(buckets, 100, 0.95)
        p100 = collect_telemetry.noncum_quantile_us(buckets, 100, 1.00)

        # Then: 精确落入对应对数桶区间
        self.assertEqual(len(collect_telemetry.CLIENT_HIST_UPPER_US), 26)
        self.assertEqual(collect_telemetry.CLIENT_HIST_UPPER_US[0], 1.0)
        self.assertEqual(collect_telemetry.CLIENT_HIST_UPPER_US[25], 33554432.0)
        self.assertGreater(p50, 512.0)
        self.assertLessEqual(p50, 1024.0)
        self.assertGreater(p95, 2097152.0)
        self.assertLessEqual(p95, 4194304.0)
        self.assertEqual(p100, 33554432.0)

    def test_given_empty_buckets_or_zero_calls_when_computing_quantile_then_returns_zero(self) -> None:
        # Given: 空桶数组与零调用计数
        # When / Then: 返回 0.0 且不抛异常
        self.assertEqual(collect_telemetry.noncum_quantile_us([], 0, 0.95), 0.0)
        self.assertEqual(collect_telemetry.noncum_quantile_us([], 10, 0.95), 0.0)
        self.assertEqual(collect_telemetry.noncum_quantile_us([0, 5, 0], 0, 0.50), 0.0)

    def test_given_client_fuse_and_rpc_spans_when_distilling_trace_then_same_hop_delta_uses_rpc_span(self) -> None:
        # Given: `fuse.Create` 耗时 9.0ms（含 6.6ms 本地开销与 2.4ms 客户端 RPC），存储节点 RPC 耗时 1.9ms
        trace_obj = {
            "traceID": "deadbeef12345678deadbeef12345678",
            "processes": {
                "p1": {"serviceName": "bowl-client"},
                "p2": {"serviceName": "bowl-storage"},
            },
            "spans": [
                {
                    "traceID": "deadbeef12345678deadbeef12345678",
                    "spanID": "root_fuse",
                    "operationName": "fuse.Create",
                    "startTime": 1700000000000000,
                    "duration": 9000,
                    "processID": "p1",
                    "references": [],
                    "tags": [{"key": "fuse.op", "value": "Create"}],
                },
                {
                    "traceID": "deadbeef12345678deadbeef12345678",
                    "spanID": "cli_rpc",
                    "operationName": "bowl.storage.v1.StorageService/Create",
                    "startTime": 1700000000006600,
                    "duration": 2400,
                    "processID": "p1",
                    "references": [{"refType": "CHILD_OF", "spanID": "root_fuse"}],
                    "tags": [{"key": "rpc.method", "value": "bowl.storage.v1.StorageService/Create"}],
                },
                {
                    "traceID": "deadbeef12345678deadbeef12345678",
                    "spanID": "srv_rpc",
                    "operationName": "bowl.storage.v1.StorageService/Create",
                    "startTime": 1700000000006850,
                    "duration": 1900,
                    "processID": "p2",
                    "references": [
                        {"refType": "CHILD_OF", "spanID": "cli_rpc"},
                        {"refType": "FOLLOWS_FROM", "spanID": "batch_link"},
                    ],
                    "tags": [{"key": "rpc.method", "value": "bowl.storage.v1.StorageService/Create"}],
                },
            ],
        }

        # When: 蒸馏单条 Jaeger 链路
        distilled = collect_telemetry.distill_jaeger_trace(trace_obj)

        # Then: 网络与编解码差值为 2.4 - 1.9 = 0.5ms，而非 9.0 - 1.9
        self.assertIsNotNone(distilled)
        assert distilled is not None
        self.assertAlmostEqual(distilled["total_duration_ms"], 9.0, places=3)
        self.assertAlmostEqual(distilled["client_max_span_ms"], 9.0, places=3)
        self.assertAlmostEqual(distilled["client_rpc_span_ms"], 2.4, places=3)
        self.assertAlmostEqual(distilled["server_rpc_span_ms"], 1.9, places=3)
        self.assertAlmostEqual(distilled["rpc_network_codec_ms"], 0.5, places=3)
        self.assertAlmostEqual(distilled["client_local_overhead_ms"], 6.6, places=3)
        self.assertEqual(distilled["span_links_count"], 1)

    def test_given_trace_starting_at_zero_epoch_with_longer_child_when_distilling_then_wall_covers_child_end(self) -> None:
        # Given: 根 Span startTime=0 耗时 2.0ms，子 Span 异步延续至 6.0ms
        trace_obj = {
            "traceID": "00000000000000000000000000000001",
            "processes": {"p1": {"serviceName": "bowl-client"}, "p2": {"serviceName": "bowl-storage"}},
            "spans": [
                {
                    "spanID": "s_root",
                    "operationName": "fuse.Flush",
                    "startTime": 0,
                    "duration": 2000,
                    "processID": "p1",
                    "references": [],
                    "tags": [],
                },
                {
                    "spanID": "s_child",
                    "operationName": "bowl.storage.v1.StorageService/Write",
                    "startTime": 2500,
                    "duration": 3500,
                    "processID": "p2",
                    "references": [{"refType": "FOLLOWS_FROM", "spanID": "s_root"}],
                    "tags": [{"key": "rpc.method", "value": "Write"}],
                },
            ],
        }

        # When: 蒸馏链路
        distilled = collect_telemetry.distill_jaeger_trace(trace_obj)

        # Then: total_duration_ms 应取 6.0ms 而非被 startTime=0 误判为 2.0ms
        self.assertIsNotNone(distilled)
        assert distilled is not None
        self.assertAlmostEqual(distilled["total_duration_ms"], 6.0, places=3)

    def test_given_server_span_before_client_rpc_span_when_distilling_then_ignores_fuse_root_parent(self) -> None:
        # Given: `spans` 数组中存储节点 Span 排在独立客户端 RPC Span 之前，且另一条路径引用了 `fuse` 根 Span
        trace_obj = {
            "traceID": "00000000000000000000000000000002",
            "processes": {"p1": {"serviceName": "bowl-client"}, "p2": {"serviceName": "bowl-storage"}},
            "spans": [
                {
                    "spanID": "srv_write",
                    "operationName": "bowl.storage.v1.StorageService/Write",
                    "startTime": 1000200,
                    "duration": 1000,
                    "processID": "p2",
                    "references": [{"refType": "CHILD_OF", "spanID": "root_fuse"}],
                    "tags": [{"key": "rpc.method", "value": "bowl.storage.v1.StorageService/Write"}],
                },
                {
                    "spanID": "root_fuse",
                    "operationName": "fuse.Write",
                    "startTime": 1000000,
                    "duration": 8000,
                    "processID": "p1",
                    "references": [],
                    "tags": [],
                },
                {
                    "spanID": "cli_write",
                    "operationName": "bowl.storage.v1.StorageService/Write",
                    "startTime": 1000100,
                    "duration": 1500,
                    "processID": "p1",
                    "references": [{"refType": "CHILD_OF", "spanID": "root_fuse"}],
                    "tags": [{"key": "rpc.method", "value": "bowl.storage.v1.StorageService/Write"}],
                },
            ],
        }

        # When: 蒸馏链路
        distilled = collect_telemetry.distill_jaeger_trace(trace_obj)

        # Then: 无论 spans 顺序如何，绝不能将 8.0ms 的 fuse.Write 误当成同跳客户端 RPC Span
        self.assertIsNotNone(distilled)
        assert distilled is not None
        self.assertNotEqual(distilled["client_rpc_span_ms"], 8.0)

    def test_given_empty_or_malformed_prom_file_when_parsing_then_skips_invalid_lines(self) -> None:
        # Given: 包含空行、注释、NaN、+Inf、畸形数字与正常指标的临时 .prom 文件
        with tempfile.TemporaryDirectory() as tmp:
            prom_path = Path(tmp) / "malformed.prom"
            prom_path.write_text(
                "# HELP m desc\n"
                "\n"
                'bowl_metric{le="0.5"} NaN\n'
                'bowl_metric{le="1.0"} 1.2.3\n'
                'bowl_metric{le="bad"} 4\n'
                'bowl_metric_bucket{le="0.5"} 3\n'
                'bowl_metric_bucket{le="bad"} 2\n'
                'bowl_metric_bucket{le="NaN"} 2\n'
                'bowl_metric_bucket{le="+Inf"} 3\n'
                'bowl_metric_sum 0.6\n'
                'bowl_metric_count 3\n',
                encoding="utf-8",
            )
            empty_path = Path(tmp) / "empty.prom"
            empty_path.write_text("", encoding="utf-8")

            # When: 解析空文件与含畸形行文件并汇总直方图
            empty_parsed = collect_telemetry.parse_prom_text(empty_path)
            parsed = collect_telemetry.parse_prom_text(prom_path)
            hist = collect_telemetry.summarize_prom_histogram(parsed, "bowl_metric", scale=1000.0)

            # Then: 空文件返回空映射，畸形行被跳过且有效直方图正常汇总
            self.assertEqual(empty_parsed, {})
            self.assertEqual(len(hist), 1)
            self.assertEqual(hist[0]["count"], 3)
            self.assertAlmostEqual(hist[0]["mean"], 200.0, places=3)

    def test_given_null_fields_in_client_snapshots_when_diffing_then_handles_gracefully(self) -> None:
        # Given: pre/post 快照中包含显式 null 字段
        pre = {
            "timestamp_unix_nano": None,
            "uptime_seconds": None,
            "cpu_usage_seconds": None,
            "fuse": {"ops": [{"op": "Write", "calls": None, "buckets": None}]},
        }
        post = {
            "timestamp_unix_nano": 5000000000,
            "uptime_seconds": 5.0,
            "cpu_usage_seconds": 1.0,
            "fuse": {
                "read_bytes": None,
                "write_bytes": None,
                "ops": [{"op": "Write", "calls": 10, "errors": 0, "bytes": 4096, "duration_nanos": 2000000, "buckets": None}],
            },
        }

        # When: 计算快照差分
        delta = collect_telemetry.diff_client_snapshots(pre, post)

        # Then: 不抛异常且返回有效差分
        self.assertEqual(delta["fuse_aggregate"]["calls"], 10)
        self.assertAlmostEqual(delta["fuse_active_ms"], 2.0, places=3)

    def test_given_malformed_jsonl_lines_when_loading_stage_timings_then_skips_bad_rows(self) -> None:
        # Given: stage_timings.jsonl 含空行、畸形 JSON、非数字 duration_sec 与别名阶段
        with tempfile.TemporaryDirectory() as tmp:
            res_dir = Path(tmp) / "results"
            res_dir.mkdir(parents=True)
            (res_dir / "stage_timings.jsonl").write_text(
                "\n"
                "{not-json\n"
                + json.dumps({"stage": "bad_dur", "duration_sec": "invalid"}) + "\n"
                + json.dumps({"stage": "cross_compile_binaries", "duration_sec": 99.0}) + "\n"
                + json.dumps({"target": "jfs_pg", "stage": "fio_seq_write", "duration_sec": 88.0}) + "\n"
                + json.dumps({"stage": "bowl_juicefs_bench", "duration_sec": 12.5}) + "\n"
                + json.dumps({"stage": "jfs_bench", "duration_sec": 12.5}) + "\n"
                + json.dumps({"directory": "mkdir", "duration_sec": 1.1}) + "\n",
                encoding="utf-8",
            )

            # When: 加载并过滤排序
            rows = collect_telemetry.load_stage_timings(res_dir)

            # Then: 仅保留合法的 Bowl 阶段且别名已去重
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["stage"], "jfs_bench")
            self.assertEqual(rows[1]["directory"], "mkdir")

    def test_given_async_pipeline_and_epoch_hierarchy_when_building_breakdowns_then_caps_share_and_links_parent(self) -> None:
        # Given: 父阶段 d9_cold_io 包含子阶段 d9_cold_seq_read，异步 RPC 累计耗时高于 FUSE 活跃耗时
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "telemetry"
            out_dir.mkdir(parents=True)
            cli_summary = {
                "phase_deltas": {
                    "d9_cold_io": {
                        "pre_epoch": 100.0,
                        "post_epoch": 130.0,
                        "window_sec": 30.0,
                        "fuse_active_ms": 5000.0,
                        "fuse_aggregate": {"calls": 1000, "total_ms": 5000.0, "mean_ms": 5.0, "p50_ms": 3.0, "p99_ms": 12.0, "errors": 0},
                    },
                    "d9_cold_seq_read": {
                        "pre_epoch": 105.0,
                        "post_epoch": 115.0,
                        "window_sec": 10.0,
                        "fuse_active_ms": 1000.0,
                        "fuse_aggregate": {"calls": 256, "total_ms": 1000.0, "mean_ms": 3.9, "p50_ms": 0.1, "p99_ms": 8.0, "errors": 0},
                    },
                }
            }
            prom_summary = {
                "client_engine": {
                    "phase_deltas": {
                        "d9_cold_seq_read": {
                            "rpc_client_latency_ms": [{"group": "Read", "count": 256, "total": 4075.0, "mean": 15.9, "p50": 14.0, "p99": 28.0}],
                        },
                    }
                },
                "server_engine": {
                    "phase_deltas": {
                        "d9_cold_seq_read": {
                            "http_request_latency_ms": [{"group": "Read", "count": 256, "total": 3200.0, "mean": 12.5, "p50": 10.0, "p99": 22.0}],
                            "rpc_server_latency_ms": [{"group": "Read", "count": 256, "total": 500.0, "mean": 1.95, "p50": 1.5, "p99": 4.0}],
                        }
                    }
                },
            }

            # When: 生成逐跳拆解并渲染 Markdown
            breakdowns = collect_telemetry.build_phase_hop_breakdowns(cli_summary, prom_summary, [], out_dir)
            md = "\n".join(collect_telemetry.render_phase_deltas_markdown(breakdowns))

            # Then: 父子关系与 100% 占比上限均准确生效
            seq_bd = breakdowns["d9_cold_seq_read"]
            self.assertEqual(seq_bd["parent_phase"], "d9_cold_io")
            self.assertEqual(breakdowns["d9_cold_io"]["child_phases"], ["d9_cold_seq_read"])
            self.assertAlmostEqual(seq_bd["async_rpc_to_fuse_ratio"], 4.08, places=2)
            self.assertLessEqual(seq_bd["hops"][0]["share_pct"], 100.0)
            self.assertLessEqual(seq_bd["hops"][2]["share_pct"], 100.0)
            self.assertIn("父汇总阶段 · 含子阶段 `d9_cold_seq_read`", md)
            self.assertIn("子阶段 · 隶属 `d9_cold_io`", md)
            self.assertIn("异步并发流水线 (累计为 FUSE 活跃 4.08x)", md)

    def test_given_loki_with_noise_and_benign_pebble_events_when_processing_logs_then_filters_alerts(self) -> None:
        # Given: Loki 结果含观测自身噪音、err="" 的正常 Pebble 事件、真实告警以及原始 panic
        with tempfile.TemporaryDirectory() as tmp:
            stage_dir = Path(tmp) / "stage"
            out_dir = Path(tmp) / "out"
            loki_dir = stage_dir / "server" / "loki"
            loki_dir.mkdir(parents=True)
            out_dir.mkdir(parents=True)
            loki_payload = {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "stream": {"service": "loki"},
                            "values": [["1000000000", 'level=warn msg="self noise"']],
                        },
                        {
                            "stream": {"service": "pebble_events"},
                            "values": [
                                ["1000000001", json.dumps({"event": "wal_created", "err": ""})],
                                ["1000000002", json.dumps({"event": "write_stall_begin", "reason": "memtable"})],
                                ["1000000003", json.dumps({"event": "table_deleted", "err": "enosys"})],
                            ],
                        },
                        {
                            "stream": {"service": "bowl-storage"},
                            "values": [
                                ["1000000004", json.dumps({"level": "INFO", "msg": "ok", "err": ""})],
                                ["1000000005", json.dumps({"level": "WARN", "msg": "slow tick"})],
                                ["1000000006", "panic: fatal crash"],
                            ],
                        },
                    ]
                },
            }
            (loki_dir / "query_range.json").write_text(json.dumps(loki_payload), encoding="utf-8")

            # When: 提取结构化日志
            res = collect_telemetry.process_logs(stage_dir, out_dir)

            # Then: 仅保留 6 条业务日志与 4 条有效告警
            self.assertEqual(res["loki_log_entries"], 6)
            self.assertEqual(res["server_alert_count"], 4)
            self.assertEqual(res["server_alerts_sample"][0]["event"], "write_stall_begin")
            self.assertEqual(res["server_alerts_sample"][1]["event"], "table_deleted")
            self.assertEqual(res["server_alerts_sample"][2]["msg"], "slow tick")
            self.assertEqual(res["server_alerts_sample"][3]["msg"], "panic: fatal crash")

    def test_given_prom_fork_and_storage_metrics_when_extracting_server_snapshot_then_computes_deltas(self) -> None:
        # Given: pre 与 post 两份 Prometheus 文本快照
        with tempfile.TemporaryDirectory() as tmp:
            pre_path = Path(tmp) / "pre.prom"
            post_path = Path(tmp) / "post.prom"
            pre_path.write_text(
                'bowl_storage_control_forks_total{result="ok"} 1\n'
                'bowl_storage_control_commits_reclaimed_total 0\n',
                encoding="utf-8",
            )
            post_path.write_text(
                "\n".join([
                    'bowl_storage_pack_physical_bytes 1048576',
                    'bowl_storage_pack_referenced_bytes 4194304',
                    'bowl_storage_data_dir_physical_bytes 2097152',
                    'bowl_storage_commits_active 3',
                    'bowl_storage_commits_physical_bytes 1048576',
                    'bowl_storage_control_commits_reclaimed_total 2',
                    'bowl_storage_control_forks_total{result="ok"} 4',
                    'bowl_storage_control_forks_total{result="error"} 1',
                    'bowl_storage_actor_tick_phase_duration_seconds_bucket{phase="checkpoint",le="0.05"} 3',
                    'bowl_storage_actor_tick_phase_duration_seconds_bucket{phase="checkpoint",le="+Inf"} 3',
                    'bowl_storage_actor_tick_phase_duration_seconds_sum{phase="checkpoint"} 0.060',
                    'bowl_storage_actor_tick_phase_duration_seconds_count{phase="checkpoint"} 3',
                    'bowl_storage_control_fork_stage_duration_seconds_bucket{stage="sanitize",le="0.05"} 3',
                    'bowl_storage_control_fork_stage_duration_seconds_bucket{stage="sanitize",le="+Inf"} 3',
                    'bowl_storage_control_fork_stage_duration_seconds_sum{stage="sanitize"} 0.045',
                    'bowl_storage_control_fork_stage_duration_seconds_count{stage="sanitize"} 3',
                ]) + "\n",
                encoding="utf-8",
            )

            # When: 提取存储节点指标差分
            pre_p = collect_telemetry.parse_prom_text(pre_path)
            post_p = collect_telemetry.parse_prom_text(post_path)
            snap = collect_telemetry.extract_server_prom_snapshot(post_p, pre_p=pre_p)

            # Then: Gauge 取现值，Counter 取增量
            cg = snap["counters_and_gauges"]
            self.assertEqual(cg["pack_physical_bytes"], 1048576.0)
            self.assertEqual(cg["pack_referenced_bytes"], 4194304.0)
            self.assertEqual(cg["data_dir_physical_bytes"], 2097152.0)
            self.assertEqual(cg["commits_active"], 3.0)
            self.assertEqual(cg["commits_reclaimed"], 2.0)
            self.assertEqual(cg["forks_by_result"], {"ok": 3.0, "error": 1.0})
            self.assertEqual(snap["control_fork_stage_ms"][0]["group"], "sanitize")
            self.assertEqual(snap["actor_tick_phase_ms"][0]["group"], "checkpoint")

    def test_given_trace_with_empty_top_spans_when_rendering_telemetry_then_renders_placeholder_without_crash(self) -> None:
        # Given: overview 含 top_spans 为空的慢链路记录
        overview = {
            "suite": "fork",
            "jaeger": {
                "sampled_traces_count": 1,
                "cross_node_traces_count": 0,
                "error_traces_count": 0,
                "slow_traces_top15": [
                    {
                        "trace_id": "1122334455667788",
                        "total_duration_ms": 5.0,
                        "root_service": "bowl-client",
                        "root_operation": "fuse.Write",
                        "top_spans": [],
                    }
                ],
            },
            "logs": {"loki_log_entries": 0, "server_alert_count": 0, "client_alert_count": 0},
        }

        # When: 渲染总览与报告片段
        overview_md = collect_telemetry.render_overview_markdown(overview)
        section_md = "\n".join(collect_telemetry.render_report_telemetry_section(overview, "## 7. 遥测诊断"))

        # Then: 不抛 IndexError 且输出占位符
        self.assertIn("1122334455667788", overview_md)
        self.assertIn("1122334455667788", section_md)

    def test_given_full_stage_bundle_with_real_pprof_adapter_when_building_telemetry_then_distills_all_outputs(self) -> None:
        # Given: 完整临时 stage 与 results 目录，包含真实可执行 pprof 适配器脚本（无 Mock）
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stage_dir = root / "stage"
            results_dir = root / "results"
            results_dir.mkdir(parents=True)

            (results_dir / "stage_timings.jsonl").write_text(
                json.dumps({"stage": "cross_compile_binaries", "duration_sec": 95.0}) + "\n"
                + json.dumps({"target": "bowl", "stage": "fio_seq_write_1job", "duration_sec": 4.0}) + "\n"
                + json.dumps({"target": "bowl", "stage": "mdtest_1p", "duration_sec": 18.2}) + "\n",
                encoding="utf-8",
            )

            cli_stats = stage_dir / "client" / "stats"
            cli_stats.mkdir(parents=True)
            pre_buckets = [0] * 10 + [10] + [0] * 15
            post_buckets = [0] * 10 + [10, 0, 90] + [0] * 13
            snap_pre = {
                "timestamp_unix_nano": 1000000000,
                "uptime_seconds": 1.0,
                "cpu_usage_seconds": 0.2,
                "memory_bytes": 67108864,
                "fuse": {
                    "total_calls": 10,
                    "total_errors": 0,
                    "read_bytes": 0,
                    "write_bytes": 40960,
                    "ops": [{"op": "Write", "calls": 10, "errors": 0, "bytes": 40960, "duration_nanos": 8000000, "buckets": pre_buckets}],
                },
                "namespace_cache": {"attr_hits": 10, "attr_misses": 2, "entry_hits": 5, "entry_misses": 5, "readlink_hits": 2, "readlink_misses": 1},
                "read_ahead": {"triggers": 0, "prefetched_bytes": 0, "hit_bytes": 0, "discarded_bytes": 0, "waits": 0, "wait_seconds": 0.0},
                "write_behind": {"staged_calls": 0, "staged_bytes": 0, "passthrough_calls": 10, "passthrough_bytes": 40960, "flush_rpcs": 0, "flush_errors": 0, "inflight": 0, "pending_slots": 0, "backpressure_waits": 0, "backpressure_wait_seconds": 0.0, "barrier_waits": 0, "barrier_wait_seconds": 0.0},
                "inode_locks": {"fast_acquisitions": 10, "contended_acquisitions": 0, "wait_seconds": 0.0},
                "queues": {"fuse_waiting": 0, "fuse_congestion_threshold": 48},
            }
            snap_post = {
                "timestamp_unix_nano": 5000000000,
                "uptime_seconds": 5.0,
                "cpu_usage_seconds": 1.7,
                "memory_bytes": 134217728,
                "fuse": {
                    "total_calls": 100,
                    "total_errors": 0,
                    "read_bytes": 1048576,
                    "write_bytes": 419430400,
                    "ops": [{"op": "Write", "calls": 100, "errors": 0, "bytes": 419430400, "duration_nanos": 308000000, "buckets": post_buckets}],
                },
                "namespace_cache": {"attr_hits": 90, "attr_misses": 12, "entry_hits": 45, "entry_misses": 15, "readlink_hits": 10, "readlink_misses": 3},
                "read_ahead": {"triggers": 4, "prefetched_bytes": 4194304, "hit_bytes": 3145728, "discarded_bytes": 0, "waits": 2, "wait_seconds": 0.004},
                "write_behind": {"staged_calls": 85, "staged_bytes": 402653184, "passthrough_calls": 15, "passthrough_bytes": 16777216, "flush_rpcs": 96, "flush_errors": 0, "inflight": 2, "pending_slots": 1, "backpressure_waits": 6, "backpressure_wait_seconds": 0.018, "barrier_waits": 1, "barrier_wait_seconds": 0.005},
                "inode_locks": {"fast_acquisitions": 100, "contended_acquisitions": 5, "wait_seconds": 0.012},
                "queues": {"fuse_waiting": 3, "fuse_congestion_threshold": 48},
            }
            (cli_stats / "fio_seq_write_1job_pre.json").write_text(json.dumps(snap_pre), encoding="utf-8")
            (cli_stats / "fio_seq_write_1job_pre.epoch").write_text("1700000001.000\n", encoding="utf-8")
            (cli_stats / "fio_seq_write_1job_post.json").write_text(json.dumps(snap_post), encoding="utf-8")
            (cli_stats / "fio_seq_write_1job_post.epoch").write_text("1700000005.250\n", encoding="utf-8")
            (cli_stats / "before_umount.json").write_text(json.dumps(snap_post), encoding="utf-8")

            cli_m = stage_dir / "client" / "metrics"
            cli_m.mkdir(parents=True)
            (cli_m / "fio_seq_write_1job_pre.prom").write_text(
                'rpc_client_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="0.005"} 4\n'
                'rpc_client_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="+Inf"} 4\n'
                'rpc_client_call_duration_seconds_sum{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 0.010\n'
                'rpc_client_call_duration_seconds_count{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 4\n',
                encoding="utf-8",
            )
            cli_prom_post = (
                'rpc_client_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="0.0025"} 64\n'
                'rpc_client_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="0.005"} 104\n'
                'rpc_client_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="+Inf"} 104\n'
                'rpc_client_call_duration_seconds_sum{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 0.260\n'
                'rpc_client_call_duration_seconds_count{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 104\n'
            )
            (cli_m / "fio_seq_write_1job_post.prom").write_text(cli_prom_post, encoding="utf-8")
            (cli_m / "before_umount.prom").write_text(cli_prom_post, encoding="utf-8")

            srv_m = stage_dir / "server" / "metrics"
            srv_m.mkdir(parents=True)
            (srv_m / "fio_seq_write_1job_pre.prom").write_text(
                'rpc_server_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="0.005"} 4\n'
                'rpc_server_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="+Inf"} 4\n'
                'rpc_server_call_duration_seconds_sum{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 0.008\n'
                'rpc_server_call_duration_seconds_count{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 4\n',
                encoding="utf-8",
            )
            srv_prom_post = (
                'bowl_storage_http_request_duration_seconds_bucket{procedure="/bowl.storage.v1.StorageService/Write",status_class="2xx",le="0.005"} 100\n'
                'bowl_storage_http_request_duration_seconds_bucket{procedure="/bowl.storage.v1.StorageService/Write",status_class="2xx",le="+Inf"} 100\n'
                'bowl_storage_http_request_duration_seconds_sum{procedure="/bowl.storage.v1.StorageService/Write",status_class="2xx"} 0.220\n'
                'bowl_storage_http_request_duration_seconds_count{procedure="/bowl.storage.v1.StorageService/Write",status_class="2xx"} 100\n'
                'rpc_server_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="0.005"} 104\n'
                'rpc_server_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="+Inf"} 104\n'
                'rpc_server_call_duration_seconds_sum{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 0.188\n'
                'rpc_server_call_duration_seconds_count{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 104\n'
                'bowl_storage_pebble_block_cache_hits_total 0\n'
                'bowl_storage_pebble_block_cache_misses_total 0\n'
            )
            (srv_m / "fio_seq_write_1job_post.prom").write_text(srv_prom_post, encoding="utf-8")
            (srv_m / "final.prom").write_text(srv_prom_post, encoding="utf-8")

            srv_pprof_dir = stage_dir / "server" / "pprof"
            srv_pprof_dir.mkdir(parents=True)
            (srv_pprof_dir / "fio_seq_write_1job_cpu.pb.gz").write_bytes(b"fake-pb-gz")
            fake_go = root / "fake_go"
            fake_go.write_text(
                "#!/usr/bin/env bash\n"
                "if [[ \"$*\" == *\"-flat\"* ]]; then\n"
                "  printf 'Showing nodes accounting for 1.8s\\n      flat  flat%%   sum%%        cum   cum%%\\n     0.95s 42.0%% 42.0%%      0.95s 42.0%%  runtime.memmove\\n'\n"
                "else\n"
                "  printf 'Showing nodes accounting for 1.8s\\n      flat  flat%%   sum%%        cum   cum%%\\n     0.05s  2.2%%  2.2%%      1.70s 75.5%%  store.(*StoreActor).runTick\\n'\n"
                "fi\n",
                encoding="utf-8",
            )
            fake_go.chmod(fake_go.stat().st_mode | stat.S_IXUSR)

            # When: 注入真实临时可执行适配器构建遥测摘要包
            overview = collect_telemetry.build_telemetry_bundle(
                suite="juicefs",
                stage_dir=stage_dir,
                results_dir=results_dir,
                go_bin=fake_go,
            )

            # Then: 全部产物落盘、阶段与跳点差值正确、0/0 BlockCache 显示 N/A 且无 None%
            telem_dir = results_dir / "telemetry"
            self.assertTrue((telem_dir / "overview.json").exists())
            self.assertTrue((telem_dir / "overview.md").exists())
            self.assertTrue((telem_dir / "phase_deltas.json").exists())
            self.assertEqual(overview["slowest_stages"][0]["stage"], "mdtest_1p")
            self.assertEqual(overview["slowest_stages"][1]["stage"], "fio_seq_write_1job")

            fio_bd = overview["phase_deltas"]["fio_seq_write_1job"]
            self.assertAlmostEqual(fio_bd["wall_clock_sec"], 4.25, places=2)
            write_hop = fio_bd["rpc_hop_deltas"]["by_method"][0]
            self.assertAlmostEqual(write_hop["client_rpc_mean_ms"], 2.50, places=3)
            self.assertAlmostEqual(write_hop["server_http_mean_ms"], 2.20, places=3)
            self.assertAlmostEqual(write_hop["server_rpc_mean_ms"], 1.80, places=3)
            self.assertAlmostEqual(write_hop["net_and_codec_mean_ms"], 0.70, places=3)
            self.assertAlmostEqual(write_hop["wire_and_client_codec_mean_ms"], 0.30, places=3)
            self.assertAlmostEqual(write_hop["server_http_codec_mean_ms"], 0.40, places=3)

            self.assertIn("runtime.memmove", overview["pprof_profiles"][0]["top_flat_entries"][0])
            self.assertIn("StoreActor", overview["pprof_profiles"][0]["top_cum_entries"][0])
            overview_md = (telem_dir / "overview.md").read_text(encoding="utf-8")
            self.assertIn("N/A (0 reads)", overview_md)
            self.assertNotIn("None%", overview_md)

    def test_given_cli_arguments_when_invoking_collect_telemetry_script_then_generates_bundle(self) -> None:
        # Given: 最小临时 stage 与 results 目录
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stage_dir = root / "stage"
            results_dir = root / "results"
            (stage_dir / "client" / "stats").mkdir(parents=True)
            results_dir.mkdir(parents=True)
            (results_dir / "stage_timings.jsonl").write_text(
                json.dumps({"stage": "smoke_phase", "duration_sec": 2.5}) + "\n",
                encoding="utf-8",
            )

            # When: 通过真实命令行接口调用脚本
            proc = subprocess.run(
                [
                    sys.executable,
                    str(BENCH_ROOT / "common" / "collect_telemetry.py"),
                    "--suite",
                    "fork",
                    "--stage-dir",
                    str(stage_dir),
                    "--results-dir",
                    str(results_dir),
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            # Then: 退出码为 0 且输出 overview.json 与 overview.md
            self.assertEqual(proc.returncode, 0, msg=f"stderr={proc.stderr}")
            self.assertTrue((results_dir / "telemetry" / "overview.json").exists())
            self.assertTrue((results_dir / "telemetry" / "overview.md").exists())

    def test_given_client_prom_when_extracting_and_building_breakdown_then_omits_locktable_and_preserves_inodelock(self) -> None:
        prom_text = (
            'bowl_client_inodelock_wait_duration_seconds_bucket{le="0.005"} 2\n'
            'bowl_client_inodelock_wait_duration_seconds_bucket{le="+Inf"} 2\n'
            'bowl_client_inodelock_wait_duration_seconds_sum 0.004\n'
            'bowl_client_inodelock_wait_duration_seconds_count 2\n'
            'bowl_client_lock_wait_duration_seconds_bucket{le="0.005"} 3\n'
            'bowl_client_lock_wait_duration_seconds_bucket{le="+Inf"} 3\n'
            'bowl_client_lock_wait_duration_seconds_sum 0.009\n'
            'bowl_client_lock_wait_duration_seconds_count 3\n'
            'bowl_client_lock_wait_seconds_total 0.009\n'
        )
        with tempfile.TemporaryDirectory() as tmp:
            prom_path = Path(tmp) / "client.prom"
            prom_path.write_text(prom_text, encoding="utf-8")
            parsed = collect_telemetry.parse_prom_text(prom_path)

        cli_prom = collect_telemetry.extract_client_prom_snapshot(parsed)

        self.assertNotIn("lock_wait_ms", cli_prom)
        self.assertNotIn("lock_wait_seconds_total", cli_prom["counters_and_gauges"])
        self.assertIn("inodelock_wait_ms", cli_prom)
        self.assertEqual(cli_prom["inodelock_wait_ms"][0]["count"], 2)

        cli_prom_with_legacy = dict(cli_prom)
        cli_prom_with_legacy["lock_wait_ms"] = [
            {"count": 3, "total_val": 9.0, "mean_val": 3.0, "p50_val": 2.5, "p99_val": 4.5}
        ]
        breakdown = collect_telemetry.build_single_phase_breakdown(
            "lock_phase",
            {"window_sec": 1.0, "inode_lock_contended": 2, "inode_lock_wait_ms": 4.0},
            cli_prom_with_legacy,
            {},
            {"lock_phase": 1.0},
        )
        components = [hop["component"] for hop in breakdown["hops"]]
        self.assertIn("Client InodeLock 等待", components)
        self.assertNotIn("Client LockTable 等待", components)


if __name__ == "__main__":
    unittest.main()
