"""Unit tests for benchmark.lib.metrics_audit."""

from __future__ import annotations

import unittest

from benchmark.lib.metrics_audit import (
    ALLOWED_CLIENT_METRICS,
    ALLOWED_SERVER_METRICS,
    audit_case,
    compute_snapshot_deltas,
    parse_cgroup_io_stat,
    parse_prometheus_text,
)

SAMPLE_SERVER_PROM_BEFORE = """
# HELP bowl_storage_http_response_bytes_total Total HTTP response bytes.
bowl_storage_http_response_bytes_total{procedure="/storage.v1.StorageService/Read"} 1000
bowl_storage_pack_physical_bytes 104857600
bowl_storage_logical_bytes 104857600
bowl_storage_pebble_wal_bytes_written_total 10000
bowl_storage_pebble_block_cache_hits_total 500
bowl_storage_pebble_block_cache_misses_total 50
bowl_storage_pebble_l0_sublevels 1
bowl_storage_pebble_compaction_debt_bytes 0
bowl_storage_pebble_read_amp 2
# Legacy removed metrics that must be ignored:
bowl_storage_projection_lag_seconds 99
bowl_storage_branch_actors_loaded 42
"""

SAMPLE_SERVER_PROM_AFTER = """
bowl_storage_http_response_bytes_total{procedure="/storage.v1.StorageService/Read"} 100001000
bowl_storage_pack_physical_bytes 209715200
bowl_storage_logical_bytes 104857600
bowl_storage_pebble_wal_bytes_written_total 50000
bowl_storage_pebble_block_cache_hits_total 800
bowl_storage_pebble_block_cache_misses_total 80
bowl_storage_pebble_l0_sublevels 3
bowl_storage_pebble_compaction_debt_bytes 4194304
bowl_storage_pebble_read_amp 4
"""

SAMPLE_CLIENT_PROM_BEFORE = """
bowl_client_fuse_ops_total{op="Read"} 10
bowl_client_fuse_ops_total{op="Lookup"} 100
bowl_client_fuse_ops_total{op="GetAttr"} 100
bowl_client_fuse_io_bytes_total{dir="read"} 0
bowl_client_fuse_io_bytes_total{dir="write"} 0
bowl_client_readahead_hit_bytes_total 0
bowl_client_readahead_discarded_bytes_total 0
bowl_client_read_ahead_wait_duration_seconds_bucket{le="0.001"} 0
bowl_client_read_ahead_wait_duration_seconds_bucket{le="+Inf"} 0
bowl_client_read_ahead_wait_duration_seconds_sum 0
bowl_client_read_ahead_wait_duration_seconds_count 0
bowl_client_stage_backpressure_wait_duration_seconds_bucket{le="0.01"} 0
bowl_client_stage_backpressure_wait_duration_seconds_bucket{le="+Inf"} 0
bowl_client_stage_backpressure_wait_duration_seconds_sum 0
bowl_client_stage_backpressure_wait_duration_seconds_count 0
bowl_client_stage_upload_rpcs_total{result="ok"} 0
bowl_client_stage_upload_rpcs_total{result="error"} 0
bowl_client_rpc_retry_exhausted_total{procedure="/storage.v1.StorageService/Read"} 0
bowl_client_stale_epoch_total 0
bowl_client_tmpfile_promotes_total 0
bowl_client_fuse_connection_waiting 1
# Legacy removed metrics that must be ignored:
bowl_client_nscache_hits_total 999
bowl_client_queue_dirty_inodes 77
"""

SAMPLE_CLIENT_PROM_AFTER = """
bowl_client_fuse_ops_total{op="Read"} 1010
bowl_client_fuse_ops_total{op="Lookup"} 6100
bowl_client_fuse_ops_total{op="GetAttr"} 4100
bowl_client_fuse_io_bytes_total{dir="read"} 100000000
bowl_client_fuse_io_bytes_total{dir="write"} 0
bowl_client_readahead_hit_bytes_total 80000000
bowl_client_readahead_discarded_bytes_total 20000000
bowl_client_read_ahead_wait_duration_seconds_bucket{le="0.001"} 20
bowl_client_read_ahead_wait_duration_seconds_bucket{le="+Inf"} 25
bowl_client_read_ahead_wait_duration_seconds_sum 0.05
bowl_client_read_ahead_wait_duration_seconds_count 25
bowl_client_stage_backpressure_wait_duration_seconds_bucket{le="0.01"} 1
bowl_client_stage_backpressure_wait_duration_seconds_bucket{le="+Inf"} 1
bowl_client_stage_backpressure_wait_duration_seconds_sum 0.005
bowl_client_stage_backpressure_wait_duration_seconds_count 1
bowl_client_stage_upload_rpcs_total{result="ok"} 12
bowl_client_stage_upload_rpcs_total{result="error"} 0
bowl_client_rpc_retry_exhausted_total{procedure="/storage.v1.StorageService/Read"} 0
bowl_client_stale_epoch_total 0
bowl_client_tmpfile_promotes_total 5
bowl_client_fuse_connection_waiting 4
"""


class TestMetricsAudit(unittest.TestCase):
    def _make_snapshots(
        self,
        *,
        client_after_text: str = SAMPLE_CLIENT_PROM_AFTER,
        server_after_text: str = SAMPLE_SERVER_PROM_AFTER,
        rbytes_before: int | None = 1_000_000,
        rbytes_after: int | None = 96_000_000,
        server_readyz: bool = True,
        client_readyz: bool = True,
    ) -> tuple[dict, dict]:
        before = {
            "server_metrics": parse_prometheus_text(
                SAMPLE_SERVER_PROM_BEFORE, allowed_names=ALLOWED_SERVER_METRICS
            ),
            "client_metrics": parse_prometheus_text(
                SAMPLE_CLIENT_PROM_BEFORE, allowed_names=ALLOWED_CLIENT_METRICS
            ),
            "server_cgroup_rbytes": rbytes_before,
            "server_readyz_ok": True,
            "client_readyz_ok": True,
        }
        after = {
            "server_metrics": parse_prometheus_text(
                server_after_text, allowed_names=ALLOWED_SERVER_METRICS
            ),
            "client_metrics": parse_prometheus_text(
                client_after_text, allowed_names=ALLOWED_CLIENT_METRICS
            ),
            "server_cgroup_rbytes": rbytes_after,
            "server_readyz_ok": server_readyz,
            "client_readyz_ok": client_readyz,
        }
        return before, after

    def test_ignores_removed_legacy_metrics(self) -> None:
        srv = parse_prometheus_text(SAMPLE_SERVER_PROM_BEFORE, allowed_names=ALLOWED_SERVER_METRICS)
        cli = parse_prometheus_text(SAMPLE_CLIENT_PROM_BEFORE, allowed_names=ALLOWED_CLIENT_METRICS)
        self.assertNotIn("bowl_storage_projection_lag_seconds", srv)
        self.assertNotIn("bowl_storage_branch_actors_loaded", srv)
        self.assertNotIn("bowl_client_nscache_hits_total", cli)
        self.assertNotIn("bowl_client_queue_dirty_inodes", cli)
        self.assertIn("bowl_storage_pack_physical_bytes", srv)
        self.assertIn("bowl_client_fuse_io_bytes_total", cli)

    def test_parse_cgroup_io_stat(self) -> None:
        raw = "259:0 rbytes=500000 wbytes=1000 rios=10 wios=2\n259:1 rbytes=250000 wbytes=500 rios=5 wios=1\n"
        parsed = parse_cgroup_io_stat(raw)
        self.assertEqual(parsed["rbytes"], 750000)
        self.assertEqual(parsed["wbytes"], 1500)
        self.assertEqual(parse_cgroup_io_stat("424242")["rbytes"], 424242)

    def test_c2_valid_run_passes(self) -> None:
        before, after = self._make_snapshots(rbytes_before=0, rbytes_after=95_000_000)
        res = audit_case(
            before,
            after,
            target="bowl_staged",
            cache_state="C2",
            expected_read_bytes=100_000_000,
            require_server_nvme=True,
        )
        self.assertEqual(res["validity_status"], "PASS")
        self.assertEqual(res["reasons"], [])
        self.assertAlmostEqual(res["deltas"]["client_fuse_read_ratio"], 1.0)
        self.assertAlmostEqual(res["deltas"]["server_nvme_read_ratio"], 0.95)
        self.assertAlmostEqual(res["deltas"]["server_space_amplification"], 2.0)
        self.assertEqual(res["deltas"]["client_tmpfile_promotes"], 5)
        self.assertEqual(res["deltas"]["client_readahead_hit_pct"], 80.0)
        self.assertEqual(res["deltas"]["client_readahead_wait"]["count"], 25)

    def test_rejects_client_cache_leak(self) -> None:
        leaked_client = SAMPLE_CLIENT_PROM_AFTER.replace(
            'bowl_client_fuse_io_bytes_total{dir="read"} 100000000',
            'bowl_client_fuse_io_bytes_total{dir="read"} 20000000',
        )
        before, after = self._make_snapshots(client_after_text=leaked_client)
        res = audit_case(
            before,
            after,
            target="bowl_staged",
            cache_state="C1",
            expected_read_bytes=100_000_000,
        )
        self.assertEqual(res["validity_status"], "INVALID")
        self.assertTrue(any("INVALID_CACHE_LEAK" in r for r in res["reasons"]))

    def test_rejects_cold_metadata_dcache_leak(self) -> None:
        leaked_client = SAMPLE_CLIENT_PROM_AFTER.replace(
            'bowl_client_fuse_ops_total{op="Lookup"} 6100',
            'bowl_client_fuse_ops_total{op="Lookup"} 110',
        ).replace(
            'bowl_client_fuse_ops_total{op="GetAttr"} 4100',
            'bowl_client_fuse_ops_total{op="GetAttr"} 110',
        )
        before, after = self._make_snapshots(client_after_text=leaked_client)
        res = audit_case(
            before,
            after,
            target="bowl_staged",
            cache_state="C1",
            expected_stat_ops=10_000,
        )
        self.assertEqual(res["validity_status"], "INVALID")
        self.assertTrue(any("INVALID_CACHE_LEAK" in r for r in res["reasons"]))

    def test_rejects_server_nvme_cache_leak(self) -> None:
        # Client FUSE read=100%, server HTTP=100%, but server cgroup rbytes=10% (served from server RAM!)
        before, after = self._make_snapshots(rbytes_before=0, rbytes_after=10_000_000)
        res = audit_case(
            before,
            after,
            target="bowl_staged",
            cache_state="C2",
            expected_read_bytes=100_000_000,
            require_server_nvme=True,
        )
        self.assertEqual(res["validity_status"], "INVALID")
        self.assertTrue(any("INVALID_SERVER_CACHE_LEAK" in r for r in res["reasons"]))

        # Also applies to non-Bowl network targets like jfs_pg_minio
        res_jfs = audit_case(
            {"server_cgroup_rbytes": 0},
            {"server_cgroup_rbytes": 5_000_000},
            target="jfs_pg_minio",
            cache_state="C2",
            expected_read_bytes=100_000_000,
            require_server_nvme=True,
        )
        self.assertEqual(res_jfs["validity_status"], "INVALID")
        self.assertTrue(any("INVALID_SERVER_CACHE_LEAK" in r for r in res_jfs["reasons"]))

    def test_rejects_silent_failures(self) -> None:
        bad_client = (
            SAMPLE_CLIENT_PROM_AFTER.replace("bowl_client_stale_epoch_total 0", "bowl_client_stale_epoch_total 1")
            .replace(
                'bowl_client_stage_upload_rpcs_total{result="error"} 0',
                'bowl_client_stage_upload_rpcs_total{result="error"} 2',
            )
            .replace(
                'bowl_client_rpc_retry_exhausted_total{procedure="/storage.v1.StorageService/Read"} 0',
                'bowl_client_rpc_retry_exhausted_total{procedure="/storage.v1.StorageService/Read"} 1',
            )
        )
        before, after = self._make_snapshots(client_after_text=bad_client, server_readyz=False)
        res = audit_case(before, after, target="bowl_staged", cache_state="C0")
        self.assertEqual(res["validity_status"], "INVALID")
        self.assertEqual(len(res["reasons"]), 4)

    def test_compute_snapshot_deltas_pack_logical_alias(self) -> None:
        before = {
            "server_metrics": parse_prometheus_text(
                "bowl_storage_pack_physical_bytes 100\nbowl_storage_pack_logical_bytes 100\n",
                allowed_names=ALLOWED_SERVER_METRICS,
            )
        }
        after = {
            "server_metrics": parse_prometheus_text(
                "bowl_storage_pack_physical_bytes 300\nbowl_storage_pack_logical_bytes 150\n",
                allowed_names=ALLOWED_SERVER_METRICS,
            )
        }
        deltas = compute_snapshot_deltas(before, after)
        self.assertAlmostEqual(deltas["server_space_amplification"], 2.0)


if __name__ == "__main__":
    unittest.main()
