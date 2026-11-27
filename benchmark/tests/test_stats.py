"""Unit tests for benchmark.lib.stats."""

from __future__ import annotations

import unittest

from benchmark.lib.stats import (
    check_variance,
    computesdk_fork_score,
    computesdk_reference_score,
    computesdk_storage_score,
    linear_slope,
    percentile,
    summarize_latency,
    summarize_series,
    summarize_steady,
)


class TestStats(unittest.TestCase):
    def test_percentile_sample_floors(self) -> None:
        s5 = [1.0, 2.0, 3.0, 4.0, 5.0]
        self.assertEqual(percentile(s5, 50), 3.0)
        self.assertIsNone(percentile(s5, 95))
        self.assertIsNone(percentile(s5, 99))
        self.assertIsNone(percentile(s5, 99.9))

        s20 = [float(i) for i in range(1, 21)]
        self.assertIsNotNone(percentile(s20, 95))
        self.assertIsNone(percentile(s20, 99))
        self.assertIsNone(percentile(s20, 99.9))

        s100 = [float(i) for i in range(1, 101)]
        self.assertIsNotNone(percentile(s100, 95))
        self.assertIsNotNone(percentile(s100, 99))
        self.assertIsNone(percentile(s100, 99.9))

        s1000 = [float(i) for i in range(1, 1001)]
        self.assertAlmostEqual(percentile(s1000, 95), 950.05, places=2)
        self.assertAlmostEqual(percentile(s1000, 99), 990.01, places=2)
        self.assertAlmostEqual(percentile(s1000, 99.9), 999.001, places=3)

    def test_summarize_small_sample_omits_unreliable_percentiles_with_warnings(self) -> None:
        res = summarize_latency([10.0, 12.0, 11.0, 10.5, 11.5])
        self.assertEqual(res["n"], 5)
        self.assertEqual(res["median"], 11.0)
        self.assertEqual(res["min"], 10.0)
        self.assertEqual(res["max"], 12.0)
        self.assertIsNone(res["p95"])
        self.assertIsNone(res["p99"])
        self.assertIsNone(res["p999"])
        self.assertFalse(res["p95_reliable"])
        self.assertFalse(res["p99_reliable"])
        self.assertFalse(res["p999_reliable"])
        self.assertTrue(any("p95 omitted" in w for w in res["warnings"]))
        self.assertEqual(res["validity_flag"], "PASS")

    def test_variance_thresholds_steady_vs_discrete(self) -> None:
        # Mean=100, stdev=20 -> CV=20%: fails steady (>15%), passes discrete (<=25%)
        samples = [80.0, 100.0, 120.0]
        steady = summarize_steady(samples)
        discrete = summarize_series(samples, kind="discrete", include_percentiles=False)
        self.assertTrue(steady["high_variance"])
        self.assertEqual(steady["validity_flag"], "HIGH_VARIANCE")
        self.assertFalse(discrete["high_variance"])
        self.assertEqual(discrete["validity_flag"], "PASS")

        high_disc = summarize_series([50.0, 100.0, 150.0], kind="discrete", include_percentiles=False)
        self.assertTrue(high_disc["high_variance"])
        self.assertEqual(high_disc["validity_flag"], "HIGH_VARIANCE")

        is_high, msg = check_variance(15.1, kind="steady")
        self.assertTrue(is_high)
        self.assertIn("HIGH_VARIANCE", msg or "")

    def test_empty_series(self) -> None:
        res = summarize_series([])
        self.assertEqual(res["n"], 0)
        self.assertIsNone(res["median"])
        self.assertEqual(res["validity_flag"], "INVALID")

    def test_computesdk_reference_score(self) -> None:
        self.assertAlmostEqual(computesdk_storage_score(0.0), 100.0)
        self.assertAlmostEqual(computesdk_storage_score(300.0), 99.0)
        self.assertAlmostEqual(computesdk_storage_score(60000.0), 0.0)
        self.assertAlmostEqual(computesdk_fork_score(600.0), 99.0)
        self.assertAlmostEqual(computesdk_reference_score(-10.0, ceiling_ms=1000.0), 100.0)

    def test_linear_slope(self) -> None:
        slope = linear_slope([1, 16, 64, 200], [10.5, 18.0, 42.0, 110.0])
        self.assertAlmostEqual(slope, 0.5, places=4)


if __name__ == "__main__":
    unittest.main()
