"""Deep I/O throughput, durability, and cache matrix benchmark suite."""

from benchmark.suites.fio_io.suite import SUITE_ID, SUPPORTED_TARGETS, run_suite

__all__ = ["SUITE_ID", "SUPPORTED_TARGETS", "run_suite"]
