"""Unit tests for benchmark.runner (--resume, registry, profiles) and MountHandle."""

from __future__ import annotations

import json
import subprocess
import tempfile
import threading
import types
import unittest
from pathlib import Path

from benchmark import runner
from benchmark.lib.context import (
    BenchContext,
    MountHandle,
    _find_free_local_port,
    _release_local_port,
)


class TestRunner(unittest.TestCase):
    def test_suite_registry_matches_contract(self) -> None:
        expected = {
            "pjdfstest": "benchmark.suites.pjdfstest.suite",
            "fsx": "benchmark.suites.fsx.suite",
            "fsstress": "benchmark.suites.fsstress.suite",
            "smallfile": "benchmark.suites.smallfile.suite",
            "juicefs-bench": "benchmark.suites.juicefs_bench.suite",
            "computesdk-bench": "benchmark.suites.computesdk_bench.suite",
            "fio-io": "benchmark.suites.fio_io.suite",
            "metadata": "benchmark.suites.metadata.suite",
            "branch-gc": "benchmark.suites.branch_gc.suite",
            "workloads": "benchmark.suites.workloads.suite",
        }
        self.assertEqual(runner.SUITE_REGISTRY, expected)

    def test_profiles_and_resolve_suites(self) -> None:
        full_cfg = runner.load_profile("full-vpc")
        smoke_cfg = runner.load_profile("smoke-local")
        self.assertEqual(full_cfg["profile"], "full-vpc")
        self.assertIsNone(full_cfg["disclaimer"])
        self.assertEqual(smoke_cfg["profile"], "smoke-local")
        self.assertTrue(smoke_cfg["disclaimer"])
        self.assertEqual(len(runner.resolve_suites("all", full_cfg)), 10)
        self.assertEqual(runner.resolve_suites("fsx,fsstress", smoke_cfg), ["fsx", "fsstress"])
        with self.assertRaises(ValueError):
            runner.resolve_suites("nonexistent-suite", smoke_cfg)

    def test_resume_skips_completed_and_reruns_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs_dir = Path(tmp) / "runs"
            nvme_root = Path(tmp) / "nvme"
            nvme_root.mkdir(parents=True)
            calls: list[str] = []
            fail_fsx = {"should_fail": True}

            def fake_loader(mod_path: str) -> types.SimpleNamespace:
                suite_id = next(k for k, v in runner.SUITE_REGISTRY.items() if v == mod_path)

                def _run(ctx: BenchContext) -> dict:
                    calls.append(suite_id)
                    if suite_id == "fsx" and fail_fsx["should_fail"]:
                        raise RuntimeError("simulated fsx crash")
                    return {
                        "suite": suite_id,
                        "cases": [
                            {
                                "case_id": f"{suite_id}-c1",
                                "target": "ext4",
                                "durability_tier": "Tier 1",
                                "cache_state": "C0",
                                "runs_n": 1,
                                "metrics": {"ops_s": 100.0},
                                "validity_status": "PASS",
                                "integrity_verified": True,
                                "counter_deltas": {},
                                "notes": "ok",
                            }
                        ],
                        "summary_markdown": "",
                    }

                return types.SimpleNamespace(SUITE_ID=suite_id, SUPPORTED_TARGETS=["ext4"], run_suite=_run)

            # First run: pjdfstest completes, fsx fails
            res1 = runner.run_benchmark(
                profile="smoke-local",
                suites=["pjdfstest", "fsx"],
                targets=["ext4"],
                runs_dir=runs_dir,
                run_id="run-resume-test",
                nvme_root=nvme_root,
                suite_loader=fake_loader,
            )
            self.assertEqual(calls, ["pjdfstest", "fsx"])
            self.assertEqual(res1["meta"]["failed_suites"], ["fsx"])
            self.assertEqual(res1["suites"]["pjdfstest"]["status"], "completed")
            self.assertEqual(res1["suites"]["fsx"]["status"], "failed")

            # Resume run: pjdfstest MUST be skipped, fsx re-run and succeeds, fsstress added
            fail_fsx["should_fail"] = False
            res2 = runner.run_benchmark(
                profile="smoke-local",
                suites=["pjdfstest", "fsx", "fsstress"],
                targets=["ext4"],
                runs_dir=runs_dir,
                resume_run_id="run-resume-test",
                nvme_root=nvme_root,
                suite_loader=fake_loader,
            )
            self.assertEqual(calls, ["pjdfstest", "fsx", "fsx", "fsstress"])
            self.assertEqual(res2["meta"]["skipped_resumed_suites"], ["pjdfstest"])
            self.assertEqual(res2["meta"]["failed_suites"], [])
            self.assertEqual(res2["suites"]["fsx"]["status"], "completed")
            self.assertEqual(res2["suites"]["fsstress"]["status"], "completed")

            run_dir = runs_dir / "run-resume-test"
            self.assertTrue((run_dir / "REPORT.md").exists())
            self.assertTrue((run_dir / "pjdfstest" / "REPORT.md").exists())
            self.assertTrue((run_dir / "fsx" / "REPORT.md").exists())
            self.assertTrue((run_dir / "fsstress" / "REPORT.md").exists())
            loaded_summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertIn("validity_gates", loaded_summary)

    def test_mount_handle_unmount_is_idempotent(self) -> None:
        count = {"n": 0}

        def _unmount() -> float:
            count["n"] += 1
            return 0.25

        handle = MountHandle(target="bowl_staged", mount_dir=Path("/tmp/fake"), _unmount_fn=_unmount)
        self.assertAlmostEqual(handle.unmount(), 0.25)
        self.assertAlmostEqual(handle.unmount(), 0.0)
        self.assertEqual(count["n"], 1)

    def test_find_free_local_port_thread_safe_and_unique(self) -> None:
        allocated: list[int] = []
        lock = threading.Lock()
        barrier = threading.Barrier(16)

        def _worker() -> None:
            barrier.wait()
            p = _find_free_local_port()
            with lock:
                allocated.append(p)

        threads = [threading.Thread(target=_worker) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        try:
            self.assertEqual(len(allocated), 16)
            self.assertEqual(len(set(allocated)), 16)
        finally:
            for p in allocated:
                _release_local_port(p)

    def test_runner_captures_called_process_error_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runs_dir = Path(tmp) / "runs"
            nvme_root = Path(tmp) / "nvme"
            nvme_root.mkdir(parents=True)

            def fake_loader(_mod_path: str) -> types.SimpleNamespace:
                def _run(_ctx: BenchContext) -> dict:
                    raise subprocess.CalledProcessError(
                        returncode=1,
                        cmd=["bowl", "mount", "disk-1", "/mnt/test"],
                        output="mount starting\n",
                        stderr="listen tcp 127.0.0.1:19090: bind: address already in use\n",
                    )

                return types.SimpleNamespace(SUITE_ID="branch-gc", SUPPORTED_TARGETS=["bowl_staged"], run_suite=_run)

            runner.run_benchmark(
                profile="smoke-local",
                suites=["branch-gc"],
                targets=["bowl_staged"],
                runs_dir=runs_dir,
                run_id="run-cpe-test",
                nvme_root=nvme_root,
                suite_loader=fake_loader,
            )
            res_path = runs_dir / "run-cpe-test" / "branch-gc" / "result.json"
            data = json.loads(res_path.read_text(encoding="utf-8"))
            self.assertEqual(data["status"], "failed")
            self.assertEqual(data["returncode"], 1)
            self.assertEqual(data["cmd"], ["bowl", "mount", "disk-1", "/mnt/test"])
            self.assertIn("address already in use", data["stderr_tail"])
            self.assertIn("mount starting", data["stdout_tail"])
            self.assertIn("CalledProcessError", data["traceback"])
            self.assertIn("address already in use", data["error"])


if __name__ == "__main__":
    unittest.main()
