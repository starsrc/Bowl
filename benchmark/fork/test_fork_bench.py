#!/usr/bin/env python3
"""Tests for BenchEnv.mount_ephemeral CLI flags in fork_bench.py."""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

BENCH_ROOT = Path(__file__).resolve().parent.parent
if str(BENCH_ROOT) not in sys.path:
    sys.path.insert(0, str(BENCH_ROOT))

from fork import fork_bench  # noqa: E402


class MountEphemeralFlagTest(unittest.TestCase):
    """Verify that BenchEnv.mount_ephemeral passes supported mount mode flags."""

    def _run_mount_ephemeral(
        self,
        disk_id: str,
        *,
        read_only: bool | None = None,
    ) -> dict[str, Any]:
        """Run BenchEnv.mount_ephemeral with mocked remote audit and subprocess.run."""
        captured: dict[str, Any] = {}

        def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
            captured["cmd"] = list(cmd)
            captured["env"] = kwargs.get("env")
            captured["check"] = kwargs.get("check")
            return subprocess.CompletedProcess(args=cmd, returncode=0)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            mp = root / "mounts" / disk_id
            with (
                mock.patch("fork.fork_bench.BenchEnv._init_remote_audit", return_value="/var/lib/bowl"),
                mock.patch("fork.fork_bench.subprocess.run", side_effect=fake_run) as mock_run,
            ):
                env = fork_bench.BenchEnv(
                    endpoint="http://127.0.0.1:9000",
                    server_priv_ip="127.0.0.1",
                    ssh_key="/dev/null",
                    bowl_bin="/usr/local/bin/bowl",
                    primary_mount=root / "primary",
                    primary_disk_id="disk-primary",
                    work_mount_root=root / "work",
                    ephemeral_log=root / "logs" / "ephemeral.log",
                    trace_sample_ratio="0.01",
                    telemetry_helper=None,
                )
                if read_only is None:
                    env.mount_ephemeral(disk_id, mp)
                else:
                    env.mount_ephemeral(disk_id, mp, read_only=read_only)

        captured["call_count"] = mock_run.call_count
        return captured

    def test_mount_ephemeral_read_only_uses_mode_ro_flag(self) -> None:
        """Verify that read_only=True passes --mode ro instead of --ro."""
        captured = self._run_mount_ephemeral("disk-ro", read_only=True)

        self.assertEqual(captured["call_count"], 1)
        self.assertIs(captured["check"], True)
        self.assertEqual(captured["env"]["OTEL_SERVICE_NAME"], "bowl-client")
        self.assertNotIn("--ro", captured["cmd"])
        self.assertEqual(captured["cmd"][-2:], ["--mode", "ro"])

    def test_mount_ephemeral_default_omits_mode_flag(self) -> None:
        """Verify that the default read-write mount omits both --ro and --mode."""
        captured = self._run_mount_ephemeral("disk-rw")

        self.assertEqual(captured["call_count"], 1)
        self.assertIs(captured["check"], True)
        self.assertEqual(captured["env"]["OTEL_SERVICE_NAME"], "bowl-client")
        self.assertNotIn("--ro", captured["cmd"])
        self.assertNotIn("--mode", captured["cmd"])


if __name__ == "__main__":
    unittest.main()
