"""Unified CLI orchestrator for Bowl Benchmark v2."""

from __future__ import annotations

import argparse
import datetime as dt
import importlib
import json
import os
import platform
import subprocess
import sys
import threading
import time
import traceback
import urllib.request
from pathlib import Path
from typing import Any

import yaml

from benchmark import report as bench_report
from benchmark.lib.context import BenchContext

SUITE_REGISTRY: dict[str, str] = {
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

BENCHMARK_ROOT = Path(__file__).resolve().parent
PROFILES_DIR = BENCHMARK_ROOT / "profiles"


def load_profile(profile_name: str) -> dict[str, Any]:
    """Load YAML profile config for full-vpc or smoke-local."""
    file_map = {
        "full-vpc": PROFILES_DIR / "full_vpc.yaml",
        "smoke-local": PROFILES_DIR / "smoke_local.yaml",
    }
    path = file_map.get(profile_name)
    if path is None:
        candidate = Path(profile_name)
        if candidate.exists():
            path = candidate
        else:
            raise ValueError(f"unknown profile {profile_name!r}; expected 'full-vpc' or 'smoke-local'")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"invalid profile YAML at {path}")
    return data


def resolve_suites(suite_arg: str, profile_cfg: dict[str, Any]) -> list[str]:
    """Parse --suite argument ('all' or comma-separated kebab-case names)."""
    raw = suite_arg.strip()
    if not raw or raw.lower() == "all":
        defaults = profile_cfg.get("default_suites") or list(SUITE_REGISTRY.keys())
        return [s for s in defaults if s in SUITE_REGISTRY]
    selected: list[str] = []
    for item in raw.split(","):
        name = item.strip()
        if not name:
            continue
        if name not in SUITE_REGISTRY:
            raise ValueError(
                f"unknown suite {name!r}; available: {', '.join(SUITE_REGISTRY.keys())}"
            )
        if name not in selected:
            selected.append(name)
    return selected


def resolve_targets(targets_arg: str | None, profile_cfg: dict[str, Any]) -> list[str]:
    """Parse --targets override or return profile defaults."""
    if not targets_arg or not targets_arg.strip():
        return list(profile_cfg.get("default_targets", ["ext4", "passthrough_ll", "bowl_staged", "bowl_sync"]))
    return [t.strip() for t in targets_arg.split(",") if t.strip()]


def _git_commit_sha() -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(BENCHMARK_ROOT.parent),
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            return proc.stdout.strip()
    except Exception:
        pass
    return os.environ.get("BOWL_GIT_COMMIT", "unknown")


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _load_suite_result(result_path: Path) -> dict[str, Any] | None:
    if not result_path.exists():
        return None
    try:
        data = json.loads(result_path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data.get("status") == "completed":
            return data
    except Exception:
        return None
    return None


def _run_profiling_pass(
    ctx_factory: Any,
    run_dir: Path,
    profile_cfg: dict[str, Any],
    server_metrics_url: str,
) -> list[dict[str, Any]]:
    """Run separate non-scoring Profiling & Gauge Attribution Pass for Bowl scenes."""
    prof_cfg = profile_cfg.get("profiling", {})
    if not prof_cfg.get("enabled", False):
        return []
    scenes = prof_cfg.get("scenes", [])
    interval_s = float(prof_cfg.get("sample_interval_s", 1.0))
    duration_s = int(prof_cfg.get("pprof_duration_s", 15))
    results: list[dict[str, Any]] = []

    for scene in scenes:
        scene_id = str(scene.get("id", "unknown"))
        scene_dir_name = str(scene.get("dir", scene_id))
        out_dir = run_dir / "profiling" / scene_dir_name
        out_dir.mkdir(parents=True, exist_ok=True)
        ctx: BenchContext = ctx_factory(f"profiling-{scene_dir_name}", out_dir)
        samples: list[dict[str, Any]] = []
        stop_ev = threading.Event()
        try:
            ctx._deploy_remote_target("bowl_staged", profiling=True)
            sampler = ctx.sample_metrics_background(interval_s, stop_ev, samples)
            pprof_files: dict[str, str] = {}
            for kind, endpoint in (
                ("cpu", f"{server_metrics_url}/debug/pprof/profile?seconds={duration_s}"),
                ("mutex", f"{server_metrics_url}/debug/pprof/mutex"),
                ("block", f"{server_metrics_url}/debug/pprof/block"),
            ):
                try:
                    with urllib.request.urlopen(endpoint, timeout=duration_s + 15) as resp:
                        dest = out_dir / f"server_{kind}.pprof"
                        dest.write_bytes(resp.read())
                        pprof_files[kind] = str(dest.relative_to(run_dir))
                except Exception:
                    pass
            stop_ev.set()
            sampler.join(timeout=5.0)
            l0_max = max((s.get("l0_sublevels", 0) for s in samples), default=0)
            debt_max = max((s.get("compaction_debt_bytes", 0) for s in samples), default=0)
            ramp_max = max((s.get("read_amp", 0) for s in samples), default=0)
            wait_max = max((s.get("fuse_conn_waiting", 0) for s in samples), default=0)
            entry = {
                "scene": scene_id,
                "archive_path": f"runs/{run_dir.name}/profiling/{scene_dir_name}/",
                "samples_count": len(samples),
                "l0_sublevels_max": l0_max,
                "compaction_debt_bytes_max": debt_max,
                "read_amp_max": ramp_max,
                "fuse_conn_waiting_max": wait_max,
                "pprof_files": pprof_files,
                "top_cpu_hotspot": "see archived server_cpu.pprof",
                "top_mutex_hotspot": "see archived server_mutex.pprof",
            }
            _atomic_write_json(out_dir / "profiling_summary.json", entry)
            results.append(entry)
        finally:
            stop_ev.set()
            ctx.cleanup_handles()
            try:
                ctx._deploy_remote_target("bowl_staged", profiling=False)
            except Exception:
                pass
    return results


def run_benchmark(
    *,
    profile: str,
    suites: list[str],
    targets: list[str],
    runs_dir: Path,
    run_id: str | None = None,
    resume_run_id: str | None = None,
    server_endpoint: str = "http://127.0.0.1:9000",
    server_metrics_url: str = "http://127.0.0.1:9090",
    server_ssh: str | None = None,
    nvme_root: Path = Path("/mnt/nvme"),
    bowl_bin: str = "bowl",
    tools_bin: Path = Path("/usr/local/bench/bin"),
    corpus_dir: Path = Path("/usr/local/bench/corpus"),
    profiling: bool = False,
    update_latest: bool = False,
    env_check_json: Path | None = None,
    suite_loader: Any = None,
) -> dict[str, Any]:
    """Orchestrate selected benchmark suites with crash-safe resume and report rendering."""
    profile_cfg = load_profile(profile)
    effective_run_id = resume_run_id or run_id or (
        dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{profile}"
    )
    run_dir = Path(runs_dir) / effective_run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    meta_path = run_dir / "run_meta.json"
    if resume_run_id and meta_path.exists():
        run_meta = json.loads(meta_path.read_text(encoding="utf-8"))
        run_meta["resumed_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
        run_meta["requested_suites"] = suites
        run_meta["targets"] = targets
    else:
        env_check_data: dict[str, Any] = {}
        if env_check_json and env_check_json.exists():
            try:
                env_check_data = json.loads(env_check_json.read_text(encoding="utf-8"))
            except Exception:
                env_check_data = {}
        run_meta = {
            "run_id": effective_run_id,
            "profile": profile,
            "disclaimer": profile_cfg.get("disclaimer"),
            "git_commit": _git_commit_sha(),
            "started_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "hostname": platform.node(),
            "platform": platform.platform(),
            "targets": targets,
            "requested_suites": suites,
            "topology": profile_cfg.get("topology", {}),
            "env_check": env_check_data,
        }
    _atomic_write_json(meta_path, run_meta)

    loader = suite_loader or importlib.import_module
    suite_results: dict[str, dict[str, Any]] = {}
    executed_suites: list[str] = []
    skipped_resumed: list[str] = []
    failed_suites: list[str] = []

    def _make_ctx(suite_name: str, suite_dir: Path) -> BenchContext:
        return BenchContext(
            profile=profile,
            profile_cfg=profile_cfg,
            targets=targets,
            run_dir=run_dir,
            suite_dir=suite_dir,
            nvme_root=nvme_root,
            bowl_bin=bowl_bin,
            server_endpoint=server_endpoint,
            server_metrics_url=server_metrics_url,
            server_ssh=server_ssh,
            tools_bin=tools_bin,
            corpus_dir=corpus_dir,
        )

    for suite_name in suites:
        suite_dir = run_dir / suite_name
        suite_dir.mkdir(parents=True, exist_ok=True)
        result_path = suite_dir / "result.json"

        if resume_run_id:
            cached = _load_suite_result(result_path)
            if cached is not None:
                suite_results[suite_name] = cached
                skipped_resumed.append(suite_name)
                suite_md = bench_report.render_suite_report(suite_name, cached, run_meta)
                (suite_dir / "REPORT.md").write_text(suite_md, encoding="utf-8")
                continue

        ctx = _make_ctx(suite_name, suite_dir)
        t0 = time.monotonic()
        try:
            mod_path = SUITE_REGISTRY[suite_name]
            mod = loader(mod_path)
            raw_res = mod.run_suite(ctx)
            if not isinstance(raw_res, dict):
                raise TypeError(f"suite {suite_name} returned non-dict result: {type(raw_res).__name__}")
            suite_payload: dict[str, Any] = {
                "suite": raw_res.get("suite", suite_name),
                "status": "completed",
                "duration_s": round(time.monotonic() - t0, 3),
                "completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "cases": list(raw_res.get("cases", [])),
                "summary_markdown": str(raw_res.get("summary_markdown", "")),
            }
            executed_suites.append(suite_name)
        except Exception as exc:
            failed_suites.append(suite_name)
            err_msg = f"{type(exc).__name__}: {exc}"
            suite_payload = {
                "suite": suite_name,
                "status": "failed",
                "error": err_msg,
                "traceback": traceback.format_exc()[-4000:],
                "duration_s": round(time.monotonic() - t0, 3),
                "completed_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "cases": [],
                "summary_markdown": "",
            }
            if isinstance(exc, subprocess.CalledProcessError):
                stderr_raw = exc.stderr if isinstance(exc.stderr, str) else (
                    exc.stderr.decode("utf-8", errors="replace") if isinstance(exc.stderr, (bytes, bytearray)) else ""
                )
                stdout_raw = exc.stdout if isinstance(exc.stdout, str) else (
                    exc.stdout.decode("utf-8", errors="replace") if isinstance(exc.stdout, (bytes, bytearray)) else ""
                )
                suite_payload["cmd"] = exc.cmd
                suite_payload["returncode"] = exc.returncode
                suite_payload["stderr_tail"] = stderr_raw[-4000:]
                suite_payload["stdout_tail"] = stdout_raw[-4000:]
                if stderr_raw.strip():
                    first_tail = stderr_raw.strip().splitlines()[-1]
                    suite_payload["error"] = f"{err_msg} | stderr: {first_tail}"
        finally:
            ctx.cleanup_handles()

        _atomic_write_json(result_path, suite_payload)
        suite_results[suite_name] = suite_payload
        suite_md = bench_report.render_suite_report(suite_name, suite_payload, run_meta)
        (suite_dir / "REPORT.md").write_text(suite_md, encoding="utf-8")

    profiling_results: list[dict[str, Any]] = []
    if profiling and profile_cfg.get("profiling", {}).get("enabled", False):
        profiling_results = _run_profiling_pass(_make_ctx, run_dir, profile_cfg, server_metrics_url)

    run_meta["finished_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    run_meta["executed_suites"] = executed_suites
    run_meta["skipped_resumed_suites"] = skipped_resumed
    run_meta["failed_suites"] = failed_suites
    _atomic_write_json(meta_path, run_meta)

    summary = {
        "meta": run_meta,
        "suites": suite_results,
        "profiling": profiling_results,
    }
    _atomic_write_json(run_dir / "summary.json", summary)

    bench_report.generate_run_reports(
        run_dir,
        update_latest=update_latest,
        benchmark_root=BENCHMARK_ROOT,
    )
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bowl Benchmark v2 orchestrator")
    parser.add_argument(
        "--profile",
        default="smoke-local",
        help="Execution profile: full-vpc or smoke-local (default: smoke-local)",
    )
    parser.add_argument(
        "--suite",
        default="all",
        help="Comma-separated suite names or 'all' (default: all)",
    )
    parser.add_argument(
        "--targets",
        default=None,
        help="Comma-separated target list overriding profile defaults",
    )
    parser.add_argument(
        "--resume",
        default=None,
        metavar="RUN_ID",
        help="Resume an existing run under runs/<RUN_ID>, skipping completed suites",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Explicit run ID for a new run",
    )
    parser.add_argument(
        "--runs-dir",
        default=str(BENCHMARK_ROOT / "runs"),
        help="Directory to store benchmark run outputs",
    )
    parser.add_argument(
        "--server-endpoint",
        default=os.environ.get("BOWL_STORAGE_ENDPOINT", "http://127.0.0.1:9000"),
        help="Bowl storage service ConnectRPC endpoint",
    )
    parser.add_argument(
        "--server-metrics-url",
        default=os.environ.get("BOWL_STORAGE_METRICS_URL", "http://127.0.0.1:9090"),
        help="Bowl storage service admin/metrics HTTP base URL",
    )
    parser.add_argument(
        "--server-ssh",
        default=os.environ.get("BOWL_SERVER_SSH"),
        help="Optional SSH target for Node B (e.g. root@10.0.0.2)",
    )
    parser.add_argument(
        "--nvme-root",
        default=os.environ.get("BOWL_BENCH_NVME_ROOT", "/mnt/nvme"),
        help="Local NVMe root directory for mounts and staging",
    )
    parser.add_argument(
        "--bowl-bin",
        default=os.environ.get("BOWL_BIN", "bowl"),
        help="Path to bowl CLI binary",
    )
    parser.add_argument(
        "--tools-bin",
        default=os.environ.get("BOWL_BENCH_TOOLS_BIN", "/usr/local/bench/bin"),
        help="Directory containing compiled benchmark binaries",
    )
    parser.add_argument(
        "--corpus-dir",
        default=os.environ.get("BOWL_BENCH_CORPUS_DIR", "/usr/local/bench/corpus"),
        help="Directory containing deterministic offline workload corpus",
    )
    parser.add_argument(
        "--env-check-json",
        default=None,
        help="Path to env_check.sh JSON output to embed in run metadata",
    )
    parser.add_argument(
        "--profiling",
        action="store_true",
        help="Run separate non-scoring Profiling & Gauge Attribution Pass after measurement suites",
    )
    parser.add_argument(
        "--update-latest",
        action="store_true",
        help="Copy generated REPORT.md and summary.json to benchmark/REPORT.md and benchmark/latest_summary.json",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    profile_cfg = load_profile(args.profile)
    suites = resolve_suites(args.suite, profile_cfg)
    targets = resolve_targets(args.targets, profile_cfg)

    summary = run_benchmark(
        profile=args.profile,
        suites=suites,
        targets=targets,
        runs_dir=Path(args.runs_dir),
        run_id=args.run_id,
        resume_run_id=args.resume,
        server_endpoint=args.server_endpoint,
        server_metrics_url=args.server_metrics_url,
        server_ssh=args.server_ssh,
        nvme_root=Path(args.nvme_root),
        bowl_bin=args.bowl_bin,
        tools_bin=Path(args.tools_bin),
        corpus_dir=Path(args.corpus_dir),
        profiling=args.profiling,
        update_latest=args.update_latest,
        env_check_json=Path(args.env_check_json) if args.env_check_json else None,
    )
    failed = summary.get("meta", {}).get("failed_suites", [])
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
