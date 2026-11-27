"""workloads suite: 6 realistic developer and AI sandbox macro-workloads."""

from __future__ import annotations

import ctypes
import hashlib
import io
import os
import platform
import shutil
import sqlite3
import subprocess
import tarfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

from benchmark.lib.context import BenchContext, MountHandle
from benchmark.lib.stats import format_stat, summarize_latency, summarize_steady

SUITE_ID = "workloads"
SUPPORTED_TARGETS = [
    "ext4",
    "passthrough_ll",
    "bowl_staged",
    "bowl_sync",
    "jfs_pg_minio",
    "jfs_redis_minio",
    "drive9",
]

_AT_FDCWD = -100
_AT_SYMLINK_FOLLOW = 0x400


def _is_smoke(ctx: BenchContext) -> bool:
    return ctx.profile == "smoke-local"


def _is_bowl(target: str) -> bool:
    return target.startswith("bowl")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            buf = fh.read(1024 * 1024)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def _combine_validity(audit_status: str, integrity_ok: bool, primary_stat: dict[str, Any] | None = None) -> str:
    if not integrity_ok or audit_status == "INVALID":
        return "INVALID"
    if primary_stat and primary_stat.get("high_variance"):
        return "HIGH_VARIANCE"
    return "PASS"


def _skipped_case(case_id: str, target: str, tier: str, cache_state: str, reason: str) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": tier,
        "cache_state": cache_state,
        "runs_n": 0,
        "metrics": {},
        "validity_status": "SKIPPED",
        "integrity_verified": True,
        "counter_deltas": {},
        "notes": reason,
    }


def _delete_disk_quiet(ctx: BenchContext, disk_id: str | None) -> None:
    if not disk_id:
        return
    ctx.bowl_rpc_cli(["disk", "delete", disk_id, "-y", "--json"], check=False)


def _ensure_go_corpus(ctx: BenchContext) -> tuple[Path, Path]:
    """Return (go_bare_repo, go_work_repo) from ctx.corpus_dir, generating a fallback if absent."""
    bare = ctx.corpus_dir / "go-repo.git"
    work = ctx.corpus_dir / "go-repo"
    if bare.is_dir() and work.is_dir():
        return bare, work

    fallback_root = ctx.suite_dir / "_fallback_corpus"
    bare = fallback_root / "go-repo.git"
    work = fallback_root / "go-repo"
    if bare.is_dir() and work.is_dir():
        return bare, work

    shutil.rmtree(work, ignore_errors=True)
    shutil.rmtree(bare, ignore_errors=True)
    work.mkdir(parents=True, exist_ok=True)
    (work / "go.mod").write_text("module bench.local/corpusapp\n\ngo 1.22\n", encoding="utf-8")

    imports: list[str] = []
    calls: list[str] = []
    for i in range(1, 9):
        pkg = f"pkg{i:02d}"
        pdir = work / "internal" / pkg
        pdir.mkdir(parents=True, exist_ok=True)
        (pdir / f"{pkg}.go").write_text(
            f'package {pkg}\n\nimport "crypto/sha256"\nimport "encoding/binary"\n\n'
            f"func Compute(n int) uint64 {{\n"
            f"\tvar sum uint64\n\tvar buf [8]byte\n"
            f"\tfor j := 0; j < n; j++ {{\n"
            f"\t\tbinary.LittleEndian.PutUint64(buf[:], uint64(j*31+{i}))\n"
            f"\t\td := sha256.Sum256(buf[:])\n"
            f"\t\tsum ^= binary.LittleEndian.Uint64(d[:8])\n"
            f"\t}}\n\treturn sum\n}}\n",
            encoding="utf-8",
        )
        (pdir / f"{pkg}_test.go").write_text(
            f'package {pkg}\n\nimport "testing"\n\n'
            f"func TestCompute(t *testing.T) {{\n"
            f'\tif Compute(16) != Compute(16) {{ t.Fatal("mismatch") }}\n}}\n',
            encoding="utf-8",
        )
        imports.append(f'\t"bench.local/corpusapp/internal/{pkg}"')
        calls.append(f"\tsum ^= {pkg}.Compute(16)")

    cmd_dir = work / "cmd" / "corpusapp"
    cmd_dir.mkdir(parents=True, exist_ok=True)
    (cmd_dir / "main.go").write_text(
        'package main\n\nimport (\n\t"fmt"\n'
        + "\n".join(imports)
        + "\n)\n\nfunc main() {\n\tvar sum uint64\n"
        + "\n".join(calls)
        + '\n\tfmt.Printf("corpusapp:%016x\\n", sum)\n}\n',
        encoding="utf-8",
    )
    git_env = {
        "GIT_AUTHOR_NAME": "Bowl Bench",
        "GIT_AUTHOR_EMAIL": "bench@bowl.local",
        "GIT_COMMITTER_NAME": "Bowl Bench",
        "GIT_COMMITTER_EMAIL": "bench@bowl.local",
    }
    ctx.run_cmd(["git", "init", "-q", "-b", "main"], cwd=work, env=git_env)
    ctx.run_cmd(["git", "add", "."], cwd=work, env=git_env)
    ctx.run_cmd(["git", "commit", "-q", "-m", "init"], cwd=work, env=git_env)
    ctx.run_cmd(["git", "clone", "--bare", "-q", str(work), str(bare)], env=git_env)
    return bare, work


def _ensure_tar_corpus(ctx: BenchContext) -> tuple[Path, Path]:
    """Return (tar_path, sha256_manifest_path) from ctx.corpus_dir, generating fallback if absent."""
    tar_path = ctx.corpus_dir / "files_5k.tar"
    manifest_path = ctx.corpus_dir / "files_5k.sha256"
    if tar_path.is_file() and manifest_path.is_file():
        return tar_path, manifest_path

    fallback_root = ctx.suite_dir / "_fallback_corpus"
    fallback_root.mkdir(parents=True, exist_ok=True)
    tar_path = fallback_root / "files_5k.tar"
    manifest_path = fallback_root / "files_5k.sha256"
    if tar_path.is_file() and manifest_path.is_file():
        return tar_path, manifest_path

    file_bytes = 4096 if _is_smoke(ctx) else 30720
    base_block = hashlib.sha256(b"bowl-tar-fallback").digest() * ((file_bytes // 32) + 2)
    lines: list[str] = []
    with tarfile.open(tar_path, "w") as tf:
        for d in range(50):
            for f in range(100):
                idx = d * 100 + f
                rel = f"d{d:02d}/f{f:03d}.dat"
                hdr = f"FILE:{idx:06d}\n".encode("ascii")
                content = hdr + base_block[: max(0, file_bytes - len(hdr))]
                lines.append(f"{hashlib.sha256(content).hexdigest()}  {rel}\n")
                info = tarfile.TarInfo(name=rel)
                info.size = len(content)
                info.mode = 0o644
                tf.addfile(info, io.BytesIO(content))
    manifest_path.write_text("".join(lines), encoding="utf-8")
    return tar_path, manifest_path


def _case_git_clone_status_diff(ctx: BenchContext, target: str) -> dict[str, Any]:
    case_id = "git-clone-checkout-status-diff"
    runs_n = 5
    bare_repo, _ = _ensure_go_corpus(ctx)

    total_s: list[float] = []
    clone_s: list[float] = []
    warm_status_ms: list[float] = []
    cold_status_ms: list[float] = []
    diff_ms: list[float] = []
    drain_s_list: list[float] = []
    integrity_ok = True
    audit_ok = True
    audit_reasons: list[str] = []
    last_deltas: dict[str, Any] = {}

    for run_idx in range(-1, runs_n):
        is_warmup = run_idx < 0
        handle: MountHandle | None = None
        ro_handle: MountHandle | None = None
        disk_id: str | None = None
        try:
            handle = ctx.prepare_target(target, jfs_cache_mode="zero", disk_name=f"wl-git-{run_idx}")
            disk_id = handle.disk_id
            repo_dir = handle.mount_dir / "repo"

            before = ctx.snapshot_counters(target, handle)
            t_all = time.perf_counter()

            t0 = time.perf_counter()
            ctx.run_cmd(["git", "clone", "-q", str(bare_repo), str(repo_dir)], timeout=180)
            c_dt = time.perf_counter() - t0

            t1 = time.perf_counter_ns()
            ctx.run_cmd(["git", "status", "--porcelain"], cwd=repo_dir, timeout=60)
            ws_ms = (time.perf_counter_ns() - t1) / 1e6

            ctx.drop_caches("C1", target=target, handle=handle)
            t2 = time.perf_counter_ns()
            ctx.run_cmd(["git", "status", "--porcelain"], cwd=repo_dir, timeout=60)
            cs_ms = (time.perf_counter_ns() - t2) / 1e6

            main_go = repo_dir / "cmd" / "corpusapp" / "main.go"
            orig_text = main_go.read_text(encoding="utf-8")
            main_go.write_text(orig_text + "\n// benchmark edit\n", encoding="utf-8")

            t3 = time.perf_counter_ns()
            diff_proc = ctx.run_cmd(["git", "diff"], cwd=repo_dir, timeout=60)
            d_ms = (time.perf_counter_ns() - t3) / 1e6
            if "benchmark edit" not in diff_proc.stdout:
                integrity_ok = False

            ctx.run_cmd(["git", "checkout", "--", "."], cwd=repo_dir, timeout=60)

            after = ctx.snapshot_counters(target, handle)
            drain_dt = handle.unmount()
            total_dt = time.perf_counter() - t_all
            handle = None

            if _is_bowl(target) and disk_id:
                ro_handle = ctx.prepare_target(target, disk_id=disk_id, branch_name="main", read_only=True)
                fsck = ctx.run_cmd(
                    ["git", "fsck", "--full"],
                    cwd=ro_handle.mount_dir / "repo",
                    timeout=180,
                    check=False,
                )
                if fsck.returncode != 0:
                    integrity_ok = False
                ro_handle.unmount()
                ro_handle = None

            if not is_warmup:
                total_s.append(round(total_dt, 4))
                clone_s.append(round(c_dt, 4))
                warm_status_ms.append(round(ws_ms, 4))
                cold_status_ms.append(round(cs_ms, 4))
                diff_ms.append(round(d_ms, 4))
                drain_s_list.append(round(drain_dt, 4))
                audit = ctx.audit_case(before, after, target=target, cache_state="C1")
                last_deltas = audit["deltas"]
                if audit["validity_status"] != "PASS":
                    audit_ok = False
                    audit_reasons.extend(audit["reasons"])
        finally:
            if ro_handle is not None:
                ro_handle.unmount()
            if handle is not None:
                handle.unmount()
            _delete_disk_quiet(ctx, disk_id)

    tot_stat = summarize_steady(total_s)
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok, tot_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 2",
        "cache_state": "C0+C1",
        "runs_n": runs_n,
        "metrics": {
            "total_s": tot_stat,
            "clone_s": summarize_steady(clone_s),
            "warm_status_ms": summarize_steady(warm_status_ms),
            "cold_status_ms": summarize_steady(cold_status_ms),
            "diff_ms": summarize_steady(diff_ms),
            "drain_s": summarize_steady(drain_s_list),
        },
        "validity_status": status,
        "integrity_verified": integrity_ok,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons) if audit_reasons else f"git fsck --full={'PASS' if integrity_ok else 'FAIL'}",
    }


def _case_go_build_and_test(ctx: BenchContext, target: str) -> dict[str, Any]:
    case_id = "go-build-and-test-offline"
    smoke = _is_smoke(ctx)
    runs_n = 2 if smoke else 5
    _, go_work = _ensure_go_corpus(ctx)

    cold_build_s: list[float] = []
    test_s: list[float] = []
    incr_build_s: list[float] = []
    total_s: list[float] = []
    ref_binary_sha: str | None = None
    integrity_ok = True
    audit_ok = True
    audit_reasons: list[str] = []
    last_deltas: dict[str, Any] = {}

    for run_idx in range(-1, runs_n):
        is_warmup = run_idx < 0
        handle: MountHandle | None = None
        ro_handle: MountHandle | None = None
        disk_id: str | None = None
        try:
            handle = ctx.prepare_target(target, jfs_cache_mode="zero", disk_name=f"wl-gobuild-{run_idx}")
            disk_id = handle.disk_id
            proj_dir = handle.mount_dir / "goproj"
            shutil.copytree(go_work, proj_dir, ignore=shutil.ignore_patterns(".git"))
            gocache = handle.mount_dir / ".gocache"
            gotmp = handle.mount_dir / ".gotmp"
            gocache.mkdir(parents=True, exist_ok=True)
            gotmp.mkdir(parents=True, exist_ok=True)
            go_env = {
                "GOPROXY": "off",
                "GOWORK": "off",
                "GOCACHE": str(gocache),
                "GOTMPDIR": str(gotmp),
                "CGO_ENABLED": "0",
            }
            bin_path = proj_dir / "corpusapp"

            before = ctx.snapshot_counters(target, handle)
            t_all = time.perf_counter()

            t0 = time.perf_counter()
            ctx.run_cmd(
                ["go", "build", "-trimpath", "-o", str(bin_path), "./cmd/corpusapp"],
                cwd=proj_dir,
                env=go_env,
                timeout=300,
            )
            cb_dt = time.perf_counter() - t0
            bin_sha = _sha256_file(bin_path)
            if ref_binary_sha is None:
                ref_binary_sha = bin_sha
            elif bin_sha != ref_binary_sha:
                integrity_ok = False

            t1 = time.perf_counter()
            ctx.run_cmd(["go", "test", "-count=1", "./..."], cwd=proj_dir, env=go_env, timeout=300)
            tst_dt = time.perf_counter() - t1

            pkg1 = proj_dir / "internal" / "pkg01" / "pkg01.go"
            pkg1.write_text(pkg1.read_text(encoding="utf-8") + "\n// incr\n", encoding="utf-8")
            t2 = time.perf_counter()
            ctx.run_cmd(
                ["go", "build", "-trimpath", "-o", str(proj_dir / "corpusapp_incr"), "./cmd/corpusapp"],
                cwd=proj_dir,
                env=go_env,
                timeout=300,
            )
            ib_dt = time.perf_counter() - t2
            incr_sha = _sha256_file(proj_dir / "corpusapp_incr")

            after = ctx.snapshot_counters(target, handle)
            handle.unmount()
            tot_dt = time.perf_counter() - t_all
            handle = None

            if _is_bowl(target) and disk_id:
                ro_handle = ctx.prepare_target(target, disk_id=disk_id, branch_name="main", read_only=True)
                if _sha256_file(ro_handle.mount_dir / "goproj" / "corpusapp") != bin_sha:
                    integrity_ok = False
                if _sha256_file(ro_handle.mount_dir / "goproj" / "corpusapp_incr") != incr_sha:
                    integrity_ok = False
                ro_handle.unmount()
                ro_handle = None

            if not is_warmup:
                cold_build_s.append(round(cb_dt, 4))
                test_s.append(round(tst_dt, 4))
                incr_build_s.append(round(ib_dt, 4))
                total_s.append(round(tot_dt, 4))
                audit = ctx.audit_case(before, after, target=target, cache_state="C0")
                last_deltas = audit["deltas"]
                if audit["validity_status"] != "PASS":
                    audit_ok = False
                    audit_reasons.extend(audit["reasons"])
        finally:
            if ro_handle is not None:
                ro_handle.unmount()
            if handle is not None:
                handle.unmount()
            _delete_disk_quiet(ctx, disk_id)

    tot_stat = summarize_steady(total_s)
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok, tot_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 2",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "total_s": tot_stat,
            "cold_build_s": summarize_steady(cold_build_s),
            "test_s": summarize_steady(test_s),
            "incremental_build_s": summarize_steady(incr_build_s),
            "binary_sha256": ref_binary_sha,
        },
        "validity_status": status,
        "integrity_verified": integrity_ok,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons) if audit_reasons else f"binary SHA-256={'PASS' if integrity_ok else 'FAIL'}",
    }


def _populate_sqlite_db(db_path: Path, target_mib: int, journal_mode: str) -> None:
    conn = sqlite3.connect(str(db_path), timeout=60.0)
    try:
        conn.execute(f"PRAGMA journal_mode={journal_mode}")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS kv (id INTEGER PRIMARY KEY, val BLOB NOT NULL, rev INTEGER NOT NULL)"
        )
        row_size = 4096
        num_rows = max(256, (target_mib * 1024 * 1024) // row_size)
        blob = b"Q" * (row_size - 64)
        batch_size = 500
        with conn:
            for start in range(0, num_rows, batch_size):
                rows = [(i, blob, 0) for i in range(start, min(num_rows, start + batch_size))]
                conn.executemany("INSERT OR REPLACE INTO kv(id, val, rev) VALUES (?, ?, ?)", rows)
    finally:
        conn.close()


def _run_sqlite_tx_loop(db_path: Path, journal_mode: str, tx_count: int) -> tuple[float, list[float], bool]:
    conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=60.0)
    lats_ms: list[float] = []
    blob = b"U" * 512
    try:
        jm = conn.execute(f"PRAGMA journal_mode={journal_mode}").fetchone()[0]
        conn.execute("PRAGMA synchronous=FULL")
        _ = jm
        row_max = int(conn.execute("SELECT MAX(id) FROM kv").fetchone()[0] or 100)
        t_start = time.perf_counter()
        for i in range(tx_count):
            row_id = (i * 17) % max(1, row_max)
            t0 = time.perf_counter_ns()
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE kv SET val = ?, rev = rev + 1 WHERE id = ?", (blob, row_id))
            conn.execute("COMMIT")
            lats_ms.append((time.perf_counter_ns() - t0) / 1e6)
        elapsed = max(1e-6, time.perf_counter() - t_start)
        chk = conn.execute("PRAGMA integrity_check").fetchone()[0]
        return round(tx_count / elapsed, 2), lats_ms, (str(chk).lower() == "ok")
    finally:
        conn.close()


def _run_sqlite_wal_concurrency(db_path: Path, write_txs: int) -> tuple[float, float, bool]:
    stop_readers = threading.Event()
    read_counts = [0] * 4
    reader_errors: list[Exception] = []

    def _reader(tid: int) -> None:
        conn = sqlite3.connect(str(db_path), timeout=60.0)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            cnt = 0
            while not stop_readers.is_set():
                _ = conn.execute("SELECT COUNT(*), SUM(rev) FROM kv WHERE id < 200").fetchone()
                cnt += 1
            read_counts[tid] = cnt
        except Exception as exc:
            reader_errors.append(exc)
        finally:
            conn.close()

    readers = [threading.Thread(target=_reader, args=(i,), daemon=True) for i in range(4)]
    for t in readers:
        t.start()
    try:
        w_tps, _, ok = _run_sqlite_tx_loop(db_path, "WAL", write_txs)
    finally:
        stop_readers.set()
        for t in readers:
            t.join(timeout=30)

    total_reads = float(sum(read_counts))
    return w_tps, total_reads, (ok and not reader_errors)


def _case_sqlite_oltp(ctx: BenchContext, target: str) -> dict[str, Any]:
    case_id = "sqlite-oltp-5k-tx-delete-wal"
    if target == "jfs_redis_minio":
        return _skipped_case(
            case_id,
            target,
            "Tier 1",
            "C0",
            "Tier 0.5 target excluded from Tier 1 SQLite synchronous=FULL benchmark",
        )

    smoke = _is_smoke(ctx)
    runs_n = 2 if smoke else 5
    tx_count = 1000 if smoke else 5000
    db_mib = 10 if smoke else 100

    del_tps_runs: list[float] = []
    wal_tps_runs: list[float] = []
    wal_conc_w_tps: list[float] = []
    del_lats: list[float] = []
    wal_lats: list[float] = []
    integrity_ok = True
    audit_ok = True
    audit_reasons: list[str] = []
    last_deltas: dict[str, Any] = {}

    handle: MountHandle | None = None
    disk_id: str | None = None
    try:
        handle = ctx.prepare_target(target, jfs_cache_mode="zero", disk_name="wl-sqlite-oltp")
        disk_id = handle.disk_id
        for run_idx in range(runs_n):
            before = ctx.snapshot_counters(target, handle)
            db_del = handle.mount_dir / f"oltp_del_{run_idx}.db"
            db_wal = handle.mount_dir / f"oltp_wal_{run_idx}.db"

            _populate_sqlite_db(db_del, db_mib, "DELETE")
            d_tps, d_lats, d_ok = _run_sqlite_tx_loop(db_del, "DELETE", tx_count)
            del_tps_runs.append(d_tps)
            del_lats.extend(d_lats)
            integrity_ok = integrity_ok and d_ok

            _populate_sqlite_db(db_wal, db_mib, "WAL")
            w_tps, w_lats, w_ok = _run_sqlite_tx_loop(db_wal, "WAL", tx_count)
            wal_tps_runs.append(w_tps)
            wal_lats.extend(w_lats)
            integrity_ok = integrity_ok and w_ok

            cw_tps, _, cw_ok = _run_sqlite_wal_concurrency(db_wal, max(100, tx_count // 5))
            wal_conc_w_tps.append(cw_tps)
            integrity_ok = integrity_ok and cw_ok

            after = ctx.snapshot_counters(target, handle)
            audit = ctx.audit_case(before, after, target=target, cache_state="C0")
            last_deltas = audit["deltas"]
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])

            for p in (db_del, db_wal, Path(f"{db_wal}-wal"), Path(f"{db_wal}-shm")):
                p.unlink(missing_ok=True)
    finally:
        if handle is not None:
            handle.unmount()
        _delete_disk_quiet(ctx, disk_id)

    wal_stat = summarize_steady(wal_tps_runs)
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok, wal_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 1 (PRAGMA synchronous=FULL)",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "tx_count_per_run": tx_count,
            "db_size_mib": db_mib,
            "delete_tx_s": summarize_steady(del_tps_runs),
            "delete_tx_latency_ms": summarize_latency(del_lats, kind="discrete"),
            "wal_tx_s": wal_stat,
            "wal_tx_latency_ms": summarize_latency(wal_lats, kind="discrete"),
            "wal_1w4r_writer_tx_s": summarize_steady(wal_conc_w_tps),
        },
        "validity_status": status,
        "integrity_verified": integrity_ok,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"PRAGMA synchronous=FULL + integrity_check={'ok' if integrity_ok else 'FAIL'}",
    }


def _case_tar_extract_and_rm(ctx: BenchContext, target: str) -> dict[str, Any]:
    case_id = "tar-extract-5k-files-and-rm"
    runs_n = 5
    tar_path, manifest_path = _ensure_tar_corpus(ctx)
    expected_manifest: dict[str, str] = {}
    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2:
            expected_manifest[parts[1]] = parts[0]

    extract_s: list[float] = []
    rm_s: list[float] = []
    total_s: list[float] = []
    integrity_ok = True
    audit_ok = True
    audit_reasons: list[str] = []
    last_deltas: dict[str, Any] = {}

    for run_idx in range(-1, runs_n):
        is_warmup = run_idx < 0
        handle: MountHandle | None = None
        remount_handle: MountHandle | None = None
        disk_id: str | None = None
        try:
            handle = ctx.prepare_target(target, jfs_cache_mode="zero", disk_name=f"wl-tar-{run_idx}")
            disk_id = handle.disk_id
            ext_dir = handle.mount_dir / "extracted"
            ext_dir.mkdir(parents=True, exist_ok=True)

            before = ctx.snapshot_counters(target, handle)
            t_all = time.perf_counter()

            t0 = time.perf_counter()
            ctx.run_cmd(["tar", "-xf", str(tar_path), "-C", str(ext_dir)], timeout=300)
            e_dt = time.perf_counter() - t0

            if _is_bowl(target) and disk_id:
                handle.unmount()
                handle = None
                remount_handle = ctx.prepare_target(target, disk_id=disk_id, branch_name="main")
                verify_dir = remount_handle.mount_dir / "extracted"
                active_handle = remount_handle
            else:
                ctx.drop_caches("C1", target=target, handle=handle)
                verify_dir = ext_dir
                active_handle = handle

            for rel, exp_sha in expected_manifest.items():
                fp = verify_dir / rel
                if not fp.is_file() or _sha256_file(fp) != exp_sha:
                    integrity_ok = False
                    break

            t1 = time.perf_counter()
            shutil.rmtree(verify_dir)
            r_dt = time.perf_counter() - t1
            if verify_dir.exists():
                integrity_ok = False

            after = ctx.snapshot_counters(target, active_handle)
            tot_dt = time.perf_counter() - t_all
            if not is_warmup:
                extract_s.append(round(e_dt, 4))
                rm_s.append(round(r_dt, 4))
                total_s.append(round(tot_dt, 4))
                audit = ctx.audit_case(before, after, target=target, cache_state="C1")
                last_deltas = audit["deltas"]
                if audit["validity_status"] != "PASS":
                    audit_ok = False
                    audit_reasons.extend(audit["reasons"])
        finally:
            if remount_handle is not None:
                remount_handle.unmount()
            if handle is not None:
                handle.unmount()
            _delete_disk_quiet(ctx, disk_id)

    tot_stat = summarize_steady(total_s)
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok, tot_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 2",
        "cache_state": "C1",
        "runs_n": runs_n,
        "metrics": {
            "file_count": len(expected_manifest),
            "extract_s": summarize_steady(extract_s),
            "rm_s": summarize_steady(rm_s),
            "total_s": tot_stat,
        },
        "validity_status": status,
        "integrity_verified": integrity_ok,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons) if audit_reasons else f"5k manifest SHA-256={'PASS' if integrity_ok else 'FAIL'}",
    }


def _o_tmpfile_flag() -> int:
    if hasattr(os, "O_TMPFILE"):
        return int(os.O_TMPFILE)
    o_dir = getattr(os, "O_DIRECTORY", 0o40000 if platform.machine() in ("aarch64", "arm64") else 0o200000)
    return 0o20000000 | o_dir


def _load_libc_linkat() -> Any:
    libc = ctypes.CDLL("libc.so.6", use_errno=True)
    libc.linkat.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
    ]
    libc.linkat.restype = ctypes.c_int
    return libc


def _write_all_fd(fd: int, data: bytes) -> None:
    view = memoryview(data)
    written = 0
    total = len(data)
    while written < total:
        n = os.write(fd, view[written:])
        if n <= 0:
            raise OSError("short write on O_TMPFILE")
        written += n


def _case_tmpfile_linkat_promote_discard(ctx: BenchContext, target: str) -> dict[str, Any]:
    case_id = "tmpfile-linkat-promote-and-discard"
    smoke = _is_smoke(ctx)
    runs_n = 2 if smoke else 5
    small_count = 100 if smoke else 1000
    small_bytes = 64 * 1024
    large_count = 2 if smoke else 20
    large_bytes = (8 if smoke else 32) * 1024 * 1024
    discard_count = 100 if smoke else 1000

    o_tmpfile = _o_tmpfile_flag()
    libc = _load_libc_linkat()

    total_s: list[float] = []
    small_promote_ms: list[float] = []
    large_write_ms: list[float] = []
    large_linkat_ms: list[float] = []
    discard_s: list[float] = []
    discard_write_rpcs_max = 0
    promotes_delta_last = 0
    integrity_ok = True
    audit_ok = True
    audit_reasons: list[str] = []
    last_deltas: dict[str, Any] = {}

    small_payload = os.urandom(small_bytes)
    small_sha = hashlib.sha256(small_payload).hexdigest()
    large_block = os.urandom(1024 * 1024)

    for run_idx in range(runs_n):
        handle: MountHandle | None = None
        ro_handle: MountHandle | None = None
        disk_id: str | None = None
        try:
            handle = ctx.prepare_target(target, disk_name=f"wl-tmpfile-{run_idx}")
            disk_id = handle.disk_id
            work_dir = handle.mount_dir / "tmpfile_work"
            work_dir.mkdir(parents=True, exist_ok=True)
            dir_fd = os.open(work_dir, os.O_RDONLY | os.O_DIRECTORY)

            expected_large_hashes: dict[str, str] = {}
            before = ctx.snapshot_counters(target, handle)
            t_all = time.perf_counter()
            try:
                for i in range(small_count):
                    fd = os.open(work_dir, o_tmpfile | os.O_RDWR, 0o644)
                    try:
                        t0 = time.perf_counter_ns()
                        _write_all_fd(fd, small_payload)
                        src = f"/proc/self/fd/{fd}".encode("ascii")
                        dst = f"small_{i:04d}.dat".encode("ascii")
                        rc = libc.linkat(_AT_FDCWD, src, dir_fd, dst, _AT_SYMLINK_FOLLOW)
                        if rc != 0:
                            err = ctypes.get_errno()
                            raise OSError(err, os.strerror(err))
                        small_promote_ms.append((time.perf_counter_ns() - t0) / 1e6)
                    finally:
                        os.close(fd)

                for i in range(large_count):
                    fd = os.open(work_dir, o_tmpfile | os.O_RDWR, 0o644)
                    h = hashlib.sha256()
                    try:
                        tw0 = time.perf_counter_ns()
                        for _ in range(large_bytes // len(large_block)):
                            _write_all_fd(fd, large_block)
                            h.update(large_block)
                        large_write_ms.append((time.perf_counter_ns() - tw0) / 1e6)

                        tl0 = time.perf_counter_ns()
                        src = f"/proc/self/fd/{fd}".encode("ascii")
                        name = f"large_{i:03d}.dat"
                        rc = libc.linkat(_AT_FDCWD, src, dir_fd, name.encode("ascii"), _AT_SYMLINK_FOLLOW)
                        if rc != 0:
                            err = ctypes.get_errno()
                            raise OSError(err, os.strerror(err))
                        large_linkat_ms.append((time.perf_counter_ns() - tl0) / 1e6)
                        expected_large_hashes[f"tmpfile_work/{name}"] = h.hexdigest()
                    finally:
                        os.close(fd)

                before_disc = ctx.snapshot_counters(target, handle)
                td0 = time.perf_counter()
                for _ in range(discard_count):
                    fd = os.open(work_dir, o_tmpfile | os.O_RDWR, 0o644)
                    try:
                        _write_all_fd(fd, small_payload)
                    finally:
                        os.close(fd)
                discard_s.append(round(time.perf_counter() - td0, 4))
                after_disc = ctx.snapshot_counters(target, handle)
                disc_audit = ctx.audit_case(before_disc, after_disc, target=target, cache_state="C0")
                disc_write_bytes = int(disc_audit["deltas"].get("server_http_write_request_bytes", 0))
                discard_write_rpcs_max = max(discard_write_rpcs_max, disc_write_bytes)
                if target == "bowl_staged" and disc_write_bytes > 0:
                    integrity_ok = False
                    audit_reasons.append(
                        f"INVALID_RUN: staged O_TMPFILE discard sent {disc_write_bytes} Write RPC bytes (expected 0)"
                    )
            finally:
                os.close(dir_fd)

            after = ctx.snapshot_counters(target, handle)
            audit = ctx.audit_case(before, after, target=target, cache_state="C0")
            last_deltas = audit["deltas"]
            promotes_delta_last = int(last_deltas.get("client_tmpfile_promotes", 0))
            if target == "bowl_staged" and promotes_delta_last < (small_count + large_count):
                integrity_ok = False
                audit_reasons.append(
                    f"INVALID_RUN: tmpfile_promotes_total delta={promotes_delta_last} < expected {small_count + large_count}"
                )
            if audit["validity_status"] != "PASS":
                audit_ok = False
                audit_reasons.extend(audit["reasons"])

            handle.unmount()
            total_s.append(round(time.perf_counter() - t_all, 4))
            handle = None

            if disk_id:
                ro_handle = ctx.prepare_target(target, disk_id=disk_id, branch_name="main", read_only=True)
                for idx in (0, small_count // 2, small_count - 1):
                    sp = ro_handle.mount_dir / "tmpfile_work" / f"small_{idx:04d}.dat"
                    if not sp.is_file() or _sha256_file(sp) != small_sha:
                        integrity_ok = False
                for rel, exp_sha in expected_large_hashes.items():
                    lp = ro_handle.mount_dir / rel
                    if not lp.is_file() or _sha256_file(lp) != exp_sha:
                        integrity_ok = False
                ro_handle.unmount()
                ro_handle = None
        finally:
            if ro_handle is not None:
                ro_handle.unmount()
            if handle is not None:
                handle.unmount()
            _delete_disk_quiet(ctx, disk_id)

    tot_stat = summarize_steady(total_s)
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok, tot_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 1 / Tier 2",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "total_s": tot_stat,
            "small_promote_ms": summarize_latency(small_promote_ms, kind="discrete"),
            "large_write_ms": summarize_latency(large_write_ms, kind="discrete"),
            "large_linkat_ms": summarize_latency(large_linkat_ms, kind="discrete"),
            "discard_s": summarize_steady(discard_s),
            "discard_server_write_bytes_max": discard_write_rpcs_max,
            "tmpfile_promotes_delta": promotes_delta_last,
        },
        "validity_status": status,
        "integrity_verified": integrity_ok,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"promotes={promotes_delta_last}, discard_rpc_bytes={discard_write_rpcs_max}, remount_sha={'PASS' if integrity_ok else 'FAIL'}",
    }


def _case_ai_sandbox_fork_build_discard(ctx: BenchContext) -> dict[str, Any]:
    case_id = "ai-sandbox-fork-build-discard"
    target = "bowl_staged"
    smoke = _is_smoke(ctx)
    runs_n = 3 if smoke else 20
    _, go_work = _ensure_go_corpus(ctx)

    cycle_s: list[float] = []
    fork_ms: list[float] = []
    mount_ms: list[float] = []
    build_test_s: list[float] = []
    commit_ms: list[float] = []
    umount_ms: list[float] = []
    delete_ms: list[float] = []
    integrity_ok = True
    audit_ok = True
    audit_reasons: list[str] = []
    last_deltas: dict[str, Any] = {}
    baseline_used_bytes = 0
    final_used_bytes = 0

    base_handle: MountHandle | None = None
    disk_id: str | None = None
    try:
        base_handle = ctx.prepare_target(target, disk_name="wl-ai-sandbox-base")
        disk_id = base_handle.disk_id
        assert disk_id is not None

        shutil.copytree(go_work, base_handle.mount_dir / "repo", ignore=shutil.ignore_patterns(".git"))
        base_handle.unmount()
        base_handle = None

        ctx.bowl_rpc_cli(["commit", "create", "-d", disk_id, "--branch", "main", "-m", "sandbox-base", "--json"])
        base_info = ctx.bowl_rpc_cli(["disk", "get", disk_id, "--json"])
        baseline_used_bytes = int(base_info.get("used_bytes", 0)) if isinstance(base_info, dict) else 0

        for run_idx in range(-1, runs_n):
            is_warmup = run_idx < 0
            br = f"sandbox_{run_idx + 1:03d}"
            sb_handle: MountHandle | None = None
            try:
                before = ctx.snapshot_counters(target)
                t_cycle = time.perf_counter()

                t0 = time.perf_counter_ns()
                ctx.bowl_rpc_cli(["branch", "create", br, "-d", disk_id, "--from", "main", "--json"])
                fk_ms = (time.perf_counter_ns() - t0) / 1e6

                t1 = time.perf_counter_ns()
                sb_handle = ctx.prepare_target(target, disk_id=disk_id, branch_name=br)
                mt_ms = (time.perf_counter_ns() - t1) / 1e6

                repo_dir = sb_handle.mount_dir / "repo"
                pkg1 = repo_dir / "internal" / "pkg01" / "pkg01.go"
                pkg1.write_text(
                    pkg1.read_text(encoding="utf-8") + f"\n// ai-sandbox-iter-{run_idx}\n",
                    encoding="utf-8",
                )

                gocache = sb_handle.mount_dir / ".gocache"
                gotmp = sb_handle.mount_dir / ".gotmp"
                gocache.mkdir(parents=True, exist_ok=True)
                gotmp.mkdir(parents=True, exist_ok=True)
                go_env = {
                    "GOPROXY": "off",
                    "GOWORK": "off",
                    "GOCACHE": str(gocache),
                    "GOTMPDIR": str(gotmp),
                    "CGO_ENABLED": "0",
                }
                bin_path = repo_dir / "sandbox_bin"
                t2 = time.perf_counter()
                ctx.run_cmd(
                    ["go", "build", "-trimpath", "-o", str(bin_path), "./cmd/corpusapp"],
                    cwd=repo_dir,
                    env=go_env,
                    timeout=300,
                )
                ctx.run_cmd(["go", "test", "-count=1", "./internal/pkg01"], cwd=repo_dir, env=go_env, timeout=180)
                bt_s = time.perf_counter() - t2
                if not bin_path.is_file() or bin_path.stat().st_size == 0:
                    integrity_ok = False
                _ = _sha256_file(bin_path)
                _ = os.statvfs(sb_handle.mount_dir)

                t3 = time.perf_counter_ns()
                ctx.bowl_rpc_cli(["commit", "create", "-d", disk_id, "--branch", br, "-m", f"sb-{run_idx}", "--json"])
                cm_ms = (time.perf_counter_ns() - t3) / 1e6

                t4 = time.perf_counter_ns()
                sb_handle.unmount()
                um_ms = (time.perf_counter_ns() - t4) / 1e6
                sb_handle = None

                t5 = time.perf_counter_ns()
                ctx.bowl_rpc_cli(["branch", "delete", br, "-d", disk_id, "-y", "--json"])
                ctx.bowl_rpc_cli(["commit", "prune", "-d", disk_id, "-y", "--json"])
                dl_ms = (time.perf_counter_ns() - t5) / 1e6

                tot_s = time.perf_counter() - t_cycle
                after = ctx.snapshot_counters(target)

                if not is_warmup:
                    cycle_s.append(round(tot_s, 4))
                    fork_ms.append(round(fk_ms, 4))
                    mount_ms.append(round(mt_ms, 4))
                    build_test_s.append(round(bt_s, 4))
                    commit_ms.append(round(cm_ms, 4))
                    umount_ms.append(round(um_ms, 4))
                    delete_ms.append(round(dl_ms, 4))
                    audit = ctx.audit_case(before, after, target=target, cache_state="C0")
                    last_deltas = audit["deltas"]
                    if audit["validity_status"] != "PASS":
                        audit_ok = False
                        audit_reasons.extend(audit["reasons"])
            finally:
                if sb_handle is not None:
                    sb_handle.unmount()
                ctx.bowl_rpc_cli(["branch", "delete", br, "-d", disk_id, "-y", "--json"], check=False)

        final_info = ctx.bowl_rpc_cli(["disk", "get", disk_id, "--json"])
        final_used_bytes = int(final_info.get("used_bytes", 0)) if isinstance(final_info, dict) else 0
        if final_used_bytes > baseline_used_bytes:
            integrity_ok = False
            audit_reasons.append(
                f"INVALID_RUN: leaked branch space after delete+prune (final={final_used_bytes} > base={baseline_used_bytes})"
            )
    finally:
        if base_handle is not None:
            base_handle.unmount()
        _delete_disk_quiet(ctx, disk_id)

    cycle_stat = summarize_latency(cycle_s, kind="discrete")
    status = _combine_validity("PASS" if audit_ok else "INVALID", integrity_ok, cycle_stat)
    return {
        "case_id": case_id,
        "target": target,
        "durability_tier": "Tier 2",
        "cache_state": "C0",
        "runs_n": runs_n,
        "metrics": {
            "cycle_s": cycle_stat,
            "fork_ms": summarize_latency(fork_ms, kind="discrete"),
            "mount_ms": summarize_latency(mount_ms, kind="discrete"),
            "build_test_s": summarize_latency(build_test_s, kind="discrete"),
            "commit_ms": summarize_latency(commit_ms, kind="discrete"),
            "umount_ms": summarize_latency(umount_ms, kind="discrete"),
            "delete_prune_ms": summarize_latency(delete_ms, kind="discrete"),
            "baseline_used_bytes": baseline_used_bytes,
            "final_used_bytes": final_used_bytes,
        },
        "validity_status": status,
        "integrity_verified": integrity_ok,
        "counter_deltas": last_deltas,
        "notes": "; ".join(audit_reasons)
        if audit_reasons
        else f"used_bytes base={baseline_used_bytes} -> final={final_used_bytes} (reclaimed)",
    }


def _render_summary_markdown(cases: list[dict[str, Any]]) -> str:
    lines = [
        "### `workloads` Detail Summary",
        "",
        "| Case | Target | Tier | Runs | Primary Metric | Notes | Gate |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for c in cases:
        m = c.get("metrics", {})
        primary = "N/A"
        if "wal_tx_s" in m:
            primary = f"WAL {format_stat(m['wal_tx_s'], unit='tx/s')}, DEL {format_stat(m.get('delete_tx_s'), unit='tx/s')}"
        elif "cycle_s" in m:
            cy = m["cycle_s"]
            primary = f"P50={cy.get('p50')} / P95={cy.get('p95')} / Max={cy.get('max')} s"
        elif "total_s" in m:
            primary = format_stat(m["total_s"], unit="s", show_range=True)
        lines.append(
            f"| `{c['case_id']}` | `{c['target']}` | {c['durability_tier']} | {c['runs_n']} | {primary} | {c['notes']} | `{c['validity_status']}` |"
        )
    return "\n".join(lines) + "\n"


def _format_exc_note(exc: Exception) -> str:
    if isinstance(exc, subprocess.CalledProcessError):
        tail = (exc.stderr or exc.stdout or "").strip().splitlines()
        detail = " | ".join(tail[-5:]) if tail else str(exc)
        return f"CalledProcessError(rc={exc.returncode}): {detail}"[:500]
    return f"{type(exc).__name__}: {exc}"[:500]


def _safe_run_case(
    case_id: str,
    target: str,
    tier: str,
    cache_state: str,
    fn: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    try:
        return fn()
    except Exception as exc:
        return {
            "case_id": case_id,
            "target": target,
            "durability_tier": tier,
            "cache_state": cache_state,
            "runs_n": 0,
            "metrics": {},
            "validity_status": "INVALID",
            "integrity_verified": False,
            "counter_deltas": {},
            "notes": _format_exc_note(exc),
        }


def run_suite(ctx: BenchContext) -> dict[str, Any]:
    smoke = _is_smoke(ctx)
    active_targets = [t for t in ctx.targets if t in SUPPORTED_TARGETS]
    cases: list[dict[str, Any]] = []

    if smoke:
        for target in active_targets:
            cases.append(
                _safe_run_case(
                    "go-build-and-test-offline",
                    target,
                    "Tier 2",
                    "C0",
                    lambda t=target: _case_go_build_and_test(ctx, t),
                )
            )
            cases.append(
                _safe_run_case(
                    "sqlite-oltp-5k-tx-delete-wal",
                    target,
                    "Tier 1 (PRAGMA synchronous=FULL)",
                    "C0",
                    lambda t=target: _case_sqlite_oltp(ctx, t),
                )
            )
        for bowl_target in ("bowl_staged", "bowl_sync"):
            if bowl_target in active_targets:
                cases.append(
                    _safe_run_case(
                        "tmpfile-linkat-promote-and-discard",
                        bowl_target,
                        "Tier 1 / Tier 2",
                        "C0",
                        lambda bt=bowl_target: _case_tmpfile_linkat_promote_discard(ctx, bt),
                    )
                )
        if "bowl_staged" in active_targets:
            cases.append(
                _safe_run_case(
                    "ai-sandbox-fork-build-discard",
                    "bowl_staged",
                    "Tier 2",
                    "C0",
                    lambda: _case_ai_sandbox_fork_build_discard(ctx),
                )
            )
    else:
        for target in active_targets:
            cases.append(
                _safe_run_case(
                    "git-clone-checkout-status-diff",
                    target,
                    "Tier 2",
                    "C0+C1",
                    lambda t=target: _case_git_clone_status_diff(ctx, t),
                )
            )
            cases.append(
                _safe_run_case(
                    "go-build-and-test-offline",
                    target,
                    "Tier 2",
                    "C0",
                    lambda t=target: _case_go_build_and_test(ctx, t),
                )
            )
            cases.append(
                _safe_run_case(
                    "sqlite-oltp-5k-tx-delete-wal",
                    target,
                    "Tier 1 (PRAGMA synchronous=FULL)",
                    "C0",
                    lambda t=target: _case_sqlite_oltp(ctx, t),
                )
            )
            cases.append(
                _safe_run_case(
                    "tar-extract-5k-files-and-rm",
                    target,
                    "Tier 2",
                    "C1",
                    lambda t=target: _case_tar_extract_and_rm(ctx, t),
                )
            )
        for bowl_target in ("bowl_staged", "bowl_sync"):
            if bowl_target in active_targets:
                cases.append(
                    _safe_run_case(
                        "tmpfile-linkat-promote-and-discard",
                        bowl_target,
                        "Tier 1 / Tier 2",
                        "C0",
                        lambda bt=bowl_target: _case_tmpfile_linkat_promote_discard(ctx, bt),
                    )
                )
        if "bowl_staged" in active_targets:
            cases.append(
                _safe_run_case(
                    "ai-sandbox-fork-build-discard",
                    "bowl_staged",
                    "Tier 2",
                    "C0",
                    lambda: _case_ai_sandbox_fork_build_discard(ctx),
                )
            )

    return {
        "suite": SUITE_ID,
        "cases": cases,
        "summary_markdown": _render_summary_markdown(cases),
    }
