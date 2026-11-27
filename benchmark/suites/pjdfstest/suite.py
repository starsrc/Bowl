from __future__ import annotations

import ctypes
import hashlib
import os
import re
import shutil
import time
from pathlib import Path
from typing import Any

from benchmark.lib.context import BenchContext, MountHandle

SUITE_ID = "pjdfstest"
SUPPORTED_TARGETS = ["bowl_staged", "bowl_sync"]

SMOKE_CATEGORIES = (
    "open",
    "rename",
    "unlink",
    "link",
    "chmod",
    "chown",
    "truncate",
    "symlink",
    "mkdir",
    "rmdir",
    "utimensat",
)
SMOKE_TEST_LIMIT = 30

_AT_FDCWD = -100
_AT_SYMLINK_FOLLOW = 0x400
_O_TMPFILE = getattr(os, "O_TMPFILE", 0o20000000)


def _find_pjdfstest_tests_dir(ctx: BenchContext) -> Path | None:
    candidates = [
        Path("/usr/local/bench/pjdfstest/tests"),
        ctx.tools_bin.parent / "pjdfstest" / "tests",
        Path("/opt/pjdfstest/tests"),
    ]
    for path in candidates:
        if path.is_dir():
            return path
    return None


def _select_test_files(tests_dir: Path, smoke: bool) -> list[Path]:
    if not smoke:
        return sorted(tests_dir.rglob("*.t"))

    per_cat: list[list[Path]] = []
    for cat in SMOKE_CATEGORIES:
        cat_dir = tests_dir / cat
        if cat_dir.is_dir():
            files = sorted(cat_dir.glob("*.t"))
            if files:
                per_cat.append(files)

    selected: list[Path] = []
    idx = 0
    while len(selected) < SMOKE_TEST_LIMIT:
        progressed = False
        for files in per_cat:
            if idx < len(files):
                selected.append(files[idx])
                progressed = True
                if len(selected) >= SMOKE_TEST_LIMIT:
                    break
        if not progressed:
            break
        idx += 1
    return selected


def parse_tap_output(log_text: str) -> dict[str, Any]:
    header_re = re.compile(
        r"^(?:\[\d{2}:\d{2}:\d{2}\]\s+)?(?:\S*/tests/)?(?P<rel>[a-zA-Z0-9_]+/[0-9a-zA-Z_.-]+\.t)\s+\.{2,}\s*$"
    )
    plan_re = re.compile(r"^1\.\.(?P<plan>\d+)")
    ok_line_re = re.compile(r"^ok\s+(?P<num>\d+)\b(?P<rest>.*)$")
    not_ok_line_re = re.compile(r"^not ok\s+(?P<num>\d+)\b(?P<rest>.*)$")
    done_timer_re = re.compile(r"^ok\s+(?P<ms>\d+)\s+ms\b")
    dubious_re = re.compile(r"^Dubious,\s+test returned\s+(?P<rc>\d+)")
    failed_subtests_re = re.compile(r"^Failed\s+(?P<failed>\d+)/(?P<total>\d+)\s+subtests")

    cases: dict[str, dict[str, Any]] = {}
    current_rel: str | None = None

    for raw_line in log_text.splitlines():
        line = raw_line.strip()
        m_hdr = header_re.match(line)
        if m_hdr:
            current_rel = m_hdr.group("rel")
            dir_name = current_rel.split("/", 1)[0]
            cases[current_rel] = {
                "case": current_rel,
                "directory": dir_name,
                "planned": 0,
                "ok_count": 0,
                "not_ok_count": 0,
                "todo_count": 0,
                "todo_expected_failures": 0,
                "duration_ms": 0,
                "failed_nums": [],
                "dubious_rc": 0,
            }
            continue

        if current_rel is None:
            continue

        entry = cases[current_rel]

        m_plan = plan_re.match(line)
        if m_plan:
            entry["planned"] = int(m_plan.group("plan"))
            continue

        m_done = done_timer_re.match(line)
        if m_done:
            entry["duration_ms"] = int(m_done.group("ms"))
            current_rel = None
            continue

        if line == "ok":
            current_rel = None
            continue

        m_ok = ok_line_re.match(line)
        if m_ok:
            entry["ok_count"] += 1
            if "# TODO" in m_ok.group("rest").upper():
                entry["todo_count"] += 1
            continue

        m_not_ok = not_ok_line_re.match(line)
        if m_not_ok:
            rest = m_not_ok.group("rest")
            if "# TODO" in rest.upper():
                entry["ok_count"] += 1
                entry["todo_count"] += 1
                entry["todo_expected_failures"] += 1
            else:
                entry["not_ok_count"] += 1
                entry["failed_nums"].append(int(m_not_ok.group("num")))
            continue

        m_dub = dubious_re.match(line)
        if m_dub:
            entry["dubious_rc"] = int(m_dub.group("rc"))
            continue

        m_fsub = failed_subtests_re.match(line)
        if m_fsub:
            failed_n = int(m_fsub.group("failed"))
            total_n = int(m_fsub.group("total"))
            if entry["planned"] == 0:
                entry["planned"] = total_n
            if entry["not_ok_count"] < failed_n:
                entry["not_ok_count"] = failed_n
            current_rel = None
            continue

    dir_summary: dict[str, dict[str, Any]] = {}
    failed_cases: list[dict[str, Any]] = []
    total_tests = 0
    passed_tests = 0
    unexpected_failures = 0
    todo_tests = 0
    todo_expected_failures = 0

    for rel in sorted(cases.keys()):
        c = cases[rel]
        total = c["ok_count"] + c["not_ok_count"]
        if total == 0 and c["planned"] > 0:
            total = c["planned"]
        failed = c["not_ok_count"]
        if c["dubious_rc"] != 0 and failed == 0:
            failed = 1
            if total == 0:
                total = 1
        passed = max(total - failed, 0)

        total_tests += total
        passed_tests += passed
        unexpected_failures += failed
        todo_tests += c["todo_count"]
        todo_expected_failures += c["todo_expected_failures"]

        d = c["directory"]
        d_entry = dir_summary.setdefault(
            d,
            {"directory": d, "files": 0, "tests": 0, "passed": 0, "failed": 0, "todo": 0, "duration_ms": 0},
        )
        d_entry["files"] += 1
        d_entry["tests"] += total
        d_entry["passed"] += passed
        d_entry["failed"] += failed
        d_entry["todo"] += c["todo_count"]
        d_entry["duration_ms"] += c["duration_ms"]

        if failed > 0:
            failed_cases.append(
                {
                    "case": rel,
                    "failed": failed,
                    "total": total,
                    "failed_nums": c["failed_nums"][:20],
                    "dubious_rc": c["dubious_rc"],
                }
            )

    return {
        "total_files": len(cases),
        "total_tests": total_tests,
        "passed_tests": passed_tests,
        "unexpected_failures": unexpected_failures,
        "todo_tests": todo_tests,
        "todo_expected_failures": todo_expected_failures,
        "directories": [dir_summary[k] for k in sorted(dir_summary.keys())],
        "failed_cases": failed_cases,
    }


def _deterministic_payload(seed: str, size: int) -> bytes:
    chunk = bytearray()
    counter = 0
    unit_cap = min(size, 65536)
    while len(chunk) < unit_cap:
        chunk.extend(hashlib.blake2b(f"{seed}:{counter}".encode("utf-8"), digest_size=64).digest())
        counter += 1
    unit = bytes(chunk[:unit_cap])
    if size <= len(unit):
        return unit[:size]
    reps, rem = divmod(size, len(unit))
    buf = bytearray(unit * reps + unit[:rem])
    head = hashlib.sha256(f"h:{seed}".encode("utf-8")).digest()
    tail = hashlib.sha256(f"t:{seed}".encode("utf-8")).digest()
    buf[: len(head)] = head
    buf[-len(tail) :] = tail
    return bytes(buf)


def _sha256_file(path: Path) -> tuple[int, str]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while True:
            buf = f.read(1024 * 1024)
            if not buf:
                break
            size += len(buf)
            h.update(buf)
    return size, h.hexdigest()


def _linkat_proc_fd(fd: int, dest: Path) -> None:
    proc_fd = f"/proc/self/fd/{fd}"
    try:
        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        libc.linkat.argtypes = [
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
        ]
        libc.linkat.restype = ctypes.c_int
        rc = libc.linkat(
            _AT_FDCWD,
            os.fsencode(proc_fd),
            _AT_FDCWD,
            os.fsencode(str(dest)),
            _AT_SYMLINK_FOLLOW,
        )
        if rc != 0:
            err = ctypes.get_errno()
            raise OSError(err, os.strerror(err), proc_fd, None, str(dest))
    except OSError:
        raise
    except Exception:
        os.link(proc_fd, str(dest), follow_symlinks=True)


def _get_disk_used_bytes(ctx: BenchContext, disk_id: str) -> int | None:
    try:
        res = ctx.bowl_rpc_cli(["--json", "disk", "get", disk_id], check=True)
        if isinstance(res, dict):
            return int(res.get("used_bytes", 0))
    except Exception:
        return None
    return None


def _run_pjdfstest_full(ctx: BenchContext, target: str) -> dict[str, Any]:
    tests_dir = _find_pjdfstest_tests_dir(ctx)
    if tests_dir is None or shutil.which("prove") is None:
        return {
            "case_id": "pjdfstest-full",
            "target": target,
            "durability_tier": "Tier 0" if target == "bowl_staged" else "Tier 1",
            "cache_state": "C0",
            "runs_n": 0,
            "metrics": {},
            "validity_status": "SKIPPED",
            "integrity_verified": False,
            "counter_deltas": {},
            "notes": "pjdfstest tests directory or prove binary not found",
        }

    smoke = ctx.profile == "smoke-local"
    test_files = _select_test_files(tests_dir, smoke=smoke)
    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(target, ttl_override=0.0, allow_other=True)
        work_dir = handle.mount_dir / "pjdfstest_work"
        work_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(work_dir, 0o777)

        before = ctx.snapshot_counters(target, handle=handle)
        t0 = time.perf_counter()
        proc = ctx.run_cmd(
            ["prove", "--timer", "-rv", *[str(p) for p in test_files]],
            cwd=work_dir,
            timeout=3600,
            check=False,
        )
        duration_s = round(time.perf_counter() - t0, 3)
        after = ctx.snapshot_counters(target, handle=handle)

        combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
        log_path = ctx.suite_dir / f"pjdfstest_{target}.log"
        log_path.write_text(combined, encoding="utf-8")

        parsed = parse_tap_output(combined)
        audit = ctx.audit_case(before, after, target=target, cache_state="C0")
        drain_s = round(handle.unmount(), 3)
        handle = None

        integrity_ok = (
            proc.returncode == 0
            and parsed["unexpected_failures"] == 0
            and parsed["total_tests"] > 0
        )
        validity = "PASS" if (integrity_ok and audit.get("validity_status") == "PASS") else "INVALID"

        reasons = list(audit.get("reasons", []))
        if not integrity_ok:
            reasons.append(
                f"prove rc={proc.returncode}, unexpected_failures={parsed['unexpected_failures']}, total={parsed['total_tests']}"
            )
        note_parts = [
            f"subset={len(test_files)} files (smoke)" if smoke else f"full={len(test_files)} files",
            "ttl_override=0.0, allow_other=True",
            "STORAGE_ATIME_MODE is inactive on HEAD 68d787b",
            f"upstream_tap_todo={parsed['todo_tests']} (expected_failures={parsed['todo_expected_failures']})",
        ]
        if reasons:
            note_parts.append("issues=" + "; ".join(reasons))

        return {
            "case_id": "pjdfstest-full",
            "target": target,
            "durability_tier": "Tier 0" if target == "bowl_staged" else "Tier 1",
            "cache_state": "C0",
            "runs_n": 1,
            "metrics": {
                "total_files": parsed["total_files"],
                "total_tests": parsed["total_tests"],
                "passed_tests": parsed["passed_tests"],
                "unexpected_failures": parsed["unexpected_failures"],
                "todo_tests": parsed["todo_tests"],
                "todo_expected_failures": parsed["todo_expected_failures"],
                "prove_returncode": proc.returncode,
                "duration_s": duration_s,
                "drain_time_s": drain_s,
                "directories": parsed["directories"],
                "failed_cases": parsed["failed_cases"][:25],
            },
            "validity_status": validity,
            "integrity_verified": integrity_ok,
            "counter_deltas": audit.get("deltas", {}),
            "notes": "; ".join(note_parts),
        }
    finally:
        if handle is not None:
            handle.unmount()


def _run_posix_tmpfile_orphan_rename(ctx: BenchContext, target: str) -> dict[str, Any]:
    handle: MountHandle | None = None
    try:
        handle = ctx.prepare_target(target, ttl_override=0.0)
        disk_id = handle.disk_id or ""
        branch = handle.branch_name or "main"
        base = handle.mount_dir / "posix_special"
        base.mkdir(parents=True, exist_ok=True)

        before = ctx.snapshot_counters(target, handle=handle)
        t0 = time.perf_counter()

        subchecks: dict[str, Any] = {}
        expected_files: dict[str, tuple[int, str]] = {}

        # 1. O_TMPFILE + linkat promotion and discard
        promo_payload = _deterministic_payload(f"tmpfile-promo-{target}", 256 * 1024 + 137)
        promo_sha = hashlib.sha256(promo_payload).hexdigest()
        promo_path = base / "promoted_tmpfile.bin"
        tmp_fd = os.open(str(base), os.O_RDWR | _O_TMPFILE, 0o644)
        try:
            view = memoryview(promo_payload)
            written = 0
            while written < len(promo_payload):
                written += os.write(tmp_fd, view[written:])
            os.fsync(tmp_fd)
            _linkat_proc_fd(tmp_fd, promo_path)
        finally:
            os.close(tmp_fd)

        p_size, p_sha = _sha256_file(promo_path)
        discard_payload = _deterministic_payload(f"tmpfile-discard-{target}", 128 * 1024)
        entries_before_discard = set(os.listdir(base))
        disc_fd = os.open(str(base), os.O_RDWR | _O_TMPFILE, 0o644)
        try:
            os.write(disc_fd, discard_payload)
        finally:
            os.close(disc_fd)
        entries_after_discard = set(os.listdir(base))
        subchecks["tmpfile_promote_and_discard"] = (
            p_size == len(promo_payload)
            and p_sha == promo_sha
            and entries_before_discard == entries_after_discard
        )
        expected_files["posix_special/promoted_tmpfile.bin"] = (len(promo_payload), promo_sha)

        # 2. Unlinked open orphan FD read/write + space reclaim on close
        orphan_path = base / "orphan_file.bin"
        part1 = _deterministic_payload(f"orphan-p1-{target}", 1024 * 1024)
        part2 = _deterministic_payload(f"orphan-p2-{target}", 512 * 1024)
        orphan_fd = os.open(str(orphan_path), os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
        orphan_rw_ok = False
        used_before_close: int | None = None
        used_after_close: int | None = None
        try:
            os.write(orphan_fd, part1)
            os.fsync(orphan_fd)
            os.unlink(str(orphan_path))
            path_gone = not orphan_path.exists()

            os.lseek(orphan_fd, 0, os.SEEK_SET)
            read1 = b""
            while len(read1) < len(part1):
                chunk = os.read(orphan_fd, 262144)
                if not chunk:
                    break
                read1 += chunk

            os.lseek(orphan_fd, 0, os.SEEK_END)
            os.write(orphan_fd, part2)
            os.fsync(orphan_fd)

            os.lseek(orphan_fd, 0, os.SEEK_SET)
            read_all = b""
            while len(read_all) < len(part1) + len(part2):
                chunk = os.read(orphan_fd, 262144)
                if not chunk:
                    break
                read_all += chunk

            orphan_rw_ok = (
                path_gone
                and read1 == part1
                and read_all == (part1 + part2)
            )
            if disk_id:
                used_before_close = _get_disk_used_bytes(ctx, disk_id)
        finally:
            os.close(orphan_fd)

        orphan_reclaim_ok = True
        if disk_id and used_before_close is not None:
            for _ in range(15):
                used_after_close = _get_disk_used_bytes(ctx, disk_id)
                if used_after_close is not None and used_after_close < used_before_close:
                    break
                time.sleep(0.2)
            if used_after_close is not None:
                orphan_reclaim_ok = used_after_close <= used_before_close

        subchecks["orphan_fd_rw"] = orphan_rw_ok
        subchecks["orphan_space_reclaim"] = orphan_reclaim_ok

        # 3. Atomic cross-directory rename overwrite
        dir_a = base / "dir_a"
        dir_b = base / "dir_b"
        dir_a.mkdir(exist_ok=True)
        dir_b.mkdir(exist_ok=True)
        src_payload = _deterministic_payload(f"rename-src-{target}", 512 * 1024 + 99)
        dst_old_payload = _deterministic_payload(f"rename-dst-old-{target}", 384 * 1024)
        src_path = dir_a / "src.bin"
        dst_path = dir_b / "dst.bin"
        with src_path.open("wb") as f:
            f.write(src_payload)
            f.flush()
            os.fsync(f.fileno())
        with dst_path.open("wb") as f:
            f.write(dst_old_payload)
            f.flush()
            os.fsync(f.fileno())

        os.replace(str(src_path), str(dst_path))
        dst_size, dst_sha = _sha256_file(dst_path)
        src_sha = hashlib.sha256(src_payload).hexdigest()
        subchecks["atomic_cross_dir_rename"] = (
            not src_path.exists()
            and dst_size == len(src_payload)
            and dst_sha == src_sha
        )
        expected_files["posix_special/dir_b/dst.bin"] = (len(src_payload), src_sha)

        after = ctx.snapshot_counters(target, handle=handle)
        audit = ctx.audit_case(before, after, target=target, cache_state="C0")

        # 4. Tier 2 unmount + fresh remount SHA-256 verification
        drain_s = round(handle.unmount(), 3)
        handle = None

        remount_ok = False
        if disk_id:
            ro_handle = ctx.prepare_target(
                target, disk_id=disk_id, branch_name=branch, read_only=True, ttl_override=0.0
            )
            try:
                ro_mount_dir = ro_handle.mount_dir
                remount_ok = True
                for rel, (exp_size, exp_sha) in expected_files.items():
                    fp = ro_mount_dir / rel
                    if not fp.is_file():
                        remount_ok = False
                        break
                    got_size, got_sha = _sha256_file(fp)
                    if got_size != exp_size or got_sha != exp_sha:
                        remount_ok = False
                        break
                if (ro_mount_dir / "posix_special/orphan_file.bin").exists():
                    remount_ok = False
                if (ro_mount_dir / "posix_special/dir_a/src.bin").exists():
                    remount_ok = False
            finally:
                ro_handle.unmount()
                ro_mount_dir = None

        subchecks["tier2_remount_sha256"] = remount_ok
        duration_s = round(time.perf_counter() - t0, 3)

        all_ok = all(bool(v) for v in subchecks.values())
        validity = "PASS" if (all_ok and audit.get("validity_status") == "PASS") else "INVALID"

        return {
            "case_id": "posix-tmpfile-orphan-rename",
            "target": target,
            "durability_tier": "Tier 2",
            "cache_state": "C2",
            "runs_n": 1,
            "metrics": {
                "duration_s": duration_s,
                "drain_time_s": drain_s,
                "used_bytes_before_orphan_close": used_before_close,
                "used_bytes_after_orphan_close": used_after_close,
                "subchecks": subchecks,
            },
            "validity_status": validity,
            "integrity_verified": all_ok,
            "counter_deltas": audit.get("deltas", {}),
            "notes": "O_TMPFILE+linkat, unlinked open orphan FD R/W + reclaim, cross-dir atomic rename, Tier 2 remount SHA-256",
        }
    finally:
        if handle is not None:
            handle.unmount()


def _render_summary_markdown(cases: list[dict[str, Any]]) -> str:
    by_target: dict[str, dict[str, dict[str, Any]]] = {}
    for c in cases:
        by_target.setdefault(c["target"], {})[c["case_id"]] = c

    lines = [
        "| 目标模式 | `pjdfstest-full` 通过/总数 | 上游 TAP `TODO` (预期失败) | 耗时 (s) | `posix-tmpfile-orphan-rename` | 重挂 SHA-256 | Gate |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for target in SUPPORTED_TARGETS:
        t_cases = by_target.get(target)
        if not t_cases:
            continue
        full_c = t_cases.get("pjdfstest-full")
        spec_c = t_cases.get("posix-tmpfile-orphan-rename")
        if full_c and full_c["validity_status"] != "SKIPPED":
            fm = full_c["metrics"]
            pass_cell = f"{fm.get('passed_tests', 0)}/{fm.get('total_tests', 0)}"
            todo_cell = f"{fm.get('todo_tests', 0)} ({fm.get('todo_expected_failures', 0)})"
            dur_cell = f"{fm.get('duration_s', 0.0):.2f}"
        else:
            pass_cell = "SKIPPED"
            todo_cell = "N/A"
            dur_cell = "N/A"

        if spec_c:
            sm = spec_c["metrics"].get("subchecks", {})
            spec_cell = spec_c["validity_status"]
            remount_cell = "PASS" if sm.get("tier2_remount_sha256") else "FAIL"
        else:
            spec_cell = "N/A"
            remount_cell = "N/A"

        statuses = [c["validity_status"] for c in t_cases.values()]
        gate = "PASS" if all(s == "PASS" for s in statuses) else ("SKIPPED" if all(s == "SKIPPED" for s in statuses) else "INVALID")
        lines.append(
            f"| `{target}` | {pass_cell} | {todo_cell} | {dur_cell} | {spec_cell} | {remount_cell} | {gate} |"
        )
    return "\n".join(lines) + "\n"


def run_suite(ctx: BenchContext) -> dict[str, Any]:
    active = [t for t in ctx.targets if t in SUPPORTED_TARGETS]
    cases: list[dict[str, Any]] = []
    for target in active:
        cases.append(_run_pjdfstest_full(ctx, target))
        cases.append(_run_posix_tmpfile_orphan_rename(ctx, target))
    return {
        "suite": SUITE_ID,
        "cases": cases,
        "summary_markdown": _render_summary_markdown(cases),
    }
