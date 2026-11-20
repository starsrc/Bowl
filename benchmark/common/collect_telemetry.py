#!/usr/bin/env python3
"""Aggregate and distill Bowl prod observability artifacts into results/telemetry/."""

from __future__ import annotations

import argparse
import json
import math
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

# Client OpSnapshot.Buckets upper bounds in microseconds: 2^0 .. 2^25 us (~33.55s, non-cumulative)
CLIENT_HIST_UPPER_US: list[float] = [float(1 << i) for i in range(26)]

PROM_LINE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<val>[-+0-9.eE]+|NaN|\+Inf|-Inf)(?:\s+\d+)?$"
)
PROM_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"')

IGNORED_STAGE_NAMES = {
    "cross_compile_binaries",
    "provision_ecs_instances",
    "deploy_server_node",
    "setup_client_and_mounts",
}
OBSERVABILITY_NOISE_SERVICES = {"loki", "prometheus", "jaeger", "grafana", "alloy"}
CLIENT_KV_LEVEL_RE = re.compile(r"\blevel=(?P<lvl>warn|warning|error|fatal|panic)\b", re.IGNORECASE)
CLIENT_RAW_ERR_RE = re.compile(
    r"(\berror\b|\berr\b|\bfailed\b|\bfatal\b|\bpanic:|\bfirst path segment in url\b|\bcannot contain colon\b|错误)",
    re.IGNORECASE,
)
OK_STATUS_CODES = {"", "ok", "0", "2xx", "200", "status_ok", "unset"}


def load_json_safe(path: Path) -> Any:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None


def load_epoch_safe(path: Path) -> float | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        val = float(path.read_text(encoding="utf-8", errors="replace").strip())
        return val if val > 0 and math.isfinite(val) else None
    except Exception:
        return None


def short_rpc_method(raw: str) -> str:
    """Normalize ConnectRPC / gRPC procedure labels to short method names (e.g. 'Write')."""
    s = (raw or "").strip().strip("/")
    if not s:
        return "unknown"
    if "/" in s:
        s = s.rsplit("/", 1)[-1]
    return s or "unknown"


def fmt_pct(val: float | None, zero_label: str = "N/A") -> str:
    if val is None or not math.isfinite(val):
        return zero_label
    return f"{val:.2f}%"


def fmt_block_cache_hit_rate(cg: dict[str, Any]) -> str:
    pct = cg.get("pebble_block_cache_hit_rate_pct")
    if pct is None or not math.isfinite(float(pct)):
        return "N/A (0 reads)"
    return f"{float(pct):.2f}%"


def fmt_opt_ms(val: float | None, digits: int = 3) -> str:
    if val is None or not math.isfinite(val):
        return "—"
    return f"{val:.{digits}f}"


# ==============================================================================
# 1. Client `bowl stats` Snapshot & Phase Delta Analysis (26 Buckets + 9 Subsystems)
# ==============================================================================


def noncum_quantile_us(buckets: list[int], total_calls: int, q: float) -> float:
    """Compute approximate quantile (in us) from non-cumulative log2 microsecond buckets."""
    if total_calls <= 0 or not buckets:
        return 0.0
    target = math.ceil(total_calls * q)
    cum = 0
    prev_upper = 0.0
    for idx, count in enumerate(buckets):
        upper = CLIENT_HIST_UPPER_US[idx] if idx < len(CLIENT_HIST_UPPER_US) else float(1 << idx)
        if count > 0 and cum + count >= target:
            frac = (target - cum) / count
            return round(prev_upper + frac * (upper - prev_upper), 2)
        cum += count
        prev_upper = upper
    return round(prev_upper if prev_upper > 0 else CLIENT_HIST_UPPER_US[min(len(buckets), len(CLIENT_HIST_UPPER_US)) - 1], 2)


def summarize_op_entry(op_obj: dict[str, Any], total_fuse_ns: int) -> dict[str, Any]:
    calls = int(op_obj.get("calls", 0))
    errors = int(op_obj.get("errors", 0))
    dur_ns = int(op_obj.get("duration_nanos", 0))
    buckets = [int(x) for x in op_obj.get("buckets", [])]
    mean_us = round((dur_ns / 1000.0) / calls, 2) if calls > 0 else 0.0
    share_pct = round((dur_ns / total_fuse_ns) * 100.0, 2) if total_fuse_ns > 0 else 0.0
    return {
        "op": op_obj.get("op", "Unknown"),
        "calls": calls,
        "errors": errors,
        "bytes": int(op_obj.get("bytes", 0)),
        "total_duration_ms": round(dur_ns / 1e6, 3),
        "share_of_fuse_time_pct": share_pct,
        "mean_us": mean_us,
        "p50_us": noncum_quantile_us(buckets, calls, 0.50),
        "p95_us": noncum_quantile_us(buckets, calls, 0.95),
        "p99_us": noncum_quantile_us(buckets, calls, 0.99),
        "buckets": buckets,
        "errnos": op_obj.get("errnos") or {},
    }


def summarize_aggregate_fuse_ops(ops_raw: list[dict[str, Any]]) -> dict[str, Any]:
    total_calls = sum(int(o.get("calls", 0)) for o in ops_raw)
    total_errors = sum(int(o.get("errors", 0)) for o in ops_raw)
    total_ns = sum(int(o.get("duration_nanos", 0)) for o in ops_raw)
    max_len = max((len(o.get("buckets") or []) for o in ops_raw), default=0)
    merged_buckets = [0] * max_len
    for o in ops_raw:
        for i, v in enumerate(o.get("buckets") or []):
            merged_buckets[i] += int(v)
    mean_ms = round((total_ns / 1e6) / total_calls, 3) if total_calls > 0 else 0.0
    return {
        "calls": total_calls,
        "errors": total_errors,
        "total_ms": round(total_ns / 1e6, 3),
        "mean_ms": mean_ms,
        "p50_ms": round(noncum_quantile_us(merged_buckets, total_calls, 0.50) / 1000.0, 3) if total_calls > 0 else 0.0,
        "p95_ms": round(noncum_quantile_us(merged_buckets, total_calls, 0.95) / 1000.0, 3) if total_calls > 0 else 0.0,
        "p99_ms": round(noncum_quantile_us(merged_buckets, total_calls, 0.99) / 1000.0, 3) if total_calls > 0 else 0.0,
    }


def diff_client_snapshots(
    pre: dict[str, Any],
    post: dict[str, Any],
    pre_epoch: float | None = None,
    post_epoch: float | None = None,
) -> dict[str, Any]:
    pre_ops = {
        o["op"]: o
        for o in ((pre.get("fuse") or {}).get("ops") or [])
        if isinstance(o, dict) and "op" in o
    }
    post_ops = {
        o["op"]: o
        for o in ((post.get("fuse") or {}).get("ops") or [])
        if isinstance(o, dict) and "op" in o
    }

    delta_ops_raw: list[dict[str, Any]] = []
    total_delta_ns = 0
    for op_name, post_o in post_ops.items():
        pre_o = pre_ops.get(op_name, {})
        d_calls = max(0, int(post_o.get("calls") or 0) - int(pre_o.get("calls") or 0))
        if d_calls == 0:
            continue
        d_errs = max(0, int(post_o.get("errors") or 0) - int(pre_o.get("errors") or 0))
        d_bytes = max(0, int(post_o.get("bytes") or 0) - int(pre_o.get("bytes") or 0))
        d_ns = max(0, int(post_o.get("duration_nanos") or 0) - int(pre_o.get("duration_nanos") or 0))
        post_b = [int(x) for x in (post_o.get("buckets") or [])]
        pre_b = [int(x) for x in (pre_o.get("buckets") or [])]
        d_buckets = [max(0, post_b[i] - (pre_b[i] if i < len(pre_b) else 0)) for i in range(len(post_b))]

        post_errnos = post_o.get("errnos") or {}
        pre_errnos = pre_o.get("errnos") or {}
        d_errnos = {
            k: int(v) - int(pre_errnos.get(k, 0))
            for k, v in post_errnos.items()
            if int(v) - int(pre_errnos.get(k, 0)) > 0
        }
        total_delta_ns += d_ns
        delta_ops_raw.append({
            "op": op_name,
            "calls": d_calls,
            "errors": d_errs,
            "bytes": d_bytes,
            "duration_nanos": d_ns,
            "buckets": d_buckets,
            "errnos": d_errnos,
        })

    agg_fuse = summarize_aggregate_fuse_ops(delta_ops_raw)
    ops_summary = []
    for o in delta_ops_raw:
        s = summarize_op_entry(o, total_delta_ns)
        s.pop("buckets", None)
        ops_summary.append(s)
    ops_summary.sort(key=lambda x: x["total_duration_ms"], reverse=True)

    def delta_field(section: str, key: str) -> float:
        a = float((pre.get(section) or {}).get(key, 0) or 0)
        b = float((post.get(section) or {}).get(key, 0) or 0)
        return max(0.0, b - a)

    eff_pre_epoch = pre_epoch
    pre_ts_ns = int(pre.get("timestamp_unix_nano") or 0)
    post_ts_ns = int(post.get("timestamp_unix_nano") or 0)
    if eff_pre_epoch is None and pre_ts_ns > 0:
        eff_pre_epoch = pre_ts_ns / 1e9
    eff_post_epoch = post_epoch
    if eff_post_epoch is None and post_ts_ns > 0:
        eff_post_epoch = post_ts_ns / 1e9

    if pre_epoch is not None and post_epoch is not None and post_epoch >= pre_epoch:
        window_sec = round(post_epoch - pre_epoch, 3)
    else:
        ts_delta_ns = post_ts_ns - pre_ts_ns
        if ts_delta_ns > 0:
            window_sec = round(ts_delta_ns / 1e9, 3)
        else:
            window_sec = round(max(0.0, float(post.get("uptime_seconds") or 0) - float(pre.get("uptime_seconds") or 0)), 3)

    attr_h, attr_m = delta_field("namespace_cache", "attr_hits"), delta_field("namespace_cache", "attr_misses")
    ent_h, ent_m = delta_field("namespace_cache", "entry_hits"), delta_field("namespace_cache", "entry_misses")
    rl_h, rl_m = delta_field("namespace_cache", "readlink_hits"), delta_field("namespace_cache", "readlink_misses")

    ra_hit_b = delta_field("read_ahead", "hit_bytes")
    ra_pref_b = delta_field("read_ahead", "prefetched_bytes")
    ra_disc_b = delta_field("read_ahead", "discarded_bytes")
    ra_waits = int(delta_field("read_ahead", "waits"))
    ra_wait_ms = round(delta_field("read_ahead", "wait_seconds") * 1000.0, 3)

    wb_post = post.get("write_behind") or {}
    wb_staged_b = delta_field("write_behind", "staged_bytes")
    wb_pass_b = delta_field("write_behind", "passthrough_bytes")
    wb_delta = {
        "staged_calls": int(delta_field("write_behind", "staged_calls")),
        "staged_bytes": int(wb_staged_b),
        "staged_mib": round(wb_staged_b / (1024.0 * 1024.0), 2),
        "passthrough_calls": int(delta_field("write_behind", "passthrough_calls")),
        "passthrough_bytes": int(wb_pass_b),
        "passthrough_mib": round(wb_pass_b / (1024.0 * 1024.0), 2),
        "flush_rpcs": int(delta_field("write_behind", "flush_rpcs")),
        "flush_errors": int(delta_field("write_behind", "flush_errors")),
        "backpressure_waits": int(delta_field("write_behind", "backpressure_waits")),
        "backpressure_wait_ms": round(delta_field("write_behind", "backpressure_wait_seconds") * 1000.0, 3),
        "barrier_waits": int(delta_field("write_behind", "barrier_waits")),
        "barrier_wait_ms": round(delta_field("write_behind", "barrier_wait_seconds") * 1000.0, 3),
        "inflight": int(wb_post.get("inflight", 0) or 0),
        "pending_slots": int(wb_post.get("pending_slots", 0) or 0),
    }

    q_post = post.get("queues") or {}
    fuse_active_ms = round(total_delta_ns / 1e6, 3)

    return {
        "pre_epoch": eff_pre_epoch,
        "post_epoch": eff_post_epoch,
        "window_sec": window_sec,
        "fuse_active_ms": fuse_active_ms,
        "fuse_active_concurrency": round((fuse_active_ms / 1000.0) / window_sec, 3) if window_sec > 0 else None,
        "fuse_aggregate": agg_fuse,
        "cpu_delta_sec": round(max(0.0, float(post.get("cpu_usage_seconds") or 0) - float(pre.get("cpu_usage_seconds") or 0)), 3),
        "read_bytes": int(max(0, int((post.get("fuse") or {}).get("read_bytes") or 0) - int((pre.get("fuse") or {}).get("read_bytes") or 0))),
        "write_bytes": int(max(0, int((post.get("fuse") or {}).get("write_bytes") or 0) - int((pre.get("fuse") or {}).get("write_bytes") or 0))),
        "nscache_attr_hit_rate_pct": round(attr_h / (attr_h + attr_m) * 100.0, 2) if (attr_h + attr_m) > 0 else None,
        "nscache_entry_hit_rate_pct": round(ent_h / (ent_h + ent_m) * 100.0, 2) if (ent_h + ent_m) > 0 else None,
        "nscache_readlink_hit_rate_pct": round(rl_h / (rl_h + rl_m) * 100.0, 2) if (rl_h + rl_m) > 0 else None,
        "nscache_negative_entry_hits": int(delta_field("namespace_cache", "negative_entry_hits")),
        "nscache_readdir_hits": int(delta_field("namespace_cache", "readdir_hits")),
        "readahead_triggers": int(delta_field("read_ahead", "triggers")),
        "readahead_hit_bytes": int(ra_hit_b),
        "readahead_prefetched_bytes": int(ra_pref_b),
        "readahead_discarded_bytes": int(ra_disc_b),
        "readahead_waits": ra_waits,
        "readahead_wait_ms": ra_wait_ms,
        "write_behind": wb_delta,
        "inode_lock_fast": int(delta_field("inode_locks", "fast_acquisitions")),
        "inode_lock_contended": int(delta_field("inode_locks", "contended_acquisitions")),
        "inode_lock_wait_ms": round(delta_field("inode_locks", "wait_seconds") * 1000.0, 3),
        "queues": {
            "fuse_waiting": int(q_post.get("fuse_waiting", 0) or 0),
            "fuse_congestion_threshold": int(q_post.get("fuse_congestion_threshold", 0) or 0),
        },
        "top_ops_by_total_time": ops_summary[:12],
    }


def process_client_stats(stage_dir: Path, out_dir: Path) -> dict[str, Any]:
    stats_src = stage_dir / "client" / "stats"
    stats_dst = out_dir / "client_stats"
    stats_dst.mkdir(parents=True, exist_ok=True)
    if not stats_src.exists():
        return {}

    snapshots: dict[str, dict[str, Any]] = {}
    epochs: dict[str, float] = {}
    for p in sorted(stats_src.glob("*.json")):
        data = load_json_safe(p)
        if isinstance(data, dict) and "fuse" in data:
            snapshots[p.stem] = data
            shutil.copy2(p, stats_dst / p.name)
    for p in sorted(stats_src.glob("*.epoch")):
        ep = load_epoch_safe(p)
        if ep is not None:
            epochs[p.stem] = ep
    for p in sorted(stats_src.glob("*.txt")):
        if p.stat().st_size > 0:
            shutil.copy2(p, stats_dst / p.name)

    if not snapshots:
        return {}

    terminal_name = next(
        (cand for cand in ("before_umount", "suite_post", "final") if cand in snapshots),
        max(snapshots.keys(), key=lambda k: int(snapshots[k].get("timestamp_unix_nano") or 0)),
    )
    term = snapshots[terminal_name]
    raw_ops = [o for o in ((term.get("fuse") or {}).get("ops") or []) if int(o.get("calls") or 0) > 0]
    total_ns = sum(int(o.get("duration_nanos", 0)) for o in raw_ops)
    summarized_ops = []
    for o in raw_ops:
        entry = summarize_op_entry(o, total_ns)
        entry.pop("buckets", None)
        summarized_ops.append(entry)

    by_total = sorted(summarized_ops, key=lambda x: x["total_duration_ms"], reverse=True)
    by_p99 = sorted(summarized_ops, key=lambda x: (x["p99_us"], x["mean_us"]), reverse=True)

    phase_deltas: dict[str, Any] = {}
    for name in sorted(snapshots.keys()):
        if name.endswith("_pre"):
            base = name[: -len("_pre")]
            post_name = f"{base}_post"
            if post_name in snapshots:
                phase_deltas[base] = diff_client_snapshots(
                    snapshots[name],
                    snapshots[post_name],
                    pre_epoch=epochs.get(name),
                    post_epoch=epochs.get(post_name),
                )

    if phase_deltas:
        (stats_dst / "phase_deltas.json").write_text(
            json.dumps(phase_deltas, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    ns = term.get("namespace_cache") or {}
    ra = term.get("read_ahead") or {}
    wb = term.get("write_behind") or {}
    il = term.get("inode_locks") or {}
    qs = term.get("queues") or {}
    attr_tot = int(ns.get("attr_hits", 0)) + int(ns.get("attr_misses", 0))
    entry_tot = int(ns.get("entry_hits", 0)) + int(ns.get("entry_misses", 0))
    rl_tot = int(ns.get("readlink_hits", 0)) + int(ns.get("readlink_misses", 0))

    return {
        "terminal_snapshot": terminal_name,
        "uptime_seconds": round(float(term.get("uptime_seconds", 0.0)), 3),
        "cpu_usage_seconds": round(float(term.get("cpu_usage_seconds", 0.0)), 3),
        "memory_mib": round(int(term.get("memory_bytes", 0)) / (1024 * 1024), 2),
        "total_calls": int(term.get("fuse", {}).get("total_calls", 0)),
        "total_errors": int(term.get("fuse", {}).get("total_errors", 0)),
        "fuse_active_ms": round(total_ns / 1e6, 3),
        "read_mib": round(int(term.get("fuse", {}).get("read_bytes", 0)) / (1024 * 1024), 2),
        "write_mib": round(int(term.get("fuse", {}).get("write_bytes", 0)) / (1024 * 1024), 2),
        "nscache": {
            "attr_hit_rate_pct": round(int(ns.get("attr_hits", 0)) / attr_tot * 100.0, 2) if attr_tot > 0 else None,
            "entry_hit_rate_pct": round(int(ns.get("entry_hits", 0)) / entry_tot * 100.0, 2) if entry_tot > 0 else None,
            "readlink_hit_rate_pct": round(int(ns.get("readlink_hits", 0)) / rl_tot * 100.0, 2) if rl_tot > 0 else None,
            "readlink_hits": int(ns.get("readlink_hits", 0)),
            "readlink_misses": int(ns.get("readlink_misses", 0)),
            "negative_entry_hits": int(ns.get("negative_entry_hits", 0)),
            "readdir_hits": int(ns.get("readdir_hits", 0)),
            "invalidations": int(ns.get("invalidations", 0)),
            "evictions": int(ns.get("evictions", 0)),
        },
        "read_ahead": {
            "triggers": int(ra.get("triggers", 0)),
            "prefetched_mib": round(int(ra.get("prefetched_bytes", 0)) / (1024 * 1024), 2),
            "hit_mib": round(int(ra.get("hit_bytes", 0)) / (1024 * 1024), 2),
            "discarded_mib": round(int(ra.get("discarded_bytes", 0)) / (1024 * 1024), 2),
            "waits": int(ra.get("waits", 0)),
            "wait_ms": round(float(ra.get("wait_seconds", 0.0)) * 1000.0, 3),
        },
        "write_behind": {
            "staged_calls": int(wb.get("staged_calls", 0)),
            "staged_mib": round(int(wb.get("staged_bytes", 0)) / (1024 * 1024), 2),
            "passthrough_calls": int(wb.get("passthrough_calls", 0)),
            "passthrough_mib": round(int(wb.get("passthrough_bytes", 0)) / (1024 * 1024), 2),
            "flush_rpcs": int(wb.get("flush_rpcs", 0)),
            "flush_errors": int(wb.get("flush_errors", 0)),
            "inflight": int(wb.get("inflight", 0)),
            "pending_slots": int(wb.get("pending_slots", 0)),
            "backpressure_waits": int(wb.get("backpressure_waits", 0)),
            "backpressure_wait_ms": round(float(wb.get("backpressure_wait_seconds", 0.0)) * 1000.0, 3),
            "barrier_waits": int(wb.get("barrier_waits", 0)),
            "barrier_wait_ms": round(float(wb.get("barrier_wait_seconds", 0.0)) * 1000.0, 3),
        },
        "inode_locks": {
            "fast_acquisitions": int(il.get("fast_acquisitions", 0)),
            "contended_acquisitions": int(il.get("contended_acquisitions", 0)),
            "wait_ms": round(float(il.get("wait_seconds", 0.0)) * 1000.0, 3),
        },
        "queues": {
            "fuse_waiting": int(qs.get("fuse_waiting", 0)),
            "fuse_congestion_threshold": int(qs.get("fuse_congestion_threshold", 0)),
        },
        "top_ops_by_total_time": by_total[:12],
        "top_ops_by_p99_latency": by_p99[:12],
        "phase_deltas": phase_deltas,
    }


# ==============================================================================
# 2. Jaeger Distributed Trace Analysis (Exact Same-Hop RPC Delta)
# ==============================================================================

INTERESTING_SPAN_TAGS = {
    "rpc.method",
    "rpc.service",
    "rpc.connect.status_code",
    "rpc.grpc.status_code",
    "rpc.response.status_code",
    "http.status_code",
    "fuse.op",
    "fuse.errno",
    "fuse.status_code",
    "disk_id",
    "ino",
    "parent",
    "offset",
    "size",
    "db.system",
    "db.operation",
    "error",
    "otel.status_code",
    "otel.status_description",
}


def _extract_span_rpc_method(sp: dict[str, Any], tags_map: dict[str, Any]) -> str | None:
    if tags_map.get("rpc.method"):
        return short_rpc_method(str(tags_map["rpc.method"]))
    op_name = str(sp.get("operationName") or "")
    if "StorageService/" in op_name:
        return short_rpc_method(op_name)
    return None


def distill_jaeger_trace(trace_obj: dict[str, Any]) -> dict[str, Any] | None:
    spans = trace_obj.get("spans") or []
    processes = trace_obj.get("processes") or {}
    if not spans:
        return None

    proc_service = {
        pid: (pinfo.get("serviceName") or "unknown")
        for pid, pinfo in processes.items()
        if isinstance(pinfo, dict)
    }

    span_by_id: dict[str, dict[str, Any]] = {}
    span_svc: dict[str, str] = {}
    span_tags: dict[str, dict[str, Any]] = {}
    for sp in spans:
        sid = sp.get("spanID")
        svc = proc_service.get(sp.get("processID", ""), "unknown")
        tmap: dict[str, Any] = {}
        for t in sp.get("tags") or []:
            k = t.get("key")
            if k in INTERESTING_SPAN_TAGS:
                tmap[k] = t.get("value")
        if sid:
            span_by_id[sid] = sp
            span_svc[sid] = svc
            span_tags[sid] = tmap

    root_span = None
    min_start = None
    max_end = None
    has_error = False
    services_set: set[str] = set()
    service_max_span_us: dict[str, int] = {}
    span_links_count = 0

    enriched_spans: list[dict[str, Any]] = []
    for sp in spans:
        st = int(sp.get("startTime", 0))
        dur = int(sp.get("duration", 0))
        ed = st + dur
        min_start = st if min_start is None else min(min_start, st)
        max_end = ed if max_end is None else max(max_end, ed)

        svc = proc_service.get(sp.get("processID", ""), "unknown")
        services_set.add(svc)
        service_max_span_us[svc] = max(service_max_span_us.get(svc, 0), dur)

        sid = sp.get("spanID", "")
        tags_map = dict(span_tags.get(sid) or {})
        span_err = False
        for t in sp.get("tags") or []:
            k = t.get("key")
            v = t.get("value")
            if k == "error" and (v is True or str(v).lower() == "true"):
                span_err = True
            if k == "otel.status_code" and str(v).upper() == "ERROR":
                span_err = True
        if span_err:
            has_error = True

        refs = sp.get("references") or []
        child_of_in_trace = False
        for r in refs:
            rtype = str(r.get("refType") or "").upper()
            if rtype in ("FOLLOWS_FROM", "LINK", "SPAN_LINK"):
                span_links_count += 1
            if r.get("spanID") in span_by_id and rtype in ("", "CHILD_OF"):
                child_of_in_trace = True
        if not child_of_in_trace and (root_span is None or st < int(root_span.get("startTime", 0))):
            root_span = sp

        enriched_spans.append({
            "service": svc,
            "operation": sp.get("operationName", ""),
            "duration_ms": round(dur / 1000.0, 3),
            "start_offset_ms": st,
            "error": span_err,
            "tags": tags_map,
        })

    if root_span is None:
        root_span = max(spans, key=lambda s: int(s.get("duration", 0)))

    base_start = min_start or 0
    for item in enriched_spans:
        item["start_offset_ms"] = round((item["start_offset_ms"] - base_start) / 1000.0, 3)

    enriched_spans.sort(key=lambda s: s["duration_ms"], reverse=True)
    wall_us = max(
        int(root_span.get("duration", 0)),
        (max_end - min_start) if (max_end is not None and min_start is not None) else 0,
    )
    root_svc = proc_service.get(root_span.get("processID", ""), "unknown")

    # 精确匹配同跳 Client ConnectRPC Span -> Server ConnectRPC Span，严禁拿客户端总 fuse.* Span 误减服务端单次 RPC
    hop_pairs_us: list[tuple[int, int, str]] = []
    client_rpc_spans_us: list[int] = []
    client_rpc_candidates: list[tuple[dict[str, Any], str]] = []
    server_rpc_spans: list[tuple[dict[str, Any], str | None]] = []
    server_rpc_candidates: list[tuple[dict[str, Any], str]] = []

    for sp in spans:
        sid = sp.get("spanID", "")
        svc = span_svc.get(sid, "unknown")
        mname = _extract_span_rpc_method(sp, span_tags.get(sid) or {})
        if svc == "bowl-client" and mname:
            client_rpc_spans_us.append(int(sp.get("duration", 0)))
            client_rpc_candidates.append((sp, mname))
        elif svc == "bowl-storage":
            server_rpc_spans.append((sp, mname))

    has_dedicated_cli_rpc = any(
        not str(cs.get("operationName") or "").startswith("fuse.") for cs, _ in client_rpc_candidates
    )

    for sp, mname in server_rpc_spans:
        # 1) 优先通过显式父子引用匹配跨机同跳客户端 RPC Span
        matched_parent = None
        for r in sp.get("references") or []:
            pid = r.get("spanID")
            if pid in span_by_id and span_svc.get(pid) == "bowl-client":
                cand_parent = span_by_id[pid]
                p_op = str(cand_parent.get("operationName") or "")
                if not p_op.startswith("fuse."):
                    matched_parent = cand_parent
                    break
                matched_parent = cand_parent
        if matched_parent is not None:
            p_op = str(matched_parent.get("operationName") or "")
            # 仅当客户端 Span 本身是 ConnectRPC Span（而非 fuse.<Op> 聚合根）时作为精确同跳对；
            # 若测试桩未单独拆出客户端 RPC Span（仅有 2 个 Span：fuse.<Op> -> StorageService/<Method>），回退使用该直接父子对
            if not p_op.startswith("fuse.") or not has_dedicated_cli_rpc:
                hop_pairs_us.append((
                    int(matched_parent.get("duration", 0)),
                    int(sp.get("duration", 0)),
                    mname or short_rpc_method(str(sp.get("operationName") or "")),
                ))
        elif mname:
            server_rpc_candidates.append((sp, mname))

    # 2) 若无直接跨机引用（如异步链接或采样裁剪），按同名 RPC 方法与时间窗包含关系配对
    if not hop_pairs_us and client_rpc_candidates and server_rpc_candidates:
        for s_sp, s_m in server_rpc_candidates:
            s_st = int(s_sp.get("startTime", 0))
            s_ed = s_st + int(s_sp.get("duration", 0))
            for c_sp, c_m in client_rpc_candidates:
                if c_m != s_m:
                    continue
                c_st = int(c_sp.get("startTime", 0))
                c_ed = c_st + int(c_sp.get("duration", 0))
                if c_st <= s_ed and s_st <= c_ed:
                    hop_pairs_us.append((int(c_sp.get("duration", 0)), int(s_sp.get("duration", 0)), s_m))
                    break

    client_max_ms = round(service_max_span_us.get("bowl-client", 0) / 1000.0, 3)
    server_max_ms = round(service_max_span_us.get("bowl-storage", 0) / 1000.0, 3)

    client_rpc_ms = None
    server_rpc_ms = None
    rpc_net_codec_ms = None
    if hop_pairs_us:
        # 取最慢的一跳跨机 RPC 计算精确同跳网络+编解码差值
        best_cli_us, best_srv_us, _ = max(hop_pairs_us, key=lambda item: item[0])
        client_rpc_ms = round(best_cli_us / 1000.0, 3)
        server_rpc_ms = round(best_srv_us / 1000.0, 3)
        rpc_net_codec_ms = round(max(0.0, client_rpc_ms - server_rpc_ms), 3)

    client_local_ms = None
    if root_svc == "bowl-client" and client_rpc_spans_us:
        root_ms = round(int(root_span.get("duration", 0)) / 1000.0, 3)
        sum_cli_rpc_ms = round(sum(client_rpc_spans_us) / 1000.0, 3)
        client_local_ms = round(max(0.0, root_ms - sum_cli_rpc_ms), 3)

    return {
        "trace_id": trace_obj.get("traceID", ""),
        "total_duration_ms": round(wall_us / 1000.0, 3),
        "root_service": root_svc,
        "root_operation": root_span.get("operationName", ""),
        "span_count": len(spans),
        "span_links_count": span_links_count,
        "services": sorted(services_set),
        "has_error": has_error,
        "client_max_span_ms": client_max_ms,
        "server_max_span_ms": server_max_ms,
        "client_rpc_span_ms": client_rpc_ms,
        "server_rpc_span_ms": server_rpc_ms,
        "rpc_network_codec_ms": rpc_net_codec_ms,
        "client_local_overhead_ms": client_local_ms,
        "network_or_client_overhead_ms": rpc_net_codec_ms,
        "top_spans": enriched_spans[:10],
    }


def process_jaeger_traces(stage_dir: Path, out_dir: Path) -> dict[str, Any]:
    jaeger_src = stage_dir / "server" / "jaeger"
    jaeger_dst = out_dir / "jaeger"
    jaeger_dst.mkdir(parents=True, exist_ok=True)
    if not jaeger_src.exists():
        return {}

    by_id: dict[str, dict[str, Any]] = {}
    operations_by_service: dict[str, list[str]] = {}
    for p in sorted(jaeger_src.glob("*.json")):
        if p.name == "services.json":
            continue
        if p.name.startswith("operations_"):
            op_payload = load_json_safe(p)
            if isinstance(op_payload, dict) and op_payload.get("service"):
                operations_by_service[str(op_payload["service"])] = list(op_payload.get("operations") or [])
            continue
        payload = load_json_safe(p)
        if not isinstance(payload, dict):
            continue
        for tr in payload.get("data") or []:
            if not isinstance(tr, dict) or not tr.get("traceID"):
                continue
            tid = tr["traceID"]
            if tid not in by_id or len(tr.get("spans") or []) > len(by_id[tid].get("spans") or []):
                by_id[tid] = tr

    distilled = [d for tr in by_id.values() if (d := distill_jaeger_trace(tr)) is not None]
    distilled.sort(key=lambda x: x["total_duration_ms"], reverse=True)

    slow_top = distilled[:40]
    err_traces = [d for d in distilled if d["has_error"]][:30]

    # 按入口操作分组保底提取代表性慢链路（防止单一高频操作占满榜单）
    by_root_op: dict[str, dict[str, Any]] = {}
    for d in distilled:
        op_key = f"{d['root_service']}:{d['root_operation']}"
        if op_key not in by_root_op:
            by_root_op[op_key] = d

    (jaeger_dst / "slow_traces.json").write_text(
        json.dumps(slow_top, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (jaeger_dst / "error_traces.json").write_text(
        json.dumps(err_traces, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    cross_node_count = sum(1 for d in distilled if "bowl-client" in d["services"] and "bowl-storage" in d["services"])
    services_payload = load_json_safe(jaeger_src / "services.json")
    services_list = (services_payload.get("data") or []) if isinstance(services_payload, dict) else []

    return {
        "services_discovered": services_list,
        "operations_by_service": operations_by_service,
        "sampled_traces_count": len(distilled),
        "cross_node_traces_count": cross_node_count,
        "error_traces_count": sum(1 for d in distilled if d["has_error"]),
        "slowest_by_operation": list(by_root_op.values())[:20],
        "slow_traces_top15": slow_top[:15],
        "error_traces_top10": err_traces[:10],
    }


# ==============================================================================
# 3. Prometheus Range Queries & Dual-Side `.prom` Exact Histogram / Status Analysis
# ==============================================================================


def parse_prom_text(path: Path) -> dict[str, list[tuple[dict[str, str], float]]]:
    if not path.exists() or path.stat().st_size == 0:
        return {}
    metrics: dict[str, list[tuple[dict[str, str], float]]] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        m = PROM_LINE_RE.match(line)
        if not m:
            continue
        val_str = m.group("val")
        if val_str == "NaN":
            continue
        try:
            val = math.inf if val_str == "+Inf" else (-math.inf if val_str == "-Inf" else float(val_str))
        except ValueError:
            continue
        if math.isnan(val):
            continue
        labels: dict[str, str] = {}
        raw_labels = m.group("labels")
        if raw_labels:
            for lm in PROM_LABEL_RE.finditer(raw_labels):
                labels[lm.group(1)] = lm.group(2)
        metrics.setdefault(m.group("name"), []).append((labels, val))
    return metrics


def prom_cum_quantile(buckets: list[tuple[float, float]], q: float) -> float:
    """Compute Prometheus histogram_quantile from cumulative (upper_bound, cum_count) pairs."""
    finite = sorted((b, c) for b, c in buckets if not math.isinf(b))
    inf_counts = [c for b, c in buckets if math.isinf(b)]
    total = inf_counts[0] if inf_counts else (finite[-1][1] if finite else 0.0)
    if total <= 0 or not finite:
        return 0.0
    rank = q * total
    prev_b = 0.0
    prev_c = 0.0
    for b, c in finite:
        if c >= rank:
            delta_c = c - prev_c
            if delta_c <= 0:
                return b
            return prev_b + ((rank - prev_c) / delta_c) * (b - prev_b)
        prev_b = b
        prev_c = c
    return finite[-1][0]


def summarize_prom_histogram(
    parsed: dict[str, list[tuple[dict[str, str], float]]],
    base_name: str,
    group_key: str | None = None,
    pre_parsed: dict[str, list[tuple[dict[str, str], float]]] | None = None,
    scale: float = 1000.0,
    normalize_rpc: bool = False,
) -> list[dict[str, Any]]:
    """Summarize cumulative Prometheus histogram (optionally subtracting pre_parsed)."""
    bucket_entries = parsed.get(f"{base_name}_bucket", [])
    sum_entries = parsed.get(f"{base_name}_sum", [])
    cnt_entries = parsed.get(f"{base_name}_count", [])
    if not cnt_entries:
        return []

    def resolve_group(labels: dict[str, str]) -> str:
        if not group_key:
            return "all"
        raw = labels.get(group_key, "all")
        return short_rpc_method(raw) if normalize_rpc else raw

    def index_by_key(
        entries: list[tuple[dict[str, str], float]], include_le: bool = False
    ) -> dict[tuple[str, str], float]:
        out: dict[tuple[str, str], float] = {}
        for labels, val in entries:
            g = resolve_group(labels)
            le = labels.get("le", "") if include_le else ""
            out[(g, le)] = out.get((g, le), 0.0) + val
        return out

    post_cnt = index_by_key(cnt_entries)
    post_sum = index_by_key(sum_entries)
    post_bkt = index_by_key(bucket_entries, include_le=True)

    pre_cnt = index_by_key(pre_parsed.get(f"{base_name}_count", [])) if pre_parsed else {}
    pre_sum = index_by_key(pre_parsed.get(f"{base_name}_sum", [])) if pre_parsed else {}
    pre_bkt = index_by_key(pre_parsed.get(f"{base_name}_bucket", []), include_le=True) if pre_parsed else {}

    results: list[dict[str, Any]] = []
    for (g, _), c_val in post_cnt.items():
        d_cnt = max(0.0, c_val - pre_cnt.get((g, ""), 0.0))
        if d_cnt <= 0:
            continue
        d_sum = max(0.0, post_sum.get((g, ""), 0.0) - pre_sum.get((g, ""), 0.0))
        buckets_for_g: list[tuple[float, float]] = []
        for (bg, le_str), b_val in post_bkt.items():
            if bg != g or not le_str:
                continue
            if le_str == "+Inf":
                bound = math.inf
            else:
                try:
                    bound = float(le_str)
                except ValueError:
                    continue
                if math.isnan(bound):
                    continue
            d_b = max(0.0, b_val - pre_bkt.get((bg, le_str), 0.0))
            buckets_for_g.append((bound, d_b))

        results.append({
            "group": g,
            "count": int(round(d_cnt)),
            "total": round(d_sum * scale, 3),
            "mean": round((d_sum / d_cnt) * scale, 3),
            "p50": round(prom_cum_quantile(buckets_for_g, 0.50) * scale, 3),
            "p95": round(prom_cum_quantile(buckets_for_g, 0.95) * scale, 3),
            "p99": round(prom_cum_quantile(buckets_for_g, 0.99) * scale, 3),
        })

    results.sort(key=lambda x: x["total"], reverse=True)
    return results


def summarize_rpc_status_split(
    parsed: dict[str, list[tuple[dict[str, str], float]]],
    base_name: str,
    method_keys: tuple[str, ...] = ("rpc_method", "procedure"),
    status_keys: tuple[str, ...] = (
        "rpc_response_status_code",
        "rpc_connect_status_code",
        "rpc_grpc_status_code",
        "code",
        "status_class",
        "status_code",
    ),
    pre_parsed: dict[str, list[tuple[dict[str, str], float]]] | None = None,
) -> dict[str, Any]:
    """Split RPC / HTTP histogram call counts into normal (OK/2xx) vs non-OK error codes."""
    cnt_entries = parsed.get(f"{base_name}_count", [])
    if not cnt_entries:
        return {}

    def extract_pair(labels: dict[str, str]) -> tuple[str, str]:
        m_val = "unknown"
        for mk in method_keys:
            if labels.get(mk):
                m_val = short_rpc_method(labels[mk])
                break
        s_val = "ok"
        for sk in status_keys:
            if sk in labels and labels[sk] != "":
                s_val = labels[sk]
                break
        return m_val, s_val

    post_map: dict[tuple[str, str], float] = {}
    for lbls, val in cnt_entries:
        key = extract_pair(lbls)
        post_map[key] = post_map.get(key, 0.0) + val

    pre_map: dict[tuple[str, str], float] = {}
    if pre_parsed:
        for lbls, val in pre_parsed.get(f"{base_name}_count", []):
            key = extract_pair(lbls)
            pre_map[key] = pre_map.get(key, 0.0) + val

    by_method: dict[str, dict[str, Any]] = {}
    total_ok = 0
    total_err = 0
    errors_by_code: dict[str, int] = {}

    for (m_name, st_code), post_v in post_map.items():
        d_cnt = int(round(max(0.0, post_v - pre_map.get((m_name, st_code), 0.0))))
        if d_cnt <= 0:
            continue
        rec = by_method.setdefault(
            m_name,
            {"method": m_name, "total_calls": 0, "ok_calls": 0, "error_calls": 0, "errors_by_code": {}},
        )
        rec["total_calls"] += d_cnt
        if st_code.strip().lower() in OK_STATUS_CODES:
            rec["ok_calls"] += d_cnt
            total_ok += d_cnt
        else:
            rec["error_calls"] += d_cnt
            rec["errors_by_code"][st_code] = rec["errors_by_code"].get(st_code, 0) + d_cnt
            total_err += d_cnt
            errors_by_code[st_code] = errors_by_code.get(st_code, 0) + d_cnt

    method_list = list(by_method.values())
    for item in method_list:
        tot = item["total_calls"]
        item["error_rate_pct"] = round((item["error_calls"] / tot) * 100.0, 2) if tot > 0 else 0.0
    method_list.sort(key=lambda x: (x["error_calls"], x["total_calls"]), reverse=True)

    grand_total = total_ok + total_err
    return {
        "total_calls": grand_total,
        "ok_calls": total_ok,
        "error_calls": total_err,
        "error_rate_pct": round((total_err / grand_total) * 100.0, 2) if grand_total > 0 else 0.0,
        "errors_by_code": errors_by_code,
        "by_method": method_list,
    }


def _sum_prom_metric(
    parsed: dict[str, list[tuple[dict[str, str], float]]],
    mname: str,
    lkey: str | None = None,
    pre_parsed: dict[str, list[tuple[dict[str, str], float]]] | None = None,
    normalize_rpc: bool = False,
) -> Any:
    entries = parsed.get(mname, [])
    if not entries:
        return None
    pre_entries = pre_parsed.get(mname, []) if pre_parsed else []
    if lkey is None:
        post_v = sum(v for _, v in entries if not math.isinf(v))
        pre_v = sum(v for _, v in pre_entries if not math.isinf(v)) if pre_parsed else 0.0
        return round(max(0.0, post_v - pre_v), 3)
    grouped: dict[str, float] = {}
    for lbls, v in entries:
        if math.isinf(v):
            continue
        raw_k = lbls.get(lkey, "all")
        k = short_rpc_method(raw_k) if normalize_rpc else raw_k
        grouped[k] = grouped.get(k, 0.0) + v
    if pre_parsed:
        pre_grouped: dict[str, float] = {}
        for lbls, v in pre_entries:
            if math.isinf(v):
                continue
            raw_k = lbls.get(lkey, "all")
            k = short_rpc_method(raw_k) if normalize_rpc else raw_k
            pre_grouped[k] = pre_grouped.get(k, 0.0) + v
        grouped = {k: max(0.0, v - pre_grouped.get(k, 0.0)) for k, v in grouped.items() if v - pre_grouped.get(k, 0.0) > 0}
    return {k: round(v, 3) for k, v in grouped.items()} if grouped else None


def extract_server_prom_snapshot(
    post_p: dict[str, list[tuple[dict[str, str], float]]],
    pre_p: dict[str, list[tuple[dict[str, str], float]]] | None = None,
) -> dict[str, Any]:
    bc_hits = _sum_prom_metric(post_p, "bowl_storage_pebble_block_cache_hits_total", pre_parsed=pre_p) or 0.0
    bc_miss = _sum_prom_metric(post_p, "bowl_storage_pebble_block_cache_misses_total", pre_parsed=pre_p) or 0.0
    read_blocks = _sum_prom_metric(post_p, "bowl_storage_blob_read_source_total", "source", pre_parsed=pre_p)
    if read_blocks is None:
        read_blocks = _sum_prom_metric(post_p, "bowl_storage_blob_read_total", "source", pre_parsed=pre_p)

    return {
        "http_request_latency_ms": summarize_prom_histogram(
            post_p, "bowl_storage_http_request_duration_seconds", group_key="procedure", pre_parsed=pre_p, scale=1000.0, normalize_rpc=True
        ),
        "http_request_status": summarize_rpc_status_split(
            post_p, "bowl_storage_http_request_duration_seconds", ("procedure",), ("status_class",), pre_parsed=pre_p
        ),
        "rpc_server_latency_ms": summarize_prom_histogram(
            post_p, "rpc_server_call_duration_seconds", group_key="rpc_method", pre_parsed=pre_p, scale=1000.0, normalize_rpc=True
        ),
        "rpc_server_status": summarize_rpc_status_split(
            post_p, "rpc_server_call_duration_seconds", ("rpc_method",), pre_parsed=pre_p
        ),
        "control_open_actor_ms": summarize_prom_histogram(
            post_p, "bowl_storage_control_open_actor_duration_seconds", group_key="result", pre_parsed=pre_p, scale=1000.0
        ),
        "control_fork_stage_ms": summarize_prom_histogram(
            post_p, "bowl_storage_control_fork_stage_duration_seconds", group_key="stage", pre_parsed=pre_p, scale=1000.0
        ),
        "control_segment_reserve_ms": summarize_prom_histogram(
            post_p, "bowl_storage_control_segment_reserve_duration_seconds", group_key="kind", pre_parsed=pre_p, scale=1000.0
        ),
        "read_op_latency_ms": summarize_prom_histogram(
            post_p, "bowl_storage_read_op_duration_seconds", group_key="op", pre_parsed=pre_p, scale=1000.0
        ),
        "kv_read_latency_ms": summarize_prom_histogram(
            post_p, "bowl_storage_kv_read_duration_seconds", group_key="op", pre_parsed=pre_p, scale=1000.0
        ),
        "blob_pread_ms": summarize_prom_histogram(
            post_p, "bowl_storage_blob_pread_duration_seconds", pre_parsed=pre_p, scale=1000.0
        ),
        "actor_queue_wait_ms": summarize_prom_histogram(
            post_p, "bowl_storage_actor_queue_wait_seconds", pre_parsed=pre_p, scale=1000.0
        ),
        "actor_tick_batch_size": summarize_prom_histogram(
            post_p, "bowl_storage_actor_tick_batch_size", pre_parsed=pre_p, scale=1.0
        ),
        "actor_tick_phase_ms": summarize_prom_histogram(
            post_p, "bowl_storage_actor_tick_phase_duration_seconds", group_key="phase", pre_parsed=pre_p, scale=1000.0
        ),
        "blob_pwrite_ms": summarize_prom_histogram(
            post_p, "bowl_storage_blob_pwrite_duration_seconds", pre_parsed=pre_p, scale=1000.0
        ),
        "blob_sync_ms": summarize_prom_histogram(
            post_p, "bowl_storage_blob_sync_duration_seconds", group_key="target", pre_parsed=pre_p, scale=1000.0
        ),
        "blob_rmw_read_ms": summarize_prom_histogram(
            post_p, "bowl_storage_blob_rmw_read_duration_seconds", pre_parsed=pre_p, scale=1000.0
        ),
        "inode_lock_wait_ms": summarize_prom_histogram(
            post_p, "bowl_storage_inode_lock_wait_seconds", pre_parsed=pre_p, scale=1000.0
        ),
        "pebble_wal_fsync_ms": summarize_prom_histogram(
            post_p, "bowl_storage_pebble_wal_fsync_duration_seconds", pre_parsed=pre_p, scale=1000.0
        ),
        "counters_and_gauges": {
            "http_request_bytes_by_proc": _sum_prom_metric(
                post_p, "bowl_storage_http_request_bytes_total", "procedure", pre_parsed=pre_p, normalize_rpc=True
            ),
            "http_response_bytes_by_proc": _sum_prom_metric(
                post_p, "bowl_storage_http_response_bytes_total", "procedure", pre_parsed=pre_p, normalize_rpc=True
            ),
            "blob_write_bytes_by_mode": _sum_prom_metric(
                post_p, "bowl_storage_blob_write_bytes_total", "mode", pre_parsed=pre_p
            ),
            "blob_read_blocks_by_source": read_blocks,
            "pebble_wal_bytes_written": _sum_prom_metric(
                post_p, "bowl_storage_pebble_wal_bytes_written_total", pre_parsed=pre_p
            ),
            "pebble_block_cache_hits": int(round(bc_hits)),
            "pebble_block_cache_misses": int(round(bc_miss)),
            "pebble_block_cache_hit_rate_pct": (
                round(bc_hits / (bc_hits + bc_miss) * 100.0, 2) if (bc_hits + bc_miss) > 0 else None
            ),
            # Gauges always read from post_p directly
            "pebble_read_amp": _sum_prom_metric(post_p, "bowl_storage_pebble_read_amp"),
            "pebble_stall_events": _sum_prom_metric(
                post_p, "bowl_storage_pebble_stall_events_total", pre_parsed=pre_p
            ),
            "pebble_disk_slow_events": _sum_prom_metric(
                post_p, "bowl_storage_pebble_disk_slow_total", pre_parsed=pre_p
            ),
            "pack_physical_bytes": _sum_prom_metric(post_p, "bowl_storage_pack_physical_bytes"),
            "pack_referenced_bytes": _sum_prom_metric(post_p, "bowl_storage_pack_referenced_bytes"),
            "data_dir_physical_bytes": _sum_prom_metric(post_p, "bowl_storage_data_dir_physical_bytes"),
            "commits_active": _sum_prom_metric(post_p, "bowl_storage_commits_active"),
            "commits_physical_bytes": _sum_prom_metric(post_p, "bowl_storage_commits_physical_bytes"),
            "commits_reclaimed": _sum_prom_metric(
                post_p, "bowl_storage_control_commits_reclaimed_total", pre_parsed=pre_p
            ),
            "forks_by_result": _sum_prom_metric(
                post_p, "bowl_storage_control_forks_total", "result", pre_parsed=pre_p
            ),
        },
    }


def extract_client_prom_snapshot(
    post_p: dict[str, list[tuple[dict[str, str], float]]],
    pre_p: dict[str, list[tuple[dict[str, str], float]]] | None = None,
) -> dict[str, Any]:
    rpc_cli_base = (
        "rpc_client_call_duration_seconds"
        if f"rpc_client_call_duration_seconds_count" in post_p
        else "rpc_client_duration_seconds"
    )
    return {
        "fuse_op_latency_ms": summarize_prom_histogram(
            post_p, "bowl_client_fuse_op_duration_seconds", group_key="op", pre_parsed=pre_p, scale=1000.0
        ),
        "rpc_client_latency_ms": summarize_prom_histogram(
            post_p, rpc_cli_base, group_key="rpc_method", pre_parsed=pre_p, scale=1000.0, normalize_rpc=True
        ),
        "rpc_client_status": summarize_rpc_status_split(
            post_p, rpc_cli_base, ("rpc_method",), pre_parsed=pre_p
        ),
        "rpc_client_logical_latency_ms": summarize_prom_histogram(
            post_p, "bowl_client_rpc_logical_duration_seconds", group_key="procedure", pre_parsed=pre_p, scale=1000.0, normalize_rpc=True
        ),
        "rpc_client_logical_status": summarize_rpc_status_split(
            post_p, "bowl_client_rpc_logical_duration_seconds", ("procedure",), ("code",), pre_parsed=pre_p
        ),
        "write_behind_backpressure_wait_ms": summarize_prom_histogram(
            post_p, "bowl_client_write_behind_backpressure_wait_duration_seconds", pre_parsed=pre_p, scale=1000.0
        ),
        "write_behind_barrier_wait_ms": summarize_prom_histogram(
            post_p, "bowl_client_write_behind_barrier_wait_duration_seconds", pre_parsed=pre_p, scale=1000.0
        ),
        "read_ahead_wait_ms": summarize_prom_histogram(
            post_p, "bowl_client_read_ahead_wait_duration_seconds", pre_parsed=pre_p, scale=1000.0
        ),
        "inodelock_wait_ms": summarize_prom_histogram(
            post_p, "bowl_client_inodelock_wait_duration_seconds", pre_parsed=pre_p, scale=1000.0
        ),
        "counters_and_gauges": {
            "write_behind_writes_by_mode": _sum_prom_metric(
                post_p, "bowl_client_write_behind_writes_total", "mode", pre_parsed=pre_p
            ),
            "write_behind_bytes_by_mode": _sum_prom_metric(
                post_p, "bowl_client_write_behind_bytes_total", "mode", pre_parsed=pre_p
            ),
            "write_behind_flush_rpcs_by_result": _sum_prom_metric(
                post_p, "bowl_client_write_behind_flush_rpcs_total", "result", pre_parsed=pre_p
            ),
            "write_behind_inflight": _sum_prom_metric(post_p, "bowl_client_write_behind_inflight"),
            "write_behind_pending_slots": _sum_prom_metric(post_p, "bowl_client_write_behind_pending_slots"),
            "nscache_hits_by_kind": _sum_prom_metric(
                post_p, "bowl_client_nscache_hits_total", "kind", pre_parsed=pre_p
            ),
            "nscache_misses_by_kind": _sum_prom_metric(
                post_p, "bowl_client_nscache_misses_total", "kind", pre_parsed=pre_p
            ),
            "rpc_retries_by_proc": _sum_prom_metric(
                post_p, "bowl_client_rpc_retries_total", "procedure", pre_parsed=pre_p, normalize_rpc=True
            ),
            "fuse_connection_waiting": _sum_prom_metric(post_p, "bowl_client_fuse_connection_waiting"),
            "fuse_connection_congestion_threshold": _sum_prom_metric(
                post_p, "bowl_client_fuse_connection_congestion_threshold"
            ),
        },
    }


def summarize_prom_query_range(query_range_payload: dict[str, Any]) -> dict[str, Any]:
    queries = query_range_payload.get("queries") or {}
    summarized: dict[str, list[dict[str, Any]]] = {}

    for qname, qentry in queries.items():
        resp = (qentry or {}).get("response") or {}
        if resp.get("status") != "success":
            continue
        series_list = (resp.get("data") or {}).get("result") or []
        series_summaries: list[dict[str, Any]] = []
        for s in series_list:
            metric_labels = s.get("metric") or {}
            vals: list[float] = []
            for pair in s.get("values") or []:
                if len(pair) >= 2:
                    try:
                        fv = float(pair[1])
                        if not math.isnan(fv) and not math.isinf(fv):
                            vals.append(fv)
                    except ValueError:
                        pass
            if not vals:
                continue
            vals_sorted = sorted(vals)
            p95_idx = min(len(vals_sorted) - 1, int(math.ceil(0.95 * len(vals_sorted))) - 1)
            series_summaries.append({
                "labels": metric_labels,
                "samples": len(vals),
                "mean": round(sum(vals) / len(vals), 6),
                "p95": round(vals_sorted[max(0, p95_idx)], 6),
                "max": round(vals_sorted[-1], 6),
            })
        if series_summaries:
            series_summaries.sort(key=lambda x: x["max"], reverse=True)
            summarized[qname] = series_summaries[:15]
    return summarized


def _select_terminal_prom(prom_files: dict[str, Path]) -> str | None:
    non_empty = {k: p for k, p in prom_files.items() if p.exists() and p.stat().st_size > 0}
    if not non_empty:
        return None
    for cand in ("before_umount", "suite_post", "final"):
        if cand in non_empty:
            return cand
    return max(non_empty.keys(), key=lambda k: non_empty[k].stat().st_size)


def process_prometheus_data(stage_dir: Path, out_dir: Path) -> dict[str, Any]:
    prom_dst = out_dir / "prometheus"
    prom_dst.mkdir(parents=True, exist_ok=True)

    srv_prom_api = stage_dir / "server" / "prometheus"
    srv_metrics_dir = stage_dir / "server" / "metrics"
    cli_metrics_dir = stage_dir / "client" / "metrics"

    targets_health: list[dict[str, Any]] = []
    targets_json = load_json_safe(srv_prom_api / "targets_live.json") or load_json_safe(srv_prom_api / "targets.json")
    if isinstance(targets_json, dict):
        for t in (targets_json.get("data") or {}).get("activeTargets") or []:
            targets_health.append({
                "job": (t.get("labels") or {}).get("job", ""),
                "scrape_url": t.get("scrapeUrl", ""),
                "health": t.get("health", ""),
                "last_error": t.get("lastError", ""),
            })

    range_summary: dict[str, Any] = {}
    qr_json = load_json_safe(srv_prom_api / "query_range.json")
    if isinstance(qr_json, dict):
        range_summary = summarize_prom_query_range(qr_json)

    server_engine_summary: dict[str, Any] = {}
    if srv_metrics_dir.exists():
        prom_files = {p.stem: p for p in srv_metrics_dir.glob("*.prom") if p.stat().st_size > 0}
        term_stem = _select_terminal_prom(prom_files)
        if term_stem:
            term_parsed = parse_prom_text(prom_files[term_stem])
            server_engine_summary = {"snapshot": term_stem, **extract_server_prom_snapshot(term_parsed)}

            phase_server_deltas: dict[str, Any] = {}
            for stem in sorted(prom_files.keys()):
                if stem.endswith("_pre"):
                    base = stem[: -len("_pre")]
                    post_stem = f"{base}_post"
                    if post_stem in prom_files:
                        pre_p = parse_prom_text(prom_files[stem])
                        post_p = parse_prom_text(prom_files[post_stem])
                        phase_server_deltas[base] = extract_server_prom_snapshot(post_p, pre_p=pre_p)
            if phase_server_deltas:
                server_engine_summary["phase_deltas"] = phase_server_deltas

    client_engine_summary: dict[str, Any] = {}
    if cli_metrics_dir.exists():
        cli_prom_files = {p.stem: p for p in cli_metrics_dir.glob("*.prom") if p.stat().st_size > 0}
        cli_term_stem = _select_terminal_prom(cli_prom_files)
        if cli_term_stem:
            cli_term_parsed = parse_prom_text(cli_prom_files[cli_term_stem])
            client_engine_summary = {"snapshot": cli_term_stem, **extract_client_prom_snapshot(cli_term_parsed)}

            phase_client_deltas: dict[str, Any] = {}
            for stem in sorted(cli_prom_files.keys()):
                if stem.endswith("_pre"):
                    base = stem[: -len("_pre")]
                    post_stem = f"{base}_post"
                    if post_stem in cli_prom_files:
                        pre_p = parse_prom_text(cli_prom_files[stem])
                        post_p = parse_prom_text(cli_prom_files[post_stem])
                        phase_client_deltas[base] = extract_client_prom_snapshot(post_p, pre_p=pre_p)
            if phase_client_deltas:
                client_engine_summary["phase_deltas"] = phase_client_deltas

    summary = {
        "targets": targets_health,
        "range_queries": range_summary,
        "server_engine": server_engine_summary,
        "client_engine": client_engine_summary,
    }
    (prom_dst / "metrics_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary


# ==============================================================================
# 4. End-to-End Per-Workload Hop-by-Hop Attribution (`phase_deltas`)
# ==============================================================================


def _aggregate_hist_rows(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    total_cnt = sum(int(r.get("count", 0)) for r in rows)
    if total_cnt <= 0:
        return None
    total_ms = sum(float(r.get("total", 0.0)) for r in rows)
    # Call-weighted P50, max/weighted tail P99 across active procedures
    w_p50 = sum(float(r.get("p50", 0.0)) * int(r.get("count", 0)) for r in rows) / total_cnt
    max_p99 = max(float(r.get("p99", 0.0)) for r in rows)
    return {
        "count": total_cnt,
        "total": round(total_ms, 3),
        "mean": round(total_ms / total_cnt, 3),
        "p50": round(w_p50, 3),
        "p99": round(max_p99, 3),
    }


def compute_same_hop_rpc_deltas(
    cli_rpc_rows: list[dict[str, Any]],
    srv_rpc_rows: list[dict[str, Any]],
    srv_http_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Compute exact same-hop Client ConnectRPC minus Server ConnectRPC transport + codec delta."""
    cli_by_m = {r["group"]: r for r in cli_rpc_rows if r.get("count", 0) > 0}
    srv_by_m = {r["group"]: r for r in srv_rpc_rows if r.get("count", 0) > 0}
    http_by_m = {r["group"]: r for r in (srv_http_rows or []) if r.get("count", 0) > 0}

    matched_methods = sorted(set(cli_by_m.keys()) & set(srv_by_m.keys()))
    per_method: list[dict[str, Any]] = []
    tot_calls = 0
    tot_net_codec_ms = 0.0
    tot_http_codec_ms = 0.0
    weighted_p50_sum = 0.0
    max_p99_delta = 0.0

    for m in matched_methods:
        c = cli_by_m[m]
        s = srv_by_m[m]
        h = http_by_m.get(m)
        cnt = min(int(c["count"]), int(s["count"]))
        if cnt <= 0:
            continue
        net_mean = max(0.0, float(c["mean"]) - float(s["mean"]))
        net_tot = max(0.0, float(c["total"]) - float(s["total"]))
        net_p50 = max(0.0, float(c["p50"]) - float(s["p50"]))
        net_p99 = max(0.0, float(c["p99"]) - float(s["p99"]))

        http_codec_mean = max(0.0, float(h["mean"]) - float(s["mean"])) if h else None
        http_codec_tot = max(0.0, float(h["total"]) - float(s["total"])) if h else 0.0
        wire_only_mean = max(0.0, float(c["mean"]) - float(h["mean"])) if h else None

        tot_calls += cnt
        tot_net_codec_ms += net_tot
        tot_http_codec_ms += http_codec_tot
        weighted_p50_sum += net_p50 * cnt
        max_p99_delta = max(max_p99_delta, net_p99)

        per_method.append({
            "method": m,
            "calls": cnt,
            "client_rpc_mean_ms": round(float(c["mean"]), 3),
            "server_http_mean_ms": round(float(h["mean"]), 3) if h else None,
            "server_rpc_mean_ms": round(float(s["mean"]), 3),
            "net_and_codec_total_ms": round(net_tot, 3),
            "net_and_codec_mean_ms": round(net_mean, 3),
            "net_and_codec_p50_ms": round(net_p50, 3),
            "net_and_codec_p99_ms": round(net_p99, 3),
            "wire_and_client_codec_mean_ms": round(wire_only_mean, 3) if wire_only_mean is not None else None,
            "server_http_codec_mean_ms": round(http_codec_mean, 3) if http_codec_mean is not None else None,
        })

    per_method.sort(key=lambda x: x["net_and_codec_total_ms"], reverse=True)
    return {
        "matched_calls": tot_calls,
        "total_ms": round(tot_net_codec_ms, 3),
        "mean_ms": round(tot_net_codec_ms / tot_calls, 3) if tot_calls > 0 else None,
        "p50_ms": round(weighted_p50_sum / tot_calls, 3) if tot_calls > 0 else None,
        "p99_ms": round(max_p99_delta, 3) if tot_calls > 0 else None,
        "server_http_codec_total_ms": round(tot_http_codec_ms, 3) if http_by_m else None,
        "by_method": per_method,
    }


def build_single_phase_breakdown(
    phase_name: str,
    cli_stats_delta: dict[str, Any] | None,
    cli_prom_delta: dict[str, Any] | None,
    srv_prom_delta: dict[str, Any] | None,
    stage_sec_lookup: dict[str, float],
) -> dict[str, Any]:
    cs = cli_stats_delta or {}
    cp = cli_prom_delta or {}
    sp = srv_prom_delta or {}

    wall_sec = float(cs.get("window_sec") or 0.0)
    if wall_sec <= 0 and phase_name in stage_sec_lookup:
        wall_sec = float(stage_sec_lookup[phase_name])
    wall_ms = round(wall_sec * 1000.0, 3)

    fuse_agg = cs.get("fuse_aggregate") or {}
    fuse_active_ms = float(cs.get("fuse_active_ms") or fuse_agg.get("total_ms") or 0.0)
    fuse_concurrency = round(fuse_active_ms / wall_ms, 3) if wall_ms > 0 and fuse_active_ms > 0 else cs.get("fuse_active_concurrency")

    cli_rpc_rows = cp.get("rpc_client_latency_ms") or []
    cli_logical_rows = cp.get("rpc_client_logical_latency_ms") or []
    srv_http_rows = sp.get("http_request_latency_ms") or []
    srv_rpc_rows = sp.get("rpc_server_latency_ms") or []

    cli_rpc_agg = _aggregate_hist_rows(cli_rpc_rows)
    cli_logical_agg = _aggregate_hist_rows(cli_logical_rows)
    srv_http_agg = _aggregate_hist_rows(srv_http_rows)
    srv_rpc_agg = _aggregate_hist_rows(srv_rpc_rows)

    rpc_totals = [
        float(agg["total"])
        for agg in (cli_logical_agg, cli_rpc_agg, srv_http_agg, srv_rpc_agg)
        if agg and float(agg.get("total") or 0.0) > 0.0
    ]
    max_rpc_total_ms = max(rpc_totals) if rpc_totals else 0.0
    async_rpc_pipeline = fuse_active_ms > 0.0 and max_rpc_total_ms > fuse_active_ms * 1.02
    async_rpc_ratio = round(max_rpc_total_ms / fuse_active_ms, 2) if async_rpc_pipeline else None

    if fuse_active_ms > 0:
        if async_rpc_pipeline:
            ref_basis = "FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)"
        else:
            ref_basis = "FUSE 活跃总耗时"
        ref_total_ms = fuse_active_ms
        rpc_ref_total_ms = max(fuse_active_ms, max_rpc_total_ms)
    elif max_rpc_total_ms > 0:
        ref_basis = "Client ConnectRPC 总耗时" if (cli_rpc_agg and cli_rpc_agg["total"] > 0) else "Server ConnectRPC 总耗时"
        ref_total_ms = max_rpc_total_ms
        rpc_ref_total_ms = max_rpc_total_ms
    else:
        ref_basis = "阶段基准耗时"
        ref_total_ms = wall_ms if wall_ms > 0 else 0.0
        rpc_ref_total_ms = ref_total_ms

    def pct_of_ref(ms_val: float, hop_stage: str) -> float | None:
        denom = ref_total_ms if hop_stage.startswith(("1.", "2.")) else rpc_ref_total_ms
        if denom <= 0:
            return None
        return round(min(100.0, (ms_val / denom) * 100.0), 1)

    hops: list[dict[str, Any]] = []

    def add_hop(
        hop_stage: str,
        component: str,
        calls: int,
        total_ms: float,
        mean_ms: float | None,
        p50_ms: float | None,
        p99_ms: float | None,
        note: str = "",
    ) -> None:
        hops.append({
            "hop_stage": hop_stage,
            "component": component,
            "calls": int(calls),
            "total_ms": round(float(total_ms), 3),
            "mean_ms": round(float(mean_ms), 3) if mean_ms is not None else None,
            "p50_ms": round(float(p50_ms), 3) if p50_ms is not None else None,
            "p99_ms": round(float(p99_ms), 3) if p99_ms is not None else None,
            "share_pct": pct_of_ref(float(total_ms), hop_stage),
            "note": note,
        })

    # Hop 1: FUSE Entry
    if fuse_agg.get("calls", 0) > 0:
        add_hop(
            "1. FUSE 入口",
            "fuse.* (总计)",
            fuse_agg["calls"],
            fuse_agg["total_ms"],
            fuse_agg["mean_ms"],
            fuse_agg["p50_ms"],
            fuse_agg["p99_ms"],
            f"错误返回 {fuse_agg.get('errors', 0)} 次",
        )
        for op in (cs.get("top_ops_by_total_time") or [])[:3]:
            add_hop(
                "1. FUSE 入口 (Top Op)",
                f"fuse.{op['op']}",
                op["calls"],
                op["total_duration_ms"],
                op["mean_us"] / 1000.0,
                op["p50_us"] / 1000.0,
                op["p99_us"] / 1000.0,
                f"占 FUSE {op['share_of_fuse_time_pct']:.1f}%",
            )

    # Hop 2: Client Cache / Lock / WriteBehind / ReadAhead Waits
    wb_cs = cs.get("write_behind") or {}
    client_wait_added = False
    for label, p_rows, fallback_calls, fallback_ms in (
        ("Client InodeLock 等待", cp.get("inodelock_wait_ms") or [], int(cs.get("inode_lock_contended", 0)), float(cs.get("inode_lock_wait_ms", 0.0))),
        ("WriteBehind 背压等待", cp.get("write_behind_backpressure_wait_ms") or [], int(wb_cs.get("backpressure_waits", 0)), float(wb_cs.get("backpressure_wait_ms", 0.0))),
        ("WriteBehind 屏障等待", cp.get("write_behind_barrier_wait_ms") or [], int(wb_cs.get("barrier_waits", 0)), float(wb_cs.get("barrier_wait_ms", 0.0))),
        ("ReadAhead 预读等待", cp.get("read_ahead_wait_ms") or [], int(cs.get("readahead_waits", 0)), float(cs.get("readahead_wait_ms", 0.0))),
    ):
        agg = _aggregate_hist_rows(p_rows)
        if agg and agg["count"] > 0:
            add_hop("2. 客户端等待", label, agg["count"], agg["total"], agg["mean"], agg["p50"], agg["p99"])
            client_wait_added = True
        elif fallback_calls > 0 or fallback_ms > 0:
            mean_v = (fallback_ms / fallback_calls) if fallback_calls > 0 else fallback_ms
            add_hop("2. 客户端等待", label, fallback_calls, fallback_ms, mean_v, None, None, "来自 bowl stats")
            client_wait_added = True

    if not client_wait_added and (cs or cp):
        add_hop(
            "2. 客户端等待",
            "客户端锁 / WriteBehind / ReadAhead 等待",
            0,
            0.0,
            0.0,
            0.0,
            0.0,
            "零阻塞等待",
        )

    # Hop 3: Client ConnectRPC + Exact Same-Hop Network + Codec Delta
    if cli_logical_agg and cli_logical_agg["count"] > 0 and cli_rpc_agg and cli_logical_agg["count"] != cli_rpc_agg["count"]:
        add_hop(
            "3. 客户端 RPC (含重试)",
            "bowl_client_rpc_logical",
            cli_logical_agg["count"],
            cli_logical_agg["total"],
            cli_logical_agg["mean"],
            cli_logical_agg["p50"],
            cli_logical_agg["p99"],
        )
    if cli_rpc_agg and cli_rpc_agg["count"] > 0:
        cli_err = (cp.get("rpc_client_status") or {}).get("error_calls", 0)
        cli_note = f"非 OK 状态 {cli_err} 次"
        if async_rpc_ratio is not None:
            cli_note += f" · 异步并发流水线 (累计为 FUSE 活跃 {async_rpc_ratio:.2f}x)"
        add_hop(
            "3. 客户端 ConnectRPC",
            "rpc_client_call (总计)",
            cli_rpc_agg["count"],
            cli_rpc_agg["total"],
            cli_rpc_agg["mean"],
            cli_rpc_agg["p50"],
            cli_rpc_agg["p99"],
            cli_note,
        )

    rpc_hop_delta = compute_same_hop_rpc_deltas(cli_rpc_rows, srv_rpc_rows, srv_http_rows)
    if rpc_hop_delta["matched_calls"] > 0:
        top_m_note = ", ".join(
            f"{m['method']}:{m['net_and_codec_mean_ms']:.3f}ms"
            for m in rpc_hop_delta["by_method"][:2]
        )
        add_hop(
            "3. 跨机网络+编解码差值",
            "同跳 Client ConnectRPC - Server ConnectRPC",
            rpc_hop_delta["matched_calls"],
            rpc_hop_delta["total_ms"],
            rpc_hop_delta["mean_ms"],
            rpc_hop_delta["p50_ms"],
            rpc_hop_delta["p99_ms"],
            top_m_note or "同名 RPC 同跳精确差值",
        )

    # Hop 4: Server HTTP Outer / Codec & Server ConnectRPC
    if srv_http_agg and srv_http_agg["count"] > 0:
        http_codec_note = ""
        if srv_rpc_agg and srv_rpc_agg["count"] > 0:
            codec_diff_mean = max(0.0, srv_http_agg["mean"] - srv_rpc_agg["mean"])
            http_codec_note = f"HTTP Body+Codec 差值均值 {codec_diff_mean:.3f} ms"
        add_hop(
            "4. Server HTTP/Codec",
            "bowl_storage_http_request (含 Body 读写+序列化)",
            srv_http_agg["count"],
            srv_http_agg["total"],
            srv_http_agg["mean"],
            srv_http_agg["p50"],
            srv_http_agg["p99"],
            http_codec_note,
        )
    if srv_rpc_agg and srv_rpc_agg["count"] > 0:
        srv_err = (sp.get("rpc_server_status") or {}).get("error_calls", 0)
        add_hop(
            "4. Server ConnectRPC",
            "rpc_server_call (总计)",
            srv_rpc_agg["count"],
            srv_rpc_agg["total"],
            srv_rpc_agg["mean"],
            srv_rpc_agg["p50"],
            srv_rpc_agg["p99"],
            f"非 OK 状态 {srv_err} 次",
        )
        for r in srv_rpc_rows[:2]:
            add_hop(
                "4. Server ConnectRPC (Top)",
                f"rpc_server:{r['group']}",
                r["count"],
                r["total"],
                r["mean"],
                r["p50"],
                r["p99"],
            )

    # Hop 5: StoreActor Queue & Server Inode Lock Wait
    for comp_name, key in (
        ("StoreActor 入队等待 (actor_queue_wait)", "actor_queue_wait_ms"),
        ("Server Inode 锁等待 (inode_lock_wait)", "inode_lock_wait_ms"),
    ):
        agg = _aggregate_hist_rows(sp.get(key) or [])
        if agg and agg["count"] > 0:
            add_hop("5. Actor 队列/Inode 锁", comp_name, agg["count"], agg["total"], agg["mean"], agg["p50"], agg["p99"])

    # Hop 6: Engine Read KV & Write Phase A / Phase B
    read_op_rows = sp.get("read_op_latency_ms") or []
    read_op_agg = _aggregate_hist_rows(read_op_rows)
    if read_op_agg and read_op_agg["count"] > 0:
        top_rop = ", ".join(f"{r['group']}({r['count']})" for r in read_op_rows[:3])
        add_hop(
            "6. 引擎 KV / Phase A&B",
            "Store 只读操作 (read_op_duration)",
            read_op_agg["count"],
            read_op_agg["total"],
            read_op_agg["mean"],
            read_op_agg["p50"],
            read_op_agg["p99"],
            top_rop,
        )
    for kv_r in sp.get("kv_read_latency_ms") or []:
        add_hop(
            "6. 引擎 KV / Phase A&B",
            f"Pebble KV 读 ({kv_r['group']})",
            kv_r["count"],
            kv_r["total"],
            kv_r["mean"],
            kv_r["p50"],
            kv_r["p99"],
        )

    tick_by_phase = {r["group"]: r for r in (sp.get("actor_tick_phase_ms") or [])}
    for ph_key, ph_label in (
        ("phase_a_validate", "Actor Phase A 校验与延迟编码 (phase_a_validate)"),
        ("phase_b_mutate", "Actor Phase B 内存状态提交 (phase_b_mutate)"),
        ("wal_apply_nosync", "Pebble WAL 批写入 NoSync (wal_apply_nosync)"),
    ):
        if ph_key in tick_by_phase:
            r = tick_by_phase[ph_key]
            add_hop("6. 引擎 KV / Phase A&B", ph_label, r["count"], r["total"], r["mean"], r["p50"], r["p99"])

    # Hop 7: BlobStore pread / pwrite / RMW
    for comp_name, key in (
        ("BlobStore .pack pread (磁盘块读)", "blob_pread_ms"),
        ("BlobStore .pack pwrite (封包写)", "blob_pwrite_ms"),
        ("BlobStore RMW 补齐读 (blob_rmw_read)", "blob_rmw_read_ms"),
    ):
        agg = _aggregate_hist_rows(sp.get(key) or [])
        if agg and agg["count"] > 0:
            add_hop("7. BlobStore 数据面", comp_name, agg["count"], agg["total"], agg["mean"], agg["p50"], agg["p99"])

    # Hop 8: Persistence Barrier (Pebble WAL Sync / Pack Fsync / Fork Checkpoint)
    for ph_key, ph_label in (
        ("wal_apply_sync", "Pebble WAL 同步完整墙钟 (wal_apply_sync)"),
        ("pack_sync_barrier", "Pack 双层同步屏障 (pack_sync_barrier)"),
        ("checkpoint", "Fork Checkpoint 硬链接切面屏障 (checkpoint)"),
        ("dead_pack_remove", "已封口死包回收 (dead_pack_remove)"),
    ):
        if ph_key in tick_by_phase:
            r = tick_by_phase[ph_key]
            add_hop("8. 持久化 Fsync 屏障", ph_label, r["count"], r["total"], r["mean"], r["p50"], r["p99"])
    for fk in sp.get("control_fork_stage_ms") or []:
        add_hop(
            "9. 控制面 Fork 分阶段",
            f"Fork 阶段 ({fk['group']})",
            fk["count"],
            fk["total"],
            fk["mean"],
            fk["p50"],
            fk["p99"],
        )
    for bs in sp.get("blob_sync_ms") or []:
        add_hop(
            "8. 持久化 Fsync 屏障",
            f"BlobStore fdatasync ({bs['group']})",
            bs["count"],
            bs["total"],
            bs["mean"],
            bs["p50"],
            bs["p99"],
        )
    wal_fsync_agg = _aggregate_hist_rows(sp.get("pebble_wal_fsync_ms") or [])
    if wal_fsync_agg and wal_fsync_agg["count"] > 0:
        add_hop(
            "8. 持久化 Fsync 屏障",
            "Pebble 底层 WAL Fsync (pebble_wal_fsync)",
            wal_fsync_agg["count"],
            wal_fsync_agg["total"],
            wal_fsync_agg["mean"],
            wal_fsync_agg["p50"],
            wal_fsync_agg["p99"],
        )

    srv_cg = sp.get("counters_and_gauges") or {}
    return {
        "phase": phase_name,
        "parent_phase": None,
        "child_phases": [],
        "wall_clock_sec": round(wall_sec, 3),
        "wall_clock_ms": wall_ms,
        "fuse_active_ms": round(fuse_active_ms, 3),
        "fuse_active_concurrency": fuse_concurrency,
        "reference_basis": ref_basis,
        "reference_total_ms": round(ref_total_ms, 3),
        "rpc_reference_total_ms": round(rpc_ref_total_ms, 3),
        "async_rpc_to_fuse_ratio": async_rpc_ratio,
        "cache_and_pipeline": {
            "nscache_attr_hit_rate_pct": cs.get("nscache_attr_hit_rate_pct"),
            "nscache_entry_hit_rate_pct": cs.get("nscache_entry_hit_rate_pct"),
            "nscache_readlink_hit_rate_pct": cs.get("nscache_readlink_hit_rate_pct"),
            "write_behind_staged_mib": wb_cs.get("staged_mib", 0.0),
            "write_behind_passthrough_mib": wb_cs.get("passthrough_mib", 0.0),
            "write_behind_flush_rpcs": wb_cs.get("flush_rpcs", 0),
            "readahead_prefetched_mib": round(int(cs.get("readahead_prefetched_bytes", 0)) / (1024.0 * 1024.0), 2),
            "readahead_hit_mib": round(int(cs.get("readahead_hit_bytes", 0)) / (1024.0 * 1024.0), 2),
            "pebble_block_cache_hit_rate_pct": srv_cg.get("pebble_block_cache_hit_rate_pct"),
        },
        "rpc_hop_deltas": rpc_hop_delta,
        "hops": hops,
    }


STAGE_NAME_ALIASES: dict[str, str] = {
    "bowl_juicefs_bench": "jfs_bench",
    "bowl_fuse_perf_baseline": "fuse_perf_baseline",
}

KNOWN_PHASE_CHILDREN: dict[str, tuple[str, ...]] = {
    "d9_cold_io": ("d9_cold_seq_read", "d9_cold_small_write_fsync"),
}


def _annotate_phase_hierarchy(
    breakdowns: dict[str, Any],
    cli_phases: dict[str, Any],
) -> None:
    intervals: dict[str, tuple[float, float]] = {}
    for pname in breakdowns:
        cs = cli_phases.get(pname) or {}
        pre_e = cs.get("pre_epoch")
        post_e = cs.get("post_epoch")
        if pre_e is not None and post_e is not None and float(post_e) >= float(pre_e):
            intervals[pname] = (float(pre_e), float(post_e))

    children_map: dict[str, set[str]] = {p: set() for p in breakdowns}
    parent_map: dict[str, str | None] = {p: None for p in breakdowns}

    # 1. 基于精确采样时间窗 [pre_epoch, post_epoch] 自动识别最紧邻父子包含关系
    for child, (c_start, c_end) in intervals.items():
        c_span = c_end - c_start
        best_parent: str | None = None
        best_span = float("inf")
        for cand, (p_start, p_end) in intervals.items():
            if cand == child:
                continue
            p_span = p_end - p_start
            if p_start <= c_start + 1e-3 and c_end <= p_end + 1e-3 and p_span > c_span + 1e-3:
                if p_span < best_span:
                    best_span = p_span
                    best_parent = cand
        if best_parent is not None:
            parent_map[child] = best_parent
            children_map[best_parent].add(child)

    # 2. 对缺少 epoch 的合成快照补齐已知拓扑关系（如 d9_cold_io 及全量汇总 suite）
    for parent, known_kids in KNOWN_PHASE_CHILDREN.items():
        if parent in breakdowns:
            for kid in known_kids:
                if kid in breakdowns and parent_map[kid] is None:
                    parent_map[kid] = parent
                    children_map[parent].add(kid)
    if "suite" in breakdowns and len(breakdowns) > 1:
        for pname in breakdowns:
            if pname != "suite" and parent_map[pname] is None and not children_map[pname]:
                if pname not in intervals or "suite" not in intervals:
                    parent_map[pname] = "suite"
                    children_map["suite"].add(pname)

    for pname, bd in breakdowns.items():
        bd["parent_phase"] = parent_map[pname]
        bd["child_phases"] = sorted(children_map[pname])


def build_phase_hop_breakdowns(
    client_summary: dict[str, Any],
    prom_summary: dict[str, Any],
    stage_rows: list[dict[str, Any]],
    out_dir: Path,
) -> dict[str, Any]:
    cli_phases = (client_summary or {}).get("phase_deltas") or {}
    cli_prom_phases = ((prom_summary or {}).get("client_engine") or {}).get("phase_deltas") or {}
    srv_prom_phases = ((prom_summary or {}).get("server_engine") or {}).get("phase_deltas") or {}

    stage_sec_lookup: dict[str, float] = {}
    for r in stage_rows:
        try:
            dur = float(r.get("duration_sec", 0.0) or 0.0)
        except (ValueError, TypeError):
            continue
        for k in (r.get("stage"), r.get("directory"), r.get("suite")):
            if k and dur > 0:
                raw_k = str(k)
                stage_sec_lookup[raw_k] = dur
                canon_k = STAGE_NAME_ALIASES.get(raw_k, raw_k)
                stage_sec_lookup[canon_k] = dur

    all_phase_names = sorted(set(cli_phases.keys()) | set(cli_prom_phases.keys()) | set(srv_prom_phases.keys()))
    breakdowns: dict[str, Any] = {}
    for pname in all_phase_names:
        bd = build_single_phase_breakdown(
            pname,
            cli_phases.get(pname),
            cli_prom_phases.get(pname),
            srv_prom_phases.get(pname),
            stage_sec_lookup,
        )
        if bd["hops"]:
            breakdowns[pname] = bd

    if breakdowns:
        _annotate_phase_hierarchy(breakdowns, cli_phases)
        (out_dir / "phase_deltas.json").write_text(
            json.dumps(breakdowns, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    return breakdowns


# ==============================================================================
# 5. Loki, Client Structured Logs & Pebble Event Log Extraction
# ==============================================================================


def _is_alert_level(lvl: str) -> bool:
    return lvl.strip().upper() in {"WARN", "WARNING", "ERROR", "FATAL", "PANIC", "CRITICAL"}


def process_logs(stage_dir: Path, out_dir: Path) -> dict[str, Any]:
    loki_dst = out_dir / "loki"
    loki_dst.mkdir(parents=True, exist_ok=True)

    loki_json = load_json_safe(stage_dir / "server" / "loki" / "query_range.json")
    container_entries: list[dict[str, Any]] = []
    server_alerts: list[dict[str, Any]] = []

    if isinstance(loki_json, dict) and loki_json.get("status") == "success":
        for stream in (loki_json.get("data") or {}).get("result") or []:
            s_labels = stream.get("stream") or {}
            svc = str(s_labels.get("service") or s_labels.get("container") or "unknown")
            if svc.lower() in OBSERVABILITY_NOISE_SERVICES:
                continue
            for ts_ns, raw_line in stream.get("values") or []:
                rec: dict[str, Any] = {"ts_ns": ts_ns, "service": svc}
                stripped = raw_line.strip()
                parsed_dict = False
                if stripped.startswith("{") and stripped.endswith("}"):
                    try:
                        parsed = json.loads(stripped)
                        if isinstance(parsed, dict):
                            rec.update(parsed)
                            parsed_dict = True
                        else:
                            rec["msg"] = stripped
                    except Exception:
                        rec["msg"] = stripped
                else:
                    rec["msg"] = stripped
                container_entries.append(rec)
                lvl = str(rec.get("level", ""))
                if _is_alert_level(lvl):
                    server_alerts.append(rec)
                elif not lvl:
                    if parsed_dict:
                        ev_name = str(rec.get("event") or "").lower()
                        err_val = str(rec.get("err") or rec.get("error") or "").strip()
                        if err_val or (ev_name and any(k in ev_name for k in ("stall", "slow", "error"))):
                            server_alerts.append(rec)
                    elif CLIENT_RAW_ERR_RE.search(stripped):
                        server_alerts.append(rec)

    container_entries.sort(key=lambda x: str(x.get("ts_ns", "")))
    if container_entries:
        with (loki_dst / "container_logs.jsonl").open("w", encoding="utf-8") as f:
            for item in container_entries[:5000]:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

    client_alerts: list[dict[str, Any]] = []
    client_lifecycle: list[dict[str, Any]] = []
    cli_log_dir = stage_dir / "client" / "logs"
    if cli_log_dir.exists():
        # 优先读取卸载后守护进程完整刷盘退出的 bowl-mount.log；若仅有 phase 备份或某文件更大则选最完整者
        candidates = [cli_log_dir / n for n in ("bowl-mount.log", "bowl-mount-phase.log") if (cli_log_dir / n).exists()]
        log_cand = None
        if candidates:
            log_cand = max(candidates, key=lambda p: (p.stat().st_size, 1 if p.name == "bowl-mount.log" else 0))
        if log_cand is not None:
            for raw_line in log_cand.read_text(encoding="utf-8", errors="replace").splitlines():
                s = raw_line.strip()
                if not s:
                    continue
                if s.startswith("{") and s.endswith("}"):
                    try:
                        obj = json.loads(s)
                        if isinstance(obj, dict):
                            lvl = str(obj.get("level", "")).upper()
                            msg = str(obj.get("msg", ""))
                            if msg in ("mount_ready", "unmount_complete"):
                                client_lifecycle.append(obj)
                            if _is_alert_level(lvl):
                                client_alerts.append(obj)
                            continue
                    except Exception:
                        pass
                kv_m = CLIENT_KV_LEVEL_RE.search(s)
                if kv_m:
                    lvl_norm = "WARN" if kv_m.group("lvl").upper().startswith("WARN") else "ERROR"
                    client_alerts.append({"level": lvl_norm, "msg": s})
                elif CLIENT_RAW_ERR_RE.search(s):
                    client_alerts.append({"level": "ERROR", "msg": s})

    if client_alerts or client_lifecycle:
        with (loki_dst / "client_logs.jsonl").open("w", encoding="utf-8") as f:
            for item in client_lifecycle + client_alerts[:1000]:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

    return {
        "loki_log_entries": len(container_entries),
        "server_alert_count": len(server_alerts),
        "client_alert_count": len(client_alerts),
        "server_alerts_sample": server_alerts[:15],
        "client_alerts_sample": client_alerts[:15],
        "client_lifecycle": client_lifecycle,
    }


def process_pebble_logs(stage_dir: Path, out_dir: Path) -> dict[str, Any]:
    pebble_src = stage_dir / "server" / "pebble"
    if not pebble_src.exists():
        return {}
    files = [p for p in sorted(pebble_src.rglob("*")) if p.is_file() and p.stat().st_size > 0]
    if not files:
        return {}

    pebble_dst = out_dir / "pebble"
    pebble_dst.mkdir(parents=True, exist_ok=True)

    event_counts: dict[str, int] = {}
    alerts: list[dict[str, Any]] = []
    raw_log_files = 0
    event_log_files = 0

    for p in files:
        rel = p.relative_to(pebble_src)
        dst_p = pebble_dst / rel
        dst_p.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, dst_p)

        if p.name.startswith("pebble_events.log"):
            event_log_files += 1
            for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
                s = line.strip()
                if not s:
                    continue
                try:
                    ev = json.loads(s)
                except Exception:
                    continue
                if not isinstance(ev, dict):
                    continue
                ename = str(ev.get("event") or ev.get("type") or ev.get("msg") or "unknown")
                event_counts[ename] = event_counts.get(ename, 0) + 1
                if any(k in ename.lower() for k in ("stall", "slow", "error")):
                    alerts.append(ev)
        elif p.name.startswith("LOG"):
            raw_log_files += 1
            for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
                low = line.lower()
                if "flushing" in low or "flushed" in low:
                    event_counts["log_flush_lines"] = event_counts.get("log_flush_lines", 0) + 1
                elif "compacting" in low or "compacted" in low:
                    event_counts["log_compaction_lines"] = event_counts.get("log_compaction_lines", 0) + 1
                if "write stall" in low or "disk slow" in low or "background error" in low:
                    alerts.append({"file": str(rel), "msg": line.strip()})

    return {
        "raw_log_files": raw_log_files,
        "event_log_files": event_log_files,
        "event_counts": event_counts,
        "alert_count": len(alerts),
        "alerts_sample": alerts[:15],
    }


# ==============================================================================
# 6. pprof Top Hotspot Extraction (`-flat` & `-cum`) via `go tool pprof`
# ==============================================================================


def _parse_pprof_top_lines(out_text: str, limit: int = 5) -> list[str]:
    return [
        ln.strip()
        for ln in out_text.splitlines()
        if ln.strip()
        and not ln.startswith(("File:", "Build ID:", "Type:", "Time:", "Duration:", "Showing", "Dropped", "      flat"))
    ][:limit]


def process_pprof_profiles(
    stage_dir: Path,
    out_dir: Path,
    server_bin: Path | None,
    client_bin: Path | None,
    go_bin: Path | str | None = None,
) -> list[dict[str, Any]]:
    pprof_dst = out_dir / "pprof_top"
    resolved_go_bin = str(go_bin) if go_bin is not None else shutil.which("go")
    if not resolved_go_bin:
        return []

    extracted: list[dict[str, Any]] = []
    for side, bin_path in (("server", server_bin), ("client", client_bin)):
        src = stage_dir / side / "pprof"
        if not src.exists():
            continue
        for pb in sorted(src.glob("*.pb.gz")):
            if pb.stat().st_size == 0:
                continue

            def run_pprof(sort_flag: str) -> str:
                cmd = [resolved_go_bin, "tool", "pprof", "-top", sort_flag, "-nodecount=25"]
                if bin_path is not None and bin_path.exists():
                    cmd.append(str(bin_path))
                cmd.append(str(pb))
                try:
                    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=False)
                except Exception:
                    return ""
                return proc.stdout.strip() if proc.returncode == 0 else ""

            flat_text = run_pprof("-flat")
            cum_text = run_pprof("-cum")
            if not flat_text and not cum_text:
                continue

            pprof_dst.mkdir(parents=True, exist_ok=True)
            txt_name = f"{side}_{pb.name[:-len('.pb.gz')]}.txt"
            combined = []
            if flat_text:
                combined.append("=== Top Self Hotspots (-flat) ===\n" + flat_text)
            if cum_text:
                combined.append("=== Top Cumulative Hotspots (-cum) ===\n" + cum_text)
            (pprof_dst / txt_name).write_text("\n\n".join(combined) + "\n", encoding="utf-8")

            top_flat = _parse_pprof_top_lines(flat_text, 5)
            top_cum = _parse_pprof_top_lines(cum_text, 5)
            extracted.append({
                "side": side,
                "profile": pb.name,
                "report_file": f"pprof_top/{txt_name}",
                "top_flat_entries": top_flat,
                "top_cum_entries": top_cum,
                "top_entries": top_cum or top_flat,
            })
    return extracted


# ==============================================================================
# 7. Overview Markdown & Suite REPORT.md Rendering
# ==============================================================================


def load_stage_timings(results_dir: Path) -> list[dict[str, Any]]:
    timings_file = results_dir / "stage_timings.jsonl"
    if not timings_file.exists():
        return []
    dedup_by_label: dict[str, dict[str, Any]] = {}
    for line in timings_file.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        target = str(row.get("target") or "")
        if target and target != "bowl":
            continue
        raw_stage = str(row.get("stage") or row.get("directory") or row.get("suite") or "")
        if raw_stage in IGNORED_STAGE_NAMES or raw_stage.startswith(("drive9_default_", "drive9_sync_")):
            continue
        if raw_stage.startswith("jfs_") and raw_stage != "jfs_bench":
            continue
        if raw_stage == "drive9_fuse_bench_cold_and_hot":
            continue
        canon_stage = STAGE_NAME_ALIASES.get(raw_stage, raw_stage)
        norm_row = dict(row)
        if "stage" in norm_row and norm_row["stage"]:
            norm_row["stage"] = canon_stage
        try:
            dur = float(norm_row.get("duration_sec", 0.0) or 0.0)
        except (ValueError, TypeError):
            continue
        prev = dedup_by_label.get(canon_stage)
        if prev is None or dur > float(prev.get("duration_sec", 0.0) or 0.0):
            dedup_by_label[canon_stage] = norm_row
    rows = list(dedup_by_label.values())
    rows.sort(key=lambda r: float(r.get("duration_sec", 0.0)), reverse=True)
    return rows


def render_phase_deltas_markdown(phase_deltas: dict[str, Any], heading_prefix: str = "###") -> list[str]:
    if not phase_deltas:
        return []
    lines: list[str] = []
    for phase_name, bd in phase_deltas.items():
        hops = bd.get("hops") or []
        if not hops:
            continue
        wall_s = float(bd.get("wall_clock_sec", 0.0))
        wall_ms = float(bd.get("wall_clock_ms", 0.0))
        fuse_ms = float(bd.get("fuse_active_ms", 0.0))
        conc = bd.get("fuse_active_concurrency")
        conc_str = f"（等效并发度 `{conc:.2f}x`）" if conc is not None else ""
        cp = bd.get("cache_and_pipeline") or {}
        bc_str = "N/A (0 reads)" if cp.get("pebble_block_cache_hit_rate_pct") is None else f"{cp['pebble_block_cache_hit_rate_pct']:.2f}%"

        child_phases = bd.get("child_phases") or []
        parent_phase = bd.get("parent_phase")
        scope_tag = ""
        if child_phases:
            kids_str = ", ".join(f"`{k}`" for k in child_phases)
            scope_tag = f"（父汇总阶段 · 含子阶段 {kids_str}）"
        elif parent_phase:
            scope_tag = f"（子阶段 · 隶属 `{parent_phase}`）"

        ref_note = f"`{bd.get('reference_basis', 'FUSE 活跃总耗时')}` (`{bd.get('reference_total_ms', 0.0):.2f} ms`"
        rpc_ref_ms = float(bd.get("rpc_reference_total_ms") or 0.0)
        if bd.get("async_rpc_to_fuse_ratio") is not None and rpc_ref_ms > 0:
            ref_note += f" / RPC `{rpc_ref_ms:.2f} ms`"
        ref_note += ")"

        lines.extend([
            f"{heading_prefix} 工作负载 `{phase_name}` 全链路逐跳拆解{scope_tag}",
            "",
            f"- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `{wall_s:.3f} s` (`{wall_ms:.1f} ms`) vs FUSE 累计活跃 `{fuse_ms:.2f} ms`{conc_str}；占比基准：{ref_note}",
            f"- **缓存与流水线命中**：NSCache Attr `{fmt_pct(cp.get('nscache_attr_hit_rate_pct'))}` / Entry `{fmt_pct(cp.get('nscache_entry_hit_rate_pct'))}` / Readlink `{fmt_pct(cp.get('nscache_readlink_hit_rate_pct'))}` · "
            f"WriteBehind 聚批 `{cp.get('write_behind_staged_mib', 0.0):.2f} MiB` / 直通 `{cp.get('write_behind_passthrough_mib', 0.0):.2f} MiB` (Flush RPC `{cp.get('write_behind_flush_rpcs', 0)}`) · "
            f"ReadAhead 预读 `{cp.get('readahead_prefetched_mib', 0.0):.2f} MiB` / 命中 `{cp.get('readahead_hit_mib', 0.0):.2f} MiB` · Pebble BlockCache `{bc_str}`",
            "",
            "| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |",
            "| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |",
        ])
        for h in hops:
            sh = f"{h['share_pct']:.1f}%" if h.get("share_pct") is not None else "—"
            note = h.get("note") or "—"
            lines.append(
                f"| `{h['hop_stage']}` | `{h['component']}` | {h['calls']:,} | {h['total_ms']:.2f} | "
                f"{fmt_opt_ms(h.get('mean_ms'))} | {fmt_opt_ms(h.get('p50_ms'))} | {fmt_opt_ms(h.get('p99_ms'))} | {sh} | {note} |"
            )
        lines.append("")
    return lines


def render_overview_markdown(overview: dict[str, Any]) -> str:
    lines: list[str] = [
        f"# Bowl `{overview['suite']}` 生产可观测性与慢路径遥测诊断总览",
        "",
        f"- **部署栈**：`deploy/prod/docker-compose.yaml` + `deploy/prod/storage.env`（PostgreSQL 18 + Storage + Jaeger + Prometheus + Loki + Alloy）",
        f"- **原始归档包**：`telemetry-raw.tar.gz`（含 `.pb.gz` 火焰图原始采样、双端全量 `.prom` 快照、Pebble `LOG`/`pebble_events.log` 与 `diskstats`）",
        "",
    ]

    # 1. Slowest Bowl Stages
    stages = overview.get("slowest_stages") or []
    if stages:
        lines.extend([
            "## 1. 最慢 Bowl 测试阶段排序（Stage Timings）",
            "",
            "| 排名 | 阶段 / 目录 | 耗时 (s) |",
            "| :--- | :--- | ---: |",
        ])
        for idx, st in enumerate(stages[:12], 1):
            label = st.get("stage") or st.get("directory") or st.get("suite") or "unknown"
            lines.append(f"| {idx} | `{label}` | {float(st.get('duration_sec', 0.0)):.3f} |")
        lines.append("")

    # 2. Per-Workload End-to-End Hop-by-Hop Breakdown (`phase_deltas`)
    phase_deltas = overview.get("phase_deltas") or {}
    if phase_deltas:
        lines.extend([
            "## 2. 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）",
            "",
        ])
        lines.extend(render_phase_deltas_markdown(phase_deltas, heading_prefix="###"))

    # 3. Client FUSE Operations & 9 Subsystems
    cli = overview.get("client_stats") or {}
    if cli.get("top_ops_by_total_time"):
        nsc = cli.get("nscache") or {}
        ra = cli.get("read_ahead") or {}
        wb = cli.get("write_behind") or {}
        il = cli.get("inode_locks") or {}
        qs = cli.get("queues") or {}
        lines.extend([
            "## 3. 客户端 FUSE 累计耗时与 9 大子系统拆解（`bowl stats`）",
            "",
            f"- **累计调用**：`{cli.get('total_calls', 0):,}` 次（错误返回 `{cli.get('total_errors', 0):,}` 次），FUSE 累计活跃 `{cli.get('fuse_active_ms', 0.0):.2f} ms`，读 `{cli.get('read_mib', 0):.2f} MiB` / 写 `{cli.get('write_mib', 0):.2f} MiB`",
            f"- **NamespaceCache 命中率**：Attr `{fmt_pct(nsc.get('attr_hit_rate_pct'))}` · Entry `{fmt_pct(nsc.get('entry_hit_rate_pct'))}` · Readlink `{fmt_pct(nsc.get('readlink_hit_rate_pct'))}` · 负缓存命中 `{nsc.get('negative_entry_hits', 0):,}` · 失效 `{nsc.get('invalidations', 0):,}`",
            f"- **WriteBehind 流水线**：聚批写 `{wb.get('staged_mib', 0.0):.2f} MiB` (`{wb.get('staged_calls', 0):,}` 次) · 直通写 `{wb.get('passthrough_mib', 0.0):.2f} MiB` (`{wb.get('passthrough_calls', 0):,}` 次) · Flush RPC `{wb.get('flush_rpcs', 0):,}` (错误 `{wb.get('flush_errors', 0)}`) · 背压等待 `{wb.get('backpressure_waits', 0):,}` 次 (`{wb.get('backpressure_wait_ms', 0.0):.2f} ms`) · 屏障等待 `{wb.get('barrier_waits', 0):,}` 次 (`{wb.get('barrier_wait_ms', 0.0):.2f} ms`)",
            f"- **ReadAhead 预读**：触发 `{ra.get('triggers', 0):,}` 次 · 预读 `{ra.get('prefetched_mib', 0):.2f} MiB` · 命中 `{ra.get('hit_mib', 0):.2f} MiB` · 丢弃 `{ra.get('discarded_mib', 0):.2f} MiB` · 等待 `{ra.get('waits', 0):,}` 次 (`{ra.get('wait_ms', 0.0):.2f} ms`)",
            f"- **Inode 锁与内核队列**：快速获取 `{il.get('fast_acquisitions', 0):,}` · 竞争获取 `{il.get('contended_acquisitions', 0):,}`（累计等待 `{il.get('wait_ms', 0):.2f} ms`） · FUSE Waiting `{qs.get('fuse_waiting', 0)}` / CongestionThreshold `{qs.get('fuse_congestion_threshold', 0)}`",
            "",
            "| 操作 (`FuseOp`) | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) | 非零 Errno |",
            "| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |",
        ])
        for op in cli["top_ops_by_total_time"][:15]:
            errno_str = ", ".join(f"{k}:{v}" for k, v in (op.get("errnos") or {}).items()) or "—"
            lines.append(
                f"| `{op['op']}` | {op['calls']:,} | {op['total_duration_ms']:.2f} | {op['share_of_fuse_time_pct']:.1f}% | "
                f"{op['mean_us']:.1f} | {op['p50_us']:.1f} | {op['p95_us']:.1f} | {op['p99_us']:.1f} | {errno_str} |"
            )
        lines.append("")

    # 4. Server HTTP / ConnectRPC & Engine Internal Phases
    prom = overview.get("prometheus") or {}
    srv_eng = prom.get("server_engine") or {}
    cli_eng = prom.get("client_engine") or {}
    if srv_eng.get("rpc_server_latency_ms") or srv_eng.get("http_request_latency_ms"):
        rpc_st = srv_eng.get("rpc_server_status") or {}
        cli_st = cli_eng.get("rpc_client_status") or {}
        st_by_method = {m["method"]: m for m in (rpc_st.get("by_method") or [])}
        lines.extend([
            "## 4. 双端 ConnectRPC / HTTP 外层与存储引擎内部延迟拆解（Prometheus）",
            "",
            f"- **服务端 RPC 状态码拆分**：总调用 `{rpc_st.get('total_calls', 0):,}` 次 · 正常 (`OK`) `{rpc_st.get('ok_calls', 0):,}` 次 · 业务/协议非 OK `{rpc_st.get('error_calls', 0):,}` 次 (`{rpc_st.get('error_rate_pct', 0.0):.2f}%`，分布 `{json.dumps(rpc_st.get('errors_by_code') or {}, ensure_ascii=False)}`)",
            f"- **客户端 RPC 状态码拆分**：总调用 `{cli_st.get('total_calls', 0):,}` 次 · 正常 (`OK`) `{cli_st.get('ok_calls', 0):,}` 次 · 非 OK `{cli_st.get('error_calls', 0):,}` 次 (`{cli_st.get('error_rate_pct', 0.0):.2f}%`)",
            "",
            "### 4.1 服务端 ConnectRPC 与 HTTP 外层耗时 Top 榜",
            "",
            "| 方法 (`procedure`) | 总调用 | OK / 非 OK | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |",
            "| :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: |",
        ])
        for r in srv_eng.get("http_request_latency_ms") or []:
            lines.append(
                f"| `http_outer:{r['group']}` | {r['count']:,} | — | {r['total']:.2f} | {r['mean']:.3f} | {r['p50']:.3f} | {r['p95']:.3f} | {r['p99']:.3f} |"
            )
        for r in (srv_eng.get("rpc_server_latency_ms") or [])[:12]:
            st_m = st_by_method.get(r["group"])
            ok_err_str = f"{st_m['ok_calls']:,} / {st_m['error_calls']:,}" if st_m else f"{r['count']:,} / 0"
            lines.append(
                f"| `rpc:{r['group']}` | {r['count']:,} | {ok_err_str} | {r['total']:.2f} | {r['mean']:.3f} | {r['p50']:.3f} | {r['p95']:.3f} | {r['p99']:.3f} |"
            )
        lines.append("")

    if srv_eng.get("actor_tick_phase_ms") or srv_eng.get("read_op_latency_ms"):
        lines.extend([
            "### 4.2 `StoreActor` 读写路径、`BlobStore` 与持久化子阶段延迟",
            "",
            "| 引擎内部子阶段 | 次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |",
            "| :--- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ])
        for ph in srv_eng.get("actor_tick_phase_ms") or []:
            lines.append(
                f"| `actor_phase:{ph['group']}` | {ph['count']:,} | {ph['total']:.2f} | {ph['mean']:.3f} | {ph['p50']:.3f} | {ph['p95']:.3f} | {ph['p99']:.3f} |"
            )
        for rop in srv_eng.get("read_op_latency_ms") or []:
            lines.append(
                f"| `read_op:{rop['group']}` | {rop['count']:,} | {rop['total']:.2f} | {rop['mean']:.3f} | {rop['p50']:.3f} | {rop['p95']:.3f} | {rop['p99']:.3f} |"
            )
        for kv in srv_eng.get("kv_read_latency_ms") or []:
            lines.append(
                f"| `kv_read:{kv['group']}` | {kv['count']:,} | {kv['total']:.2f} | {kv['mean']:.3f} | {kv['p50']:.3f} | {kv['p95']:.3f} | {kv['p99']:.3f} |"
            )
        for label, key in (
            ("control_open_actor", "control_open_actor_ms"),
            ("actor_queue_wait", "actor_queue_wait_ms"),
            ("inode_lock_wait", "inode_lock_wait_ms"),
            ("blob_pread", "blob_pread_ms"),
            ("blob_pwrite", "blob_pwrite_ms"),
            ("blob_rmw_read", "blob_rmw_read_ms"),
            ("pebble_wal_fsync", "pebble_wal_fsync_ms"),
        ):
            for item in srv_eng.get(key) or []:
                g_suffix = f":{item['group']}" if item.get("group") and item["group"] != "all" else ""
                lines.append(
                    f"| `{label}{g_suffix}` | {item['count']:,} | {item['total']:.2f} | {item['mean']:.3f} | {item['p50']:.3f} | {item['p95']:.3f} | {item['p99']:.3f} |"
                )
        for item in srv_eng.get("blob_sync_ms") or []:
            lines.append(
                f"| `blob_sync:{item['group']}` | {item['count']:,} | {item['total']:.2f} | {item['mean']:.3f} | {item['p50']:.3f} | {item['p95']:.3f} | {item['p99']:.3f} |"
            )
        cg = srv_eng.get("counters_and_gauges") or {}
        lines.extend([
            "",
            f"- **Pebble 状态**：BlockCache 命中率 `{fmt_block_cache_hit_rate(cg)}` · ReadAmp `{cg.get('pebble_read_amp')}` · WriteStall `{cg.get('pebble_stall_events')}` · WAL 写入 `{cg.get('pebble_wal_bytes_written')}` B",
            f"- **BlobStore 读写分布**：写块字节 `{json.dumps(cg.get('blob_write_bytes_by_mode'), ensure_ascii=False)}` · 读源块计数 `{json.dumps(cg.get('blob_read_blocks_by_source'), ensure_ascii=False)}`",
            "",
        ])

    # 5. Jaeger Slow Traces
    jg = overview.get("jaeger") or {}
    if jg.get("slow_traces_top15"):
        lines.extend([
            "## 5. Jaeger 跨端慢链路 Top 15（端到端瀑布定位）",
            "",
            f"- 已采样链路：`{jg.get('sampled_traces_count', 0)}` 条（跨 Client+Server 全链路 `{jg.get('cross_node_traces_count', 0)}` 条，异常链路 `{jg.get('error_traces_count', 0)}` 条；完整明细见 `jaeger/slow_traces.json`）",
            "",
            "| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |",
            "| :--- | ---: | :--- | ---: | ---: | ---: | :--- |",
        ])
        for tr in jg["slow_traces_top15"]:
            top_spans = tr.get("top_spans") or []
            if top_spans:
                top_sub = top_spans[1] if len(top_spans) > 1 else top_spans[0]
                top_sub_str = f"`{top_sub['service']}:{top_sub['operation']}` ({top_sub['duration_ms']:.2f} ms)"
            else:
                top_sub_str = "—"
            c_rpc = fmt_opt_ms(tr.get("client_rpc_span_ms"), 2) if tr.get("client_rpc_span_ms") is not None else fmt_opt_ms(tr.get("client_max_span_ms"), 2)
            s_rpc = fmt_opt_ms(tr.get("server_rpc_span_ms"), 2) if tr.get("server_rpc_span_ms") is not None else fmt_opt_ms(tr.get("server_max_span_ms"), 2)
            ovh = fmt_opt_ms(tr.get("rpc_network_codec_ms"), 2)
            lines.append(
                f"| `{tr['trace_id'][:16]}` | {tr['total_duration_ms']:.2f} | `{tr['root_service']}:{tr['root_operation']}` | "
                f"{c_rpc} | {s_rpc} | {ovh} | "
                f"{top_sub_str} |"
            )
        lines.append("")

    # 6. Loki, Client Alerts & Pebble Events
    lg = overview.get("logs") or {}
    pb_lg = overview.get("pebble_logs") or {}
    lines.extend([
        "## 6. 结构化日志、Pebble 事件与异常审计（Loki + Client Log + Pebble）",
        "",
        f"- **Loki 业务容器日志条数**：`{lg.get('loki_log_entries', 0)}` · **服务端 Warn/Error 数**：`{lg.get('server_alert_count', 0)}` · **客户端 Warn/Error 数**：`{lg.get('client_alert_count', 0)}`（明细见 `loki/`）",
    ])
    if pb_lg:
        lines.append(
            f"- **Pebble 引擎日志**：`LOG` 文件 `{pb_lg.get('raw_log_files', 0)}` 个 · `pebble_events.log` `{pb_lg.get('event_log_files', 0)}` 个 · 告警事件 `{pb_lg.get('alert_count', 0)}` 条 · 事件统计 `{json.dumps(pb_lg.get('event_counts') or {}, ensure_ascii=False)}`"
        )
    lines.append("")

    # 7. pprof Hotspots (-flat & -cum)
    pprofs = overview.get("pprof_profiles") or []
    if pprofs:
        lines.extend([
            "## 7. 在线剖析热点索引（`pprof_top/`，同时提取 `-flat` 与 `-cum`）",
            "",
            "| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |",
            "| :--- | :--- | :--- | :--- | :--- |",
        ])
        for pp in pprofs:
            top_flat = (pp.get("top_flat_entries") or pp.get("top_entries") or ["—"])[0]
            top_cum = (pp.get("top_cum_entries") or pp.get("top_entries") or ["—"])[0]
            lines.append(f"| `{pp['side']}` | `{pp['profile']}` | `{top_flat}` | `{top_cum}` | `{pp['report_file']}` |")
        lines.append("")

    return "\n".join(lines)


def load_telemetry_overview(results_dir: Path) -> dict[str, Any]:
    """Load distilled results/telemetry/overview.json if present."""
    p = results_dir / "telemetry" / "overview.json"
    if not p.exists():
        return {}
    data = load_json_safe(p)
    return data if isinstance(data, dict) else {}


def render_report_telemetry_section(overview: dict[str, Any], heading: str) -> list[str]:
    """Render a comprehensive telemetry & per-workload hop-by-hop attribution section for suite REPORT.md."""
    if not overview:
        return []

    lines: list[str] = [
        "---",
        "",
        heading,
        "",
        f"- **生产可观测性编排栈**：`{overview.get('deploy_profile', 'deploy/prod')}`（完整诊断明细见 [`results/telemetry/overview.md`](results/telemetry/overview.md)，原始采样包见 `results/telemetry-raw.tar.gz`）",
    ]

    lg = overview.get("logs") or {}
    jg = overview.get("jaeger") or {}
    pb_lg = overview.get("pebble_logs") or {}
    pebble_note = (
        f" · Pebble 事件日志 `{pb_lg.get('event_log_files', 0)}` 份（告警 `{pb_lg.get('alert_count', 0)}` 条）"
        if pb_lg
        else ""
    )
    lines.append(
        f"- **遥测信号覆盖**：Jaeger 采样链路 `{jg.get('sampled_traces_count', 0)}` 条"
        f"（跨 Client+Server 全链路 `{jg.get('cross_node_traces_count', 0)}` 条，异常链路 `{jg.get('error_traces_count', 0)}` 条）· "
        f"Loki 业务日志 `{lg.get('loki_log_entries', 0)}` 条（服务端 Warn/Error `{lg.get('server_alert_count', 0)}` 条，客户端 Warn/Error `{lg.get('client_alert_count', 0)}` 条）{pebble_note}"
    )
    lines.append("")

    # 1. Per-Workload Hop-by-Hop Breakdown (`phase_deltas`) directly in REPORT.md
    phase_deltas = overview.get("phase_deltas") or {}
    if phase_deltas:
        lines.extend([
            "### 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）",
            "",
        ])
        lines.extend(render_phase_deltas_markdown(phase_deltas, heading_prefix="####"))

    # 2. Client Cumulative FUSE Ops & Subsystems
    cli = overview.get("client_stats") or {}
    if cli.get("top_ops_by_total_time"):
        nsc = cli.get("nscache") or {}
        ra = cli.get("read_ahead") or {}
        wb = cli.get("write_behind") or {}
        lines.extend([
            "### 客户端 FUSE 累计耗时 Top 操作与子系统统计（`bowl stats`）",
            "",
            f"NamespaceCache 命中率：Attr `{fmt_pct(nsc.get('attr_hit_rate_pct'))}` / Entry `{fmt_pct(nsc.get('entry_hit_rate_pct'))}` / Readlink `{fmt_pct(nsc.get('readlink_hit_rate_pct'))}`（负缓存命中 `{nsc.get('negative_entry_hits', 0):,}`）；"
            f"WriteBehind 聚批 `{wb.get('staged_mib', 0.0):.2f} MiB` / 直通 `{wb.get('passthrough_mib', 0.0):.2f} MiB`（Flush RPC `{wb.get('flush_rpcs', 0):,}`，背压等待 `{wb.get('backpressure_wait_ms', 0.0):.2f} ms`，屏障等待 `{wb.get('barrier_wait_ms', 0.0):.2f} ms`）；"
            f"ReadAhead 预读 `{ra.get('prefetched_mib', 0):.2f} MiB`（命中 `{ra.get('hit_mib', 0):.2f} MiB`，等待 `{ra.get('wait_ms', 0.0):.2f} ms`）。",
            "",
            "| FUSE 操作 | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) |",
            "| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ])
        for op in cli["top_ops_by_total_time"][:8]:
            lines.append(
                f"| `{op['op']}` | {op['calls']:,} | {op['total_duration_ms']:.2f} | {op['share_of_fuse_time_pct']:.1f}% | "
                f"{op['mean_us']:.1f} | {op.get('p50_us', 0.0):.1f} | {op['p95_us']:.1f} | {op['p99_us']:.1f} |"
            )
        lines.append("")

    # 3. Server ConnectRPC, HTTP Outer & Engine Phases
    srv_eng = (overview.get("prometheus") or {}).get("server_engine") or {}
    if srv_eng.get("rpc_server_latency_ms") or srv_eng.get("http_request_latency_ms"):
        rpc_st = srv_eng.get("rpc_server_status") or {}
        st_by_m = {m["method"]: m for m in (rpc_st.get("by_method") or [])}
        lines.extend([
            "### 服务端 HTTP / ConnectRPC 与存储引擎内部延迟（Prometheus）",
            "",
            "| RPC / 引擎内部阶段 | 调用次数 (OK/Err) | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |",
            "| :--- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ])
        for r in (srv_eng.get("http_request_latency_ms") or [])[:4]:
            lines.append(
                f"| `http_outer:{r['group']}` | {r['count']:,} | {r['total']:.2f} | {r['mean']:.3f} | {r.get('p50', 0.0):.3f} | {r['p95']:.3f} | {r['p99']:.3f} |"
            )
        for r in (srv_eng.get("rpc_server_latency_ms") or [])[:6]:
            sm = st_by_m.get(r["group"])
            cnt_label = f"{r['count']:,} ({sm['ok_calls']}/{sm['error_calls']})" if sm else f"{r['count']:,}"
            lines.append(
                f"| `rpc:{r['group']}` | {cnt_label} | {r['total']:.2f} | {r['mean']:.3f} | {r.get('p50', 0.0):.3f} | {r['p95']:.3f} | {r['p99']:.3f} |"
            )
        for ph in srv_eng.get("actor_tick_phase_ms") or []:
            lines.append(
                f"| `actor_phase:{ph['group']}` | {ph['count']:,} | {ph['total']:.2f} | {ph['mean']:.3f} | {ph.get('p50', 0.0):.3f} | {ph['p95']:.3f} | {ph['p99']:.3f} |"
            )
        for rop in (srv_eng.get("read_op_latency_ms") or [])[:4]:
            lines.append(
                f"| `read_op:{rop['group']}` | {rop['count']:,} | {rop['total']:.2f} | {rop['mean']:.3f} | {rop.get('p50', 0.0):.3f} | {rop['p95']:.3f} | {rop['p99']:.3f} |"
            )
        for kv in srv_eng.get("kv_read_latency_ms") or []:
            lines.append(
                f"| `kv_read:{kv['group']}` | {kv['count']:,} | {kv['total']:.2f} | {kv['mean']:.3f} | {kv.get('p50', 0.0):.3f} | {kv['p95']:.3f} | {kv['p99']:.3f} |"
            )
        for label, key in (("blob_pread", "blob_pread_ms"), ("blob_pwrite", "blob_pwrite_ms")):
            for item in srv_eng.get(key) or []:
                lines.append(
                    f"| `{label}` | {item['count']:,} | {item['total']:.2f} | {item['mean']:.3f} | {item.get('p50', 0.0):.3f} | {item['p95']:.3f} | {item['p99']:.3f} |"
                )
        cg = srv_eng.get("counters_and_gauges") or {}
        lines.extend([
            "",
            f"Pebble BlockCache 命中率 `{fmt_block_cache_hit_rate(cg)}` · ReadAmp `{cg.get('pebble_read_amp')}` · WriteStall `{cg.get('pebble_stall_events')}`。",
            "",
        ])

    if jg.get("slow_traces_top15"):
        lines.extend([
            "### Jaeger 端到端最慢采样链路 Top 5",
            "",
            "| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |",
            "| :--- | ---: | :--- | ---: | ---: | ---: | :--- |",
        ])
        for tr in jg["slow_traces_top15"][:5]:
            top_spans = tr.get("top_spans") or []
            if top_spans:
                top_sub = top_spans[1] if len(top_spans) > 1 else top_spans[0]
                top_sub_str = f"`{top_sub['service']}:{top_sub['operation']}` ({top_sub['duration_ms']:.2f} ms)"
            else:
                top_sub_str = "—"
            c_rpc = fmt_opt_ms(tr.get("client_rpc_span_ms"), 2) if tr.get("client_rpc_span_ms") is not None else fmt_opt_ms(tr.get("client_max_span_ms"), 2)
            s_rpc = fmt_opt_ms(tr.get("server_rpc_span_ms"), 2) if tr.get("server_rpc_span_ms") is not None else fmt_opt_ms(tr.get("server_max_span_ms"), 2)
            ovh = fmt_opt_ms(tr.get("rpc_network_codec_ms"), 2)
            lines.append(
                f"| `{tr['trace_id'][:16]}` | {tr['total_duration_ms']:.2f} | `{tr['root_service']}:{tr['root_operation']}` | "
                f"{c_rpc} | {s_rpc} | {ovh} | {top_sub_str} |"
            )
        lines.append("")

    pprofs = overview.get("pprof_profiles") or []
    if pprofs:
        lines.extend([
            "### `pprof` CPU / Heap / Mutex 采样热点索引（`-flat` 与 `-cum`）",
            "",
            "| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |",
            "| :--- | :--- | :--- | :--- | :--- |",
        ])
        for pp in pprofs:
            top_flat = (pp.get("top_flat_entries") or pp.get("top_entries") or ["—"])[0]
            top_cum = (pp.get("top_cum_entries") or pp.get("top_entries") or ["—"])[0]
            lines.append(
                f"| `{pp['side']}` | `{pp['profile']}` | `{top_flat}` | `{top_cum}` | [`{pp['report_file']}`](results/telemetry/{pp['report_file']}) |"
            )
        lines.append("")

    return lines


def build_telemetry_bundle(
    suite: str,
    stage_dir: Path,
    results_dir: Path,
    server_bin: Path | None = None,
    client_bin: Path | None = None,
    go_bin: Path | str | None = None,
) -> dict[str, Any]:
    telem_out = results_dir / "telemetry"
    if telem_out.exists():
        shutil.rmtree(telem_out)
    telem_out.mkdir(parents=True, exist_ok=True)

    client_summary = process_client_stats(stage_dir, telem_out)
    jaeger_summary = process_jaeger_traces(stage_dir, telem_out)
    prom_summary = process_prometheus_data(stage_dir, telem_out)
    logs_summary = process_logs(stage_dir, telem_out)
    pebble_summary = process_pebble_logs(stage_dir, telem_out)
    pprof_summary = process_pprof_profiles(stage_dir, telem_out, server_bin, client_bin, go_bin=go_bin)
    slowest_stages = load_stage_timings(results_dir)
    phase_breakdowns = build_phase_hop_breakdowns(client_summary, prom_summary, slowest_stages, telem_out)

    overview = {
        "suite": suite,
        "deploy_profile": "deploy/prod (docker-compose.yaml + storage.env)",
        "slowest_stages": slowest_stages[:15],
        "phase_deltas": phase_breakdowns,
        "client_stats": client_summary,
        "jaeger": jaeger_summary,
        "prometheus": prom_summary,
        "logs": logs_summary,
        "pebble_logs": pebble_summary,
        "pprof_profiles": pprof_summary,
    }

    (telem_out / "overview.json").write_text(
        json.dumps(overview, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (telem_out / "overview.md").write_text(render_overview_markdown(overview), encoding="utf-8")
    return overview


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, help="Benchmark suite name (juicefs, drive9, pjdfstest, fork)")
    parser.add_argument("--stage-dir", required=True, type=Path, help="Staging directory containing client/ and server/")
    parser.add_argument("--results-dir", required=True, type=Path, help="Benchmark results directory")
    parser.add_argument("--server-bin", type=Path, default=None, help="Optional storage-service binary for pprof")
    parser.add_argument("--client-bin", type=Path, default=None, help="Optional bowl CLI binary for pprof")
    args = parser.parse_args()

    build_telemetry_bundle(
        suite=args.suite,
        stage_dir=args.stage_dir,
        results_dir=args.results_dir,
        server_bin=args.server_bin,
        client_bin=args.client_bin,
    )


if __name__ == "__main__":
    main()
