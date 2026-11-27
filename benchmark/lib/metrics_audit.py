"""Prometheus metrics scraping, counter delta math, and anti-cache-leak auditing.

Parses only verified HEAD 68d787b Bowl server and client metrics plus Linux
cgroup v2 io.stat counters.
"""

from __future__ import annotations

import json
import math
import re
import urllib.error
import urllib.request
from typing import Any

ALLOWED_SERVER_METRICS: frozenset[str] = frozenset(
    {
        "bowl_storage_http_request_duration_seconds",
        "bowl_storage_http_request_bytes_total",
        "bowl_storage_http_response_bytes_total",
        "bowl_storage_disks_loaded",
        "bowl_storage_logical_bytes",
        "bowl_storage_pack_logical_bytes",
        "bowl_storage_pack_physical_bytes",
        "bowl_storage_inodes",
        "bowl_storage_pebble_wal_bytes_in_total",
        "bowl_storage_pebble_wal_bytes_written_total",
        "bowl_storage_pebble_l0_sublevels",
        "bowl_storage_pebble_l0_tables",
        "bowl_storage_pebble_read_amp",
        "bowl_storage_pebble_compaction_debt_bytes",
        "bowl_storage_pebble_compactions_in_progress",
        "bowl_storage_pebble_flushes_in_progress",
        "bowl_storage_pebble_filter_hits_total",
        "bowl_storage_pebble_filter_misses_total",
        "bowl_storage_pebble_open_table_iters",
        "bowl_storage_pebble_block_cache_bytes",
        "bowl_storage_pebble_block_cache_count",
        "bowl_storage_pebble_block_cache_hits_total",
        "bowl_storage_pebble_block_cache_misses_total",
    }
)

ALLOWED_CLIENT_METRICS: frozenset[str] = frozenset(
    {
        "bowl_client_fuse_op_duration_seconds",
        "bowl_client_fuse_ops_total",
        "bowl_client_fuse_inflight_ops",
        "bowl_client_fuse_op_errors_total",
        "bowl_client_fuse_io_bytes_total",
        "bowl_client_capcache_hits_total",
        "bowl_client_capcache_misses_total",
        "bowl_client_capcache_grants_total",
        "bowl_client_capcache_epoch_invalidations_total",
        "bowl_client_capcache_entries",
        "bowl_client_readahead_triggers_total",
        "bowl_client_readahead_prefetched_bytes_total",
        "bowl_client_readahead_hit_bytes_total",
        "bowl_client_readahead_discarded_bytes_total",
        "bowl_client_readahead_inflight",
        "bowl_client_read_ahead_wait_duration_seconds",
        "bowl_client_stage_writes_total",
        "bowl_client_stage_bytes_total",
        "bowl_client_stage_upload_rpcs_total",
        "bowl_client_stage_inflight",
        "bowl_client_stage_pending_chunks",
        "bowl_client_stage_used_bytes",
        "bowl_client_stage_capacity_bytes",
        "bowl_client_stage_backpressure_wait_duration_seconds",
        "bowl_client_stage_barrier_wait_duration_seconds",
        "bowl_client_tmpfile_active_files",
        "bowl_client_tmpfile_resident_bytes",
        "bowl_client_tmpfile_promotes_total",
        "bowl_client_open_handles",
        "bowl_client_inodelock_acquisitions_total",
        "bowl_client_inodelock_wait_seconds_total",
        "bowl_client_inodelock_wait_duration_seconds",
        "bowl_client_inodelock_active",
        "bowl_client_queue_depth",
        "bowl_client_queue_ops_total",
        "bowl_client_fuse_connection_waiting",
        "bowl_client_fuse_connection_congestion_threshold",
        "bowl_client_rpc_retries_total",
        "bowl_client_rpc_retry_exhausted_total",
        "bowl_client_stale_epoch_total",
        "bowl_client_rpc_logical_duration_seconds",
    }
)

ALL_ALLOWED_METRICS: frozenset[str] = ALLOWED_SERVER_METRICS | ALLOWED_CLIENT_METRICS

_SAMPLE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?|[+-]?Inf|NaN)(?:\s+\S+)?$"
)
_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"')

_HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count")


def _base_metric_name(name: str) -> str:
    for suffix in _HISTOGRAM_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def _unescape_label(raw: str) -> str:
    return raw.replace('\\"', '"').replace("\\n", "\n").replace("\\\\", "\\")


def parse_prometheus_text(
    text: str,
    *,
    allowed_names: frozenset[str] | set[str] | None = ALL_ALLOWED_METRICS,
) -> dict[str, list[dict[str, Any]]]:
    """Parse Prometheus/OpenMetrics text exposition into {metric_name: [{labels, value}]}.

    Metrics outside allowed_names (including removed legacy metrics) are ignored.
    """
    result: dict[str, list[dict[str, Any]]] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _SAMPLE_RE.match(line)
        if not match:
            continue
        name, labels_raw, value_raw = match.groups()
        if allowed_names is not None:
            if name not in allowed_names and _base_metric_name(name) not in allowed_names:
                continue
        try:
            val = float(value_raw)
        except ValueError:
            continue
        if math.isnan(val):
            continue
        labels: dict[str, str] = {}
        if labels_raw:
            for k, v in _LABEL_RE.findall(labels_raw):
                labels[k] = _unescape_label(v)
        result.setdefault(name, []).append({"labels": labels, "value": val})
    return result


def parse_cgroup_io_stat(text: str) -> dict[str, int]:
    """Parse Linux cgroup v2 io.stat text or a plain integer rbytes string."""
    totals = {"rbytes": 0, "wbytes": 0, "rios": 0, "wios": 0}
    stripped = text.strip()
    if not stripped:
        return totals
    if stripped.isdigit():
        totals["rbytes"] = int(stripped)
        return totals
    for line in stripped.splitlines():
        for token in line.split():
            if "=" not in token:
                continue
            key, val = token.split("=", 1)
            if key in totals and val.isdigit():
                totals[key] += int(val)
    return totals


def scrape_http_text(url: str, timeout_s: float = 5.0) -> tuple[int, str]:
    """Perform an HTTP GET and return (status_code, body_text)."""
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = getattr(resp, "status", 200)
            return int(status), resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        return int(exc.code), body
    except Exception:
        return 0, ""


def scrape_http_json(url: str, timeout_s: float = 5.0) -> tuple[int, dict | None]:
    """Perform an HTTP GET expecting JSON and return (status_code, parsed_dict)."""
    status, text = scrape_http_text(url, timeout_s=timeout_s)
    if status != 200 or not text:
        return status, None
    try:
        data = json.loads(text)
        return status, data if isinstance(data, dict) else {"value": data}
    except json.JSONDecodeError:
        return status, None


def check_readyz(base_url: str, timeout_s: float = 3.0) -> tuple[bool, int]:
    """Check GET <base_url>/readyz and return (ok, status_code)."""
    url = base_url.rstrip("/") + "/readyz"
    status, _ = scrape_http_text(url, timeout_s=timeout_s)
    return status == 200, status


def extract_metric_sum(
    parsed: dict[str, list[dict[str, Any]]],
    name: str,
    label_filter: dict[str, str] | None = None,
) -> float:
    """Sum sample values for metric name matching all key=value entries in label_filter."""
    samples = parsed.get(name, [])
    total = 0.0
    for sample in samples:
        labels = sample.get("labels", {})
        if label_filter and any(labels.get(k) != v for k, v in label_filter.items()):
            continue
        total += float(sample.get("value", 0.0))
    return total


def extract_metric_by_label(
    parsed: dict[str, list[dict[str, Any]]],
    name: str,
    label_key: str,
) -> dict[str, float]:
    """Group and sum metric samples by a single label key."""
    grouped: dict[str, float] = {}
    for sample in parsed.get(name, []):
        k = sample.get("labels", {}).get(label_key, "")
        grouped[k] = grouped.get(k, 0.0) + float(sample.get("value", 0.0))
    return grouped


def extract_histogram_delta(
    before: dict[str, list[dict[str, Any]]],
    after: dict[str, list[dict[str, Any]]],
    base_name: str,
    label_filter: dict[str, str] | None = None,
) -> dict[str, float | int | None]:
    """Compute count, sum_s, mean_ms, and approximate bucket percentiles for a histogram."""
    count_before = extract_metric_sum(before, f"{base_name}_count", label_filter)
    count_after = extract_metric_sum(after, f"{base_name}_count", label_filter)
    sum_before = extract_metric_sum(before, f"{base_name}_sum", label_filter)
    sum_after = extract_metric_sum(after, f"{base_name}_sum", label_filter)

    delta_count = max(0, int(round(count_after - count_before)))
    delta_sum = max(0.0, sum_after - sum_before)

    def _collect_buckets(src: dict[str, list[dict[str, Any]]]) -> dict[float, float]:
        buckets: dict[float, float] = {}
        for sample in src.get(f"{base_name}_bucket", []):
            labels = sample.get("labels", {})
            if label_filter and any(labels.get(k) != v for k, v in label_filter.items()):
                continue
            le_raw = labels.get("le")
            if le_raw is None:
                continue
            le_val = math.inf if le_raw in ("+Inf", "Inf") else float(le_raw)
            buckets[le_val] = buckets.get(le_val, 0.0) + float(sample.get("value", 0.0))
        return buckets

    b_before = _collect_buckets(before)
    b_after = _collect_buckets(after)
    all_les = sorted(set(b_before.keys()) | set(b_after.keys()))
    delta_buckets = [(le, max(0.0, b_after.get(le, 0.0) - b_before.get(le, 0.0))) for le in all_les]

    def _hist_quantile_ms(q: float, min_n: int) -> float | None:
        if delta_count < min_n or not delta_buckets:
            return None
        target = q * delta_count
        prev_le = 0.0
        prev_cnt = 0.0
        for le, cnt in delta_buckets:
            if cnt >= target:
                if math.isinf(le):
                    return round(prev_le * 1000.0, 4)
                bucket_cnt = cnt - prev_cnt
                if bucket_cnt <= 0:
                    return round(le * 1000.0, 4)
                frac = (target - prev_cnt) / bucket_cnt
                est_s = prev_le + frac * (le - prev_le)
                return round(est_s * 1000.0, 4)
            if not math.isinf(le):
                prev_le = le
            prev_cnt = cnt
        return None

    mean_ms = round((delta_sum / delta_count) * 1000.0, 4) if delta_count > 0 else 0.0
    return {
        "count": delta_count,
        "sum_s": round(delta_sum, 6),
        "mean_ms": mean_ms,
        "p50_ms": _hist_quantile_ms(0.50, 1),
        "p95_ms": _hist_quantile_ms(0.95, 20),
        "p99_ms": _hist_quantile_ms(0.99, 100),
    }


def _counter_delta(
    before: dict[str, list[dict[str, Any]]],
    after: dict[str, list[dict[str, Any]]],
    name: str,
    label_filter: dict[str, str] | None = None,
) -> float:
    return max(0.0, extract_metric_sum(after, name, label_filter) - extract_metric_sum(before, name, label_filter))


def _server_logical_bytes(parsed: dict[str, list[dict[str, Any]]]) -> float:
    if "bowl_storage_logical_bytes" in parsed:
        return extract_metric_sum(parsed, "bowl_storage_logical_bytes")
    return extract_metric_sum(parsed, "bowl_storage_pack_logical_bytes")


def compute_snapshot_deltas(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Compute counter deltas and post-run gauge values from before/after snapshots."""
    s_before = before.get("server_metrics", {}) or {}
    s_after = after.get("server_metrics", {}) or {}
    c_before = before.get("client_metrics", {}) or {}
    c_after = after.get("client_metrics", {}) or {}

    fuse_ops_before = extract_metric_by_label(c_before, "bowl_client_fuse_ops_total", "op")
    fuse_ops_after = extract_metric_by_label(c_after, "bowl_client_fuse_ops_total", "op")
    fuse_ops_delta = {
        op: int(round(max(0.0, fuse_ops_after.get(op, 0.0) - fuse_ops_before.get(op, 0.0))))
        for op in sorted(set(fuse_ops_before.keys()) | set(fuse_ops_after.keys()))
    }

    fuse_err_before = extract_metric_by_label(c_before, "bowl_client_fuse_op_errors_total", "errno")
    fuse_err_after = extract_metric_by_label(c_after, "bowl_client_fuse_op_errors_total", "errno")
    fuse_err_delta = {
        err: int(round(max(0.0, fuse_err_after.get(err, 0.0) - fuse_err_before.get(err, 0.0))))
        for err in sorted(set(fuse_err_before.keys()) | set(fuse_err_after.keys()))
        if fuse_err_after.get(err, 0.0) - fuse_err_before.get(err, 0.0) > 0
    }

    pack_phys_before = extract_metric_sum(s_before, "bowl_storage_pack_physical_bytes")
    pack_phys_after = extract_metric_sum(s_after, "bowl_storage_pack_physical_bytes")
    logical_before = _server_logical_bytes(s_before)
    logical_after = _server_logical_bytes(s_after)
    space_amp = round(pack_phys_after / logical_after, 4) if logical_after > 0 else None

    rbytes_before = before.get("server_cgroup_rbytes")
    rbytes_after = after.get("server_cgroup_rbytes")
    if rbytes_before is not None and rbytes_after is not None:
        nvme_rbytes_delta: int | None = max(0, int(rbytes_after) - int(rbytes_before))
    else:
        nvme_rbytes_delta = None

    ra_hits = int(round(_counter_delta(c_before, c_after, "bowl_client_readahead_hit_bytes_total")))
    ra_discards = int(round(_counter_delta(c_before, c_after, "bowl_client_readahead_discarded_bytes_total")))
    ra_total = ra_hits + ra_discards
    ra_hit_pct = round((ra_hits / ra_total) * 100.0, 2) if ra_total > 0 else None
    ra_discard_pct = round((ra_discards / ra_total) * 100.0, 2) if ra_total > 0 else None

    return {
        "client_fuse_read_bytes": int(
            round(_counter_delta(c_before, c_after, "bowl_client_fuse_io_bytes_total", {"dir": "read"}))
        ),
        "client_fuse_write_bytes": int(
            round(_counter_delta(c_before, c_after, "bowl_client_fuse_io_bytes_total", {"dir": "write"}))
        ),
        "client_fuse_lookup_ops": fuse_ops_delta.get("Lookup", 0),
        "client_fuse_getattr_ops": fuse_ops_delta.get("GetAttr", 0),
        "client_fuse_readdirplus_ops": fuse_ops_delta.get("ReadDirPlus", 0),
        "client_fuse_read_ops": fuse_ops_delta.get("Read", 0),
        "client_fuse_write_ops": fuse_ops_delta.get("Write", 0),
        "client_fuse_ops_by_op": fuse_ops_delta,
        "client_fuse_errors_by_errno": fuse_err_delta,
        "client_readahead_hit_bytes": ra_hits,
        "client_readahead_discarded_bytes": ra_discards,
        "client_readahead_hit_pct": ra_hit_pct,
        "client_readahead_discard_pct": ra_discard_pct,
        "client_readahead_wait": extract_histogram_delta(
            c_before, c_after, "bowl_client_read_ahead_wait_duration_seconds"
        ),
        "client_stage_backpressure_wait": extract_histogram_delta(
            c_before, c_after, "bowl_client_stage_backpressure_wait_duration_seconds"
        ),
        "client_stage_barrier_wait": extract_histogram_delta(
            c_before, c_after, "bowl_client_stage_barrier_wait_duration_seconds"
        ),
        "client_stage_upload_ok": int(
            round(_counter_delta(c_before, c_after, "bowl_client_stage_upload_rpcs_total", {"result": "ok"}))
        ),
        "client_stage_upload_errors": int(
            round(_counter_delta(c_before, c_after, "bowl_client_stage_upload_rpcs_total", {"result": "error"}))
        ),
        "client_rpc_retries": int(round(_counter_delta(c_before, c_after, "bowl_client_rpc_retries_total"))),
        "client_rpc_retry_exhausted": int(
            round(_counter_delta(c_before, c_after, "bowl_client_rpc_retry_exhausted_total"))
        ),
        "client_stale_epoch": int(round(_counter_delta(c_before, c_after, "bowl_client_stale_epoch_total"))),
        "client_tmpfile_promotes": int(round(_counter_delta(c_before, c_after, "bowl_client_tmpfile_promotes_total"))),
        "client_fuse_connection_waiting_after": int(
            round(extract_metric_sum(c_after, "bowl_client_fuse_connection_waiting"))
        ),
        "server_http_read_response_bytes": int(
            round(
                _counter_delta(
                    s_before,
                    s_after,
                    "bowl_storage_http_response_bytes_total",
                    {"procedure": "/storage.v1.StorageService/Read"},
                )
            )
        ),
        "server_http_write_request_bytes": int(
            round(
                _counter_delta(
                    s_before,
                    s_after,
                    "bowl_storage_http_request_bytes_total",
                    {"procedure": "/storage.v1.StorageService/Write"},
                )
            )
        ),
        "server_pebble_wal_bytes_written": int(
            round(_counter_delta(s_before, s_after, "bowl_storage_pebble_wal_bytes_written_total"))
        ),
        "server_pebble_block_cache_hits": int(
            round(_counter_delta(s_before, s_after, "bowl_storage_pebble_block_cache_hits_total"))
        ),
        "server_pebble_block_cache_misses": int(
            round(_counter_delta(s_before, s_after, "bowl_storage_pebble_block_cache_misses_total"))
        ),
        "server_pebble_l0_sublevels_after": int(
            round(extract_metric_sum(s_after, "bowl_storage_pebble_l0_sublevels"))
        ),
        "server_pebble_compaction_debt_bytes_after": int(
            round(extract_metric_sum(s_after, "bowl_storage_pebble_compaction_debt_bytes"))
        ),
        "server_pebble_read_amp_after": int(round(extract_metric_sum(s_after, "bowl_storage_pebble_read_amp"))),
        "server_pack_physical_bytes_after": int(round(pack_phys_after)),
        "server_pack_physical_bytes_delta": int(round(pack_phys_after - pack_phys_before)),
        "server_logical_bytes_after": int(round(logical_after)),
        "server_logical_bytes_delta": int(round(logical_after - logical_before)),
        "server_space_amplification": space_amp,
        "server_nvme_rbytes_delta": nvme_rbytes_delta,
        "server_readyz_ok": after.get("server_readyz_ok"),
        "client_readyz_ok": after.get("client_readyz_ok"),
    }


def _is_bowl_target(target: str) -> bool:
    return target.startswith("bowl")


def _is_local_ceiling(target: str) -> bool:
    return target in ("ext4", "passthrough_ll")


def audit_case(
    before: dict[str, Any],
    after: dict[str, Any],
    *,
    target: str,
    cache_state: str = "C0",
    expected_read_bytes: int = 0,
    expected_stat_ops: int = 0,
    min_fuse_ratio: float = 0.95,
    min_nvme_ratio: float = 0.90,
    require_server_nvme: bool = False,
) -> dict[str, Any]:
    """Compute counter deltas and enforce fairness, health, and cache-bypass gates."""
    deltas = compute_snapshot_deltas(before, after)
    reasons: list[str] = []
    checks: dict[str, bool | None] = {
        "zero_silent_failures": True,
        "client_fuse_bypass": None,
        "server_nvme_bypass": None,
    }

    is_bowl = _is_bowl_target(target)
    is_local = _is_local_ceiling(target)

    if is_bowl:
        if deltas["client_rpc_retry_exhausted"] > 0:
            checks["zero_silent_failures"] = False
            reasons.append(
                f"INVALID_RUN: bowl_client_rpc_retry_exhausted_total delta={deltas['client_rpc_retry_exhausted']}"
            )
        if deltas["client_stale_epoch"] > 0:
            checks["zero_silent_failures"] = False
            reasons.append(f"INVALID_RUN: bowl_client_stale_epoch_total delta={deltas['client_stale_epoch']}")
        if deltas["client_stage_upload_errors"] > 0:
            checks["zero_silent_failures"] = False
            reasons.append(
                f"INVALID_RUN: bowl_client_stage_upload_rpcs_total(error) delta={deltas['client_stage_upload_errors']}"
            )
        if deltas["server_readyz_ok"] is False:
            checks["zero_silent_failures"] = False
            reasons.append("INVALID_RUN: server /readyz returned non-200")
        if deltas["client_readyz_ok"] is False:
            checks["zero_silent_failures"] = False
            reasons.append("INVALID_RUN: client /readyz returned non-200")

    cold_client = cache_state in ("C1", "C2", "C3", "C3-WarmServer", "C3-ColdServer")
    cold_server = cache_state in ("C2", "C3-ColdServer")

    if is_bowl and cold_client and expected_read_bytes > 0:
        fuse_ratio = deltas["client_fuse_read_bytes"] / float(expected_read_bytes)
        deltas["client_fuse_read_ratio"] = round(fuse_ratio, 4)
        if fuse_ratio < min_fuse_ratio:
            checks["client_fuse_bypass"] = False
            reasons.append(
                f"INVALID_CACHE_LEAK: client FUSE read ratio {fuse_ratio:.4f} < {min_fuse_ratio:.2f} "
                f"({deltas['client_fuse_read_bytes']}/{expected_read_bytes} bytes)"
            )
        else:
            checks["client_fuse_bypass"] = True

    if is_bowl and cache_state in ("C1", "C2") and expected_stat_ops > 0:
        stat_ops = deltas["client_fuse_lookup_ops"] + deltas["client_fuse_getattr_ops"]
        stat_ratio = stat_ops / float(expected_stat_ops)
        deltas["client_fuse_stat_ratio"] = round(stat_ratio, 4)
        if stat_ratio < min_fuse_ratio:
            checks["client_fuse_bypass"] = False
            reasons.append(
                f"INVALID_CACHE_LEAK: client FUSE Lookup+GetAttr ratio {stat_ratio:.4f} < {min_fuse_ratio:.2f} "
                f"({stat_ops}/{expected_stat_ops} ops)"
            )
        elif checks["client_fuse_bypass"] is None:
            checks["client_fuse_bypass"] = True

    if is_bowl and cold_server and expected_read_bytes > 0:
        srv_ratio = deltas["server_http_read_response_bytes"] / float(expected_read_bytes)
        deltas["server_read_rpc_ratio"] = round(srv_ratio, 4)
        if srv_ratio < min_fuse_ratio:
            checks["server_nvme_bypass"] = False
            reasons.append(
                f"INVALID_SERVER_CACHE_LEAK: server Read HTTP response ratio {srv_ratio:.4f} < {min_fuse_ratio:.2f} "
                f"({deltas['server_http_read_response_bytes']}/{expected_read_bytes} bytes)"
            )

    if not is_local and cold_server and expected_read_bytes > 0:
        nvme_delta = deltas["server_nvme_rbytes_delta"]
        if nvme_delta is None:
            if require_server_nvme:
                checks["server_nvme_bypass"] = False
                reasons.append("INVALID_SERVER_CACHE_LEAK: server cgroup io.stat rbytes unavailable")
        else:
            nvme_ratio = nvme_delta / float(expected_read_bytes)
            deltas["server_nvme_read_ratio"] = round(nvme_ratio, 4)
            if nvme_ratio < min_nvme_ratio:
                checks["server_nvme_bypass"] = False
                reasons.append(
                    f"INVALID_SERVER_CACHE_LEAK: server NVMe cgroup rbytes ratio {nvme_ratio:.4f} < {min_nvme_ratio:.2f} "
                    f"({nvme_delta}/{expected_read_bytes} bytes)"
                )
            elif checks["server_nvme_bypass"] is None:
                checks["server_nvme_bypass"] = True

    validity_status = "INVALID" if reasons else "PASS"
    return {
        "deltas": deltas,
        "validity_status": validity_status,
        "reasons": reasons,
        "checks": checks,
    }
