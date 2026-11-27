"""Statistical aggregation and percentile sample-floor enforcement."""

from __future__ import annotations

import math
from typing import Sequence

MIN_SAMPLES_P95 = 20
MIN_SAMPLES_P99 = 100
MIN_SAMPLES_P999 = 1000

CV_THRESHOLD_STEADY = 15.0
CV_THRESHOLD_DISCRETE = 25.0


def min_samples_for_percentile(q: float) -> int:
    """Return the minimum independent sample count required for percentile q."""
    if q >= 99.9:
        return MIN_SAMPLES_P999
    if q >= 99.0:
        return MIN_SAMPLES_P99
    if q >= 95.0:
        return MIN_SAMPLES_P95
    return 1


def percentile(
    values: Sequence[float],
    q: float,
    *,
    enforce_min_samples: bool = True,
) -> float | None:
    """Compute linear-interpolated percentile q in [0, 100].

    Returns None when values is empty or when enforce_min_samples is True and
    len(values) falls below the required floor (P95>=20, P99>=100, P99.9>=1000).
    """
    if q < 0.0 or q > 100.0:
        raise ValueError(f"percentile q must be in [0, 100], got {q}")
    clean = sorted(float(v) for v in values if v is not None and not math.isnan(float(v)))
    n = len(clean)
    if n == 0:
        return None
    if enforce_min_samples and n < min_samples_for_percentile(q):
        return None
    if n == 1:
        return clean[0]
    pos = (q / 100.0) * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return clean[lo]
    frac = pos - lo
    return clean[lo] * (1.0 - frac) + clean[hi] * frac


def check_variance(cv_pct: float, *, kind: str = "steady") -> tuple[bool, str | None]:
    """Check whether coefficient of variation exceeds the charter threshold."""
    if kind not in ("steady", "discrete"):
        raise ValueError(f"kind must be 'steady' or 'discrete', got {kind!r}")
    threshold = CV_THRESHOLD_STEADY if kind == "steady" else CV_THRESHOLD_DISCRETE
    if cv_pct > threshold:
        return True, f"HIGH_VARIANCE: cv_pct={cv_pct:.2f}% exceeds {kind} threshold {threshold:.0f}%"
    return False, None


def summarize_series(
    values: Sequence[float],
    *,
    kind: str = "discrete",
    include_percentiles: bool = True,
) -> dict:
    """Summarize numeric samples with median, min, max, CV%, and gated percentiles."""
    clean = sorted(float(v) for v in values if v is not None and not math.isnan(float(v)))
    n = len(clean)
    if n == 0:
        return {
            "n": 0,
            "median": None,
            "mean": None,
            "min": None,
            "max": None,
            "stdev": None,
            "cv_pct": None,
            "p50": None,
            "p95": None,
            "p99": None,
            "p999": None,
            "p95_reliable": False,
            "p99_reliable": False,
            "p999_reliable": False,
            "high_variance": False,
            "validity_flag": "INVALID",
            "warnings": ["empty sample series"],
        }

    mean_val = sum(clean) / n
    med_val = percentile(clean, 50.0, enforce_min_samples=False)
    min_val = clean[0]
    max_val = clean[-1]

    if n >= 2:
        variance = sum((x - mean_val) ** 2 for x in clean) / (n - 1)
        stdev_val = math.sqrt(variance)
        cv_pct = (stdev_val / abs(mean_val)) * 100.0 if mean_val != 0.0 else 0.0
    else:
        stdev_val = 0.0
        cv_pct = 0.0

    warnings: list[str] = []
    p95_reliable = n >= MIN_SAMPLES_P95
    p99_reliable = n >= MIN_SAMPLES_P99
    p999_reliable = n >= MIN_SAMPLES_P999

    if include_percentiles:
        p95_val = percentile(clean, 95.0, enforce_min_samples=True)
        p99_val = percentile(clean, 99.0, enforce_min_samples=True)
        p999_val = percentile(clean, 99.9, enforce_min_samples=True)
        if not p95_reliable:
            warnings.append(f"p95 omitted: n={n} < {MIN_SAMPLES_P95}")
        if not p99_reliable:
            warnings.append(f"p99 omitted: n={n} < {MIN_SAMPLES_P99}")
        if not p999_reliable:
            warnings.append(f"p999 omitted: n={n} < {MIN_SAMPLES_P999}")
    else:
        p95_val = None
        p99_val = None
        p999_val = None

    high_var, var_warn = check_variance(cv_pct, kind=kind)
    if var_warn:
        warnings.append(var_warn)

    return {
        "n": n,
        "median": round(med_val, 6) if med_val is not None else None,
        "mean": round(mean_val, 6),
        "min": round(min_val, 6),
        "max": round(max_val, 6),
        "stdev": round(stdev_val, 6),
        "cv_pct": round(cv_pct, 2),
        "p50": round(med_val, 6) if med_val is not None else None,
        "p95": round(p95_val, 6) if p95_val is not None else None,
        "p99": round(p99_val, 6) if p99_val is not None else None,
        "p999": round(p999_val, 6) if p999_val is not None else None,
        "p95_reliable": p95_reliable,
        "p99_reliable": p99_reliable,
        "p999_reliable": p999_reliable,
        "high_variance": high_var,
        "validity_flag": "HIGH_VARIANCE" if high_var else "PASS",
        "warnings": warnings,
    }


def summarize_steady(values: Sequence[float]) -> dict:
    """Summarize steady-state throughput or IOPS runs (Median/Min/Max/CV%, 15% CV cap)."""
    return summarize_series(values, kind="steady", include_percentiles=False)


def summarize_latency(values: Sequence[float], *, kind: str = "discrete") -> dict:
    """Summarize latency samples with sample-gated P50/P95/P99/P99.9."""
    return summarize_series(values, kind=kind, include_percentiles=True)


def computesdk_reference_score(latency_ms: float, *, ceiling_ms: float = 60000.0) -> float:
    """Compute ComputeSDK reference score (non-gate informational column only).

    Formula: clamp((1 - latency_ms / ceiling_ms) * 100, 0, 100).
    """
    if ceiling_ms <= 0:
        raise ValueError(f"ceiling_ms must be positive, got {ceiling_ms}")
    raw = (1.0 - float(latency_ms) / float(ceiling_ms)) * 100.0
    return round(max(0.0, min(100.0, raw)), 4)


def computesdk_storage_score(latency_ms: float) -> float:
    """ComputeSDK storage.bench.ts reference score (30,000 ms ceiling)."""
    return computesdk_reference_score(latency_ms, ceiling_ms=30000.0)


def computesdk_fork_score(latency_ms: float) -> float:
    """ComputeSDK snapshot-fork.bench.ts reference score (60,000 ms ceiling)."""
    return computesdk_reference_score(latency_ms, ceiling_ms=60000.0)


def linear_slope(xs: Sequence[float], ys: Sequence[float]) -> float:
    """Compute ordinary least-squares slope dy/dx for scaling curves."""
    if len(xs) != len(ys) or len(xs) < 2:
        return 0.0
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0.0:
        return 0.0
    return round(num / den, 6)


def format_stat(
    stat: dict | float | int | None,
    *,
    unit: str = "",
    precision: int = 2,
    show_range: bool = False,
) -> str:
    """Render a summary dict or scalar cleanly for Markdown tables."""
    if stat is None:
        return "N/A"
    suffix = f" {unit}" if unit else ""
    if isinstance(stat, (int, float)):
        return f"{stat:.{precision}f}{suffix}"
    med = stat.get("median")
    if med is None:
        return "N/A"
    cv = stat.get("cv_pct")
    if show_range:
        lo = stat.get("min")
        hi = stat.get("max")
        if lo is not None and hi is not None and cv is not None:
            return f"{med:.{precision}f} ({lo:.{precision}f}..{hi:.{precision}f}, CV {cv:.1f}%){suffix}"
    if cv is not None and stat.get("n", 0) >= 2:
        return f"{med:.{precision}f} (CV {cv:.1f}%){suffix}"
    return f"{med:.{precision}f}{suffix}"
