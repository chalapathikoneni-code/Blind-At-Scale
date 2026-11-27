"""Shared definitions for the Blind at Scale experimental program.

Design: PREREGISTRATION.md (locked 2026-11-06 + Amendments 1-2).
Extends ~/workspace/ieee-access-stress-test/pilot1/common.py (hash + load
functions carried over; pipeline set and arms are the full program's).
"""
import numpy as np

K = 20_000              # key space size for every key column
P_PARTITIONS = 128
TOP_K = 1024            # heavy hitters in the synopsis
S_SUBKEY = 100_000      # subkey space for the distinct-count pipeline
T_LOAD_FRAC = 0.02      # T_load = 0.02 * N_fact
T_DISTINCT = 50_000

KEY_COLS = ["kA", "kB", "kC", "kN"]

# 11 pipeline definitions: (id, key_col, kind) kind in
# {"group", "join", "factfact", "window", "distinct"}
PIPELINES = [
    ("G_A", "kA", "group"),
    ("G_B", "kB", "group"),
    ("G_C", "kC", "group"),
    ("G_N", "kN", "group"),
    ("J_A", "kA", "join"),
    ("J_B", "kB", "join"),
    ("J_N", "kN", "join"),
    ("F_A", "kA", "factfact"),
    ("F_B", "kB", "factfact"),
    ("W_A", "kA", "window"),
    ("D_A", "kA", "distinct"),
]


def splitmix64_hash(keys: np.ndarray) -> np.ndarray:
    z = (keys.astype(np.uint64) + np.uint64(1)).copy()
    z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    z = z ^ (z >> np.uint64(31))
    return z


def max_partition_load(counts: np.ndarray, p: int = P_PARTITIONS) -> float:
    """counts[k] = rows for key k. Hash-partition by key; return max load."""
    keys = np.arange(len(counts), dtype=np.int64)
    parts = (splitmix64_hash(keys) % np.uint64(p)).astype(np.int64)
    loads = np.bincount(parts, weights=counts.astype(np.float64), minlength=p)
    return float(loads.max())


def distinct_estimate(counts: np.ndarray, s: int = S_SUBKEY) -> float:
    """Occupancy estimator of max distinct subkeys (Amendment 1a)."""
    c = counts.astype(np.float64)
    d = s * (1.0 - (1.0 - 1.0 / s) ** c)
    return float(d.max())


def loads_from_counts(fact_counts, fact2_counts, null_count, dim_present,
                      distinct_counts=None, n_fact=None):
    """Compute the 11 pipeline loads from per-key count vectors.

    fact_counts / fact2_counts: dict col -> int64 array[K] (non-null counts).
    null_count: NULL-group size of kN in fact.
    dim_present: bool array[K] (which dim keys exist in this view of the data;
    all True for ground truth / synthesis arms).
    distinct_counts: exact per-key distinct subkeys for kA (ground truth
    only); otherwise the occupancy estimator is applied to fact kA counts.
    Returns dict pipeline_id -> load (float).
    """
    out = {}
    for pid, col, kind in PIPELINES:
        c = fact_counts[col].astype(np.float64)
        if kind == "group":
            load = float(c.max())
            if col == "kN":
                load = max(load, float(null_count))
        elif kind == "window":
            load = float(c.max())
        elif kind == "join":
            masked = np.where(dim_present, c, 0.0)
            load = max_partition_load(masked)
        elif kind == "factfact":
            prod = c * fact2_counts[col].astype(np.float64)
            load = max_partition_load(prod)
        elif kind == "distinct":
            if distinct_counts is not None:
                load = float(distinct_counts.max())
            else:
                load = distinct_estimate(fact_counts[col])
        else:
            raise ValueError(kind)
        out[pid] = load
    return out


def verdict(load: float, pid: str, n_fact: int) -> str:
    t = T_DISTINCT if pid == "D_A" else T_LOAD_FRAC * n_fact
    return "FAIL" if load > t else "PASS"


def threshold(pid: str, n_fact: int) -> float:
    return float(T_DISTINCT if pid == "D_A" else T_LOAD_FRAC * n_fact)
