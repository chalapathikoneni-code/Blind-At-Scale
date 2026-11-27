"""Classical baseline arm A9: end-biased sampling (Estan & Naughton,
ICDE 2006, "End-biased Samples for Join Cardinality Estimation").

PREREGISTRATION_EXP_A.md Section 2 (locked 2026-11-24 01:30 EST,
pre-run). Construction follows the paper's Section 3.1 exactly; the
F (fact-fact) estimator is the paper's Eq. 1 exactly; the single-table
loads use the Horvitz-Thompson per-value reconstruction under the same
sampling design (an adaptation, documented in the pre-registration:
the paper estimates join size only).

Construction, per column, from the profiler's full frequency list:
  - threshold T: smallest integer with expected retained count <= 1024
    (expected = sum_v min(1, f_v / T)); binary search on [1, N].
  - value v kept iff f_v >= T (exact), else iff h(v) <= f_v / T,
    where h is the SHARED hash h_H(v) = splitmix64(v + 1 + H) / 2^64,
    the same H for all tables and columns (the paper's coordination).
  - retained values store their EXACT frequency.
Estimation:
  - single-table loads (G, J, W, D): HT reconstruction c_hat_v = f_v
    if f_v >= T, T if retained with f_v < T, 0 otherwise; loads via
    common.loads_from_counts (D_A by the Amendment 1a occupancy
    estimator, as for every arm).
  - F loads: Eq. 1 per-key contributions (see f_contributions), then
    common.max_partition_load. NOT the product of HT estimates.
The kN NULL group is a value with frequency null_count and pseudo-key
K for the hash; null_count >> T on every dataset, so it is exact-head.
A9 has NO numpy RNG stream: decisions come from the fixed hash, so
results are deterministic given the counts and H (immune to the
numpy-version caveat). Testbed rows: 0 (direct prediction, like A8).
"""
import numpy as np

from common import K, splitmix64_hash

FACT_COLS = ("kA", "kB", "kC", "kN")
PAIR_COLS = ("kA", "kB")
TARGET_RETAINED = 1024
HASH_SEED_HEADLINE = 0
# Sensitivity seeds (PREREGISTRATION_EXP_A.md Amendment 1): well
# separated -- adjacent seeds give shifted, correlated hash vectors
# (h_{H+1}(v) = h_H(v+1)), so they are not independent replications.
HASH_SEEDS_SENSITIVITY = (0x9E3779B97F4A7C15, 0x6A09E667F3BCC909)
NULL_PSEUDO_KEY = K


def sampling_hash(keys, hseed=HASH_SEED_HEADLINE):
    """h_H(v) in [0, 1): splitmix64 of (v + 1 + H), scaled by 2**-64."""
    k = np.asarray(keys, dtype=np.uint64) + np.uint64(hseed)
    return splitmix64_hash(k).astype(np.float64) / float(1 << 64)


def _expected_retained(sorted_desc, total, threshold):
    """sum_v min(1, f_v / T) from a descending-sorted count vector."""
    head = int(np.searchsorted(-sorted_desc, -threshold, side="right"))
    tail_mass = total - int(sorted_desc[:head].sum()) if head else total
    return head + tail_mass / threshold


def find_threshold(counts, target=TARGET_RETAINED):
    """Smallest integer T in [1, N] with expected retained <= target.

    E(T) = sum_v min(1, f_v / T) is non-increasing in T; E(N) = 1, so
    the binary search always terminates. Returns (T, expected).
    """
    c = np.asarray(counts, dtype=np.int64)
    total = int(c.sum())
    if total == 0:
        return 1, 0.0
    srt = np.sort(c)[::-1]
    lo, hi = 1, total  # E(hi) = 1 <= target (target >= 1)
    while lo < hi:
        mid = (lo + hi) // 2
        if _expected_retained(srt, total, mid) <= target:
            hi = mid
        else:
            lo = mid + 1
    return lo, _expected_retained(srt, total, lo)


def build_sample(counts, hseed=HASH_SEED_HEADLINE, target=TARGET_RETAINED):
    """End-biased sample of one column. Returns dict with threshold,
    retained key ids, retained exact counts, and the HT reconstruction.

    ht[v] = f_v if f_v >= T; T if retained with f_v < T; 0 otherwise.
    """
    c = np.asarray(counts, dtype=np.int64)
    total = int(c.sum())
    threshold, expected = find_threshold(c, target=target)
    keys = np.arange(len(c), dtype=np.int64)
    h = sampling_hash(keys, hseed=hseed)
    head = c >= threshold
    retained = head | (h <= c.astype(np.float64) / threshold)
    retained[ c == 0] = False
    ht = np.zeros(len(c), dtype=np.float64)
    ht[head] = c[head].astype(np.float64)
    tail_ret = retained & ~head
    ht[tail_ret] = float(threshold)
    rkeys = np.nonzero(retained)[0]
    return {
        "threshold": int(threshold),
        "expected_retained": float(expected),
        "retained_keys": rkeys.astype(np.int64),
        "retained_counts": c[rkeys].astype(np.int64),
        "retained_mask": retained,
        "ht": ht,
        "total": total,
    }


def build_packet(counts, hseed=HASH_SEED_HEADLINE, target=TARGET_RETAINED):
    """End-biased samples for all fact/fact2 columns + the NULL value.

    Returns (packet, samples): packet is the JSON-able serialised form
    (parallel keys/counts arrays per column, as the A7X packet
    convention); samples holds the arrays the estimator needs.
    """
    packet = {"hash_seed": int(hseed), "target_retained": int(target),
              "fact": {}, "fact2": {}}
    samples = {"fact": {}, "fact2": {}}
    for col in FACT_COLS:
        s = build_sample(counts[f"fact_{col}"], hseed=hseed, target=target)
        samples["fact"][col] = s
        packet["fact"][col] = {
            "threshold": s["threshold"], "total": s["total"],
            "keys": [int(x) for x in s["retained_keys"]],
            "counts": [int(x) for x in s["retained_counts"]],
        }
    for col in PAIR_COLS:
        s = build_sample(counts[f"fact2_{col}"], hseed=hseed, target=target)
        samples["fact2"][col] = s
        packet["fact2"][col] = {
            "threshold": s["threshold"], "total": s["total"],
            "keys": [int(x) for x in s["retained_keys"]],
            "counts": [int(x) for x in s["retained_counts"]],
        }
    # NULL group as a value with pseudo-key K, judged against the kN
    # threshold (null_count >> T on every dataset: exact-head).
    null_count = int(counts["null_count"])
    t_kn = samples["fact"]["kN"]["threshold"]
    h_null = float(sampling_hash(np.array([NULL_PSEUDO_KEY]),
                                 hseed=hseed)[0])
    if null_count >= t_kn:
        null_ht = float(null_count)
        null_retained = True
    else:
        null_retained = h_null <= null_count / t_kn
        null_ht = float(t_kn) if null_retained else 0.0
    packet["null"] = {"count": null_count, "threshold": t_kn,
                      "retained": bool(null_retained)}
    samples["null_ht"] = null_ht
    samples["null_retained"] = null_retained
    return packet, samples


def f_contributions(sample_a, sample_b, counts_a, counts_b):
    """Per-key Eq. 1 contributions for one F pair column.

    sample_a/b: build_sample dicts for fact / fact2. counts_a/b: the
    true per-key count vectors (the retained values' exact frequencies
    are read from them at retained keys -- the sample stores them).
    Returns float array[K] of estimated per-key join-output counts.
    """
    a = np.asarray(counts_a, dtype=np.float64)
    b = np.asarray(counts_b, dtype=np.float64)
    ta = float(sample_a["threshold"])
    tb = float(sample_b["threshold"])
    both = sample_a["retained_mask"] & sample_b["retained_mask"]
    out = np.zeros(len(a), dtype=np.float64)
    a_hi = a >= ta
    b_hi = b >= tb
    m = both & a_hi & b_hi
    out[m] = a[m] * b[m]
    m = both & (~a_hi) & b_hi
    out[m] = ta * b[m]
    m = both & a_hi & (~b_hi)
    out[m] = a[m] * tb
    m = both & (~a_hi) & (~b_hi)
    out[m] = np.maximum(ta * b[m], tb * a[m])
    return out


def predict_A9(counts, hseed=HASH_SEED_HEADLINE, target=TARGET_RETAINED):
    """Returns (pred_fact, pred_fact2, pred_null, dim_present,
    testbed_rows, f_contrib, packet, samples).

    pred_* are HT reconstructed full-scale count vectors (float);
    f_contrib[col] is the Eq. 1 per-key join-output vector for F_A/F_B
    (the driver must use max_partition_load on it, NOT the product of
    the HT vectors). testbed_rows is 0 (direct prediction, like A8).
    """
    packet, samples = build_packet(counts, hseed=hseed, target=target)
    pf = {c: samples["fact"][c]["ht"] for c in FACT_COLS}
    pf2 = {c: samples["fact2"][c]["ht"] for c in PAIR_COLS}
    f_contrib = {}
    for col in PAIR_COLS:
        f_contrib[col] = f_contributions(
            samples["fact"][col], samples["fact2"][col],
            counts[f"fact_{col}"], counts[f"fact2_{col}"])
    alldim = np.ones(K, dtype=bool)
    return (pf, pf2, float(samples["null_ht"]), alldim, 0,
            f_contrib, packet, samples)
