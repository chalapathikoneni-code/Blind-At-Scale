"""Equal-information sampling arms A7 / A7X (PREREGISTRATION Amendment 5
Section A5.1, locked 2026-11-06 12:15 UTC).

A7  (HH-stratified sampler, ids-only packet): per column, two strata
    (hot = keys in that column's top-1024 HH list; tail = rest).
    Expected sampled rows = r * column total, allocated beta=0.9 to hot:
        r_hot  = beta * r / f_hot
        r_tail = (1 - beta) * r / (1 - f_hot)
        f_hot  = (total - tail_mass) / total
    Sampled counts Binomial(c_k, r_stratum(k)); HT prediction
    sampled / r_stratum(k). kN NULL rows are tail: Binomial(null_count,
    r_tail(kN)) / r_tail(kN).
A7X (exact-HH sampler, counts packet): HH counts taken as known.
    Tail rows of every column sampled at a single per-dataset rate
        r_tail = r * N_fact / tail_mass_fact(kA),
        N_fact = counts['fact_kA'].sum()
    Predicted: exact on HH keys, sampled / r_tail on tail keys.
    kN NULL rows: Binomial(null_count, r_tail) / r_tail.

Packets are built from the profiler synopsis (synopsis.json): fact HH
ids / counts come from syn['cols'][col]; fact2 HH ids are the top-1024
by fact2 counts computed from counts.npz (packet content, costed), and
fact2 HH counts (A7X only) are the fact2 counts at those keys.

Testbed size convention (as arms.py A1): sampled fact rows counted on
kA plus sampled NULL rows.
"""
import numpy as np

from common import K, TOP_K

FACT_COLS = ("kA", "kB", "kC", "kN")
PAIR_COLS = ("kA", "kB")
R_DEFAULT = 0.01
BETA = 0.9


# ---------------- packet builders -------------------------------------------
def _fact2_top(counts, col):
    c = counts[f"fact2_{col}"]
    order = np.argsort(-c, kind="stable")[:TOP_K]
    return order, c[order]


def build_packet_A7(counts, syn):
    """Ids-only packet: HH key ids (NO counts), column total, tail mass."""
    packet = {"dim_rows": K, "fact": {}, "fact2": {}}
    for col in FACT_COLS:
        sc = syn["cols"][col]
        packet["fact"][col] = {
            "hh_keys": [int(x) for x in sc["hh_keys"]],
            "total": int(sc["total"]),
            "tail_mass": int(sc["tail_mass"]),
        }
    for col in PAIR_COLS:
        order, hh_counts = _fact2_top(counts, col)
        total = int(counts[f"fact2_{col}"].sum())
        packet["fact2"][col] = {
            "hh_keys": [int(x) for x in order.tolist()],
            "total": total,
            "tail_mass": int(total - int(hh_counts.sum())),
        }
    return packet


def build_packet_A7X(counts, syn):
    """Counts packet: HH key ids WITH exact counts, total, tail mass."""
    packet = {"dim_rows": K, "fact": {}, "fact2": {}}
    for col in FACT_COLS:
        sc = syn["cols"][col]
        packet["fact"][col] = {
            "hh_keys": [int(x) for x in sc["hh_keys"]],
            "hh_counts": [int(x) for x in sc["hh_counts"]],
            "total": int(sc["total"]),
            "tail_mass": int(sc["tail_mass"]),
        }
    for col in PAIR_COLS:
        order, hh_counts = _fact2_top(counts, col)
        total = int(counts[f"fact2_{col}"].sum())
        packet["fact2"][col] = {
            "hh_keys": [int(x) for x in order.tolist()],
            "hh_counts": [int(x) for x in hh_counts.tolist()],
            "total": total,
            "tail_mass": int(total - int(hh_counts.sum())),
        }
    return packet


# ---------------- helpers ----------------------------------------------------
def _hot_mask(hh_keys):
    mask = np.zeros(K, dtype=bool)
    mask[np.asarray(hh_keys, dtype=np.int64)] = True
    return mask


def _a7_rates(entry, r=R_DEFAULT, beta=BETA):
    total = float(entry["total"])
    tail_mass = float(entry["tail_mass"])
    f_hot = (total - tail_mass) / total
    r_hot = beta * r / f_hot
    r_tail = (1.0 - beta) * r / (1.0 - f_hot)
    return r_hot, r_tail, f_hot


# ---------------- arm predictions -------------------------------------------
def predict_A7(counts, packet, seed, r=R_DEFAULT, beta=BETA):
    """Returns (pred_fact, pred_fact2, pred_null, dim_present, size).

    pred_* are FULL-SCALE predicted count vectors (float), via
    Horvitz-Thompson division by the per-stratum sampling rate.
    Draw order: fact kA,kB,kC,kN; fact2 kA,kB; then NULL (as arms.py A1).
    """
    rng = np.random.default_rng(seed)
    null = int(counts["null_count"])
    alldim = np.ones(K, dtype=bool)

    pf, sampled_fact = {}, {}
    for col in FACT_COLS:
        entry = packet["fact"][col]
        r_hot, r_tail, _ = _a7_rates(entry, r=r, beta=beta)
        rates = np.where(_hot_mask(entry["hh_keys"]), r_hot, r_tail)
        sampled = rng.binomial(counts[f"fact_{col}"].astype(np.int64), rates)
        sampled_fact[col] = sampled
        pf[col] = sampled.astype(np.float64) / rates

    pf2 = {}
    for col in PAIR_COLS:
        entry = packet["fact2"][col]
        r_hot, r_tail, _ = _a7_rates(entry, r=r, beta=beta)
        rates = np.where(_hot_mask(entry["hh_keys"]), r_hot, r_tail)
        sampled = rng.binomial(counts[f"fact2_{col}"].astype(np.int64), rates)
        pf2[col] = sampled.astype(np.float64) / rates

    _, r_tail_kN, _ = _a7_rates(packet["fact"]["kN"], r=r, beta=beta)
    sampled_null = int(rng.binomial(null, r_tail_kN))
    pn = sampled_null / r_tail_kN

    size = int(sampled_fact["kA"].sum()) + sampled_null
    return pf, pf2, float(pn), alldim, size


def a7x_r_tail(counts, packet, r=R_DEFAULT):
    """Single per-dataset tail rate for A7X (Amendment 5 Section A5.1)."""
    n_fact = int(counts["fact_kA"].sum())
    tail_kA = float(packet["fact"]["kA"]["tail_mass"])
    return r * n_fact / tail_kA


def predict_A7X(counts, packet, seed, r=R_DEFAULT):
    """Returns (pred_fact, pred_fact2, pred_null, dim_present, size).

    HH counts exact (known, not sampled); tail keys sampled at the
    single per-dataset r_tail. Draw order: fact kA,kB,kC,kN (tail keys
    only, in key-id order); fact2 kA,kB; then NULL.
    """
    rng = np.random.default_rng(seed)
    null = int(counts["null_count"])
    alldim = np.ones(K, dtype=bool)
    r_tail = a7x_r_tail(counts, packet, r=r)

    def reconstruct(table, col):
        entry = packet[table][col]
        hh_keys = np.asarray(entry["hh_keys"], dtype=np.int64)
        hh_counts = np.asarray(entry["hh_counts"], dtype=np.float64)
        src = counts[f"{'fact' if table == 'fact' else 'fact2'}_{col}"]
        out = np.zeros(K, dtype=np.float64)
        out[hh_keys] = hh_counts
        tail_mask = ~_hot_mask(entry["hh_keys"])
        tail_keys = np.nonzero(tail_mask)[0]
        sampled_tail = rng.binomial(src[tail_mask].astype(np.int64), r_tail)
        out[tail_keys] = sampled_tail.astype(np.float64) / r_tail
        return out, int(sampled_tail.sum())

    pf, sampled_kA = {}, 0
    for col in FACT_COLS:
        pf[col], s = reconstruct("fact", col)
        if col == "kA":
            sampled_kA = s
    pf2 = {}
    for col in PAIR_COLS:
        pf2[col], _ = reconstruct("fact2", col)

    sampled_null = int(rng.binomial(null, r_tail))
    pn = sampled_null / r_tail

    size = int(sampled_kA) + int(sampled_null)
    return pf, pf2, float(pn), alldim, size
