"""Synopsis construction + testbed arms (PREREGISTRATION.md S.4).

A5/A8 functions receive ONLY the synopsis dict (code-structure enforced:
they never touch the true count arrays). A1-A4 are sampling/synthesis
baselines. Every arm returns predicted FULL-SCALE per-key count vectors;
loads are then computed by the same common.loads_from_counts used for
ground truth.
"""
import json
import numpy as np

from common import K, TOP_K, splitmix64_hash, loads_from_counts, verdict

FACT_COLS = ("kA", "kB", "kC", "kN")
PAIR_COLS = ("kA", "kB")


# ---------------- synopsis (profiler output; the ONLY input to A5/A8) ------
def _fit_tail(hh_counts):
    """Power-law fit on the heavy-hitter rank-count curve."""
    m = len(hh_counts)
    ranks = np.arange(1, m + 1, dtype=np.float64)
    c = np.maximum(hh_counts.astype(np.float64), 1.0)
    b, loga = np.polyfit(np.log(ranks), np.log(c), 1)
    return float(loga), float(-b)


def _tail_expected(hh_counts, tail_mass, n_tail_keys):
    """Expected tail counts (descending) from the fitted law, normalised
    to tail_mass, for ranks TOP_K+1 .. TOP_K+n_tail_keys."""
    loga, b = _fit_tail(hh_counts)
    ranks = np.arange(len(hh_counts) + 1, len(hh_counts) + n_tail_keys + 1,
                      dtype=np.float64)
    raw = np.exp(loga) * ranks ** (-b)
    if raw.sum() <= 0:
        return np.full(n_tail_keys, tail_mass / max(n_tail_keys, 1))
    return raw / raw.sum() * tail_mass


def _canonical_tail_order(tail_keys):
    """Deterministic pseudo-random order of tail key ids (splitmix64)."""
    h = splitmix64_hash(tail_keys.astype(np.int64))
    return tail_keys[np.argsort(h, kind="stable")]


def build_synopsis(counts):
    """counts: dict from counts.npz. Returns a JSON-able dict."""
    syn = {"dim_rows": K, "cols": {}, "pairs": {}}
    for col in FACT_COLS:
        c = counts[f"fact_{col}"]
        order = np.argsort(-c, kind="stable")
        hh_keys = order[:TOP_K]
        hh = c[hh_keys]
        syn["cols"][col] = {
            "total": int(c.sum()), "cardinality": int((c > 0).sum()),
            "hh_keys": hh_keys.tolist(), "hh_counts": hh.tolist(),
            "tail_mass": int(c.sum() - hh.sum()),
        }
        if col == "kN":
            syn["cols"][col]["null_mass"] = int(counts["null_count"])
    for col in PAIR_COLS:
        c1 = counts[f"fact_{col}"]
        c2 = counts[f"fact2_{col}"]
        order = np.argsort(-c1, kind="stable")
        hh_keys = order[:TOP_K]
        syn["pairs"][col] = {
            "hh_keys": hh_keys.tolist(),
            "hh_c1": c1[hh_keys].tolist(), "hh_c2": c2[hh_keys].tolist(),
            "tail_mass_c1": int(c1.sum() - c1[hh_keys].sum()),
            "tail_mass_c2": int(c2.sum() - c2[hh_keys].sum()),
            "fact2_hh_counts": np.sort(c2)[::-1][:TOP_K].tolist(),
        }
    return syn


def _reconstruct_col(syn_col, r=None, rng=None):
    """Full-scale expected counts (r=None, A8) or testbed counts (A5)
    for one fact column, from the synopsis alone."""
    hh_keys = np.array(syn_col["hh_keys"], dtype=np.int64)
    hh_counts = np.array(syn_col["hh_counts"], dtype=np.float64)
    tail_keys = np.setdiff1d(np.arange(K), hh_keys)
    order = _canonical_tail_order(tail_keys)
    tail_exp = _tail_expected(hh_counts, syn_col["tail_mass"], len(tail_keys))
    out = np.zeros(K, dtype=np.float64)
    if r is None:
        out[hh_keys] = hh_counts
        out[order] = tail_exp
    else:
        out[hh_keys] = np.round(hh_counts * r)
        n_tail = int(round(syn_col["tail_mass"] * r))
        p = tail_exp / tail_exp.sum()
        out[order] = rng.multinomial(n_tail, p)
    return out


def _reconstruct_pair(syn_pair, r=None, rng=None):
    """(c1, c2) full-scale expected (A8) or testbed (A5) counts for an
    F-pair column. Tail assigned comonotonically along the canonical
    order (declared synopsis model of the joint tail)."""
    hh_keys = np.array(syn_pair["hh_keys"], dtype=np.int64)
    c1_hh = np.array(syn_pair["hh_c1"], dtype=np.float64)
    c2_hh = np.array(syn_pair["hh_c2"], dtype=np.float64)
    tail_keys = np.setdiff1d(np.arange(K), hh_keys)
    order = _canonical_tail_order(tail_keys)
    t1 = _tail_expected(c1_hh, syn_pair["tail_mass_c1"], len(tail_keys))
    c2_hh_sorted = np.array(syn_pair["fact2_hh_counts"], dtype=np.float64)
    t2 = _tail_expected(c2_hh_sorted, syn_pair["tail_mass_c2"], len(tail_keys))
    c1 = np.zeros(K); c2 = np.zeros(K)
    if r is None:
        c1[hh_keys] = c1_hh; c2[hh_keys] = c2_hh
        c1[order] = t1; c2[order] = t2
    else:
        c1[hh_keys] = np.round(c1_hh * r); c2[hh_keys] = np.round(c2_hh * r)
        n1 = int(round(syn_pair["tail_mass_c1"] * r))
        n2 = int(round(syn_pair["tail_mass_c2"] * r))
        c1[order] = rng.multinomial(n1, t1 / t1.sum())
        c2[order] = rng.multinomial(n2, t2 / t2.sum())
    return c1, c2


# ---------------- arms ------------------------------------------------------
def arm_predictions(arm, counts, syn, seed, r=0.01):
    """Returns (pred_fact, pred_fact2, pred_null, dim_present, testbed_rows).
    pred_* are FULL-SCALE predicted count vectors (float)."""
    rng = np.random.default_rng(seed)
    fact = {c: counts[f"fact_{c}"].astype(np.float64) for c in FACT_COLS}
    fact2 = {c: counts[f"fact2_{c}"].astype(np.float64) for c in PAIR_COLS}
    null = float(counts["null_count"])
    alldim = np.ones(K, dtype=bool)

    if arm == "A1":
        pf = {c: rng.binomial(fact[c].astype(np.int64), r) / r for c in FACT_COLS}
        pf2 = {c: rng.binomial(fact2[c].astype(np.int64), r) / r for c in PAIR_COLS}
        pn = rng.binomial(int(null), r) / r
        dimp = rng.random(K) < r
        size = int(round(pf["kA"].sum() * r + pn * r))  # sampled fact rows
        return pf, pf2, pn, dimp, size

    if arm == "A2":
        # strata hash is seeded per dataset (a deployment's hash seed),
        # so the same keys are not systematically invisible in every run
        buckets = (splitmix64_hash(np.arange(K) + np.uint64(seed)) % np.uint64(100)) < 10
        pf, pf2 = {}, {}
        for c in FACT_COLS:
            s = np.where(buckets, rng.binomial(fact[c].astype(np.int64), 0.1), 0)
            pf[c] = np.where(buckets, s / 0.1, 0.0)
        for c in PAIR_COLS:
            s = np.where(buckets, rng.binomial(fact2[c].astype(np.int64), 0.1), 0)
            pf2[c] = np.where(buckets, s / 0.1, 0.0)
        bnull = int(splitmix64_hash(np.array([K + seed]))[0] % np.uint64(100)) < 10
        pn = (rng.binomial(int(null), 0.1) / 0.1) if bnull else 0.0
        size = int(round(pf["kA"].sum() * 0.1 + pn * 0.1))  # sampled fact rows
        return pf, pf2, pn, buckets, size

    if arm == "A3":
        sel = rng.random(K) < 0.10
        pf, pf2 = {}, {}
        for c in FACT_COLS:
            s = np.where(sel, rng.binomial(fact[c].astype(np.int64), 0.1), 0)
            pf[c] = np.where(sel, s / 0.1, 0.0)
        for c in PAIR_COLS:
            s = np.where(sel, rng.binomial(fact2[c].astype(np.int64), 0.1), 0)
            pf2[c] = np.where(sel, s / 0.1, 0.0)
        pn = (rng.binomial(int(null), 0.1) / 0.1) if rng.random() < 0.10 else 0.0
        size = int(round(pf["kA"].sum() * 0.1 + pn * 0.1))  # sampled rows
        return pf, pf2, pn, sel, size

    if arm == "A4":
        pf, pf2 = {}, {}
        for c in FACT_COLS:
            pmf = fact[c] / fact[c].sum()
            pf[c] = rng.multinomial(int(round(fact[c].sum() * r)), pmf) / r
        perm = rng.permutation(K)
        for c in PAIR_COLS:
            pmf = fact2[c] / fact2[c].sum()
            pf2[c] = rng.multinomial(int(round(fact2[c].sum() * r)), pmf[perm]) / r
        pn = rng.binomial(int(null), r) / r
        return pf, pf2, pn, alldim, int(round(fact["kA"].sum() * r))

    if arm in ("A5", "A8"):
        gen = (arm == "A5")
        rr = r if gen else None
        pf, pf2 = {}, {}
        for c in FACT_COLS:
            v = _reconstruct_col(syn["cols"][c], r=rr, rng=rng)
            pf[c] = v / r if gen else v
        for c in PAIR_COLS:
            v1, v2 = _reconstruct_pair(syn["pairs"][c], r=rr, rng=rng)
            if gen:
                pf[c] = v1 / r
                pf2[c] = v2 / r
            else:
                pf[c] = v1
                pf2[c] = v2
        nm = syn["cols"]["kN"].get("null_mass", 0)
        pn = float(nm) if not gen else round(nm * r) / r
        size = int(round(syn["cols"]["kA"]["total"] * r)) if gen else 0
        return pf, pf2, pn, alldim, size

    raise ValueError(arm)


def run_arms(counts, syn, seed, arms, r=0.01):
    """Returns (gt_loads, gt_verdicts, results) where results[arm] =
    {pred_loads, pred_verdicts, testbed_fact_rows}."""
    n = int(counts["fact_kA"].sum())
    fact = {c: counts[f"fact_{c}"] for c in FACT_COLS}
    fact2 = {c: counts[f"fact2_{c}"] for c in PAIR_COLS}
    alldim = np.ones(K, dtype=bool)
    gt_loads = loads_from_counts(fact, fact2, float(counts["null_count"]),
                                 alldim, distinct_counts=counts["distinct_A"])
    gt_verd = {p: verdict(l, p, n) for p, l in gt_loads.items()}
    results = {}
    for i, arm in enumerate(arms):
        pf, pf2, pn, dimp, size = arm_predictions(
            arm, counts, syn, seed=seed * 1000 + 17 * i + 5, r=r)
        pl = loads_from_counts(pf, pf2, pn, dimp, distinct_counts=None)
        results[arm] = {
            "pred_loads": pl,
            "pred_verdicts": {p: verdict(l, p, n) for p, l in pl.items()},
            "testbed_fact_rows": size,
        }
    return gt_loads, gt_verd, results
