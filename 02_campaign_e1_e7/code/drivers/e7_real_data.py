"""E7 -- Real-data information efficiency (FINAL_EXECUTION_PROTOCOL S.6).

Applies the synthetic-study A8 estimator definition (exact heavy-hitter
counts + power-law tail reconstruction via arms._tail_expected /
arms._canonical_tail_order, aligned pair handling with comonotonic tail
per arms._reconstruct_pair) to the NYC TLC January 2026 instance set,
at synopsis depths k in {16, 64, 256, 1024}. Loads are computed ONLY
with the real-world model's own load function
(realdata_model.partition_load over contribution dicts built per
realdata_model.contribution_map's definitions); common.loads_from_counts
is never used. Ground truth is the model's analytic load
(realdata_model.exact_load), consistency-checked against
analytic_gt_calibration.json for 2026-01 before any prediction runs.

Adapter notes (no estimator or load-function change):
  * Key universe for the location columns is the zone-lookup key set
    (LocationID 1..265, the dimension table) -- the real-data analog of
    the synthetic key space arange(K) stored as dim_rows in the
    synopsis. Tail keys are the universe keys not in the HH set, in
    arms._canonical_tail_order (splitmix64 order of the integer ids).
  * RatecodeID has no dimension table; its universe is its observed
    distinct values. Its NULL group is handled exactly as in the
    synthetic A8 path: the synopsis stores null_mass and the predicted
    contribution carries the NULL group at its exact mass.
  * Predicted counts are floats (expected tail counts). Contribution
    dicts follow contribution_map's definitions (group/window: all
    keys; fact_dimension: dimension-filtered, NULL excluded; fact_fact:
    per-key product over common non-NULL keys) and are passed to
    partition_load, which rounds each contribution as the model defines.
  * D_A is diagnostic only (the model defines no prediction for
    distinct workloads); it is excluded from every scored denominator.
  * A9 is NOT run: see E7_REAL_DATA_INFORMATION_REPORT.md.
"""
import csv
import json
import os
import sys
from collections import Counter

import numpy as np
import pyarrow.parquet as pq

BASE = os.path.expanduser("~/workspace/blind-at-scale")
sys.path.insert(0, os.path.join(BASE, "code"))
sys.path.insert(0, os.path.join(BASE, "realdata_emr_package", "code"))

import arms as A  # noqa: E402  (synthetic estimator: _tail_expected etc.)
import realdata_model as M  # noqa: E402  (real-world model / load function)

SAMPLE = os.path.join(BASE, "realdata_arm", "sample")
OUT = os.path.join(BASE, "final_scientific_strengthening")
KS = (16, 64, 256, 1024)
SCORED = ["G_A", "G_B", "G_C", "G_N", "J_A", "J_B", "J_N",
          "F_A", "F_B", "W_A"]
EXPECTED_JAN = {"G_A": 199975, "G_B": 191226, "G_C": 10596,
                "G_N": 2390495, "J_A": 199975, "J_B": 191226,
                "J_N": 10596, "F_A": 173798004, "F_B": 330477337,
                "W_A": 199975, "D_A": 259}


def get_counts(path, column):
    """Replicates dry_run_realdata.get_counts without DuckDB:
    NULL -> NULL_TOKEN, else str(int value)."""
    col = pq.read_table(path, columns=[column])[column]
    out = {}
    for v in col.to_pylist():
        key = M.NULL_TOKEN if v is None else str(int(v))
        out[key] = out.get(key, 0) + 1
    return out


def get_distinct_counts(path, group_column, distinct_column):
    tbl = pq.read_table(path, columns=[group_column, distinct_column])
    groups = {}
    for g, d in zip(tbl[group_column].to_pylist(),
                    tbl[distinct_column].to_pylist()):
        key = M.NULL_TOKEN if g is None else str(int(g))
        groups.setdefault(key, set()).add(d)
    return {k: len(v) for k, v in groups.items()}


def load_instance():
    y = os.path.join(SAMPLE, "yellow_tripdata_2026-01.parquet")
    g = os.path.join(SAMPLE, "green_tripdata_2026-01.parquet")
    zones = set()
    with open(os.path.join(SAMPLE, "taxi_zone_lookup.csv")) as fh:
        for row in csv.DictReader(fh):
            zones.add(str(int(row["LocationID"])))
    data = {
        "y_pu": get_counts(y, "PULocationID"),
        "y_do": get_counts(y, "DOLocationID"),
        "y_rc": get_counts(y, "RatecodeID"),
        "g_pu": get_counts(g, "PULocationID"),
        "g_do": get_counts(g, "DOLocationID"),
        "zones": zones,
        "y_distinct": get_distinct_counts(y, "PULocationID", "DOLocationID"),
    }
    data["n_yellow"] = sum(data["y_pu"].values())
    data["n_green"] = sum(data["g_pu"].values())
    return data


def instance_inputs(data, pipeline):
    kind = M.PIPELINE_KIND[pipeline]
    t1, k1, t2, k2, use_dim, _dc = M.PIPELINE_DEFS[pipeline]
    keymap = {("yellow", "PULocationID"): data["y_pu"],
              ("yellow", "DOLocationID"): data["y_do"],
              ("yellow", "RatecodeID"): data["y_rc"],
              ("green", "PULocationID"): data["g_pu"],
              ("green", "DOLocationID"): data["g_do"]}
    c1 = keymap[(t1, k1)]
    c2 = keymap[(t2, k2)] if t2 else None
    dim = data["zones"] if use_dim else None
    distinct = data["y_distinct"] if kind == "distinct" else None
    n_fact = data["n_yellow"] if t1 == "yellow" else data["n_green"]
    return kind, c1, c2, dim, distinct, n_fact


# ---------------- A8 adapter (estimator definition unchanged) -------------

def sorted_items(counts):
    """(key, count) sorted by (-count, key) -- the synopsis HH order."""
    return sorted(((str(k), int(v)) for k, v in counts.items() if int(v) > 0),
                  key=lambda kv: (-kv[1], kv[0]))


def reconstruct_col(counts, k, universe_ids):
    """A8 column reconstruction over the real key universe.

    counts: dict str->int of NON-NULL counts for one column.
    universe_ids: sorted list of int key ids forming the key space.
    Returns dict str->float predicted counts (dense over the universe),
    plus the truncated synopsis block for this column.
    Mirrors arms._reconstruct_col: HH exact; tail expected values from
    arms._tail_expected assigned in arms._canonical_tail_order.
    """
    items = sorted_items(counts)
    hh = items[:k]
    hh_keys = [key for key, _ in hh]
    hh_counts = np.array([v for _, v in hh], dtype=np.float64)
    tail_mass = int(sum(v for _, v in items) - sum(v for _, v in hh))
    syn_col = {"total": int(sum(v for _, v in items)),
               "cardinality": len(items),
               "hh_keys": hh_keys, "hh_counts": [int(v) for _, v in hh],
               "tail_mass": tail_mass}
    out = {key: float(v) for key, v in hh}
    hh_id_set = set(int(key) for key in hh_keys)
    tail_ids = np.array([i for i in universe_ids if i not in hh_id_set],
                        dtype=np.int64)
    if len(tail_ids) > 0 and tail_mass > 0:
        order = A._canonical_tail_order(tail_ids)
        tail_exp = A._tail_expected(hh_counts, float(tail_mass),
                                    len(tail_ids))
        for key_id, val in zip(order.tolist(), tail_exp.tolist()):
            out[str(int(key_id))] = float(val)
    elif len(tail_ids) > 0:
        for key_id in tail_ids.tolist():
            out.setdefault(str(int(key_id)), 0.0)
    return out, syn_col


def reconstruct_pair(c1_counts, c2_counts, k, universe_ids):
    """A8 pair reconstruction, mirroring arms._reconstruct_pair.

    HH keys by the fact (table 1) column; both tables' counts at those
    keys exact; tails fitted separately (table 2's tail from table 2's
    own top-k counts, as the synopsis stores fact2_hh_counts) and
    assigned comonotonically along the same canonical tail order.
    Returns (pred_c1, pred_c2, syn_pair).
    """
    items1 = sorted_items(c1_counts)
    items2 = sorted_items(c2_counts)
    hh = items1[:k]
    hh_keys = [key for key, _ in hh]
    hh_c1 = np.array([v for _, v in hh], dtype=np.float64)
    hh_c2 = np.array([int(c2_counts.get(key, 0)) for key in hh_keys],
                     dtype=np.float64)
    fact2_hh = np.array([v for _, v in items2[:k]], dtype=np.float64)
    total1 = int(sum(v for _, v in items1))
    total2 = int(sum(v for _, v in items2))
    tail_mass_c1 = int(total1 - sum(v for _, v in hh))
    tail_mass_c2 = int(total2 - int(hh_c2.sum()))
    syn_pair = {"hh_keys": hh_keys,
                "hh_c1": [int(v) for _, v in hh],
                "hh_c2": [int(v) for v in hh_c2.tolist()],
                "tail_mass_c1": tail_mass_c1,
                "tail_mass_c2": tail_mass_c2,
                "fact2_hh_counts": [int(v) for v in fact2_hh.tolist()]}
    out1 = {key: float(v) for key, v in hh}
    out2 = {key: float(v) for key, v in zip(hh_keys, hh_c2.tolist())}
    hh_id_set = set(int(key) for key in hh_keys)
    tail_ids = np.array([i for i in universe_ids if i not in hh_id_set],
                        dtype=np.int64)
    if len(tail_ids) > 0:
        order = A._canonical_tail_order(tail_ids)
        if tail_mass_c1 > 0:
            t1 = A._tail_expected(hh_c1, float(tail_mass_c1), len(tail_ids))
            for key_id, val in zip(order.tolist(), t1.tolist()):
                out1[str(int(key_id))] = float(val)
        if tail_mass_c2 > 0:
            t2 = A._tail_expected(fact2_hh, float(tail_mass_c2),
                                  len(tail_ids))
            for key_id, val in zip(order.tolist(), t2.tolist()):
                out2[str(int(key_id))] = float(val)
        for key_id in tail_ids.tolist():
            out1.setdefault(str(int(key_id)), 0.0)
            out2.setdefault(str(int(key_id)), 0.0)
    return out1, out2, syn_pair


def pred_load(kind, c1, c2=None, dim=None):
    """Predicted load via the model's OWN load function on contribution
    dicts built per contribution_map's definitions (float counts)."""
    if kind in ("group", "window"):
        contrib = {k: float(v) for k, v in c1.items() if float(v) > 0}
    elif kind == "fact_dimension":
        valid = dim or set()
        contrib = {k: float(v) for k, v in c1.items()
                   if k != M.NULL_TOKEN and k in valid and float(v) > 0}
    elif kind == "fact_fact":
        common = (set(c1) & set(c2)) - {M.NULL_TOKEN}
        contrib = {k: float(c1[k]) * float(c2[k]) for k in common
                   if float(c1[k]) > 0 and float(c2[k]) > 0}
    else:
        raise ValueError(kind)
    return M.partition_load(contrib)


def q_error(gt, pred):
    return M.q_error(gt, pred)


def main():
    os.makedirs(os.path.join(OUT, "results"), exist_ok=True)
    data = load_instance()
    zone_ids = sorted(int(z) for z in data["zones"])

    # ---------- Task 1: consistency check (analytic GT, Jan 2026) ----------
    with open(os.path.join(BASE, "realdata_emr_package", "config",
                           "analytic_gt_calibration.json")) as fh:
        calib = json.load(fh)["instances"]["NYC_TLC_2026-01"]
    gt_loads, thresholds = {}, {}
    mismatches = []
    for pipe, _kind in M.PIPELINES:
        kind, c1, c2, dim, distinct, n_fact = instance_inputs(data, pipe)
        gt = M.exact_load(kind, c1, c2, dim, distinct)
        contrib = M.contribution_map(kind, c1, c2, dim, distinct)
        t_rel = M.threshold_relative(contrib, kind)
        t_abs = (M.threshold_absolute(n_fact) if kind != "distinct"
                 else float(M.TDISTINCT))
        gt_loads[pipe] = gt
        thresholds[pipe] = (t_rel, t_abs, n_fact)
        if gt != EXPECTED_JAN[pipe] or gt != calib[pipe]["gt_load"]:
            mismatches.append((pipe, gt, EXPECTED_JAN[pipe],
                               calib[pipe]["gt_load"]))
        if abs(t_rel - calib[pipe]["threshold_rel"]) > 1e-6 or \
                abs(t_abs - calib[pipe]["threshold_abs"]) > 1e-6:
            mismatches.append((pipe + " threshold", t_rel,
                               calib[pipe]["threshold_rel"], t_abs))
    consistency_ok = not mismatches
    print(f"consistency check: {'PASS' if consistency_ok else 'FAIL'} "
          f"{mismatches}", flush=True)
    if not consistency_ok:
        write_not_executed(data, mismatches)
        return 1

    gt_verdicts_rel = {p: M.verdict(gt_loads[p], thresholds[p][0])
                       for p in SCORED}
    gt_verdicts_abs = {p: M.verdict(gt_loads[p], thresholds[p][1])
                       for p in SCORED}

    # ---------- Tasks 2-4: A8 at each k ----------
    # Non-null count vectors per column; Ratecode NULL handled separately.
    y_rc_nonnull = {k: v for k, v in data["y_rc"].items()
                    if k != M.NULL_TOKEN}
    rc_null = int(data["y_rc"].get(M.NULL_TOKEN, 0))
    rc_universe = sorted(int(k) for k in y_rc_nonnull)
    cols = {"yellow_PULocationID": (data["y_pu"], zone_ids),
            "yellow_DOLocationID": (data["y_do"], zone_ids),
            "green_PULocationID": (data["g_pu"], zone_ids),
            "yellow_RatecodeID": (y_rc_nonnull, rc_universe)}

    per_k = {}
    for k in KS:
        recon, syn_cols = {}, {}
        for name, (counts, universe) in cols.items():
            recon[name], syn_cols[name] = reconstruct_col(counts, k,
                                                          universe)
        # G_N: synthetic A8 carries the NULL group at exact null_mass.
        if rc_null > 0:
            recon["yellow_RatecodeID"] = dict(recon["yellow_RatecodeID"])
            recon["yellow_RatecodeID"][M.NULL_TOKEN] = float(rc_null)
        syn_cols["yellow_RatecodeID"]["null_mass"] = rc_null
        pair_pu = reconstruct_pair(data["y_pu"], data["g_pu"], k, zone_ids)
        pair_do = reconstruct_pair(data["y_do"], data["g_do"], k, zone_ids)
        synopsis = {"dim_rows": len(zone_ids), "cols": syn_cols,
                    "pairs": {"PULocationID": pair_pu[2],
                              "DOLocationID": pair_do[2]}}
        n_bytes = len(json.dumps(synopsis))
        pred = {
            "G_A": pred_load("group", recon["yellow_PULocationID"]),
            "G_B": pred_load("group", recon["yellow_DOLocationID"]),
            "G_C": pred_load("group", recon["green_PULocationID"]),
            "G_N": pred_load("group", recon["yellow_RatecodeID"]),
            "J_A": pred_load("fact_dimension",
                             recon["yellow_PULocationID"],
                             dim=data["zones"]),
            "J_B": pred_load("fact_dimension",
                             recon["yellow_DOLocationID"],
                             dim=data["zones"]),
            "J_N": pred_load("fact_dimension",
                             recon["green_PULocationID"],
                             dim=data["zones"]),
            "F_A": pred_load("fact_fact", pair_pu[0], pair_pu[1]),
            "F_B": pred_load("fact_fact", pair_do[0], pair_do[1]),
            "W_A": pred_load("window", recon["yellow_PULocationID"]),
        }
        per_k[k] = {"pred": pred, "bytes": n_bytes, "synopsis": synopsis}
        print(f"k={k}: bytes={n_bytes} pred={pred}", flush=True)

    # ---------- Task 5: metrics ----------
    summary = {"consistency_ok": True, "gt_loads": gt_loads,
               "gt_verdicts_rel": gt_verdicts_rel,
               "gt_verdicts_abs": gt_verdicts_abs, "per_k": {}}
    rows = []
    env = "local, realdata venv python 3.12.3"
    ds, scale = "NYC_TLC_2026-01", "3724889 yellow + 40272 green rows"

    def add(metric, value, denom, notes, k=None):
        rows.append({"experiment": "E7", "dataset": ds, "scale": scale,
                     "environment": env, "metric": metric, "value": value,
                     "denominator": denom, "classification": "analytical",
                     "artifact": "code/e7_real_data.py",
                     "notes": notes})

    for pipe in SCORED + ["D_A"]:
        add(f"gt_load_{pipe}", gt_loads[pipe], "",
            "analytic GT (model exact_load); F_A/F_B analytic-vs-analytic"
            if pipe in ("F_A", "F_B") else
            "analytic GT (model exact_load); D_A diagnostic, excluded "
            "from scored denominators" if pipe == "D_A" else
            "analytic GT (model exact_load)")
    for k in KS:
        pred = per_k[k]["pred"]
        qs = {p: q_error(gt_loads[p], pred[p]) for p in SCORED}
        agree_rel = sum(1 for p in SCORED
                        if M.verdict(pred[p], thresholds[p][0])
                        == gt_verdicts_rel[p])
        agree_abs = sum(1 for p in SCORED
                        if M.verdict(pred[p], thresholds[p][1])
                        == gt_verdicts_abs[p])
        tp = sum(1 for p in SCORED if gt_verdicts_rel[p] == "FAIL"
                 and M.verdict(pred[p], thresholds[p][0]) == "FAIL")
        fn = sum(1 for p in SCORED if gt_verdicts_rel[p] == "FAIL"
                 and M.verdict(pred[p], thresholds[p][0]) == "PASS")
        worst_under = max(gt_loads[p] / pred[p] if pred[p] > 0
                          else float("inf") for p in SCORED)
        summary["per_k"][k] = {
            "bytes": per_k[k]["bytes"], "pred": pred, "q": qs,
            "agreement_rel": agree_rel, "agreement_abs": agree_abs,
            "median_q": float(np.median(list(qs.values()))),
            "max_q": float(max(qs.values())), "worst_under": worst_under,
            "tp": tp, "fn": fn}
        add(f"synopsis_bytes_k{k}", per_k[k]["bytes"], "",
            "len(json.dumps(synopsis_dict)) default separators (S.2 "
            "synopsis convention); truncated synopsis at k=%d" % k, k)
        add(f"verdict_agreement_rel_k{k}", agree_rel, 10,
            "primary threshold (relative 2x mean); GT all-FAIL saturated "
            "set, agreement == recall here", k)
        add(f"verdict_agreement_abs_k{k}", agree_abs, 10,
            "secondary threshold (absolute 0.02 x N)", k)
        add(f"recall_rel_k{k}", tp / (tp + fn) if tp + fn else "", 10,
            "FAIL is positive class; denominator = 10 GT-FAIL pipelines; "
            "no GT-PASS pipelines exist, precision/specificity vacuous", k)
        add(f"median_q_k{k}", round(float(np.median(list(qs.values()))), 6),
            10, "Q-error over 10 scored pipelines; F_A/F_B "
            "analytic-vs-analytic", k)
        add(f"max_q_k{k}", round(float(max(qs.values())), 6), 10,
            "Q-error over 10 scored pipelines; F_A/F_B "
            "analytic-vs-analytic", k)
        add(f"worst_underprediction_k{k}", round(float(worst_under), 6), 10,
            "max GT/pred over 10 scored pipelines", k)
        for p in SCORED:
            add(f"q_error_{p}_k{k}", round(float(qs[p]), 6), "",
                f"GT {gt_loads[p]} vs pred {pred[p]}; "
                + ("analytic-vs-analytic (GT itself analytic for F)"
                   if p in ("F_A", "F_B") else "analytic GT vs analytic "
                   "prediction"), k)
            add(f"pred_load_{p}_k{k}", pred[p], "",
                "A8 prediction via model partition_load", k)

    fields = ["experiment", "dataset", "scale", "environment", "metric",
              "value", "denominator", "classification", "artifact", "notes"]
    with open(os.path.join(OUT, "results", "rows_e7.csv"), "w",
              newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    with open(os.path.join(OUT, "results", "e7_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=1, default=str)
    print(json.dumps({str(k): {m: summary["per_k"][k][m] for m in
                               ("bytes", "agreement_rel", "agreement_abs",
                                "median_q", "max_q", "worst_under")}
                      for k in KS}, indent=1))
    return 0


def write_not_executed(data, mismatches):
    rows = [{"experiment": "E7", "dataset": "NYC_TLC_2026-01",
             "scale": "3724889 yellow + 40272 green rows",
             "environment": "local, realdata venv python 3.12.3",
             "metric": "status", "value": "NOT EXECUTED", "denominator": "",
             "classification": "analytical", "artifact": "code/e7_real_data.py",
             "notes": "Consistency check failed: analytic GT reproduced "
                      f"from local parquet does not match calibration: "
                      f"{mismatches}"}]
    fields = ["experiment", "dataset", "scale", "environment", "metric",
              "value", "denominator", "classification", "artifact", "notes"]
    with open(os.path.join(OUT, "results", "rows_e7.csv"), "w",
              newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    lines = ["# E7 -- Real-Data Information Efficiency: NOT EXECUTED", "",
             "The frozen consistency check (FINAL_EXECUTION_PROTOCOL S.6) "
             "failed: analytic ground-truth loads reproduced from the "
             "local January 2026 parquet do not match "
             "analytic_gt_calibration.json exactly.", "",
             f"Mismatches (pipeline, reproduced, expected): {mismatches}",
             "", "Per the protocol, the experiment stops here. No A8 "
             "predictions were computed. Consequence for claims: no "
             "real-data information-efficiency result exists in this "
             "campaign; the manuscript may not claim A8 depth behaviour "
             "on real data from this campaign."]
    with open(os.path.join(OUT, "E7_REAL_DATA_INFORMATION_REPORT.md"),
              "w") as fh:
        fh.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    sys.exit(main())
