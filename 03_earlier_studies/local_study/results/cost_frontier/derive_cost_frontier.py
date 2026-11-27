"""Derive Experiment D cost/frontier tables from existing artifacts only.

Reads results/summary.json, results/eqinfo/summary.json,
results/ablation/ablation_summary.json, results/post_review_analysis.json,
data/*/synopsis.json + counts.npz, results/eqinfo/packet_*.json, and the
D-SCALE cloud reports' recorded values (embedded with source pointers;
those two numbers come from the reports, not from re-reading Spark logs).

Re-measurements performed here (files already on disk, no new runs):
  * truncated-synopsis bytes at k=16/64/256: ablation.truncate_syn on each
    on-disk synopsis.json, serialized with json.dumps default separators
    (verified byte-identical to run_dataset.py's on-disk format, 9/9).
  * A4 marginal-counts artifact bytes: np.savez_compressed of exactly the
    6 per-key count arrays A4 consumes (same compression as counts.npz).
  * packet/synopsis/counts/parquet bytes: os.path.getsize on disk.
Outputs: cost_access_table.csv, frontier.csv (same directory).
"""
import csv
import io
import json
import os
import sys

sys.path.insert(0, os.path.expanduser("~/workspace/blind-at-scale/code"))
from ablation import truncate_syn  # noqa: E402

BASE = os.path.expanduser("~/workspace/blind-at-scale")
OUT = os.path.join(BASE, "results", "cost_frontier")
TAGS = [f"{f}_s{s}" for f in ("zipf", "lognormal", "mixed") for s in (101, 202, 303)]


def load(p):
    with open(os.path.join(BASE, p)) as f:
        return json.load(f)


def stats(vals):
    return round(sum(vals) / len(vals), 1), min(vals), max(vals)


def main():
    main_sum = load("results/summary.json")
    eq_sum = load("results/eqinfo/summary.json")
    abl_sum = load("results/ablation/ablation_summary.json")
    # Experiment A (2026-11-24): classical baseline A9, filled per the
    # COST_FRONTIER_REPORT.md section 5 protocol. Metrics from
    # summary_exp_a.json; packet bytes measured on disk; J-family median
    # Q computed from the per-instance predictions CSVs (27 J instances).
    expa_sum = load("results/exp_a/summary_exp_a.json")
    a9 = expa_sum["arms"]["A9"]
    a9_bytes = [os.path.getsize(os.path.join(
        BASE, f"results/exp_a/packet_A9_{t}.json")) for t in TAGS]
    a9_mean, a9_min, a9_max = stats(a9_bytes)
    a9_j_q = []
    for t in TAGS:
        with open(os.path.join(
                BASE, f"results/exp_a/predictions_exp_a_{t}.csv")) as f:
            for row in csv.DictReader(f):
                if row["arm"] == "A9" and row["pipeline"] in (
                        "J_A", "J_B", "J_N"):
                    pred, gt = float(row["pred_load"]), float(row["gt_load"])
                    a9_j_q.append(max(pred / gt, gt / pred))
    a9_j_q.sort()
    a9_j_med = a9_j_q[len(a9_j_q) // 2]

    # ---- measured byte sizes -------------------------------------------
    syn_full = [os.path.getsize(os.path.join(BASE, f"data/{t}/synopsis.json"))
                for t in TAGS]
    syn_mean, syn_min, syn_max = stats(syn_full)
    trunc = {k: [] for k in (16, 64, 256)}
    for t in TAGS:
        syn = load(f"data/{t}/synopsis.json")
        # verify default-separator serialization == on-disk file, per dataset
        assert len(json.dumps(syn)) == os.path.getsize(
            os.path.join(BASE, f"data/{t}/synopsis.json")), t
        for k in trunc:
            trunc[k].append(len(json.dumps(truncate_syn(syn, k))))
    trunc_stats = {k: stats(v) for k, v in trunc.items()}
    pkt = {}
    for arm in ("A7", "A7X"):
        vals = [os.path.getsize(os.path.join(
            BASE, f"results/eqinfo/packet_{arm}_{t}.json")) for t in TAGS]
        pkt[arm] = stats(vals)
    # A4: savez_compressed of exactly the 6 arrays A4 consumes
    import numpy as np
    a4_vals = []
    for t in TAGS:
        z = np.load(os.path.join(BASE, f"data/{t}/counts.npz"))
        arrs = {f"fact_{c}": z[f"fact_{c}"] for c in ("kA", "kB", "kC", "kN")}
        arrs.update({f"fact2_{c}": z[f"fact2_{c}"] for c in ("kA", "kB")})
        buf = io.BytesIO()
        np.savez_compressed(buf, **arrs)
        a4_vals.append(len(buf.getvalue()))
    a4_mean, a4_min, a4_max = stats(a4_vals)

    # ---- cost / access table -------------------------------------------
    m = main_sum["arms"]
    e = eq_sum["arms"]
    tb = main_sum["testbed_rows"]
    tb_eq = eq_sum["testbed_rows"]
    rows = [
        {
            "arm": "A1", "arm_type": "uniform sample (r=0.01)",
            "recall": m["A1"]["recall"], "precision": m["A1"]["precision"],
            "f1": m["A1"]["f1"], "spearman_rho": m["A1"]["spearman_rho"],
            "median_q_error": m["A1"]["median_q_error"],
            "tp": m["A1"]["tp"], "fp": m["A1"]["fp"], "fn": m["A1"]["fn"],
            "tn": m["A1"]["tn"], "n_instances": 99,
            "side_info_bytes_mean": "NOT MEASURED",
            "side_info_bytes_min": "", "side_info_bytes_max": "",
            "side_info_bytes_method": "no testbed artifact on disk; "
                "sampling is count-level (arms.py A1)",
            "testbed_fact_rows_mean": round(tb["A1"], 1),
            "rows_scanned_fact": 10000000, "rows_scanned_fact2": 6000000,
            "dim_read": "yes", "dim_columns_read": "dim_key only",
            "dim_keys_retained": "mean 201.4 of 20000 (measured)",
            "profiler_passes": "1 per table (Bernoulli decision per row)",
            "privileges_access": "row-level read of fact+fact2 key cols "
                "(kA,kB,kC,kN; kA,kB) + dim keys; sampled rows leave source",
            "storage_persistent": "testbed rows only (bytes NOT MEASURED)",
            "source": "results/summary.json; code/arms.py A1; "
                "post_review_analysis.json A1_dim_keys_retained_mean",
        },
        {
            "arm": "A2", "arm_type": "stratified (hash buckets, 10%)",
            "recall": m["A2"]["recall"], "precision": m["A2"]["precision"],
            "f1": m["A2"]["f1"], "spearman_rho": m["A2"]["spearman_rho"],
            "median_q_error": m["A2"]["median_q_error"],
            "tp": m["A2"]["tp"], "fp": m["A2"]["fp"], "fn": m["A2"]["fn"],
            "tn": m["A2"]["tn"], "n_instances": 99,
            "side_info_bytes_mean": "NOT MEASURED",
            "side_info_bytes_min": "", "side_info_bytes_max": "",
            "side_info_bytes_method": "no testbed artifact on disk; "
                "sampling is count-level (arms.py A2)",
            "testbed_fact_rows_mean": round(tb["A2"], 1),
            "rows_scanned_fact": 10000000, "rows_scanned_fact2": 6000000,
            "dim_read": "yes", "dim_columns_read": "dim_key only",
            "dim_keys_retained": "~10% of keys by hash bucket (by design)",
            "profiler_passes": "1 per table (data-independent hash decision)",
            "privileges_access": "row-level read of fact+fact2 key cols "
                "+ dim keys; sampled rows leave source",
            "storage_persistent": "testbed rows only (bytes NOT MEASURED)",
            "source": "results/summary.json; code/arms.py A2",
        },
        {
            "arm": "A3", "arm_type": "FK-subsetting (10% dim keys)",
            "recall": m["A3"]["recall"], "precision": m["A3"]["precision"],
            "f1": m["A3"]["f1"], "spearman_rho": m["A3"]["spearman_rho"],
            "median_q_error": m["A3"]["median_q_error"],
            "tp": m["A3"]["tp"], "fp": m["A3"]["fp"], "fn": m["A3"]["fn"],
            "tn": m["A3"]["tn"], "n_instances": 99,
            "side_info_bytes_mean": "NOT MEASURED",
            "side_info_bytes_min": "", "side_info_bytes_max": "",
            "side_info_bytes_method": "no testbed artifact on disk; "
                "sampling is count-level (arms.py A3)",
            "testbed_fact_rows_mean": round(tb["A3"], 1),
            "rows_scanned_fact": 10000000, "rows_scanned_fact2": 6000000,
            "dim_read": "yes", "dim_columns_read": "dim_key only",
            "dim_keys_retained": "10% of keys, random (2000 expected)",
            "profiler_passes": "1 per table + dim-key selection pre-step",
            "privileges_access": "row-level read of fact+fact2 key cols "
                "+ dim keys; sampled rows leave source",
            "storage_persistent": "testbed rows only (bytes NOT MEASURED)",
            "source": "results/summary.json; code/arms.py A3",
        },
        {
            "arm": "A4", "arm_type": "marginal synthesis",
            "recall": m["A4"]["recall"], "precision": m["A4"]["precision"],
            "f1": m["A4"]["f1"], "spearman_rho": m["A4"]["spearman_rho"],
            "median_q_error": m["A4"]["median_q_error"],
            "tp": m["A4"]["tp"], "fp": m["A4"]["fp"], "fn": m["A4"]["fn"],
            "tn": m["A4"]["tn"], "n_instances": 99,
            "side_info_bytes_mean": a4_mean,
            "side_info_bytes_min": a4_min, "side_info_bytes_max": a4_max,
            "side_info_bytes_method": "re-measured: np.savez_compressed of "
                "exactly the 6 per-key count arrays A4 consumes "
                "(fact kA/kB/kC/kN + fact2 kA/kB); raw 960000 B",
            "testbed_fact_rows_mean": round(tb["A4"], 1),
            "rows_scanned_fact": 10000000, "rows_scanned_fact2": 6000000,
            "dim_read": "no", "dim_columns_read": "none (assumes full dim)",
            "dim_keys_retained": "n/a (all K keys assumed present)",
            "profiler_passes": "1 per table (full per-key marginals)",
            "privileges_access": "aggregate read (per-key GROUP BY counts) "
                "of fact+fact2 key cols; no row-level data retained",
            "storage_persistent": "full marginal count vectors (see bytes)",
            "source": "results/summary.json; code/arms.py A4; re-measured "
                "from data/*/counts.npz by derive_cost_frontier.py",
        },
        {
            "arm": "A5", "arm_type": "synopsis testbed (r=0.01)",
            "recall": m["A5"]["recall"], "precision": m["A5"]["precision"],
            "f1": m["A5"]["f1"], "spearman_rho": m["A5"]["spearman_rho"],
            "median_q_error": m["A5"]["median_q_error"],
            "tp": m["A5"]["tp"], "fp": m["A5"]["fp"], "fn": m["A5"]["fn"],
            "tn": m["A5"]["tn"], "n_instances": 99,
            "side_info_bytes_mean": syn_mean,
            "side_info_bytes_min": syn_min, "side_info_bytes_max": syn_max,
            "side_info_bytes_method": "os.path.getsize of on-disk "
                "data/*/synopsis.json (json.dump default separators)",
            "testbed_fact_rows_mean": round(tb["A5"], 1),
            "rows_scanned_fact": 10000000, "rows_scanned_fact2": 6000000,
            "dim_read": "no", "dim_columns_read": "none (dim_rows=K scalar)",
            "dim_keys_retained": "n/a (dim emitted in full by method)",
            "profiler_passes": "1 per table (synopsis build)",
            "privileges_access": "aggregate read (top-1024 HH + totals + "
                "tail masses + aligned pair counts); only synopsis leaves "
                "the profiler",
            "storage_persistent": "synopsis JSON (see bytes)",
            "source": "results/summary.json (synopsis_bytes 91379 for "
                "zipf_s101); data/*/synopsis.json on-disk sizes; "
                "code/arms.py build_synopsis",
        },
        {
            "arm": "A7", "arm_type": "HH-stratified sampler (ids-only pkt)",
            "recall": e["A7"]["recall"], "precision": e["A7"]["precision"],
            "f1": e["A7"]["f1"], "spearman_rho": e["A7"]["spearman_rho"],
            "median_q_error": e["A7"]["median_q_error"],
            "tp": e["A7"]["tp"], "fp": e["A7"]["fp"], "fn": e["A7"]["fn"],
            "tn": e["A7"]["tn"], "n_instances": 99,
            "side_info_bytes_mean": pkt["A7"][0],
            "side_info_bytes_min": pkt["A7"][1],
            "side_info_bytes_max": pkt["A7"][2],
            "side_info_bytes_method": "os.path.getsize of on-disk "
                "results/eqinfo/packet_A7_*.json (compact JSON)",
            "testbed_fact_rows_mean": round(tb_eq["A7"], 1),
            "rows_scanned_fact": 10000000, "rows_scanned_fact2": 6000000,
            "dim_read": "no", "dim_columns_read": "none (dim_rows=K scalar)",
            "dim_keys_retained": "n/a (dim emitted in full by method)",
            "profiler_passes": "2 (profiler pass for HH lists, then "
                "stratified sampling pass; strata need the HH list first)",
            "privileges_access": "aggregate read for packet (as A5) + "
                "row-level stratified sample leaves source",
            "storage_persistent": "ids-only HH packet (see bytes) + "
                "sampled testbed rows",
            "source": "results/eqinfo/summary.json; "
                "results/eqinfo/packet_A7_*.json on-disk sizes; "
                "code/eqinfo_arms.py build_packet_A7",
        },
        {
            "arm": "A7X", "arm_type": "exact-HH sampler (counts packet)",
            "recall": e["A7X"]["recall"], "precision": e["A7X"]["precision"],
            "f1": e["A7X"]["f1"], "spearman_rho": e["A7X"]["spearman_rho"],
            "median_q_error": e["A7X"]["median_q_error"],
            "tp": e["A7X"]["tp"], "fp": e["A7X"]["fp"], "fn": e["A7X"]["fn"],
            "tn": e["A7X"]["tn"], "n_instances": 99,
            "side_info_bytes_mean": pkt["A7X"][0],
            "side_info_bytes_min": pkt["A7X"][1],
            "side_info_bytes_max": pkt["A7X"][2],
            "side_info_bytes_method": "os.path.getsize of on-disk "
                "results/eqinfo/packet_A7X_*.json (compact JSON)",
            "testbed_fact_rows_mean": round(tb_eq["A7X"], 1),
            "rows_scanned_fact": 10000000, "rows_scanned_fact2": 6000000,
            "dim_read": "no", "dim_columns_read": "none (dim_rows=K scalar)",
            "dim_keys_retained": "n/a (dim emitted in full by method)",
            "profiler_passes": "2 (profiler pass for HH lists+counts, "
                "then tail sampling pass)",
            "privileges_access": "aggregate read for packet (as A5) + "
                "row-level tail sample leaves source (HH rows known)",
            "storage_persistent": "counts packet (see bytes) + sampled "
                "tail rows",
            "source": "results/eqinfo/summary.json; "
                "results/eqinfo/packet_A7X_*.json on-disk sizes; "
                "code/eqinfo_arms.py build_packet_A7X",
        },
        {
            "arm": "A8", "arm_type": "direct prediction (synopsis only)",
            "recall": m["A8"]["recall"], "precision": m["A8"]["precision"],
            "f1": m["A8"]["f1"], "spearman_rho": m["A8"]["spearman_rho"],
            "median_q_error": m["A8"]["median_q_error"],
            "tp": m["A8"]["tp"], "fp": m["A8"]["fp"], "fn": m["A8"]["fn"],
            "tn": m["A8"]["tn"], "n_instances": 99,
            "side_info_bytes_mean": syn_mean,
            "side_info_bytes_min": syn_min, "side_info_bytes_max": syn_max,
            "side_info_bytes_method": "os.path.getsize of on-disk "
                "data/*/synopsis.json (same synopsis as A5)",
            "testbed_fact_rows_mean": round(tb["A8"], 1),
            "rows_scanned_fact": 10000000, "rows_scanned_fact2": 6000000,
            "dim_read": "no", "dim_columns_read": "none (dim_rows=K scalar)",
            "dim_keys_retained": "n/a (dim emitted in full by method)",
            "profiler_passes": "1 per table (synopsis build); prediction "
                "needs no further data access",
            "privileges_access": "aggregate read (as A5); only synopsis "
                "leaves the profiler; no testbed materialized",
            "storage_persistent": "synopsis JSON (see bytes)",
            "source": "results/summary.json; data/*/synopsis.json "
                "on-disk sizes; code/arms.py A8",
        },
        {
            "arm": "A6 (supplementary)", "arm_type": "A5 + feedback round",
            "recall": "", "precision": "", "f1": "", "spearman_rho": "",
            "median_q_error": "NOT MEASURED (feedback study reports "
                "repair rates, not Q-error)",
            "tp": "", "fp": "", "fn": "", "tn": "", "n_instances": 99,
            "side_info_bytes_mean": syn_mean,
            "side_info_bytes_min": syn_min, "side_info_bytes_max": syn_max,
            "side_info_bytes_method": "A5 synopsis + feedback verdict "
                "packet (11 verdicts/dataset; packet bytes NOT MEASURED, "
                "no packet artifact)",
            "testbed_fact_rows_mean": round(tb["A5"], 1),
            "rows_scanned_fact": 10000000, "rows_scanned_fact2": 6000000,
            "dim_read": "no", "dim_columns_read": "none (as A5)",
            "dim_keys_retained": "n/a",
            "profiler_passes": "1 per table (as A5) + 1 feedback round "
                "(no new data access; reuses A5 verdicts)",
            "privileges_access": "as A5; feedback packet is verdicts only",
            "storage_persistent": "synopsis JSON + verdict packet",
            "source": "results/feedback_summary.json (FB-SYN repaired "
                "51/51); code/feedback.py",
        },
        {
            "arm": "A9 (classical)",
            "arm_type": "classical estimator: end-biased sampling "
                "(Estan-Naughton 2006); Eq. 1 for F loads, HT per-value "
                "reconstruction for single-table loads",
            "recall": a9["recall"], "precision": a9["precision"],
            "f1": a9["f1"], "spearman_rho": a9["spearman_rho"],
            "median_q_error": a9["median_q_error"], "tp": a9["tp"],
            "fp": a9["fp"], "fn": a9["fn"], "tn": a9["tn"],
            "n_instances": a9["n"],
            "side_info_bytes_mean": a9_mean, "side_info_bytes_min": a9_min,
            "side_info_bytes_max": a9_max,
            "side_info_bytes_method": "os.path.getsize of "
                "results/exp_a/packet_A9_*.json (compact JSON, parallel "
                "keys/counts arrays per column, A7X packet convention); "
                "per-dataset 58601-64382 B",
            "testbed_fact_rows_mean": 0.0,
            "rows_scanned_fact": 10000000, "rows_scanned_fact2": 6000000,
            "dim_read": "no",
            "dim_columns_read": "none (dim_rows=K scalar; full dim "
                "assumed, as A5/A8)",
            "dim_keys_retained": "n/a (dim emitted in full by method)",
            "profiler_passes": "2 (frequency-list pass, then shared-hash "
                "sampling decisions; construction needs the full frequency "
                "list, per Estan-Naughton - not read-once)",
            "privileges_access": "aggregate read only (full per-key "
                "frequency lists, as the synopsis profiler); only the "
                "end-biased sample packet leaves the profiler; no testbed "
                "materialized",
            "storage_persistent": "end-biased sample packet (see bytes)",
            "source": "results/exp_a/summary_exp_a.json (pooled metrics); "
                "results/exp_a/packet_A9_*.json (sizes measured on disk); "
                "code/classical_arms.py (construction per Estan-Naughton "
                "sec. 3.1, Eq. 1 for F)",
        },
    ]
    fields = ["arm", "arm_type", "recall", "precision", "f1", "spearman_rho",
              "median_q_error", "tp", "fp", "fn", "tn", "n_instances",
              "side_info_bytes_mean", "side_info_bytes_min",
              "side_info_bytes_max", "side_info_bytes_method",
              "testbed_fact_rows_mean", "rows_scanned_fact",
              "rows_scanned_fact2", "dim_read", "dim_columns_read",
              "dim_keys_retained", "profiler_passes", "privileges_access",
              "storage_persistent", "source"]
    with open(os.path.join(OUT, "cost_access_table.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # ---- frontier --------------------------------------------------------
    ab = abl_sum["rungs"]
    fpts = []
    for rung, k in (("hh_k16", 16), ("hh_k64", 64), ("hh_k256", 256)):
        p = ab[rung]["pooled"]
        fpts.append({
            "point": f"A8_k{k}", "arm": "A8", "hh_depth_k": k,
            "side_info_bytes_mean": trunc_stats[k][0],
            "side_info_bytes_min": trunc_stats[k][1],
            "side_info_bytes_max": trunc_stats[k][2],
            "median_q_error": p["median_q_error"],
            "median_q_error_j_family":
                ab[rung]["per_pipeline_family"]["J"]["median_q_error"],
            "recall": p["recall"], "spearman_rho": p["spearman_rho"],
            "n_instances": 99, "q_error_basis": "local_99inst",
            "pareto_optimal": "yes" if k in (16, 64) else "no",
            "source": "results/ablation/ablation_summary.json; truncated "
                "synopsis bytes re-measured by derive_cost_frontier.py",
        })
    fpts.append({
        "point": "A8_k1024", "arm": "A8", "hh_depth_k": 1024,
        "side_info_bytes_mean": syn_mean,
        "side_info_bytes_min": syn_min, "side_info_bytes_max": syn_max,
        "median_q_error": m["A8"]["median_q_error"],
        "median_q_error_j_family":
            ab["full_k1024"]["per_pipeline_family"]["J"]["median_q_error"],
        "recall": m["A8"]["recall"], "spearman_rho": m["A8"]["spearman_rho"],
        "n_instances": 99, "q_error_basis": "local_99inst",
        "pareto_optimal": "yes",
        "source": "results/summary.json; data/*/synopsis.json on-disk; "
            "ablation full_k1024 control reproduces A8 to CSV rounding",
    })
    fpts.append({
        "point": "A5_k1024", "arm": "A5", "hh_depth_k": 1024,
        "side_info_bytes_mean": syn_mean,
        "side_info_bytes_min": syn_min, "side_info_bytes_max": syn_max,
        "median_q_error": m["A5"]["median_q_error"],
        "median_q_error_j_family": "",
        "recall": m["A5"]["recall"], "spearman_rho": m["A5"]["spearman_rho"],
        "n_instances": 99, "q_error_basis": "local_99inst",
        "pareto_optimal": "no",
        "source": "results/summary.json; data/*/synopsis.json on-disk",
    })
    for arm, mqj in (("A7", eq_sum["median_q_by_pipeline_group"]["J"]["A7"]),
                     ("A7X", eq_sum["median_q_by_pipeline_group"]["J"]["A7X"])):
        fpts.append({
            "point": f"{arm}_k1024", "arm": arm, "hh_depth_k": 1024,
            "side_info_bytes_mean": pkt[arm][0],
            "side_info_bytes_min": pkt[arm][1],
            "side_info_bytes_max": pkt[arm][2],
            "median_q_error": e[arm]["median_q_error"],
            "median_q_error_j_family": mqj,
            "recall": e[arm]["recall"],
            "spearman_rho": e[arm]["spearman_rho"],
            "n_instances": 99, "q_error_basis": "local_99inst",
            "pareto_optimal": "no",
            "source": "results/eqinfo/summary.json; "
                f"results/eqinfo/packet_{arm}_*.json on-disk",
        })
    fpts.append({
        "point": "A4_full_marginals", "arm": "A4", "hh_depth_k": "K=20000",
        "side_info_bytes_mean": a4_mean,
        "side_info_bytes_min": a4_min, "side_info_bytes_max": a4_max,
        "median_q_error": m["A4"]["median_q_error"],
        "median_q_error_j_family": "",
        "recall": m["A4"]["recall"], "spearman_rho": m["A4"]["spearman_rho"],
        "n_instances": 99, "q_error_basis": "local_99inst",
        "pareto_optimal": "no",
        "source": "results/summary.json; 6-array counts artifact "
            "re-measured by derive_cost_frontier.py",
    })
    # scale ladder: A5/A8 vs Databricks GT (different Q basis; 9 pipelines)
    for scale, syn_b, q5, q8 in ((10000000, 91379, 1.081, ""),
                                (40000000, 96514, 1.081, 1.090),
                                (100000000, 101586, 1.085, 1.089)):
        fpts.append({
            "point": f"A5_scale_{scale // 1000000}M", "arm": "A5",
            "hh_depth_k": 1024, "side_info_bytes_mean": syn_b,
            "side_info_bytes_min": syn_b, "side_info_bytes_max": syn_b,
            "median_q_error": q5, "median_q_error_j_family": "",
            "recall": 1.0, "spearman_rho": "",
            "n_instances": 9, "q_error_basis": "databricks_9pipe",
            "pareto_optimal": "n/a (different Q basis)",
            "source": "cloud_program/results/EXECUTION_REPORT.md; "
                "cloud_program/results/d-scale-40m/D_SCALE_40M_REPORT.md; "
                "cloud_program/results/d-scale-100m/D_SCALE_100M_REPORT.md",
        })
        if q8 != "":
            fpts.append({
                "point": f"A8_scale_{scale // 1000000}M", "arm": "A8",
                "hh_depth_k": 1024, "side_info_bytes_mean": syn_b,
                "side_info_bytes_min": syn_b, "side_info_bytes_max": syn_b,
                "median_q_error": q8, "median_q_error_j_family": "",
                "recall": 1.0, "spearman_rho": "",
                "n_instances": 9, "q_error_basis": "databricks_9pipe",
                "pareto_optimal": "n/a (different Q basis)",
                "source": "cloud_program/results/d-scale-40m/"
                    "D_SCALE_40M_REPORT.md; cloud_program/results/"
                    "d-scale-100m/D_SCALE_100M_REPORT.md",
            })
    fpts.append({
        "point": "CLASSICAL_k1024", "arm": "CLASSICAL (A9)",
        "hh_depth_k": "1024 (expected retained per column; realised "
            "1044-1104)",
        "side_info_bytes_mean": a9_mean, "side_info_bytes_min": a9_min,
        "side_info_bytes_max": a9_max,
        "median_q_error": a9["median_q_error"],
        "median_q_error_j_family": a9_j_med,
        "recall": a9["recall"], "spearman_rho": a9["spearman_rho"],
        "n_instances": a9["n"], "q_error_basis": "local_99inst",
        "pareto_optimal": "no (dominated by A8_k64: 6744 B / 1.000043, "
            "and by A7X_k1024: 58471 B / 1.000487 - both fewer bytes AND "
            "lower median Q)",
        "source": "results/exp_a/summary_exp_a.json; "
            "results/exp_a/packet_A9_*.json (sizes measured on disk); "
            "J-family median Q computed from "
            "results/exp_a/predictions_exp_a_*.csv (27 J instances)",
    })
    ffields = ["point", "arm", "hh_depth_k", "side_info_bytes_mean",
               "side_info_bytes_min", "side_info_bytes_max",
               "median_q_error", "median_q_error_j_family", "recall",
               "spearman_rho", "n_instances", "q_error_basis",
               "pareto_optimal", "source"]
    with open(os.path.join(OUT, "frontier.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ffields)
        w.writeheader()
        for r in fpts:
            w.writerow(r)
    print("synopsis mean/min/max:", syn_mean, syn_min, syn_max)
    print("trunc:", trunc_stats)
    print("packets:", pkt)
    print("A4:", a4_mean, a4_min, a4_max)
    print("wrote cost_access_table.csv (%d rows) and frontier.csv (%d rows)"
          % (len(rows), len(fpts)))


if __name__ == "__main__":
    main()
