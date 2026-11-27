"""Scorer for equal-information arms A7 / A7X (Amendment 5 Section A5.1).

Mirrors code/score.py conventions exactly (FAIL positive; pooled over
99 instances; Spearman rho on log10 loads; median Q-error =
median(max(p/g, g/p)); McNemar exact via scipy.stats.binomtest on
verdict-correctness discordants; paired bootstrap of delta-recall,
10,000 resamples, np.random.default_rng(7)).

Reads:
  - data/<tag>/predictions_eqinfo.csv  (A7, A7X; this study)
  - data/<tag>/predictions_main.csv     (A5 recorded verdicts/loads;
                                         also the A1 sanity reproduction
                                         and the GT-verdict cross-check)
  - results/summary.json                (A1-A5/A8 comparison values)
  - results/eqinfo/packet_{A7,A7X}_<tag>.json (byte sizes via getsize)

Writes results/eqinfo/summary.json and results/eqinfo/metrics.csv.
"""
import csv, json, os
import numpy as np
from scipy import stats

BASE = os.path.expanduser("~/workspace/blind-at-scale")
DATASETS = [f"{f}_s{s}" for f in ("zipf", "lognormal", "mixed")
            for s in (101, 202, 303)]
ARMS = ["A7", "A7X"]
REF_ARMS = ["A1", "A2", "A3", "A4", "A5", "A8"]


def load_preds(tags, rtag):
    rows = []
    for tag in tags:
        with open(os.path.join(BASE, "data", tag,
                               f"predictions_{rtag}.csv")) as f:
            rows.extend(csv.DictReader(f))
    return rows


def prf(sub):
    tp = sum(1 for r in sub if r["gt"] == "FAIL" and r["pred"] == "FAIL")
    fp = sum(1 for r in sub if r["gt"] == "PASS" and r["pred"] == "FAIL")
    fn = sum(1 for r in sub if r["gt"] == "FAIL" and r["pred"] == "PASS")
    tn = sum(1 for r in sub if r["gt"] == "PASS" and r["pred"] == "PASS")
    rec = tp / (tp + fn) if tp + fn else float("nan")
    prec = tp / (tp + fp) if tp + fp else float("nan")
    f1 = 2 * rec * prec / (rec + prec) if rec + prec else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "recall": rec, "precision": prec, "f1": f1, "n": len(sub)}


def load_metrics(sub_inst, arm):
    loads = np.array([e[arm + "_load"] for e in sub_inst])
    gt = np.array([e["gt_load"] for e in sub_inst])
    rho = stats.spearmanr(np.log10(loads), np.log10(gt)).statistic
    q = np.maximum(loads / gt, gt / loads)
    m = prf([{"gt": e["gt"], "pred": e[arm]} for e in sub_inst])
    m["spearman_rho"] = float(rho)
    m["median_q_error"] = float(np.median(q))
    return m


def median_q(sub_inst, arm):
    loads = np.array([e[arm + "_load"] for e in sub_inst])
    gt = np.array([e["gt_load"] for e in sub_inst])
    return float(np.median(np.maximum(loads / gt, gt / loads)))


def main():
    with open(os.path.join(BASE, "results", "summary.json")) as f:
        ref_summary = json.load(f)

    eq_rows = load_preds(DATASETS, "eqinfo")
    main_rows = load_preds(DATASETS, "main")

    # ---- instance table: gt + A7/A7X (eqinfo) + A5 (recorded, main) ----
    inst = {}
    for r in eq_rows:
        key = (r["dataset"], r["pipeline"])
        e = inst.setdefault(key, {"dataset": r["dataset"],
                                  "family": r["family"],
                                  "pipeline": r["pipeline"],
                                  "gt": r["gt_verdict"],
                                  "gt_load": float(r["gt_load"]),
                                  "n_fact": int(r["n_fact"]),
                                  "synopsis_bytes": int(r["synopsis_bytes"])})
        e[r["arm"]] = r["pred_verdict"]
        e[r["arm"] + "_load"] = float(r["pred_load"])
        e["size_" + r["arm"]] = int(r["testbed_fact_rows"])
    gt_main = {}
    for r in main_rows:
        key = (r["dataset"], r["pipeline"])
        gt_main.setdefault(key, (r["gt_verdict"], float(r["gt_load"])))
        if r["arm"] == "A5":
            e = inst[key]
            e["A5"] = r["pred_verdict"]
            e["A5_load"] = float(r["pred_load"])
            e["size_A5"] = int(r["testbed_fact_rows"])
    inst = list(inst.values())

    # ---- GT cross-check vs predictions_main.csv -------------------------
    gt_mismatch = sum(1 for e in inst
                      if gt_main[(e["dataset"], e["pipeline"])][0] != e["gt"])
    gt_load_maxdiff = max(
        abs(gt_main[(e["dataset"], e["pipeline"])][1] - e["gt_load"])
        for e in inst)

    # ---- sanity: reproduce A1 pooled numbers from predictions_main.csv --
    a1_rows = [{"gt": r["gt_verdict"], "pred": r["pred_verdict"]}
               for r in main_rows if r["arm"] == "A1"]
    a1_prf = prf(a1_rows)
    a1_loads = np.array([float(r["pred_load"]) for r in main_rows
                         if r["arm"] == "A1"])
    a1_gt = np.array([float(r["gt_load"]) for r in main_rows
                      if r["arm"] == "A1"])
    a1_rho = float(stats.spearmanr(np.log10(a1_loads),
                                   np.log10(a1_gt)).statistic)
    a1_q = float(np.median(np.maximum(a1_loads / a1_gt, a1_gt / a1_loads)))
    ref_a1 = ref_summary["arms"]["A1"]
    sanity = {
        "arm": "A1", "source": "predictions_main.csv via score_eqinfo.prf",
        "reproduced": {"recall": a1_prf["recall"],
                       "precision": a1_prf["precision"],
                       "f1": a1_prf["f1"], "spearman_rho": a1_rho,
                       "median_q_error": a1_q,
                       "tp": a1_prf["tp"], "fp": a1_prf["fp"],
                       "fn": a1_prf["fn"], "tn": a1_prf["tn"]},
        "reference_results_summary_json": {
            "recall": ref_a1["recall"], "precision": ref_a1["precision"],
            "f1": ref_a1["f1"], "spearman_rho": ref_a1["spearman_rho"],
            "median_q_error": ref_a1["median_q_error"],
            "tp": ref_a1["tp"], "fp": ref_a1["fp"],
            "fn": ref_a1["fn"], "tn": ref_a1["tn"]},
        "matches": bool(
            a1_prf["tp"] == ref_a1["tp"] and a1_prf["fp"] == ref_a1["fp"]
            and a1_prf["fn"] == ref_a1["fn"] and a1_prf["tn"] == ref_a1["tn"]
            and abs(a1_prf["recall"] - ref_a1["recall"]) < 1e-12
            and abs(a1_rho - ref_a1["spearman_rho"]) < 1e-9
            and abs(a1_q - ref_a1["median_q_error"]) < 1e-9),
    }

    S = {"study": "Amendment 5 A5.1 equal-information arms A7/A7X",
         "n_instances": len(inst),
         "n_gt_fail": sum(1 for e in inst if e["gt"] == "FAIL"),
         "arms": {}, "per_distribution_family": {},
         "per_pipeline_family": {}, "median_q_by_pipeline_group": {},
         "testbed_rows": {}, "testbed_rows_per_dataset": {},
         "packet_bytes": {}, "gt_verdict_cross_check": {
             "n_instances": len(inst), "mismatches_vs_predictions_main": gt_mismatch,
             "max_abs_gt_load_diff_vs_predictions_main": gt_load_maxdiff},
         "sanity_check_A1_reproduction": sanity,
         "reference_arms_from_results_summary_json": ref_summary["arms"],
         "reference_testbed_rows_from_results_summary_json":
             ref_summary["testbed_rows"]}

    for arm in ARMS:
        S["arms"][arm] = load_metrics(inst, arm)
        S["testbed_rows"][arm] = float(
            np.mean([e["size_" + arm] for e in inst]))
        S["testbed_rows_per_dataset"][arm] = {}
    # per-dataset testbed sizes (size is per-dataset, repeated per pipeline)
    seen = {}
    for e in inst:
        for arm in ARMS:
            seen.setdefault((arm, e["dataset"]), e["size_" + arm])
    for (arm, tag), size in sorted(seen.items()):
        S["testbed_rows_per_dataset"][arm][tag] = size

    for fam in ("zipf", "lognormal", "mixed"):
        sub = [e for e in inst if e["family"] == fam]
        S["per_distribution_family"][fam] = {
            arm: load_metrics(sub, arm) for arm in ARMS}
    pfam = {"direct (G,D)": ("G_A", "G_B", "G_C", "G_N", "D_A"),
            "join-emergent (J,F)": ("J_A", "J_B", "J_N", "F_A", "F_B"),
            "window (W)": ("W_A",)}
    for name, pids in pfam.items():
        sub = [e for e in inst if e["pipeline"] in pids]
        S["per_pipeline_family"][name] = {
            arm: load_metrics(sub, arm) for arm in ARMS}
    for name, pids in {"F": ("F_A", "F_B"),
                       "J": ("J_A", "J_B", "J_N")}.items():
        sub = [e for e in inst if e["pipeline"] in pids]
        S["median_q_by_pipeline_group"][name] = {
            arm: median_q(sub, arm) for arm in ARMS}
        S["median_q_by_pipeline_group"][name]["n"] = len(sub)

    # ---- packet byte sizes ----------------------------------------------
    max_ratio = {}
    for tag in DATASETS:
        syn_b = os.path.getsize(os.path.join(BASE, "data", tag,
                                             "synopsis.json"))
        entry = {"synopsis_bytes": syn_b}
        for arm in ARMS:
            pb = os.path.getsize(os.path.join(
                BASE, "results", "eqinfo", f"packet_{arm}_{tag}.json"))
            entry[arm + "_packet_bytes"] = pb
            entry[arm + "_ratio_to_synopsis"] = pb / syn_b
            max_ratio[arm] = max(max_ratio.get(arm, 0.0), pb / syn_b)
        S["packet_bytes"][tag] = entry
    S["max_packet_synopsis_ratio"] = max_ratio
    S["packet_exceeds_synopsis_any"] = {
        arm: bool(max_ratio[arm] > 1.0) for arm in ARMS}

    # ---- McNemar exact: A5 vs A7, A5 vs A7X (verdict correctness) -------
    def mcnemar(other):
        only_a5 = sum(1 for e in inst
                      if (e["A5"] == e["gt"]) and (e[other] != e["gt"]))
        only_other = sum(1 for e in inst
                         if (e[other] == e["gt"]) and (e["A5"] != e["gt"]))
        p = stats.binomtest(min(only_a5, only_other),
                            only_a5 + only_other, 0.5).pvalue \
            if only_a5 + only_other else 1.0
        return {"only_A5_correct": only_a5,
                "only_other_correct": only_other, "p_exact": float(p)}
    S["mcnemar_A5_vs_A7"] = mcnemar("A7")
    S["mcnemar_A5_vs_A7X"] = mcnemar("A7X")

    # ---- paired bootstrap delta-recall (A5 - arm), seed 7, 10,000 ------
    def recall(data, arm):
        tp = sum(1 for e in data if e["gt"] == "FAIL" and e[arm] == "FAIL")
        fn = sum(1 for e in data if e["gt"] == "FAIL" and e[arm] == "PASS")
        return tp / (tp + fn) if tp + fn else float("nan")
    rng = np.random.default_rng(7)
    idx = np.arange(len(inst))
    deltas = {arm: [] for arm in ARMS}
    for _ in range(10_000):
        sample = [inst[i] for i in rng.choice(idx, size=len(idx),
                                              replace=True)]
        r5 = recall(sample, "A5")
        for arm in ARMS:
            deltas[arm].append(r5 - recall(sample, arm))
    def ci(d):
        d = np.array(d)
        return {"mean": float(d.mean()),
                "ci95": [float(np.percentile(d, 2.5)),
                         float(np.percentile(d, 97.5))]}
    a5_rec = ref_summary["arms"]["A5"]["recall"]
    a5_q = ref_summary["arms"]["A5"]["median_q_error"]
    for arm in ARMS:
        S[f"delta_recall_A5_vs_{arm}"] = {
            "point": float(a5_rec - S["arms"][arm]["recall"]),
            "bootstrap": ci(deltas[arm])}

    # ---- pre-registered interpretation rule (Amendment 5 A5.1) ----------
    def matches(arm):
        rec_match = bool(S["arms"][arm]["recall"] == a5_rec)
        q_rel = abs(S["arms"][arm]["median_q_error"] - a5_q) / a5_q
        return {"recall": S["arms"][arm]["recall"],
                "recall_matches_A5": rec_match,
                "median_q_error": S["arms"][arm]["median_q_error"],
                "median_q_relative_diff_vs_A5": float(q_rel),
                "median_q_within_10pct_of_A5": bool(q_rel <= 0.10),
                "matches_both": bool(rec_match and q_rel <= 0.10)}
    m7, m7x = matches("A7"), matches("A7X")
    if m7["matches_both"]:
        branch, outcome = 1, ('A7 matches A5 on recall and median Q-error: '
                              'mechanism claim narrows to "any consumer of '
                              'the heavy-hitter list"')
    elif m7x["matches_both"]:
        branch, outcome = 2, ('Only A7X matches A5 on both: claim narrows '
                              'to "stored exact heavy-hitter counts, '
                              'consumed by sampling or prediction alike"')
    else:
        branch, outcome = 3, ('Neither A7 nor A7X matches A5 on load '
                              'fidelity: the method-vs-information '
                              'separation stands as written')
    S["interpretation"] = {
        "rule": ("if A7 matches A5 on recall AND its median Q-error is "
                 "within 10% relative of A5's, narrow to 'any consumer "
                 "of the heavy-hitter list'; if only A7X matches on "
                 "both, narrow to 'stored exact heavy-hitter counts, "
                 "consumed by sampling or prediction alike'; if neither "
                 "matches A5 on load fidelity, the method-vs-information "
                 "separation stands as written"),
        "A5_reference": {"recall": a5_rec, "median_q_error": a5_q},
        "A7": m7, "A7X": m7x, "branch": branch, "outcome": outcome}

    outdir = os.path.join(BASE, "results", "eqinfo")
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "summary.json"), "w") as f:
        json.dump(S, f, indent=1)
    with open(os.path.join(outdir, "metrics.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["arm", "recall", "precision", "f1", "spearman_rho",
                    "median_q_error", "tp", "fp", "fn", "tn",
                    "mean_testbed_fact_rows", "source"])
        for arm in ARMS:
            m = S["arms"][arm]
            w.writerow([arm, f"{m['recall']:.4f}",
                        f"{m['precision']:.4f}", f"{m['f1']:.4f}",
                        f"{m['spearman_rho']:.4f}",
                        f"{m['median_q_error']:.3f}", m["tp"], m["fp"],
                        m["fn"], m["tn"],
                        f"{S['testbed_rows'][arm]:.0f}", "predictions_eqinfo"])
        for arm in REF_ARMS:
            m = ref_summary["arms"][arm]
            prec = m["precision"]
            w.writerow([arm, f"{m['recall']:.4f}",
                        f"{prec:.4f}" if prec == prec else "nan",
                        f"{m['f1']:.4f}", f"{m['spearman_rho']:.4f}",
                        f"{m['median_q_error']:.3f}", m["tp"], m["fp"],
                        m["fn"], m["tn"],
                        f"{ref_summary['testbed_rows'][arm]:.0f}",
                        "results/summary.json"])
    print(json.dumps({"arms": S["arms"],
                      "join_emergent": S["per_pipeline_family"]
                      ["join-emergent (J,F)"],
                      "packet_bytes": S["packet_bytes"],
                      "max_ratio": max_ratio,
                      "mcnemar_A5_vs_A7": S["mcnemar_A5_vs_A7"],
                      "mcnemar_A5_vs_A7X": S["mcnemar_A5_vs_A7X"],
                      "delta_A5_A7": S["delta_recall_A5_vs_A7"],
                      "delta_A5_A7X": S["delta_recall_A5_vs_A7X"],
                      "interpretation": S["interpretation"],
                      "gt_check": S["gt_verdict_cross_check"],
                      "sanity_A1_matches": sanity["matches"],
                      "testbed_rows": S["testbed_rows"]}, indent=1))


if __name__ == "__main__":
    main()
