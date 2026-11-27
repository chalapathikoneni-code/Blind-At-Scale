"""Scoring for Experiment A (PREREGISTRATION_EXP_A.md Sections 4-7).

Reads results/exp_a/predictions_exp_a_<tag>.csv (A9, headline hash H0),
the recorded data/<tag>/predictions_main.csv (A1-A5, A8) and
predictions_eqinfo.csv (A7, A7X), results/summary.json and
results/eqinfo/summary.json (reference values, read not recomputed --
with a recomputation cross-check of A5/A7 pooled metrics from the
CSVs). Writes results/exp_a/summary_exp_a.json,
summary_near.json, threshold_sensitivity.json, metrics_exp_a.csv.

Conventions mirror score.py / score_eqinfo.py: FAIL is the positive
class; median Q-error = median(max(p/g, g/p)); Spearman rho on log10
loads; McNemar exact on verdict correctness; paired bootstrap of
delta-recall, 10,000 resamples, RNG seed 7.

Usage (cwd = this code/ directory), after run_exp_a.py and
run_near_threshold.py:
    python score_exp_a.py
"""
import csv, json, os
import numpy as np
from scipy import stats

BASE = os.path.expanduser("~/workspace/blind-at-scale")
EXP = os.path.join(BASE, "results", "exp_a")
DATASETS = [f"{f}_s{s}" for f in ("zipf", "lognormal", "mixed")
            for s in (101, 202, 303)]
SCALE_TAG = "zipf_s101_40m"
NEAR_TAGS = [f"near_s{s:03d}_s{seed}"
             for s in (650, 680, 720, 740) for seed in (101, 202)]
PFAM = {"direct (G,D)": ("G_A", "G_B", "G_C", "G_N", "D_A"),
        "join-emergent (J,F)": ("J_A", "J_B", "J_N", "F_A", "F_B"),
        "window (W)": ("W_A",)}


def prf(sub):
    tp = sum(1 for r in sub if r["gt"] == "FAIL" and r["pred"] == "FAIL")
    fp = sum(1 for r in sub if r["gt"] == "PASS" and r["pred"] == "FAIL")
    fn = sum(1 for r in sub if r["gt"] == "FAIL" and r["pred"] == "PASS")
    tn = sum(1 for r in sub if r["gt"] == "PASS" and r["pred"] == "PASS")
    rec = tp / (tp + fn) if tp + fn else float("nan")
    prec = tp / (tp + fp) if tp + fp else float("nan")
    f1 = 2 * rec * prec / (rec + prec) if rec + prec else 0.0
    fpr = fp / (fp + tn) if fp + tn else float("nan")
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn, "recall": rec,
            "precision": prec, "f1": f1, "fpr": fpr, "n": len(sub)}


def metrics_for(inst, arm):
    m = prf([{"gt": e["gt"], "pred": e[arm]} for e in inst])
    loads = np.array([e[arm + "_load"] for e in inst])
    gt = np.array([e["gt_load"] for e in inst])
    m["spearman_rho"] = float(stats.spearmanr(np.log10(loads),
                                              np.log10(gt)).statistic)
    m["median_q_error"] = float(np.median(np.maximum(loads / gt, gt / loads)))
    return m


def load_grid(tags, pred_files):
    """Instance table keyed (dataset, pipeline). pred_files: list of
    (path, arms_to_read). Returns list of instance dicts."""
    inst = {}
    for path, arms in pred_files:
        with open(path) as f:
            for r in csv.DictReader(f):
                if r["arm"] not in arms:
                    continue
                key = (r["dataset"], r["pipeline"])
                e = inst.setdefault(key, {
                    "dataset": r["dataset"], "family": r["family"],
                    "pipeline": r["pipeline"], "gt": r["gt_verdict"],
                    "gt_load": float(r["gt_load"]),
                    "n_fact": int(r["n_fact"])})
                e[r["arm"]] = r["pred_verdict"]
                e[r["arm"] + "_load"] = float(r["pred_load"])
    return [inst[k] for k in sorted(inst)]


def mcnemar(inst, a, b):
    only_a = sum(1 for e in inst if (e[a] == e["gt"]) and (e[b] != e["gt"]))
    only_b = sum(1 for e in inst if (e[b] == e["gt"]) and (e[a] != e["gt"]))
    p = stats.binomtest(min(only_a, only_b), only_a + only_b, 0.5).pvalue \
        if only_a + only_b else 1.0
    return {f"only_{a}_correct": only_a, f"only_{b}_correct": only_b,
            "p_exact": float(p)}


def bootstrap_delta_recall(inst, a, b, n_iter=10_000, seed=7):
    rng = np.random.default_rng(seed)

    def recall(data, arm):
        tp = sum(1 for e in data if e["gt"] == "FAIL" and e[arm] == "FAIL")
        fn = sum(1 for e in data if e["gt"] == "FAIL" and e[arm] == "PASS")
        return tp / (tp + fn) if tp + fn else float("nan")

    idx = np.arange(len(inst))
    deltas = []
    for _ in range(n_iter):
        sample = [inst[i] for i in rng.choice(idx, size=len(idx),
                                              replace=True)]
        deltas.append(recall(sample, a) - recall(sample, b))
    d = np.array(deltas)
    return {"point": float(recall(inst, a) - recall(inst, b)),
            "bootstrap": {"mean": float(d.mean()),
                          "ci95": [float(np.percentile(d, 2.5)),
                                   float(np.percentile(d, 97.5))]}}


def threshold_sensitivity(inst, arms):
    """Re-derive verdicts at T*0.8 and T*1.2 from recorded loads (no
    re-run). Returns per-arm flip counts and the near-threshold band
    size. T_distinct = 50000 for D_A, else 0.02 * n_fact."""
    def thr(e):
        return 50_000.0 if e["pipeline"] == "D_A" else 0.02 * e["n_fact"]

    band = sum(1 for e in inst if abs(e["gt_load"] - thr(e)) <= 0.2 * thr(e))
    out = {"n_instances": len(inst), "n_gt_within_20pct_of_T": band}
    for arm in arms:
        flips = {}
        for scale in (0.8, 1.2):
            n_flip = 0
            for e in inst:
                t = thr(e) * scale
                v_now = "FAIL" if e[arm + "_load"] > thr(e) else "PASS"
                v_new = "FAIL" if e[arm + "_load"] > t else "PASS"
                if v_now != v_new:
                    n_flip += 1
            flips[f"T_x{scale}"] = n_flip
        # GT flips too (how many GT verdicts move): the band's meaning
        gt_flips = {}
        for scale in (0.8, 1.2):
            n_flip = sum(1 for e in inst
                         if ("FAIL" if e["gt_load"] > thr(e) else "PASS")
                         != ("FAIL" if e["gt_load"] > thr(e) * scale
                             else "PASS"))
            gt_flips[f"T_x{scale}"] = n_flip
        out[arm] = {"verdict_flips_vs_T": flips, "gt_flips": gt_flips}
    return out


def main():
    # ---- 99-instance grid: A9 + references ----
    pred_files = []
    for tag in DATASETS:
        pred_files.append((os.path.join(EXP, f"predictions_exp_a_{tag}.csv"),
                           ["A9"]))
        pred_files.append((os.path.join(BASE, "data", tag,
                                        "predictions_main.csv"),
                           ["A1", "A2", "A3", "A4", "A5", "A8"]))
        pred_files.append((os.path.join(BASE, "data", tag,
                                        "predictions_eqinfo.csv"),
                           ["A7", "A7X"]))
    inst = load_grid(DATASETS, pred_files)
    assert len(inst) == 99, len(inst)
    assert all("A9" in e and "A5" in e and "A7" in e for e in inst)

    with open(os.path.join(BASE, "results", "summary.json")) as f:
        ref_main = json.load(f)
    with open(os.path.join(BASE, "results", "eqinfo", "summary.json")) as f:
        ref_eq = json.load(f)

    # cross-check: recomputed A5/A7 pooled metrics must match records
    xcheck = {}
    for arm, ref in (("A5", ref_main["arms"]["A5"]),
                     ("A7", ref_eq["arms"]["A7"])):
        m = metrics_for(inst, arm)
        xcheck[arm] = {
            "recomputed_recall": m["recall"], "recorded_recall": ref["recall"],
            "recomputed_median_q": m["median_q_error"],
            "recorded_median_q": ref["median_q_error"],
            "recall_match": m["recall"] == ref["recall"],
            "q_match_within_1e9": abs(m["median_q_error"]
                                      - ref["median_q_error"]) < 1e-9,
        }
        assert xcheck[arm]["recall_match"], arm
        assert xcheck[arm]["q_match_within_1e9"], arm

    S = {"n_instances": len(inst),
         "n_gt_fail": sum(1 for e in inst if e["gt"] == "FAIL"),
         "cross_check_vs_recorded_summaries": xcheck,
         "arms": {}, "per_family": {}, "per_pipeline_family": {},
         "f_family": {}, "per_pipeline_median_q": {}}
    for arm in ("A1", "A2", "A3", "A4", "A5", "A7", "A7X", "A8", "A9"):
        S["arms"][arm] = metrics_for(inst, arm)
    for fam in ("zipf", "lognormal", "mixed"):
        sub = [e for e in inst if e["family"] == fam]
        S["per_family"][fam] = {arm: metrics_for(sub, arm)
                                for arm in ("A5", "A7", "A9")}
    for name, pids in PFAM.items():
        sub = [e for e in inst if e["pipeline"] in pids]
        S["per_pipeline_family"][name] = {
            arm: metrics_for(sub, arm) for arm in ("A5", "A7", "A9")}
    f_sub = [e for e in inst if e["pipeline"] in ("F_A", "F_B")]
    S["f_family"] = {arm: metrics_for(f_sub, arm)
                     for arm in ("A1", "A5", "A7", "A9")}
    for pid, _, _ in [("G_A", 0, 0), ("G_B", 0, 0), ("G_C", 0, 0),
                      ("G_N", 0, 0), ("J_A", 0, 0), ("J_B", 0, 0),
                      ("J_N", 0, 0), ("F_A", 0, 0), ("F_B", 0, 0),
                      ("W_A", 0, 0), ("D_A", 0, 0)]:
        sub = [e for e in inst if e["pipeline"] == pid]
        S["per_pipeline_median_q"][pid] = {
            arm: metrics_for(sub, arm)["median_q_error"]
            for arm in ("A1", "A5", "A7", "A9")}

    S["mcnemar_A5_vs_A9"] = mcnemar(inst, "A5", "A9")
    S["mcnemar_A7_vs_A9"] = mcnemar(inst, "A7", "A9")
    S["delta_recall_A5_minus_A9"] = bootstrap_delta_recall(inst, "A5", "A9")
    S["delta_recall_A7_minus_A9"] = bootstrap_delta_recall(inst, "A7", "A9")

    # packet bytes: A9 (this experiment) vs synopsis vs A7/A7X packets
    with open(os.path.join(EXP, "run_info.json")) as f:
        run_info = json.load(f)
    S["packet_bytes"] = {}
    for tag in DATASETS:
        S["packet_bytes"][tag] = {
            "synopsis_bytes": run_info[tag]["synopsis_bytes"],
            "A9_packet_bytes": run_info[tag]["packet_bytes"],
            "A9_ratio_to_synopsis": (run_info[tag]["packet_bytes"]
                                     / run_info[tag]["synopsis_bytes"]),
            "A7_packet_bytes": ref_eq["packet_bytes"][tag]["A7_packet_bytes"],
            "A7X_packet_bytes": ref_eq["packet_bytes"][tag]["A7X_packet_bytes"],
        }
    S["packet_bytes_max_ratio"] = max(
        v["A9_ratio_to_synopsis"] for v in S["packet_bytes"].values())
    S["packet_exceeds_synopsis_any"] = any(
        v["A9_packet_bytes"] > v["synopsis_bytes"]
        for v in S["packet_bytes"].values())

    # hash sensitivity: verdict agreement H0 vs H1/H2 over 99 + 40M
    sens = {}
    for suffix in ("h1", "h2"):
        disagree = 0
        total = 0
        for tag in DATASETS + [SCALE_TAG]:
            with open(os.path.join(EXP, f"predictions_exp_a_{tag}.csv")) as f:
                h0 = {(r["pipeline"]): r["pred_verdict"]
                      for r in csv.DictReader(f)}
            with open(os.path.join(
                    EXP, f"predictions_exp_a_{tag}_{suffix}.csv")) as f:
                hs = {(r["pipeline"]): r["pred_verdict"]
                      for r in csv.DictReader(f)}
            for pid in h0:
                total += 1
                if h0[pid] != hs[pid]:
                    disagree += 1
        sens[suffix] = {"n_instances": total, "verdict_disagreements": disagree}
    S["hash_sensitivity"] = sens

    # ---- 40M scale check ----
    inst40 = load_grid([SCALE_TAG], [
        (os.path.join(EXP, f"predictions_exp_a_{SCALE_TAG}.csv"), ["A9"]),
        (os.path.join(BASE, "data", SCALE_TAG, "predictions_main.csv"),
         ["A1", "A5", "A8"])])
    S["scale_40m"] = {
        "n_instances": len(inst40),
        "n_gt_fail": sum(1 for e in inst40 if e["gt"] == "FAIL"),
        "arms": {arm: metrics_for(inst40, arm) for arm in ("A1", "A5", "A8", "A9")},
        "packet_bytes": {
            "synopsis_bytes": run_info[SCALE_TAG]["synopsis_bytes"],
            "A9_packet_bytes": run_info[SCALE_TAG]["packet_bytes"]},
    }

    # ---- decision rule (PREREGISTRATION_EXP_A.md Section 4) ----
    a9 = S["arms"]["A9"]
    a5 = S["arms"]["A5"]
    leg_recall = a9["recall"] == a5["recall"]
    leg_q = a9["median_q_error"] <= 1.10 * a5["median_q_error"]
    leg_bytes = not S["packet_exceeds_synopsis_any"]
    if leg_recall and leg_q and leg_bytes:
        branch, outcome = 1, ("NARROW: A9 matches A5 on recall and median "
                              "Q-error (within 10%) at fewer bytes than the "
                              "synopsis -- the method claim narrows to "
                              "'heavy-hitter information, consumable by a "
                              "published classical estimator'")
    elif leg_recall:
        branch, outcome = 2, ("PARTIAL: A9 matches A5 on recall but not on "
                              "load fidelity (median Q outside the 10% "
                              "band) -- the synopsis tail model retains a "
                              "measured load-fidelity advantage; claim stays "
                              "narrowed to 'heavy-hitter information'")
    else:
        branch, outcome = 3, ("NO MATCH: A9 recall below A5 recall -- the "
                              "classical estimator does not match on the "
                              "max-load verdict task")
    f_a9 = S["f_family"]["A9"]
    f_a5 = S["f_family"]["A5"]
    S["decision_rule"] = {
        "legs": {"recall_matches_A5": bool(leg_recall),
                 "median_q_within_10pct_of_A5": bool(leg_q),
                 "packet_bytes_le_synopsis_all_datasets": bool(leg_bytes)},
        "A9_recall": a9["recall"], "A5_recall": a5["recall"],
        "A9_median_q": a9["median_q_error"],
        "A5_median_q": a5["median_q_error"],
        "A9_over_A5_median_q_ratio": a9["median_q_error"] / a5["median_q_error"],
        "branch": branch, "outcome": outcome,
        "f_family_subrule": {
            "A9_f_recall": f_a9["recall"], "A5_f_recall": f_a5["recall"],
            "A9_f_median_q": f_a9["median_q_error"],
            "A5_f_median_q": f_a5["median_q_error"],
            "A9_matches_or_beats_A5_on_F":
                bool(f_a9["recall"] == f_a5["recall"]
                     and f_a9["median_q_error"] <= f_a5["median_q_error"])},
    }

    with open(os.path.join(EXP, "summary_exp_a.json"), "w") as f:
        json.dump(S, f, indent=1)

    # metrics CSV (A9 vs references, 99 instances)
    with open(os.path.join(EXP, "metrics_exp_a.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["arm", "recall", "precision", "f1", "spearman_rho",
                    "median_q_error", "tp", "fp", "fn", "tn"])
        for arm in ("A1", "A2", "A3", "A4", "A5", "A7", "A7X", "A8", "A9"):
            m = S["arms"][arm]
            w.writerow([arm, f"{m['recall']:.4f}", f"{m['precision']:.4f}",
                        f"{m['f1']:.4f}", f"{m['spearman_rho']:.4f}",
                        f"{m['median_q_error']:.3f}", m["tp"], m["fp"],
                        m["fn"], m["tn"]])

    # ---- near-threshold grid ----
    near_files = [(os.path.join(EXP, f"predictions_near_{tag}.csv"),
                   ["A1", "A2", "A3", "A4", "A5", "A7", "A8", "A9"])
                  for tag in NEAR_TAGS]
    ninst = load_grid(NEAR_TAGS, near_files)
    N = {"n_instances": len(ninst),
         "n_gt_fail": sum(1 for e in ninst if e["gt"] == "FAIL"),
         "arms": {}, "per_s": {}, "per_pipeline_gt": {}}
    for arm in ("A1", "A2", "A3", "A4", "A5", "A7", "A8", "A9"):
        N["arms"][arm] = metrics_for(ninst, arm)
    for s in (650, 680, 720, 740):
        sub = [e for e in ninst if e["dataset"].startswith(f"near_s{s:03d}")]
        N["per_s"][f"s_{s/1000:.2f}"] = {
            "n": len(sub),
            "n_gt_fail": sum(1 for e in sub if e["gt"] == "FAIL"),
            "arms": {arm: metrics_for(sub, arm)
                     for arm in ("A1", "A5", "A7", "A8", "A9")}}
    for pid in ("G_A", "G_B", "G_C", "G_N", "J_A", "J_B", "J_N", "F_A",
                "F_B", "W_A", "D_A"):
        sub = [e for e in ninst if e["pipeline"] == pid]
        N["per_pipeline_gt"][pid] = {
            "n": len(sub),
            "n_gt_fail": sum(1 for e in sub if e["gt"] == "FAIL"),
            "gt_load_min": float(min(e["gt_load"] for e in sub)),
            "gt_load_max": float(max(e["gt_load"] for e in sub)),
            "arms": {arm: {"recall": metrics_for(sub, arm)["recall"],
                           "fpr": metrics_for(sub, arm)["fpr"],
                           "median_q_error":
                               metrics_for(sub, arm)["median_q_error"]}
                     for arm in ("A1", "A5", "A7", "A8", "A9")}}
    with open(os.path.join(EXP, "summary_near.json"), "w") as f:
        json.dump(N, f, indent=1)

    # ---- threshold sensitivity (+/-20%), both grids ----
    sens_out = {
        "headline_99": threshold_sensitivity(inst, ["A1", "A5", "A7", "A8", "A9"]),
        "near_threshold_88": threshold_sensitivity(ninst, ["A1", "A5", "A7", "A8", "A9"]),
    }
    with open(os.path.join(EXP, "threshold_sensitivity.json"), "w") as f:
        json.dump(sens_out, f, indent=1)

    print(json.dumps({"A9": S["arms"]["A9"],
                      "decision_rule": S["decision_rule"],
                      "near_n": N["n_instances"],
                      "near_gt_fail": N["n_gt_fail"],
                      "near_arms": {a: N["arms"][a] for a in
                                    ("A1", "A5", "A7", "A8", "A9")}},
                     indent=1))


if __name__ == "__main__":
    main()
