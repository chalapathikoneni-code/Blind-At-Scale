"""Pooled scoring + statistics (PREREGISTRATION.md S.6-7).

Reads predictions_main.csv from the 9 program datasets (spot checks
scored separately), writes results/summary.json, metrics_by_arm.csv,
spot_checks.json and figures. FAIL is the positive class.
"""
import csv, json, os
import numpy as np
from scipy import stats

BASE = os.path.expanduser("~/workspace/blind-at-scale")
DATASETS = [f"{f}_s{s}" for f in ("zipf", "lognormal", "mixed") for s in (101, 202, 303)]
ARMS = ["A1", "A2", "A3", "A4", "A5", "A8"]
BASELINES = ["A1", "A2", "A3", "A4"]


def load_preds(tags, rtag="main"):
    rows = []
    for tag in tags:
        with open(os.path.join(BASE, "data", tag, f"predictions_{rtag}.csv")) as f:
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


def main():
    rows = load_preds(DATASETS)
    # instance table: one entry per (dataset, pipeline) with gt + per-arm pred
    inst = {}
    for r in rows:
        key = (r["dataset"], r["pipeline"])
        e = inst.setdefault(key, {"dataset": r["dataset"], "family": r["family"],
                                  "pipeline": r["pipeline"],
                                  "gt": r["gt_verdict"],
                                  "gt_load": float(r["gt_load"]),
                                  "n_fact": int(r["n_fact"])})
        e[r["arm"]] = r["pred_verdict"]
        e[r["arm"] + "_load"] = float(r["pred_load"])
        e["synopsis_bytes"] = int(r["synopsis_bytes"])
        e["size_" + r["arm"]] = int(r["testbed_fact_rows"])
    inst = list(inst.values())
    S = {"n_instances": len(inst),
         "n_gt_fail": sum(1 for e in inst if e["gt"] == "FAIL"),
         "arms": {}, "per_family": {}, "per_pipeline_family": {},
         "testbed_rows": {}, "synopsis_bytes": inst[0]["synopsis_bytes"]}

    def sub_for(arm, data):
        return [{"gt": e["gt"], "pred": e[arm]} for e in data]

    for arm in ARMS:
        m = prf(sub_for(arm, inst))
        loads = np.array([e[arm + "_load"] for e in inst])
        gt = np.array([e["gt_load"] for e in inst])
        rho = stats.spearmanr(np.log10(loads), np.log10(gt)).statistic
        q = np.maximum(loads / gt, gt / loads)
        m["spearman_rho"] = float(rho)
        m["median_q_error"] = float(np.median(q))
        S["arms"][arm] = m
        S["testbed_rows"][arm] = float(np.mean([e["size_" + arm] for e in inst]))
    for fam in ("zipf", "lognormal", "mixed"):
        S["per_family"][fam] = {arm: prf(sub_for(arm, [e for e in inst
                                                       if e["family"] == fam]))
                                for arm in ARMS}
        S["per_family"][fam]["A5_rho"] = float(stats.spearmanr(
            np.log10([e["A5_load"] for e in inst if e["family"] == fam]),
            np.log10([e["gt_load"] for e in inst if e["family"] == fam])).statistic)
    pfam = {"direct (G,D)": ("G_A", "G_B", "G_C", "G_N", "D_A"),
            "join-emergent (J,F)": ("J_A", "J_B", "J_N", "F_A", "F_B"),
            "window (W)": ("W_A",)}
    for name, pids in pfam.items():
        S["per_pipeline_family"][name] = {
            arm: prf(sub_for(arm, [e for e in inst if e["pipeline"] in pids]))
            for arm in ARMS}
    # borderline sensitivity: gt load within +/-10% of threshold
    def thr(e):
        return 50_000 if e["pipeline"] == "D_A" else 0.02 * e["n_fact"]
    nb = [e for e in inst if abs(e["gt_load"] - thr(e)) > 0.1 * thr(e)]
    S["nonborderline"] = {"n": len(nb),
                          "excluded": len(inst) - len(nb),
                          **{arm: prf(sub_for(arm, nb)) for arm in ARMS}}
    # paired bootstrap for delta recall (A5 - best baseline; A5 - A8)
    rng = np.random.default_rng(7)
    best_base = max(BASELINES, key=lambda a: S["arms"][a]["recall"])
    S["best_baseline"] = best_base

    def recall(data, arm):
        tp = sum(1 for e in data if e["gt"] == "FAIL" and e[arm] == "FAIL")
        fn = sum(1 for e in data if e["gt"] == "FAIL" and e[arm] == "PASS")
        return tp / (tp + fn) if tp + fn else float("nan")

    idx = np.arange(len(inst))
    deltas_b, deltas_8 = [], []
    for _ in range(10_000):
        sample = [inst[i] for i in rng.choice(idx, size=len(idx), replace=True)]
        deltas_b.append(recall(sample, "A5") - recall(sample, best_base))
        deltas_8.append(recall(sample, "A5") - recall(sample, "A8"))
    def ci(d):
        d = np.array(d)
        return {"mean": float(d.mean()),
                "ci95": [float(np.percentile(d, 2.5)), float(np.percentile(d, 97.5))]}
    S["delta_recall_A5_vs_best_baseline"] = {
        "point": S["arms"]["A5"]["recall"] - S["arms"][best_base]["recall"],
        "bootstrap": ci(deltas_b)}
    S["delta_recall_A5_vs_A8"] = {
        "point": S["arms"]["A5"]["recall"] - S["arms"]["A8"]["recall"],
        "bootstrap": ci(deltas_8)}
    # McNemar exact (correctness discordants)
    def mcnemar(a, b):
        only_a = sum(1 for e in inst if (e[a] == e["gt"]) and (e[b] != e["gt"]))
        only_b = sum(1 for e in inst if (e[b] == e["gt"]) and (e[a] != e["gt"]))
        p = stats.binomtest(min(only_a, only_b), only_a + only_b, 0.5).pvalue \
            if only_a + only_b else 1.0
        return {"only_A5_correct": only_a, "only_other_correct": only_b,
                "p_exact": float(p)}
    S["mcnemar_A5_vs_best_baseline"] = mcnemar("A5", best_base)
    S["mcnemar_A5_vs_A8"] = mcnemar("A5", "A8")
    # kill condition (PREREGISTRATION S.7)
    d = S["delta_recall_A5_vs_best_baseline"]
    cond_a = d["point"] >= 0.10 and d["bootstrap"]["ci95"][0] > 0
    fam_rho_ok = sum(1 for f in ("zipf", "lognormal", "mixed")
                     if S["per_family"][f]["A5_rho"] >= 0.6)
    cond_b = fam_rho_ok >= 2
    S["kill_condition"] = {
        "a_delta_recall": {"pass": bool(cond_a), "point": d["point"],
                            "ci_lower": d["bootstrap"]["ci95"][0]},
        "b_family_rho": {"pass": bool(cond_b),
                         "families_with_rho_ge_0.6": fam_rho_ok},
        "KILLED": not (cond_a and cond_b),
        "narrowing_trigger_A8": bool(
            S["arms"]["A8"]["recall"] >= S["arms"]["A5"]["recall"] - 0.03)}
    with open(os.path.join(BASE, "results", "summary.json"), "w") as f:
        json.dump(S, f, indent=1)
    with open(os.path.join(BASE, "results", "metrics_by_arm.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["arm", "recall", "precision", "f1", "spearman_rho",
                    "median_q_error", "tp", "fp", "fn", "tn",
                    "mean_testbed_fact_rows"])
        for arm in ARMS:
            m = S["arms"][arm]
            w.writerow([arm, f"{m['recall']:.4f}", f"{m['precision']:.4f}",
                        f"{m['f1']:.4f}", f"{m['spearman_rho']:.4f}",
                        f"{m['median_q_error']:.3f}", m["tp"], m["fp"],
                        m["fn"], m["tn"], f"{S['testbed_rows'][arm]:.0f}"])
    # spot checks
    spot = {}
    try:
        rows40 = load_preds(["zipf_s101_40m"])
        spot["scale_40m"] = {arm: prf([{"gt": r["gt_verdict"],
                                        "pred": r["pred_verdict"]}
                                       for r in rows40 if r["arm"] == arm])
                             for arm in ("A1", "A5", "A8")}
    except FileNotFoundError:
        spot["scale_40m"] = "not run"
    try:
        rowsr = load_preds(["zipf_s101"], rtag="r0001")
        spot["r_0.001"] = {arm: prf([{"gt": r["gt_verdict"],
                                      "pred": r["pred_verdict"]}
                                     for r in rowsr if r["arm"] == arm])
                           for arm in ("A1", "A5")}
    except FileNotFoundError:
        spot["r_0.001"] = "not run"
    with open(os.path.join(BASE, "results", "spot_checks.json"), "w") as f:
        json.dump(spot, f, indent=1)
    print(json.dumps({"arms": {a: S["arms"][a] for a in ARMS},
                      "kill": S["kill_condition"],
                      "best_baseline": best_base}, indent=1))


if __name__ == "__main__":
    main()
