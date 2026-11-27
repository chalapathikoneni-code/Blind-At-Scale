"""Pooled + per-pipeline scoring for the negative family (Amendment 4).

FAIL is the positive class. Precision is None (JSON null) when an arm
predicts no FAIL; F1 is then 0.0 if any ground-truth FAIL exists.
"""
import csv, glob, json, os

BASE = os.path.expanduser("~/workspace/blind-at-scale")
ARMS = ["A8-NF", "A5-NF", "A1-NF", "A4-NF"]
PIPES = ["V1", "V2", "V3", "V4"]


def prf(sub):
    tp = sum(1 for r in sub if r["gt"] == "FAIL" and r["pred"] == "FAIL")
    fp = sum(1 for r in sub if r["gt"] == "PASS" and r["pred"] == "FAIL")
    fn = sum(1 for r in sub if r["gt"] == "FAIL" and r["pred"] == "PASS")
    tn = sum(1 for r in sub if r["gt"] == "PASS" and r["pred"] == "PASS")
    rec = tp / (tp + fn) if tp + fn else None
    prec = tp / (tp + fp) if tp + fp else None
    if prec is None:
        f1 = 0.0 if tp + fn else None
    elif rec and prec:
        f1 = 2 * rec * prec / (rec + prec)
    else:
        f1 = 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "recall": rec, "precision": prec, "f1": f1, "n": len(sub)}


def main():
    rows = []
    for path in sorted(glob.glob(os.path.join(BASE, "data", "negfam_s*",
                                              "predictions_negfam.csv"))):
        with open(path) as f:
            for r in csv.DictReader(f):
                rows.append({"dataset": r["dataset"], "seed": int(r["seed"]),
                             "pipeline": r["pipeline"], "gt": r["gt_verdict"],
                             "arm": r["arm"], "pred": r["pred_verdict"],
                             "gt_load": float(r["gt_load"]),
                             "pred_load": float(r["pred_load"])})
    datasets = sorted({r["dataset"] for r in rows})
    S = {"datasets": datasets,
         "n_instances": len({(r["dataset"], r["pipeline"]) for r in rows}),
         "n_gt_fail": len({(r["dataset"], r["pipeline"]) for r in rows
                           if r["gt"] == "FAIL"}),
         "pooled": {}, "per_pipeline": {}, "per_dataset": {}}
    for arm in ARMS:
        S["pooled"][arm] = prf([r for r in rows if r["arm"] == arm])
        S["per_pipeline"][arm] = {
            p: prf([r for r in rows if r["arm"] == arm and r["pipeline"] == p])
            for p in PIPES}
        S["per_dataset"][arm] = {
            ds: prf([r for r in rows if r["arm"] == arm and r["dataset"] == ds])
            for ds in datasets}
    outdir = os.path.join(BASE, "results", "negfam")
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "summary.json"), "w") as f:
        json.dump(S, f, indent=1)
    with open(os.path.join(outdir, "metrics_by_arm.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["scope", "arm", "recall", "precision", "f1", "tp", "fp", "fn", "tn", "n"])
        def emit(scope, arm, m):
            fmt = lambda x: "" if x is None else f"{x:.4f}"  # noqa: E731
            w.writerow([scope, arm, fmt(m["recall"]), fmt(m["precision"]),
                        fmt(m["f1"]), m["tp"], m["fp"], m["fn"], m["tn"], m["n"]])
        for arm in ARMS:
            emit("pooled", arm, S["pooled"][arm])
            for p in PIPES:
                emit(f"pipeline_{p}", arm, S["per_pipeline"][arm][p])
    print(json.dumps(S, indent=1))


if __name__ == "__main__":
    main()
