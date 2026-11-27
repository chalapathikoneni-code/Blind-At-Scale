"""Feedback / pass-rate study (PREREGISTRATION.md S.5; surrogate agent).

For every program dataset: fixable pipelines that FAIL at full scale are
'revised' by a deterministic rewriter whose fix is selected by a feedback
packet. Packet sources: FB-SYN (A5 verdicts), FB-SAMPLE (A1 verdicts),
FB-PLACEBO (A5 verdicts rotated across pipelines -- mismatched fix types
have no effect), FB-RETRY (never), FB-REWRITE (oracle verdicts).
Revised loads are computed exactly from the full per-key counts using
the analytic fix-effect models of Amendment 2.
"""
import csv, json, os
import numpy as np

from common import K, PIPELINES, loads_from_counts, verdict, threshold, splitmix64_hash

DATA = os.path.expanduser("~/workspace/blind-at-scale/data")
DATASETS = [f"{f}_s{s}" for f in ("zipf", "lognormal", "mixed") for s in (101, 202, 303)]

FIX_TYPE = {"J_A": "preagg_join", "J_B": "preagg_join", "J_N": "preagg_join",
            "F_A": "preagg_ff", "F_B": "preagg_ff",
            "G_A": "salt", "G_N": "salt", "W_A": "salt"}
SALTS = 16


def revised_load(pid, counts):
    fact = {c: counts[f"fact_{c}"] for c in ("kA", "kB", "kC", "kN")}
    col = {p: c for p, c, _ in PIPELINES}[pid]
    ft = FIX_TYPE[pid]
    if ft == "salt":
        m = float(fact[col].max())
        if pid == "G_N":
            m = max(m, float(counts["null_count"]))
        return -(-m // SALTS)  # ceil
    keys = np.arange(K)
    parts = (splitmix64_hash(keys) % np.uint64(128)).astype(np.int64)
    if ft == "preagg_join":
        present = fact[col] > 0
    else:  # preagg_ff: shared keys only
        present = (fact[col] > 0) & (counts[f"fact2_{col}"] > 0)
    return float(np.bincount(parts[present], minlength=128).max())


def main():
    rows = []
    for tag in DATASETS:
        d = os.path.join(DATA, tag)
        z = np.load(os.path.join(d, "counts.npz"))
        counts = {k: z[k] for k in z.files}
        counts["null_count"] = int(counts["null_count"])
        n = int(counts["fact_kA"].sum())
        fact = {c: counts[f"fact_{c}"] for c in ("kA", "kB", "kC", "kN")}
        fact2 = {c: counts[f"fact2_{c}"] for c in ("kA", "kB")}
        gt_loads = loads_from_counts(fact, fact2, float(counts["null_count"]),
                                     np.ones(K, dtype=bool),
                                     distinct_counts=counts["distinct_A"])
        gt_v = {p: verdict(l, p, n) for p, l in gt_loads.items()}
        pred = {}
        with open(os.path.join(d, "predictions_main.csv")) as f:
            for row in csv.DictReader(f):
                pred.setdefault(row["arm"], {})[row["pipeline"]] = row["pred_verdict"]
        pids = [p for p, _, _ in PIPELINES]
        for pid in pids:
            fixable = pid in FIX_TYPE
            rev = revised_load(fix_type_pid(pid), counts) if fixable else None
            def repaired(fixed):
                return bool(fixed and fixable and gt_v[pid] == "FAIL"
                            and rev <= threshold(pid, n))
            fb = {}
            fb["FB-SYN"] = repaired(pred["A5"][pid] == "FAIL")
            fb["FB-SAMPLE"] = repaired(pred["A1"][pid] == "FAIL")
            src = pids[(pids.index(pid) + 3) % len(pids)]
            placebo_fix = (pred["A5"][src] == "FAIL" and src in FIX_TYPE
                           and FIX_TYPE.get(src) == FIX_TYPE.get(pid))
            fb["FB-PLACEBO"] = repaired(placebo_fix)
            fb["FB-RETRY"] = repaired(False)
            fb["FB-REWRITE"] = repaired(gt_v[pid] == "FAIL")
            rows.append({"dataset": tag, "pipeline": pid,
                         "gt_verdict": gt_v[pid], "fixable": fixable,
                         "revised_load": rev, **fb})
    out = os.path.expanduser("~/workspace/blind-at-scale/results/feedback_per_pipeline.csv")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    # pooled summary
    arms = ["FB-SYN", "FB-SAMPLE", "FB-PLACEBO", "FB-RETRY", "FB-REWRITE"]
    summ = {}
    denom = [r for r in rows if r["gt_verdict"] == "FAIL" and r["fixable"]]
    for a in arms:
        rep = sum(1 for r in denom if r[a])
        passed = sum(1 for r in rows if r["gt_verdict"] == "PASS") + rep
        summ[a] = {"repaired": rep, "fixable_failures": len(denom),
                   "repair_rate": rep / len(denom),
                   "full_scale_pass_rate": passed / len(rows)}
    with open(os.path.expanduser("~/workspace/blind-at-scale/results/feedback_summary.json"), "w") as f:
        json.dump(summ, f, indent=1)
    print(json.dumps(summ, indent=1))


def fix_type_pid(pid):
    return pid


if __name__ == "__main__":
    main()
