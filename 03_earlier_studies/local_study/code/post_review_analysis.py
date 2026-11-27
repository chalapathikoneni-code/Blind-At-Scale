"""Post-review analyses (2026-11-06), all computed from existing artefacts
plus the corrected A3 re-run. Written in response to HOSTILE_REVIEW.md
items N1, N3, N5, N6 and fixable items 7.1.7, 7.1.8, 7.1.13, 7.2.10.
Output: results/post_review_analysis.json
"""
import copy
import glob
import json
import os
import sys
import time

import numpy as np
import pandas as pd
from scipy.stats import binomtest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import arms as A  # noqa: E402
import common as C  # noqa: E402

ROOT = os.path.expanduser("~/workspace/blind-at-scale")
out = {}

# ---------- 1. A3 subset statistics + McNemar A5 vs A3 (corrected A3) ----
frames = []
for f in sorted(glob.glob(f"{ROOT}/data/*/predictions_main.csv")):
    ds = os.path.basename(os.path.dirname(f))
    if "40m" in ds or "smoke" in ds:
        continue
    d = pd.read_csv(f)
    d["q_error"] = np.maximum(d.pred_load / d.gt_load, d.gt_load / d.pred_load)
    frames.append(d)
df = pd.concat(frames, ignore_index=True)
df["pipeline_family"] = df.pipeline.str[0]

a3 = {}
for fam in ("J", "F"):
    sub = df[(df.arm == "A3") & (df.pipeline_family == fam)]
    a3[fam] = {"median_q_error": float(sub.q_error.median()),
               "median_pred_over_gt": float((sub.pred_load / sub.gt_load).median())}
out["A3_subsets"] = a3

piv = df.pivot_table(index=["dataset", "pipeline"], columns="arm",
                     values="pred_verdict", aggfunc="first")
gt = df.drop_duplicates(["dataset", "pipeline"]).set_index(
    ["dataset", "pipeline"]).gt_verdict
correct = piv.eq(gt, axis=0)
b = int((correct.A5 & ~correct.A3).sum())
c = int((~correct.A5 & correct.A3).sum())
out["mcnemar_A5_vs_A3_corrected"] = {
    "only_A5_correct": b, "only_A3_correct": c,
    "p_exact": float(binomtest(min(b, c), b + c, 0.5).pvalue) if b + c else 1.0}

# ---------- 2. A1 dimension-key retention (exact mask used in the runs) --
ret = {}
datasets = sorted(glob.glob(f"{ROOT}/data/*/counts.npz"))
for f in datasets:
    ds = os.path.basename(os.path.dirname(f))
    if "40m" in ds or "smoke" in ds:
        continue
    counts = dict(np.load(f))
    syn = json.load(open(os.path.join(os.path.dirname(f), "synopsis.json")))
    seed = int(ds.rsplit("s", 1)[1])
    # A1 is arm index 0 in run_dataset's arm list -> seed*1000 + 5
    _, _, _, dimp, _ = A.arm_predictions("A1", counts, syn, seed=seed * 1000 + 5)
    ret[ds] = int(dimp.sum())
out["A1_dim_keys_retained_of_20000"] = ret
out["A1_dim_keys_retained_mean"] = float(np.mean(list(ret.values())))

# ---------- 3. Transcription fraction: A8 with the fitted tail removed ---
# Zero every tail mass in a copy of the synopsis, so A8 sees ONLY the exact
# stored heavy hitters and aligned pairs. An instance counts as
# "stored-determined" if this degraded prediction is within 1% of the
# ground-truth load.
n_det, n_tot = 0, 0
per_fam = {}
for f in datasets:
    ds = os.path.basename(os.path.dirname(f))
    if "40m" in ds or "smoke" in ds:
        continue
    counts = dict(np.load(f))
    syn = json.load(open(os.path.join(os.path.dirname(f), "synopsis.json")))
    seed = int(ds.rsplit("s", 1)[1])
    syn0 = copy.deepcopy(syn)
    for col in syn0["cols"].values():
        col["tail_mass"] = 0
    for pair in syn0["pairs"].values():
        for k in list(pair.keys()):
            if "tail" in k:
                pair[k] = 0
    gt_loads, gt_verd, res = A.run_arms(counts, syn0, seed, ["A8"])
    for p, l in gt_loads.items():
        pred = res["A8"]["pred_loads"][p]
        det = abs(pred - l) / l <= 0.01 if l > 0 else pred == l
        n_tot += 1
        n_det += int(det)
        fam = p[0]
        a, b_ = per_fam.get(fam, (0, 0))
        per_fam[fam] = (a + int(det), b_ + 1)
out["stored_only_A8"] = {
    "instances_within_1pct_of_gt": n_det, "instances_total": n_tot,
    "fraction": n_det / n_tot,
    "per_pipeline_family": {k: {"determined": v[0], "total": v[1]}
                            for k, v in sorted(per_fam.items())}}

# ---------- 4. Dataset-cluster bootstrap of delta recall (A5 - A4) -------
rng = np.random.default_rng(7)
by_ds = {}
for ds_name, g in correct.groupby(level=0):
    sub = g.reset_index()
    gt_sub = gt.loc[ds_name]
    rec = {}
    for arm in ("A5", "A4"):
        tp = int(((sub[arm] == "FAIL") & (gt_sub.values == "FAIL")).sum())
        fn = int(((sub[arm] == "PASS") & (gt_sub.values == "FAIL")).sum())
        rec[arm] = tp / (tp + fn) if tp + fn else 1.0
    by_ds[ds_name] = rec["A5"] - rec["A4"]
names = list(by_ds)
deltas = []
for _ in range(10000):
    pick = rng.choice(names, size=len(names), replace=True)
    deltas.append(np.mean([by_ds[p] for p in pick]))
out["cluster_bootstrap_delta_recall_A5_minus_A4"] = {
    "point": float(np.mean([by_ds[n] for n in names])),
    "ci95": [float(np.percentile(deltas, 2.5)), float(np.percentile(deltas, 97.5))],
    "clusters": len(names)}

# ---------- 5. Timings (zipf_s101): synopsis build + all-arm evaluation --
f = f"{ROOT}/data/zipf_s101/counts.npz"
counts = dict(np.load(f))
t0 = time.time()
syn = A.build_synopsis(counts)
t_syn = time.time() - t0
t0 = time.time()
A.run_arms(counts, syn, 101, ["A1", "A2", "A3", "A4", "A5", "A8"])
t_arms = time.time() - t0
out["timing_zipf_s101_seconds"] = {
    "synopsis_build": round(t_syn, 3), "all_arms_evaluation": round(t_arms, 3)}

# ---------- 6. One-sided 95% lower bounds for perfect proportions ---------
out["perfect_proportion_lower_bounds_95"] = {
    "recall_60_of_60": float(0.05 ** (1 / 60)),
    "repair_51_of_51": float(0.05 ** (1 / 51))}

with open(f"{ROOT}/results/post_review_analysis.json", "w") as fh:
    json.dump(out, fh, indent=1)
print(json.dumps(out, indent=1))
