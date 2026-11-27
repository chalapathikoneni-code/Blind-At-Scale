"""POST-HOC (labelled exploratory; NOT pre-registered) threshold
sensitivity: verdict agreement recomputed from the RECORDED loads in
predictions_main.csv at threshold multipliers m x T, per arm, overall
and on the fact-fact (F) subset. If an arm's loads are faithful, its
verdicts stay correct as m moves; if its loads are biased (e.g. A4 on
F), verdicts flip once m passes the bias factor.

Also produces the manuscript figures.
"""
import glob, json, os
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE = os.path.expanduser("~/workspace/blind-at-scale")
DATASETS = [f"{f}_s{s}" for f in ("zipf", "lognormal", "mixed") for s in (101, 202, 303)]
MULTS = [0.25, 0.5, 1, 2, 5, 10, 20, 50, 100]

rows = []
for tag in DATASETS:
    d = pd.read_csv(os.path.join(BASE, "data", tag, "predictions_main.csv"))
    rows.append(d)
df = pd.concat(rows, ignore_index=True)


def thr(row, m):
    t = 50_000 if row["pipeline"] == "D_A" else 0.02 * row["n_fact"]
    return t * m


def recall_at(data, arm, m):
    sub = data[data.arm == arm]
    gt_fail = sub.apply(lambda r: r.gt_load > thr(r, m), axis=1)
    pr_fail = sub.apply(lambda r: r.pred_load > thr(r, m), axis=1)
    tp = int((gt_fail & pr_fail).sum()); fn = int((gt_fail & ~pr_fail).sum())
    fp = int((~gt_fail & pr_fail).sum())
    return {"recall": tp / (tp + fn) if tp + fn else float("nan"),
            "precision": tp / (tp + fp) if tp + fp else float("nan")}


out = {"overall": {}, "factfact_subset": {}}
fdf = df[df.pipeline.isin(["F_A", "F_B"])]
for arm in ("A1", "A2", "A3", "A4", "A5", "A8"):
    out["overall"][arm] = {str(m): recall_at(df, arm, m) for m in MULTS}
    out["factfact_subset"][arm] = {str(m): recall_at(fdf, arm, m) for m in MULTS}
with open(os.path.join(BASE, "results", "threshold_sensitivity.json"), "w") as f:
    json.dump(out, f, indent=1)
print("F-subset recall by threshold multiplier:")
for arm in ("A1", "A3", "A4", "A5", "A8"):
    print(arm, {m: round(out["factfact_subset"][arm][str(m)]["recall"], 2) for m in MULTS})

# ---- figures ----
os.makedirs(os.path.join(BASE, "figures"), exist_ok=True)
S = json.load(open(os.path.join(BASE, "results", "summary.json")))
arms = ["A1", "A2", "A3", "A4", "A5", "A8"]
fig, ax = plt.subplots(figsize=(5.2, 2.9))
x = np.arange(len(arms)); w = 0.38
ax.bar(x - w / 2, [S["arms"][a]["recall"] for a in arms], w, label="Recall")
ax.bar(x + w / 2, [S["arms"][a]["precision"] for a in arms], w, label="Precision")
ax.set_xticks(x); ax.set_xticklabels(arms); ax.set_ylim(0, 1.08)
ax.set_ylabel("FAIL prediction"); ax.legend(frameon=False, fontsize=8)
ax.set_title("Verdict prediction by arm (99 instances)", fontsize=9)
fig.tight_layout(); fig.savefig(os.path.join(BASE, "figures", "fig_verdicts.png"), dpi=200)
plt.close(fig)

fig, axes = plt.subplots(1, 3, figsize=(7.4, 2.7), sharex=True, sharey=True)
for ax, arm in zip(axes, ("A1", "A4", "A5")):
    sub = df[df.arm == arm]
    ax.scatter(sub.gt_load, sub.pred_load, s=7, alpha=0.55)
    lim = [3e2, 3e9]
    ax.plot(lim, lim, "k--", lw=0.7)
    ax.axhline(200_000, color="grey", lw=0.6); ax.axvline(200_000, color="grey", lw=0.6)
    ax.set_xscale("log"); ax.set_yscale("log"); ax.set_xlim(lim); ax.set_ylim(lim)
    ax.set_title(arm, fontsize=9); ax.set_xlabel("ground-truth load", fontsize=8)
axes[0].set_ylabel("predicted load", fontsize=8)
fig.tight_layout(); fig.savefig(os.path.join(BASE, "figures", "fig_loads.png"), dpi=200)
plt.close(fig)

fig, ax = plt.subplots(figsize=(5.2, 2.9))
for arm in ("A1", "A3", "A4", "A5", "A8"):
    ys = [out["factfact_subset"][arm][str(m)]["recall"] for m in MULTS]
    ax.plot(MULTS, ys, marker="o", ms=3.5, label=arm)
ax.set_xscale("log"); ax.set_xlabel("failure-threshold multiplier (x T)")
ax.set_ylabel("recall on fact-fact joins"); ax.set_ylim(-0.05, 1.08)
ax.legend(frameon=False, fontsize=8)
ax.set_title("Post-hoc sensitivity: verdicts vs threshold (F subset)", fontsize=9)
fig.tight_layout(); fig.savefig(os.path.join(BASE, "figures", "fig_sensitivity.png"), dpi=200)
plt.close(fig)
print("figures written")
