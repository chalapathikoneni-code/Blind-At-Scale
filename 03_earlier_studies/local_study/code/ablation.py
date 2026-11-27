"""Synopsis-information ablation for Blind at Scale (WORKSTREAM 2).

Answers hostile-review item 7.2.3 / N1: fidelity as a function of synopsis
information content, separating the synopsis METHOD from mere TRANSCRIPTION
of stored heavy hitters. Every rung is an A8-style direct prediction
(PREREGISTRATION.md S.4: analytic prediction from the synopsis
reconstruction, loads computed on the reconstructed count vectors). A5 is
not re-run: A5 == A8 on verdicts in the headline grid (summary.json,
McNemar 0/0), and A5's only extra variance is testbed multinomial sampling
noise, which is not synopsis information.

Ground truth is REUSED, never regenerated: per-dataset counts.npz ->
common.loads_from_counts with the exact distinct counts (identical to
code/run_dataset.py), cross-checked against the recorded gt_load values in
data/<tag>/predictions_main.csv. Datasets: the existing 9-dataset grid
{zipf,lognormal,mixed} x seeds {101,202,303} = 99 instances.

RUNGS (each a full pass over all 99 instances):
  full_k1024          control: unmodified synopsis (= headline A8).
  hh_k16/hh_k64/hh_k256
                      heavy-hitter truncation: keep top-k stored HH
                      (cols and pairs), dropped mass folded into
                      tail_mass; tail refit by arms._tail_expected exactly
                      as in the A8 path.
  no_aligned_pairs    fan-out pairs withheld: the stored aligned c2 values
                      are permuted across the stored HH key set with a
                      fixed permutation (seed PAIR_PERM_SEED below), so
                      both marginal multisets and the tail model are
                      preserved but the (c1(k), c2(k)) pairing is
                      destroyed. Fact/fact2 treated as independent
                      marginals at the HH level.
  no_join_conditioning
                      pairs block dropped entirely: F loads use the
                      per-column marginal reconstruction for fact and an
                      INDEPENDENT marginal reconstruction for fact2 built
                      only from its stored marginal summary
                      (fact2_hh_counts + implied total): fact2 HH counts
                      are assigned to the 1024 keys with smallest
                      splitmix64(key + FACT2_KEY_OFFSET) hash and the tail
                      (fitted law, mass = total2 - sum(fact2_hh_counts))
                      over the remaining keys in that same independent
                      order. No HH alignment, no comonotonic tail. All
                      non-F pipelines are structurally unaffected (A8's
                      G/J/W/D loads never consume pair info; the dim is
                      emitted in full by declared method property), so
                      they reuse the control prediction by construction.
  noise_eps8/noise_eps1
                      DP-style noise: independent Laplace(0, 1/eps) noise
                      added to every stored heavy-hitter count (cols,
                      pair c1 -- the SAME noisy values as the corresponding
                      column, since the synopsis stores them twice --,
                      pair c2, and fact2_hh_counts). SENSITIVITY
                      ASSUMPTION: per-count sensitivity 1 (a single row
                      changes any one stored count by at most 1), so the
                      Laplace scale is 1/eps per count. Noisy counts are
                      clipped at >= 0 and totals are renormalised: each
                      column/pair-side is rescaled so HH sum + tail mass
                      equals the original total exactly. RNG: one
                      numpy Generator per dataset,
                      seed = NOISE_SEED_BASE + dataset seed, draws in the
                      fixed order cols kA,kB,kC,kN then pairs
                      (kA c2, kA fact2_hh, kB c2, kB fact2_hh). This is
                      per-count Laplace noise in the style of DP; NO formal
                      DP guarantee is claimed for the synopsis as a whole
                      (key identities, totals and tail masses are exact).
  uniform_tail        coarsening (the ONE chosen coarsening): the fitted
                      power-law tail is replaced by a UNIFORM-over-tail
                      law -- every tail key gets tail_mass / n_tail_keys.
                      Stored HH counts are untouched. Implemented by
                      patching arms._tail_expected during the rung.
  stored_only         transcription floor: heavy hitters only, all tail
                      masses set to 0 (prediction uses ONLY stored values,
                      no fitted/predicted tail at all).

Outputs (nothing existing is overwritten):
  results/ablation/predictions_<rung>.csv   per-instance predictions
  results/ablation/ablation_summary.json    pooled + per-family metrics
  results/ablation/ablation_table.md        markdown tables
"""
import copy
import csv
import json
import os
import sys

import numpy as np
from scipy import stats

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import arms as A  # noqa: E402
import common as C  # noqa: E402

BASE = os.path.expanduser("~/workspace/blind-at-scale")
OUTDIR = os.path.join(BASE, "results", "ablation")
DATASETS = [f"{f}_s{s}" for f in ("zipf", "lognormal", "mixed")
            for s in (101, 202, 303)]
PAIR_PERM_SEED = 20261106
FACT2_KEY_OFFSET = 104729
NOISE_SEED_BASE = 777000

RUNGS = ["full_k1024", "hh_k16", "hh_k64", "hh_k256",
         "no_aligned_pairs", "no_join_conditioning",
         "noise_eps8", "noise_eps1", "uniform_tail", "stored_only"]

PIPE_FAMILIES = {
    "G/D direct": ("G_A", "G_B", "G_C", "G_N", "D_A"),
    "J": ("J_A", "J_B", "J_N"),
    "F": ("F_A", "F_B"),
    "W": ("W_A",),
}


# ---------------- synopsis transforms --------------------------------------
def truncate_syn(syn, k):
    s = copy.deepcopy(syn)
    for col in s["cols"].values():
        col["hh_keys"] = col["hh_keys"][:k]
        col["hh_counts"] = col["hh_counts"][:k]
        col["tail_mass"] = int(col["total"] - sum(col["hh_counts"]))
    for pair in s["pairs"].values():
        total1 = sum(pair["hh_c1"]) + pair["tail_mass_c1"]
        total2 = sum(pair["hh_c2"]) + pair["tail_mass_c2"]
        pair["hh_keys"] = pair["hh_keys"][:k]
        pair["hh_c1"] = pair["hh_c1"][:k]
        pair["hh_c2"] = pair["hh_c2"][:k]
        pair["fact2_hh_counts"] = pair["fact2_hh_counts"][:k]
        pair["tail_mass_c1"] = int(total1 - sum(pair["hh_c1"]))
        pair["tail_mass_c2"] = int(total2 - sum(pair["hh_c2"]))
    return s


def permute_pairs_syn(syn):
    s = copy.deepcopy(syn)
    perm = np.random.default_rng(PAIR_PERM_SEED).permutation(C.TOP_K)
    for pair in s["pairs"].values():
        c2 = np.array(pair["hh_c2"])
        pair["hh_c2"] = c2[perm].tolist()
    return s


def zero_tail_syn(syn):
    s = copy.deepcopy(syn)
    for col in s["cols"].values():
        col["tail_mass"] = 0
    for pair in s["pairs"].values():
        pair["tail_mass_c1"] = 0
        pair["tail_mass_c2"] = 0
    return s


def noise_syn(syn, eps, dataset_seed):
    s = copy.deepcopy(syn)
    rng = np.random.default_rng(NOISE_SEED_BASE + dataset_seed)
    scale = 1.0 / eps

    def noisy(counts):
        v = np.array(counts, dtype=np.float64)
        return np.clip(v + rng.laplace(0.0, scale, size=len(v)), 0.0, None)

    col_noisy = {}
    for name in A.FACT_COLS:
        col = s["cols"][name]
        nv = noisy(col["hh_counts"])
        f = col["total"] / (nv.sum() + col["tail_mass"])
        col["hh_counts"] = (nv * f).tolist()
        col["tail_mass"] = int(round(col["tail_mass"] * f))
        col_noisy[name] = col["hh_counts"]
    for name in A.PAIR_COLS:
        pair = s["pairs"][name]
        pair["hh_c1"] = list(col_noisy[name])  # same stored counts as col
        total1 = sum(syn["pairs"][name]["hh_c1"]) + syn["pairs"][name]["tail_mass_c1"]
        pair["tail_mass_c1"] = int(round(total1 - sum(pair["hh_c1"])))
        total2 = sum(syn["pairs"][name]["hh_c2"]) + syn["pairs"][name]["tail_mass_c2"]
        nv2 = noisy(pair["hh_c2"])
        f2 = total2 / (nv2.sum() + syn["pairs"][name]["tail_mass_c2"])
        pair["hh_c2"] = (nv2 * f2).tolist()
        pair["tail_mass_c2"] = int(round(syn["pairs"][name]["tail_mass_c2"] * f2))
        fh = np.array(syn["pairs"][name]["fact2_hh_counts"], dtype=np.float64)
        nfh = np.clip(fh + rng.laplace(0.0, scale, size=len(fh)), 0.0, None)
        pair["fact2_hh_counts"] = (nfh * (fh.sum() / nfh.sum())).tolist()
    return s


# ---------------- prediction ------------------------------------------------
def _uniform_tail(hh_counts, tail_mass, n_tail_keys):
    return np.full(n_tail_keys, tail_mass / max(n_tail_keys, 1))


def fact2_independent_vector(syn_pair):
    """fact2 count vector from its marginal summary only, key assignment
    independent of fact (see module docstring, rung no_join_conditioning)."""
    hh = np.array(syn_pair["fact2_hh_counts"], dtype=np.float64)
    total2 = sum(syn_pair["hh_c2"]) + syn_pair["tail_mass_c2"]
    tail_mass2 = total2 - hh.sum()
    keys = np.arange(C.K, dtype=np.int64)
    order = keys[np.argsort(C.splitmix64_hash(keys + FACT2_KEY_OFFSET),
                            kind="stable")]
    out = np.zeros(C.K)
    out[order[:len(hh)]] = hh
    out[order[len(hh):]] = A._tail_expected(hh, tail_mass2, C.K - len(hh))
    return out


def predict(syn, rung, counts, seed):
    """Returns pred_loads dict for one dataset under one rung (A8-style)."""
    if rung == "uniform_tail":
        orig = A._tail_expected
        A._tail_expected = _uniform_tail
        try:
            pf, pf2, pn, dimp, _ = A.arm_predictions("A8", counts, syn, seed=seed)
        finally:
            A._tail_expected = orig
    else:
        pf, pf2, pn, dimp, _ = A.arm_predictions("A8", counts, syn, seed=seed)
    loads = C.loads_from_counts(pf, pf2, pn, dimp, distinct_counts=None)
    if rung == "no_join_conditioning":
        for col, pid in (("kA", "F_A"), ("kB", "F_B")):
            c2i = fact2_independent_vector(syn["pairs"][col])
            loads[pid] = C.max_partition_load(pf[col] * c2i)
    return loads


def degraded_syn(syn, rung, dataset_seed):
    if rung in ("full_k1024", "uniform_tail", "no_join_conditioning"):
        return copy.deepcopy(syn)
    if rung.startswith("hh_k"):
        return truncate_syn(syn, int(rung[4:]))
    if rung == "no_aligned_pairs":
        return permute_pairs_syn(syn)
    if rung == "stored_only":
        return zero_tail_syn(syn)
    if rung == "noise_eps8":
        return noise_syn(syn, 8.0, dataset_seed)
    if rung == "noise_eps1":
        return noise_syn(syn, 1.0, dataset_seed)
    raise ValueError(rung)


# ---------------- scoring ----------------------------------------------------
def prf(rows):
    tp = sum(1 for r in rows if r["gt"] == "FAIL" and r["pred"] == "FAIL")
    fp = sum(1 for r in rows if r["gt"] == "PASS" and r["pred"] == "FAIL")
    fn = sum(1 for r in rows if r["gt"] == "FAIL" and r["pred"] == "PASS")
    tn = sum(1 for r in rows if r["gt"] == "PASS" and r["pred"] == "PASS")
    rec = tp / (tp + fn) if tp + fn else float("nan")
    prec = tp / (tp + fp) if tp + fp else float("nan")
    f1 = 2 * rec * prec / (rec + prec) if (rec + prec) else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "recall": rec, "precision": prec, "f1": f1, "n": len(rows)}


def main():
    os.makedirs(OUTDIR, exist_ok=True)
    all_rows = {rung: [] for rung in RUNGS}
    control_check = []
    for tag in DATASETS:
        d = os.path.join(BASE, "data", tag)
        with open(os.path.join(d, "meta.json")) as f:
            meta = json.load(f)
        with open(os.path.join(d, "synopsis.json")) as f:
            syn = json.load(f)
        z = np.load(os.path.join(d, "counts.npz"))
        counts = {k: z[k] for k in z.files}
        counts["null_count"] = int(counts["null_count"])
        n = int(counts["fact_kA"].sum())
        fact = {c: counts[f"fact_{c}"] for c in A.FACT_COLS}
        fact2 = {c: counts[f"fact2_{c}"] for c in A.PAIR_COLS}
        gt_loads = C.loads_from_counts(fact, fact2, float(counts["null_count"]),
                                       np.ones(C.K, dtype=bool),
                                       distinct_counts=counts["distinct_A"])
        gt_verd = {p: C.verdict(l, p, n) for p, l in gt_loads.items()}
        # cross-check GT against the recorded predictions file
        recorded = {}
        with open(os.path.join(d, "predictions_main.csv")) as f:
            for r in csv.DictReader(f):
                if r["arm"] == "A8":
                    recorded[r["pipeline"]] = (float(r["gt_load"]),
                                               float(r["pred_load"]))
        for rung in RUNGS:
            dsyn = degraded_syn(syn, rung, meta["seed"])
            loads = predict(dsyn, rung, counts, meta["seed"])
            for pid, _, _ in C.PIPELINES:
                row = {"dataset": tag, "family": meta["family"],
                       "seed": meta["seed"], "n_fact": n, "pipeline": pid,
                       "gt_load": gt_loads[pid], "gt": gt_verd[pid],
                       "pred_load": loads[pid],
                       "pred": C.verdict(loads[pid], pid, n)}
                all_rows[rung].append(row)
                if rung == "full_k1024":
                    gl, pl = recorded[pid]
                    control_check.append((abs(gl - gt_loads[pid]),
                                          abs(pl - loads[pid])))
        print(f"{tag}: done", flush=True)
    mx_gt = max(x[0] for x in control_check)
    mx_pred = max(x[1] for x in control_check)
    print(f"control check: max |recomputed GT - recorded GT| = {mx_gt:.2f}; "
          f"max |control pred - recorded A8 pred| = {mx_pred:.2f} "
          f"(recorded CSVs are rounded to 0.1)")

    summary = {"n_instances": len(all_rows["full_k1024"]),
               "control_check_max_abs_pred_diff_vs_recorded_A8": mx_pred,
               "control_check_max_abs_gt_diff_vs_recorded": mx_gt,
               "rungs": {}}
    for rung in RUNGS:
        rows = all_rows[rung]
        with open(os.path.join(OUTDIR, f"predictions_{rung}.csv"), "w",
                  newline="") as f:
            w = csv.writer(f)
            w.writerow(["dataset", "family", "seed", "n_fact", "pipeline",
                        "gt_load", "gt_verdict", "rung", "pred_load",
                        "pred_verdict"])
            for r in rows:
                w.writerow([r["dataset"], r["family"], r["seed"], r["n_fact"],
                            r["pipeline"], f"{r['gt_load']:.1f}", r["gt"],
                            rung, f"{r['pred_load']:.1f}", r["pred"]])
        m = prf(rows)
        pred = np.array([r["pred_load"] for r in rows])
        gt = np.array([r["gt_load"] for r in rows])
        m["spearman_rho"] = float(stats.spearmanr(
            np.log10(np.maximum(pred, 1e-9)), np.log10(gt)).statistic)
        m["median_q_error"] = float(np.median(np.maximum(pred / gt, gt / pred)))
        fam = {}
        for name, pids in PIPE_FAMILIES.items():
            sub = [r for r in rows if r["pipeline"] in pids]
            fm = prf(sub)
            sp = np.array([r["pred_load"] for r in sub])
            sg = np.array([r["gt_load"] for r in sub])
            fm["median_q_error"] = float(np.median(
                np.maximum(sp / sg, sg / sp)))
            fm["mean_q_error"] = float(np.mean(
                np.maximum(sp / sg, sg / sp)))
            fm["median_pred_over_gt"] = float(np.median(sp / sg))
            fam[name] = fm
        summary["rungs"][rung] = {"pooled": m, "per_pipeline_family": fam}
    with open(os.path.join(OUTDIR, "ablation_summary.json"), "w") as f:
        json.dump(summary, f, indent=1)

    def fmt(x):
        return "nan" if x != x else f"{x:.3f}"

    lines = ["# Synopsis-information ablation (A8-style direct prediction, "
             "99 instances)", "",
             "Pooled over the 9-dataset grid. FAIL is the positive class. "
             "full_k1024 is the unmodified synopsis (headline A8 control).",
             "", "| Rung | Recall | Precision | F1 | Spearman rho | Median Q-error | TP | FP | FN | TN |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    for rung in RUNGS:
        m = summary["rungs"][rung]["pooled"]
        lines.append(f"| {rung} | {fmt(m['recall'])} | {fmt(m['precision'])} | "
                     f"{fmt(m['f1'])} | {fmt(m['spearman_rho'])} | "
                     f"{fmt(m['median_q_error'])} | {m['tp']} | {m['fp']} | "
                     f"{m['fn']} | {m['tn']} |")
    lines += ["", "## Per-pipeline-family recall", "",
              "| Rung | G/D direct (n=45) | J (n=27) | F (n=18) | W (n=9) |",
              "|---|---|---|---|---|"]
    for rung in RUNGS:
        fam = summary["rungs"][rung]["per_pipeline_family"]
        lines.append(f"| {rung} | {fmt(fam['G/D direct']['recall'])} | "
                     f"{fmt(fam['J']['recall'])} | {fmt(fam['F']['recall'])} | "
                     f"{fmt(fam['W']['recall'])} |")
    lines += ["", "## Per-pipeline-family median Q-error (load fidelity)", "",
              "Verdict metrics saturate on this grid (F ground-truth loads "
              "exceed the threshold by orders of magnitude), so Q-error is "
              "the discriminating metric for the pair rungs.", "",
              "| Rung | G/D direct | J | F | W | F median pred/GT |",
              "|---|---|---|---|---|---|"]
    for rung in RUNGS:
        fam = summary["rungs"][rung]["per_pipeline_family"]
        lines.append(f"| {rung} | {fmt(fam['G/D direct']['median_q_error'])} | "
                     f"{fmt(fam['J']['median_q_error'])} | "
                     f"{fmt(fam['F']['median_q_error'])} | "
                     f"{fmt(fam['W']['median_q_error'])} | "
                     f"{fmt(fam['F']['median_pred_over_gt'])} |")
    lines += ["", "Rung definitions, noise mechanism (per-count sensitivity 1, "
              "Laplace scale 1/eps, clip >= 0, totals renormalised, seeds in "
              "script header) and the uniform-tail coarsening are documented "
              "in code/ablation.py. DP-style noise only; no formal DP claim "
              "for the synopsis as a whole."]
    with open(os.path.join(OUTDIR, "ablation_table.md"), "w") as f:
        f.write("\n".join(lines) + "\n")
    print(json.dumps({r: summary["rungs"][r]["pooled"] for r in RUNGS},
                     indent=1))


if __name__ == "__main__":
    main()
