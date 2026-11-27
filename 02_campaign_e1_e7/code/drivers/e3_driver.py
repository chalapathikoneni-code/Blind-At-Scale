"""E3 driver: near-threshold depth sensitivity (FINAL_EXECUTION_PROTOCOL S.3).

Modes:
  main  Consistency check + NEW runs (A8 k in {16,64,256}; A9 targets
        {16,64,256}) on the existing 8-dataset near grid; reuse of the
        existing A1/A5/A7/A8@1024/A9@1024 rows; F_B-by-depth table;
        J_B hash sensitivity (A9 targets {1024,64} x HASH_SEEDS_SENSITIVITY).
  ext   Held-out extension grid (data_ext/, 4 datasets): arms
        {A1, A5, A8@1024, A8@64, A9@1024, A9@64}, same metrics.

All study code is used UNMODIFIED (arms, ablation.truncate_syn,
classical_arms.predict_A9, common). Nothing existing is overwritten;
outputs go to final_scientific_strengthening/results/.

Run (cwd = ~/workspace/blind-at-scale/code, system python3):
    python3 ../final_scientific_strengthening/code/e3_driver.py main
    python3 ../final_scientific_strengthening/code/e3_driver.py ext
"""
import csv
import json
import os
import sys

sys.path.insert(0, os.path.expanduser("~/workspace/blind-at-scale/code"))

import numpy as np  # noqa: E402

import arms  # noqa: E402
import common  # noqa: E402
from ablation import truncate_syn  # noqa: E402
from classical_arms import HASH_SEEDS_SENSITIVITY, predict_A9  # noqa: E402

BASE = os.path.expanduser("~/workspace/blind-at-scale")
FSS = os.path.join(BASE, "final_scientific_strengthening")
OUTDIR = os.path.join(FSS, "results")
ENV_MAIN = "local, python 3.12.3/numpy 1.26.4"
ENV_EXT_GEN = ("local, realdata_arm venv python 3.12.3/numpy 2.5.5 "
               "(generation only); predictions local, python 3.12.3/numpy 1.26.4")

NEAR_TAGS = [f"near_s{s}_s{seed}" for s in (650, 680, 720, 740)
             for seed in (101, 202)]
EXT_TAGS = [f"near_s{s}_s{seed}" for s in (700, 780) for seed in (404, 505)]

FAMILIES = {
    "G": ("G_A", "G_B", "G_C", "G_N"),
    "J": ("J_A", "J_B", "J_N"),
    "F": ("F_A", "F_B"),
    "W": ("W_A",),
    "D": ("D_A",),
}
A8_KS = (16, 64, 256)
A9_TS = (16, 64, 256)


def load_counts(ddir):
    z = np.load(os.path.join(ddir, "counts.npz"))
    counts = {k: z[k] for k in z.files}
    counts["null_count"] = int(counts["null_count"])
    return counts


def gt_from_counts(counts):
    n = int(counts["fact_kA"].sum())
    fact = {c: counts[f"fact_{c}"] for c in arms.FACT_COLS}
    fact2 = {c: counts[f"fact2_{c}"] for c in arms.PAIR_COLS}
    loads = common.loads_from_counts(
        fact, fact2, float(counts["null_count"]),
        np.ones(common.K, dtype=bool), distinct_counts=counts["distinct_A"])
    verd = {p: common.verdict(l, p, n) for p, l in loads.items()}
    return loads, verd, n


def qerr(pred, gt):
    if gt == 0 and pred == 0:
        return 1.0
    if gt == 0 or pred == 0:
        return float("inf")
    return max(pred / gt, gt / pred)


def a9_loads(counts, target, hseed=0):
    pf, pf2, pn, dimp, _, f_contrib, packet, _ = predict_A9(
        counts, hseed=hseed, target=target)
    n = int(counts["fact_kA"].sum())
    loads = common.loads_from_counts(pf, pf2, pn, dimp, distinct_counts=None)
    for col in ("kA", "kB"):
        loads[f"F_{col[1]}"] = common.max_partition_load(f_contrib[col])
    verd = {p: common.verdict(l, p, n) for p, l in loads.items()}
    nbytes = len(json.dumps(packet, separators=(",", ":")))
    return loads, verd, nbytes


def a8_loads(counts, syn_trunc, seed):
    pf, pf2, pn, dimp, _ = arms.arm_predictions("A8", counts, syn_trunc,
                                                seed=seed)
    n = int(counts["fact_kA"].sum())
    loads = common.loads_from_counts(pf, pf2, pn, dimp, distinct_counts=None)
    verd = {p: common.verdict(l, p, n) for p, l in loads.items()}
    nbytes = len(json.dumps(syn_trunc))
    return loads, verd, nbytes


def metrics(records):
    """records: dicts with dataset,pipeline,gt_load,gt_verdict,pred_load,
    pred_verdict,n_fact. Returns the frozen S.3 metric bundle."""
    tp = sum(1 for r in records
             if r["gt_verdict"] == "FAIL" and r["pred_verdict"] == "FAIL")
    fp = sum(1 for r in records
             if r["gt_verdict"] == "PASS" and r["pred_verdict"] == "FAIL")
    fn = sum(1 for r in records
             if r["gt_verdict"] == "FAIL" and r["pred_verdict"] == "PASS")
    tn = sum(1 for r in records
             if r["gt_verdict"] == "PASS" and r["pred_verdict"] == "PASS")
    nfail = tp + fn
    npass = tn + fp
    qs = [qerr(r["pred_load"], r["gt_load"]) for r in records]
    fam = {}
    for name, pids in FAMILIES.items():
        sub = [qerr(r["pred_load"], r["gt_load"]) for r in records
               if r["pipeline"] in pids]
        fam[name] = {"median_q": float(np.median(sub)),
                     "max_q": float(np.max(sub)), "n": len(sub)}

    def err_list(kind):
        out = []
        for r in records:
            is_err = (r["gt_verdict"] == "FAIL" and r["pred_verdict"] == "PASS"
                      if kind == "FN" else
                      r["gt_verdict"] == "PASS" and r["pred_verdict"] == "FAIL")
            if is_err:
                thr = common.threshold(r["pipeline"], r["n_fact"])
                out.append({
                    "dataset": r["dataset"], "pipeline": r["pipeline"],
                    "gt_load": r["gt_load"], "pred_load": r["pred_load"],
                    "gt_over_threshold": r["gt_load"] / thr,
                })
        return out

    by_pipe = {}
    for kind, lst in (("FN", err_list("FN")), ("FP", err_list("FP"))):
        d = {}
        for e in lst:
            d[e["pipeline"]] = d.get(e["pipeline"], 0) + 1
        by_pipe[kind] = d
    return {
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "recall": tp / nfail if nfail else float("nan"),
        "specificity": tn / npass if npass else float("nan"),
        "n_gt_fail": nfail, "n_gt_pass": npass, "n": len(records),
        "median_q": float(np.median(qs)), "max_q": float(np.max(qs)),
        "per_family_q": fam,
        "fn_list": err_list("FN"), "fp_list": err_list("FP"),
        "errors_by_pipeline": by_pipe,
    }


def write_instance_csv(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["dataset", "seed", "n_fact", "pipeline", "gt_load",
                    "gt_verdict", "arm_point", "pred_load", "pred_verdict",
                    "bytes"])
        for r in rows:
            w.writerow([r["dataset"], r["seed"], r["n_fact"], r["pipeline"],
                        f"{r['gt_load']:.1f}", r["gt_verdict"], r["arm_point"],
                        f"{r['pred_load']:.1f}", r["pred_verdict"], r["bytes"]])


SUMMARY_ROWS = []


def summary_row(experiment, dataset, scale, environment, metric, value,
                denominator, artifact, notes):
    SUMMARY_ROWS.append({
        "experiment": experiment, "dataset": dataset, "scale": scale,
        "environment": environment, "metric": metric, "value": value,
        "denominator": denominator, "classification": "simulated",
        "artifact": artifact, "notes": notes})


def emit_point_rows(experiment, dataset_label, scale, environment, point,
                    m, bytes_list, artifact):
    bmean = float(np.mean(bytes_list)) if bytes_list else ""
    bmin = int(np.min(bytes_list)) if bytes_list else ""
    bmax = int(np.max(bytes_list)) if bytes_list else ""
    summary_row(experiment, dataset_label, scale, environment,
                f"{point} recall", f"{m['recall']:.4f}", m["n_gt_fail"],
                artifact, f"TP={m['tp']} FN={m['fn']}")
    summary_row(experiment, dataset_label, scale, environment,
                f"{point} specificity", f"{m['specificity']:.4f}",
                m["n_gt_pass"], artifact, f"TN={m['tn']} FP={m['fp']}")
    summary_row(experiment, dataset_label, scale, environment,
                f"{point} median Q-error", f"{m['median_q']:.4f}", m["n"],
                artifact, "Q = max(pred/GT, GT/pred) over all instances")
    summary_row(experiment, dataset_label, scale, environment,
                f"{point} worst-case (max) Q-error", f"{m['max_q']:.4f}",
                m["n"], artifact, "")
    for fam_name, fq in m["per_family_q"].items():
        summary_row(experiment, dataset_label, scale, environment,
                    f"{point} family {fam_name} median Q-error",
                    f"{fq['median_q']:.4f}", fq["n"], artifact, "")
        summary_row(experiment, dataset_label, scale, environment,
                    f"{point} family {fam_name} max Q-error",
                    f"{fq['max_q']:.4f}", fq["n"], artifact, "")
    if bytes_list:
        summary_row(experiment, dataset_label, scale, environment,
                    f"{point} bytes mean/min/max",
                    f"{bmean:.1f}/{bmin}/{bmax}", len(bytes_list), artifact,
                    "A8: len(json.dumps(truncated synopsis)) default "
                    "separators; A9: compact-separator packet JSON size")


def run_main():
    os.makedirs(OUTDIR, exist_ok=True)
    # ---------- 1. consistency check + reuse of existing rows ----------
    consistency = {"n_instances": 0, "verdict_mismatches": [],
                   "max_abs_gt_load_diff": 0.0}
    existing = {}  # (arm) -> records, for A1/A5/A7/A8@1024/A9@1024
    per_ds = {}
    for tag in NEAR_TAGS:
        ddir = os.path.join(BASE, "data", tag)
        counts = load_counts(ddir)
        gt_loads, gt_verd, n = gt_from_counts(counts)
        seed = int(tag.rsplit("_s", 1)[1])
        per_ds[tag] = {"counts": counts, "gt_loads": gt_loads,
                       "gt_verd": gt_verd, "n": n, "seed": seed}
        csv_path = os.path.join(
            BASE, "results", "exp_a", f"predictions_near_{tag}.csv")
        seen = set()
        with open(csv_path) as f:
            for row in csv.DictReader(f):
                pid = row["pipeline"]
                if pid in seen:
                    continue
                # GT fields are identical on every arm row of an instance
                seen.add(pid)
                consistency["n_instances"] += 1
                if row["gt_verdict"] != gt_verd[pid]:
                    consistency["verdict_mismatches"].append(
                        {"dataset": tag, "pipeline": pid,
                         "csv": row["gt_verdict"],
                         "recomputed": gt_verd[pid]})
                consistency["max_abs_gt_load_diff"] = max(
                    consistency["max_abs_gt_load_diff"],
                    abs(float(row["gt_load"]) - gt_loads[pid]))
        # existing arm rows (all arms, for reuse)
        with open(csv_path) as f:
            for row in csv.DictReader(f):
                arm = row["arm"]
                if arm not in ("A1", "A5", "A7", "A8", "A9"):
                    continue
                point = {"A8": "A8@1024", "A9": "A9@1024"}.get(arm, arm)
                existing.setdefault(point, []).append({
                    "dataset": tag, "seed": seed, "n_fact": n,
                    "pipeline": row["pipeline"],
                    "gt_load": float(row["gt_load"]),
                    "gt_verdict": row["gt_verdict"],
                    "pred_load": float(row["pred_load"]),
                    "pred_verdict": row["pred_verdict"],
                    "arm_point": point, "bytes": "",
                })
    with open(os.path.join(OUTDIR, "e3_consistency_check.json"), "w") as f:
        json.dump(consistency, f, indent=1)
    print(f"consistency: {consistency['n_instances']} instances, "
          f"{len(consistency['verdict_mismatches'])} verdict mismatches, "
          f"max |gt_load diff| = {consistency['max_abs_gt_load_diff']:.3f}")
    if consistency["verdict_mismatches"]:
        print("STOP: GT verdict mismatch - see e3_consistency_check.json")
        return 2

    points = {}   # point -> records
    bytes_by_point = {}
    for p, recs in existing.items():
        points[p] = recs

    # ---------- 2. NEW runs: A8 k in {16,64,256}, A9 targets {16,64,256}
    # plus fresh A8@1024 / A9@1024 for environment cross-check only.
    new_rows = []
    fresh_check = {"A8@1024": 0.0, "A9@1024": 0.0}
    for tag in NEAR_TAGS:
        info = per_ds[tag]
        counts, seed, n = info["counts"], info["seed"], info["n"]
        with open(os.path.join(BASE, "data", tag, "synopsis.json")) as f:
            syn = json.load(f)
        for k in A8_KS + (1024,):
            tsyn = truncate_syn(syn, k) if k < 1024 else syn
            loads, verd, nbytes = a8_loads(counts, tsyn, seed)
            point = f"A8@{k}"
            bytes_by_point.setdefault(point, []).append(nbytes)
            for pid, _, _ in common.PIPELINES:
                rec = {"dataset": tag, "seed": seed, "n_fact": n,
                       "pipeline": pid, "gt_load": info["gt_loads"][pid],
                       "gt_verdict": info["gt_verd"][pid],
                       "pred_load": loads[pid], "pred_verdict": verd[pid],
                       "arm_point": point, "bytes": nbytes}
                if k == 1024:
                    ref = [r for r in existing["A8@1024"]
                           if r["dataset"] == tag and r["pipeline"] == pid][0]
                    fresh_check["A8@1024"] = max(
                        fresh_check["A8@1024"],
                        abs(ref["pred_load"] - loads[pid]))
                else:
                    points.setdefault(point, []).append(rec)
                    new_rows.append(rec)
        for t in A9_TS + (1024,):
            loads, verd, nbytes = a9_loads(counts, t)
            point = f"A9@{t}"
            bytes_by_point.setdefault(point, []).append(nbytes)
            for pid, _, _ in common.PIPELINES:
                rec = {"dataset": tag, "seed": seed, "n_fact": n,
                       "pipeline": pid, "gt_load": info["gt_loads"][pid],
                       "gt_verdict": info["gt_verd"][pid],
                       "pred_load": loads[pid], "pred_verdict": verd[pid],
                       "arm_point": point, "bytes": nbytes}
                if t == 1024:
                    ref = [r for r in existing["A9@1024"]
                           if r["dataset"] == tag and r["pipeline"] == pid][0]
                    fresh_check["A9@1024"] = max(
                        fresh_check["A9@1024"],
                        abs(ref["pred_load"] - loads[pid]))
                else:
                    points.setdefault(point, []).append(rec)
                    new_rows.append(rec)
        print(f"{tag}: new A8/A9 depth runs done", flush=True)
    write_instance_csv(os.path.join(OUTDIR, "predictions_e3_new.csv"),
                       new_rows)

    # ---------- 3. J_B hash sensitivity ----------
    hash_rows = []
    hash_summary = {}
    headline = {}  # (target, tag, pid) -> headline verdict (H0)
    for t in (1024, 64):
        for tag in NEAR_TAGS:
            info = per_ds[tag]
            loads0, verd0, _ = a9_loads(info["counts"], t, hseed=0)
            for pid, _, _ in common.PIPELINES:
                headline[(t, tag, pid)] = verd0[pid]
    for hi, hseed in enumerate(HASH_SEEDS_SENSITIVITY):
        for t in (1024, 64):
            flips, jb_flips, recs = 0, 0, []
            for tag in NEAR_TAGS:
                info = per_ds[tag]
                loads, verd, nbytes = a9_loads(info["counts"], t, hseed=hseed)
                for pid, _, _ in common.PIPELINES:
                    flip = verd[pid] != headline[(t, tag, pid)]
                    flips += int(flip)
                    if pid == "J_B":
                        jb_flips += int(flip)
                    rec = {"dataset": tag, "seed": info["seed"],
                           "n_fact": info["n"], "pipeline": pid,
                           "gt_load": info["gt_loads"][pid],
                           "gt_verdict": info["gt_verd"][pid],
                           "pred_load": loads[pid], "pred_verdict": verd[pid],
                           "arm_point": f"A9@{t}_hash{hi+1}", "bytes": nbytes}
                    recs.append(rec)
                    hash_rows.append(rec)
            m = metrics(recs)
            hash_summary[f"A9@{t}_hash{hi+1}"] = {
                "hseed": str(hseed), "target": t,
                "verdict_flips_vs_headline_H0": flips, "of": 88,
                "jb_flips_vs_headline_H0": jb_flips, "jb_of": 8,
                "recall": m["recall"], "specificity": m["specificity"],
                "tp": m["tp"], "fp": m["fp"], "fn": m["fn"], "tn": m["tn"],
            }
            print(f"hash{hi+1} A9@{t}: flips={flips}/88 J_B flips="
                  f"{jb_flips}/8 recall={m['recall']:.3f}", flush=True)
    write_instance_csv(
        os.path.join(OUTDIR, "predictions_e3_hash_sensitivity.csv"), hash_rows)

    # ---------- 4. pooled metrics + F_B by depth ----------
    summary = {"consistency": consistency, "fresh_vs_existing_max_abs_diff":
               fresh_check, "points": {}, "hash_sensitivity": hash_summary}
    for point in ("A1", "A5", "A7", "A8@16", "A8@64", "A8@256", "A8@1024",
                  "A9@16", "A9@64", "A9@256", "A9@1024"):
        m = metrics(points[point])
        summary["points"][point] = m
        bl = bytes_by_point.get(point, [])
        emit_point_rows("E3", "near_grid_8ds", "near-threshold grid, 88 instances",
                        ENV_MAIN, point, m, bl,
                        "final_scientific_strengthening/results/"
                        "predictions_e3_new.csv")
    # F_B specifically per depth (near grid)
    fb = {}
    for point, recs in points.items():
        sub = [r for r in recs if r["pipeline"] == "F_B"]
        if sub:
            qs = [qerr(r["pred_load"], r["gt_load"]) for r in sub]
            fb[point] = {"median_q": float(np.median(qs)),
                         "max_q": float(np.max(qs)), "n": len(sub),
                         "per_dataset_q": {
                             r["dataset"]: qerr(r["pred_load"], r["gt_load"])
                             for r in sub}}
            summary_row("E3", "near_grid_8ds",
                        "near-threshold grid, F_B only (8 instances)",
                        ENV_MAIN, f"{point} F_B median Q-error",
                        f"{fb[point]['median_q']:.4f}", 8,
                        "final_scientific_strengthening/results/"
                        "predictions_e3_new.csv",
                        "near grid is Zipf-only; mixed-family F_B lives on "
                        "the headline grid (E2)")
            summary_row("E3", "near_grid_8ds",
                        "near-threshold grid, F_B only (8 instances)",
                        ENV_MAIN, f"{point} F_B max Q-error",
                        f"{fb[point]['max_q']:.4f}", 8,
                        "final_scientific_strengthening/results/"
                        "predictions_e3_new.csv", "")
    summary["fb_by_depth"] = fb
    for key, hs in hash_summary.items():
        summary_row("E3", "near_grid_8ds", "near-threshold grid, 88 instances",
                    ENV_MAIN, f"{key} verdict flips vs headline H0",
                    hs["verdict_flips_vs_headline_H0"], 88,
                    "final_scientific_strengthening/results/"
                    "predictions_e3_hash_sensitivity.csv",
                    "sensitivity hashes are correlated by construction "
                    "(h_{H+1}(v) = h_H(v+1)), not independent replications")
        summary_row("E3", "near_grid_8ds", "near-threshold grid, J_B only",
                    ENV_MAIN, f"{key} J_B verdict flips vs headline H0",
                    hs["jb_flips_vs_headline_H0"], 8,
                    "final_scientific_strengthening/results/"
                    "predictions_e3_hash_sensitivity.csv", "")
    with open(os.path.join(OUTDIR, "e3_summary.json"), "w") as f:
        json.dump(summary, f, indent=1)
    write_summary_rows(os.path.join(OUTDIR, "rows_e3.csv"))
    print(json.dumps({p: {k: summary["points"][p][k] for k in
                           ("recall", "specificity", "tp", "fp", "fn", "tn",
                            "median_q", "max_q")}
                      for p in summary["points"]}, indent=1))
    print(f"fresh-vs-existing max abs pred diff: {fresh_check}")
    return 0


def write_summary_rows(path, append=False):
    exists = append and os.path.exists(path)
    with open(path, "a" if exists else "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "experiment", "dataset", "scale", "environment", "metric",
            "value", "denominator", "classification", "artifact", "notes"])
        if not exists:
            w.writeheader()
        for r in SUMMARY_ROWS:
            w.writerow(r)
    SUMMARY_ROWS.clear()


def run_ext():
    os.makedirs(OUTDIR, exist_ok=True)
    rows_path = os.path.join(OUTDIR, "rows_e3.csv")
    all_recs = {}
    detail = {}
    for tag in EXT_TAGS:
        ddir = os.path.join(FSS, "data_ext", tag)
        with open(os.path.join(ddir, "meta.json")) as f:
            meta = json.load(f)
        with open(os.path.join(ddir, "validation.json")) as f:
            val = json.load(f)
        assert meta["validation_ok"] and val["all_exact"], \
            f"{tag}: validation not all-exact"
        counts = load_counts(ddir)
        seed = meta["seed"]
        syn = arms.build_synopsis(counts)
        with open(os.path.join(ddir, "synopsis.json"), "w") as f:
            json.dump(syn, f)
        gt_loads, gt_verd, results = arms.run_arms(
            counts, syn, seed, ["A1", "A2", "A3", "A4", "A5", "A8"], r=0.01)
        n = int(counts["fact_kA"].sum())
        detail[tag] = {"gt_fail": sum(1 for v in gt_verd.values()
                                      if v == "FAIL"),
                       "validation_ok": True}
        src = {"A1": results["A1"], "A5": results["A5"],
               "A8@1024": results["A8"]}
        for point, res in src.items():
            for pid, _, _ in common.PIPELINES:
                all_recs.setdefault(point, []).append({
                    "dataset": tag, "seed": seed, "n_fact": n,
                    "pipeline": pid, "gt_load": gt_loads[pid],
                    "gt_verdict": gt_verd[pid],
                    "pred_load": res["pred_loads"][pid],
                    "pred_verdict": res["pred_verdicts"][pid],
                    "arm_point": point, "bytes": len(json.dumps(syn))
                    if point == "A8@1024" else ""})
        # A8@64
        tsyn = truncate_syn(syn, 64)
        loads, verd, nbytes = a8_loads(counts, tsyn, seed)
        for pid, _, _ in common.PIPELINES:
            all_recs.setdefault("A8@64", []).append({
                "dataset": tag, "seed": seed, "n_fact": n, "pipeline": pid,
                "gt_load": gt_loads[pid], "gt_verdict": gt_verd[pid],
                "pred_load": loads[pid], "pred_verdict": verd[pid],
                "arm_point": "A8@64", "bytes": nbytes})
        # A9@1024, A9@64
        for t in (1024, 64):
            loads, verd, nbytes = a9_loads(counts, t)
            for pid, _, _ in common.PIPELINES:
                all_recs.setdefault(f"A9@{t}", []).append({
                    "dataset": tag, "seed": seed, "n_fact": n,
                    "pipeline": pid, "gt_load": gt_loads[pid],
                    "gt_verdict": gt_verd[pid], "pred_load": loads[pid],
                    "pred_verdict": verd[pid], "arm_point": f"A9@{t}",
                    "bytes": nbytes})
        print(f"{tag}: extension arms done (GT FAIL "
              f"{detail[tag]['gt_fail']}/11)", flush=True)
    flat = [r for recs in all_recs.values() for r in recs]
    write_instance_csv(os.path.join(OUTDIR, "predictions_e3_extension.csv"),
                       flat)
    summary = {"datasets": detail, "points": {}}
    for point, recs in all_recs.items():
        m = metrics(recs)
        summary["points"][point] = m
        bl = [r["bytes"] for r in recs if isinstance(r["bytes"], int)]
        # bytes are per-dataset; dedupe by dataset
        seen = {}
        for r in recs:
            if isinstance(r["bytes"], int):
                seen[r["dataset"]] = r["bytes"]
        emit_point_rows("E3-extension", "extension_grid_4ds",
                        "held-out extension grid, 44 instances", ENV_EXT_GEN,
                        point, m, list(seen.values()),
                        "final_scientific_strengthening/results/"
                        "predictions_e3_extension.csv")
    with open(os.path.join(OUTDIR, "e3_extension_summary.json"), "w") as f:
        json.dump(summary, f, indent=1)
    write_summary_rows(rows_path, append=True)
    print(json.dumps({p: {k: summary["points"][p][k] for k in
                           ("recall", "specificity", "tp", "fp", "fn", "tn")}
                      for p in summary["points"]}, indent=1))
    return 0


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "main"
    sys.exit(run_main() if mode == "main" else run_ext())
