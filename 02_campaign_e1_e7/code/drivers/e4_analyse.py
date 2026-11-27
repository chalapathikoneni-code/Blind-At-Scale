"""E4 analysis per FINAL_EXECUTION_PROTOCOL.md section 4 (+ section 7).

Sets (never pooled): real-data Stage C 24 physical + 6 analytic F;
synthetic 7 physical + 2 analytic F. Stage attribution: ONLY the
verifier-recorded gt_load_stage_id in each ground-truth JSON.
Synthetic analytic load = verifier-recorded D8 (in-job partition-id)
load; F loads are analytic by construction (executed == analytic).
"""
import csv
import json
import math
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

BASE = os.path.expanduser("~/workspace/blind-at-scale")
FS = os.path.join(BASE, "final_scientific_strengthening")
ENV = "AWS EMR (user-executed), logs re-analysed locally"
PHYS = ["G_A", "G_B", "G_C", "G_N", "J_A", "J_B", "J_N", "W_A"]
SYN_PHYS = ["G_A", "G_B", "G_N", "J_A", "J_B", "J_N", "W_A"]


def ranks(x):
    order = np.argsort(x)
    r = np.empty(len(x), float)
    i = 0
    while i < len(x):
        j = i
        while j + 1 < len(x) and x[order[j + 1]] == x[order[i]]:
            j += 1
        r[order[i:j + 1]] = (i + j) / 2.0 + 1
        i = j + 1
    return r


def spearman(x, y):
    if len(x) < 3 or len(set(x)) < 2 or len(set(y)) < 2:
        return None
    return float(np.corrcoef(ranks(np.asarray(x, float)),
                             ranks(np.asarray(y, float)))[0, 1])


def pearson_log(x, y):
    if len(x) < 3:
        return None
    return float(np.corrcoef(np.log10(x), np.log10(y))[0, 1])


def ratio_stats(ratios):
    r = np.asarray(ratios, float)
    return {"n": int(len(r)), "median": float(np.median(r)),
            "min": float(r.min()), "max": float(r.max()),
            "mean": float(r.mean())}


def load_json(p):
    return json.load(open(p))


def main():
    syn_agg = load_json(f"{FS}/results/e4_stage_aggregates_synthetic.json")
    rd_agg = load_json(f"{FS}/results/e4_stage_aggregates_realdata.json")
    syn_gt = load_json(f"{BASE}/emr_results_synthetic/round2/emr_verify_out/"
                       "emr_ground_truth_100m.json")["pipelines"]
    rd_gt = load_json(f"{BASE}/emr_results_realdata/realdata/"
                      "realdata_verify_out/realdata_ground_truth.json")["entries"]

    rows = []          # rows_e4.csv
    fig_pts = []       # (campaign, analytic, executed, kind, label, spill)
    detail = {"sets": {}, "pipelines": [], "spill_events": []}

    def emit(dataset, scale, metric, value, denom, classification, artifact, notes):
        rows.append({"experiment": "E4", "dataset": dataset, "scale": scale,
                     "environment": ENV, "metric": metric, "value": value,
                     "denominator": denom, "classification": classification,
                     "artifact": artifact, "notes": notes})

    # ---------- synthetic set ----------
    syn_pts = []
    for p in SYN_PHYS + ["F_A", "F_B"]:
        v = syn_gt[p]
        kind = "physical" if p in SYN_PHYS else "analytic"
        sid = v.get("gt_load_stage_id")
        st = syn_agg["stages"].get(str(sid)) if sid is not None else None
        analytic = v["d8_load"] if kind == "physical" else v["gt_load"]
        executed = v["gt_load"] if kind == "physical" else None
        rec = {"campaign": "synthetic", "pipeline": p, "kind": kind,
               "stage_id": sid, "analytic_load": analytic,
               "executed_load_verifier": executed,
               "executed_load_logderived": st["max_read_records"] if st else None,
               "stage": st}
        detail["pipelines"].append(rec)
        if kind == "physical":
            syn_pts.append((p, analytic, executed, st))
            assert st and st["max_read_records"] == int(executed), p
            fig_pts.append(("synthetic", analytic, executed, "physical", p,
                            bool(st and st["spill_total"] > 0)))
        else:
            fig_pts.append(("synthetic", analytic, analytic, "analytic", p, False))
    ratios = [e / a for _, a, e, _ in syn_pts]
    detail["sets"]["synthetic_physical"] = {
        "ratio": ratio_stats(ratios),
        "spearman_rho": spearman([a for _, a, _, _ in syn_pts],
                                 [e for _, _, e, _ in syn_pts]),
        "pearson_log10": pearson_log([a for _, a, _, _ in syn_pts],
                                     [e for _, _, e, _ in syn_pts]),
        "exec_vs_maxdur_spearman": spearman(
            [e for _, _, e, _ in syn_pts], [s["dur_max_ms"] for _, _, _, s in syn_pts]),
        "exec_vs_spill_spearman": spearman(
            [e for _, _, e, _ in syn_pts], [s["spill_total"] for _, _, _, s in syn_pts]),
    }

    # ---------- real-data Stage C ----------
    months = ["NYC_TLC_2026-01", "NYC_TLC_2026-02", "NYC_TLC_2026-03"]
    c_pts, f_pts = [], []
    for m in months:
        for p in PHYS + ["F_A", "F_B"]:
            v = rd_gt[f"C/{m}/{p}"]
            kind = "physical" if p in PHYS else "analytic"
            sid = v.get("gt_load_stage_id")
            st = rd_agg["stages"].get(str(sid)) if sid is not None else None
            rec = {"campaign": "realdata_stageC", "instance": m, "pipeline": p,
                   "kind": kind, "stage_id": sid,
                   "analytic_load": v["analytic_load"],
                   "executed_load_verifier": v["gt_load"] if kind == "physical" else None,
                   "executed_load_logderived": st["max_read_records"] if st else None,
                   "stage": st}
            detail["pipelines"].append(rec)
            if kind == "physical":
                assert st and st["max_read_records"] == int(v["gt_load"]), (m, p)
                c_pts.append((m, p, v["analytic_load"], v["gt_load"], st))
                fig_pts.append(("realdata_C", v["analytic_load"], v["gt_load"],
                                "physical", f"{m[-2:]}/{p}",
                                bool(st and st["spill_total"] > 0)))
            else:
                f_pts.append((m, p, v["analytic_load"]))
                fig_pts.append(("realdata_C", v["analytic_load"],
                                v["analytic_load"], "analytic",
                                f"{m[-2:]}/{p}", False))
    ratios_c = [e / a for _, _, a, e, _ in c_pts]
    detail["sets"]["realdata_stageC_physical_24"] = {
        "ratio": ratio_stats(ratios_c),
        "spearman_rho": spearman([a for _, _, a, _, _ in c_pts],
                                 [e for _, _, _, e, _ in c_pts]),
        "pearson_log10": pearson_log([a for _, _, a, _, _ in c_pts],
                                     [e for _, _, _, e, _ in c_pts]),
        "exec_vs_maxdur_spearman": spearman(
            [e for _, _, _, e, _ in c_pts], [s["dur_max_ms"] for *_, s in c_pts]),
        "spill_all_zero": all(s["spill_total"] == 0 for *_, s in c_pts),
        "per_point": [{"instance": m, "pipeline": p, "analytic": a,
                       "executed": e, "ratio": e / a}
                      for m, p, a, e, _ in c_pts],
    }
    detail["sets"]["realdata_stageC_analyticF_6"] = {
        "n": len(f_pts), "note": "executed == analytic by construction (D4); "
        "ratio 1.0 for all 6; never pooled with physical set",
        "points": [{"instance": m, "pipeline": p, "analytic": a} for m, p, a in f_pts]}
    detail["sets"]["synthetic_analyticF_2"] = {
        "n": 2, "note": "analytic by construction (D4); ratio 1.0; never pooled"}

    # Stage S spill attribution via verifier per-entry spill totals
    s_entries = {k: v for k, v in rd_gt.items() if v["stage"] == "S"}
    detail["stageS"] = {k: {"gt_load": v.get("gt_load"),
                            "analytic": v.get("analytic_load"),
                            "spill_total": v.get("spill_bytes_total"),
                            "stage_id": v.get("gt_load_stage_id")}
                        for k, v in s_entries.items()}

    # ---------- spill events (re-derived from logs) ----------
    for camp, agg, gtmap in (("synthetic", syn_agg, None), ("realdata", rd_agg, None)):
        for sid, st in agg["stages"].items():
            if st["spill_total"] > 0:
                detail["spill_events"].append({
                    "campaign": camp, "stage_id": int(sid),
                    "spill_bytes_logderived": st["spill_total"],
                    "spill_tasks": st["spill_tasks"],
                    "spill_max_task_bytes": st["spill_max_task"],
                    "stage_name_head": (st["stage_name"] or "")[:60],
                    "n_failed_in_stage": st["n_failed"]})

    # ---------- rows_e4.csv ----------
    for rec in detail["pipelines"]:
        camp = rec["campaign"]
        ds = "synthetic_zipf_s101_100M" if camp == "synthetic" else \
            f"NYC_TLC_{rec['instance']}"
        scale = "100M rows" if camp == "synthetic" else "Stage C monthly slice"
        p = rec["pipeline"]
        cls = "measured execution" if rec["kind"] == "physical" else "analytical"
        if rec["kind"] == "physical":
            st = rec["stage"]
            emit(ds, scale, f"{p}_executed_over_analytic_ratio",
                 f"{rec['executed_load_verifier']/rec['analytic_load']:.6f}",
                 "1 pipeline-instance", "measured execution",
                 "e4_stage_aggregates + verifier GT JSON",
                 f"principal stage {rec['stage_id']} (verifier-recorded); "
                 f"log-derived max shuffle-read records "
                 f"{rec['executed_load_logderived']} == verifier gt_load")
            emit(ds, scale, f"{p}_principal_stage_shuffle_read_bytes",
                 st["tot_read_bytes"], f"{st['n_tasks']} tasks in stage {rec['stage_id']}",
                 "measured execution", "event log (this re-analysis)", "")
            emit(ds, scale, f"{p}_principal_stage_shuffle_write_bytes",
                 st["tot_write_bytes"], f"{st['n_tasks']} tasks in stage {rec['stage_id']}",
                 "measured execution", "event log (this re-analysis)", "")
            emit(ds, scale, f"{p}_principal_stage_task_duration_max_ms",
                 st["dur_max_ms"], f"{st['n_tasks']} tasks", "measured execution",
                 "event log (this re-analysis)", "")
            emit(ds, scale, f"{p}_principal_stage_task_duration_median_ms",
                 st["dur_median_ms"], f"{st['n_tasks']} tasks", "measured execution",
                 "event log (this re-analysis)", "")
            emit(ds, scale, f"{p}_principal_stage_task_duration_max_over_median",
                 f"{st['dur_max_over_median']:.4f}" if st["dur_max_over_median"] else "",
                 f"{st['n_tasks']} tasks", "measured execution",
                 "event log (this re-analysis)", "")
            emit(ds, scale, f"{p}_principal_stage_spill_bytes",
                 st["spill_total"], f"{st['spill_tasks']} tasks affected",
                 "measured execution", "event log (this re-analysis)",
                 "memory+disk spilled, summed over stage tasks")
            emit(ds, scale, f"{p}_principal_stage_failed_tasks",
                 st["n_failed"], f"{st['n_tasks']} tasks", "measured execution",
                 "event log (this re-analysis)", "")
        else:
            emit(ds, scale, f"{p}_analytic_load", rec["analytic_load"],
                 "1 pipeline-instance", "analytical",
                 "verifier GT JSON (D4 analytic)",
                 "not materialised; no executed load exists; ratio vs analytic "
                 "is 1.0 by construction and is never pooled")
    for name, s in (("realdata_stageC_physical_24", detail["sets"]["realdata_stageC_physical_24"]),
                    ("synthetic_physical_7", detail["sets"]["synthetic_physical"])):
        ds = "NYC_TLC_StageC" if name.startswith("realdata") else "synthetic_zipf_s101_100M"
        n = s["ratio"]["n"]
        emit(ds, "Stage C" if ds.startswith("NYC") else "100M rows",
             "executed_over_analytic_ratio_median", f"{s['ratio']['median']:.6f}",
             f"n={n} comparisons", "measured execution", "this analysis",
             f"range {s['ratio']['min']:.6f}..{s['ratio']['max']:.6f}")
        emit(ds, "Stage C" if ds.startswith("NYC") else "100M rows",
             "spearman_rho_analytic_vs_executed",
             f"{s['spearman_rho']:.6f}" if s["spearman_rho"] is not None else "undefined",
             f"n={n} comparisons", "measured execution", "this analysis",
             "within-campaign only (protocol s.4)")
        emit(ds, "Stage C" if ds.startswith("NYC") else "100M rows",
             "pearson_log10_analytic_vs_executed",
             f"{s['pearson_log10']:.6f}" if s["pearson_log10"] is not None else "undefined",
             f"n={n} comparisons", "measured execution", "this analysis", "")
        if s.get("exec_vs_maxdur_spearman") is not None:
            emit(ds, "Stage C" if ds.startswith("NYC") else "100M rows",
                 "spearman_executed_load_vs_principal_stage_max_task_duration",
                 f"{s['exec_vs_maxdur_spearman']:.6f}", f"n={n} comparisons",
                 "measured execution", "this analysis", "descriptive only")
    for ev in detail["spill_events"]:
        emit("synthetic_zipf_s101_100M" if ev["campaign"] == "synthetic" else "NYC_TLC_100M_StageS",
             "100M rows", f"spill_event_stage_{ev['stage_id']}_bytes",
             ev["spill_bytes_logderived"], f"{ev['spill_tasks']} tasks affected",
             "measured execution", "event log (this re-analysis)",
             "all spilled tasks succeeded; 0 failed tasks in either log")

    with open(f"{FS}/results/rows_e4.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["experiment", "dataset", "scale",
                                          "environment", "metric", "value",
                                          "denominator", "classification",
                                          "artifact", "notes"])
        w.writeheader()
        w.writerows(rows)
    with open(f"{FS}/results/e4_analysis_detail.json", "w") as f:
        json.dump(detail, f, indent=1, default=str)

    # ---------- figure ----------
    fig, ax = plt.subplots(figsize=(7.5, 6))
    style = {("synthetic", "physical"): ("o", "tab:blue", "Synthetic 100M -- physical (n=7)"),
             ("synthetic", "analytic"): ("s", "tab:blue", "Synthetic 100M -- analytic F (n=2)"),
             ("realdata_C", "physical"): ("o", "tab:orange", "Real-data Stage C -- physical (n=24)"),
             ("realdata_C", "analytic"): ("s", "tab:orange", "Real-data Stage C -- analytic F (n=6)")}
    seen = set()
    for camp, a, e, kind, label, spill in fig_pts:
        mk, col, lab = style[(camp, kind)]
        ax.scatter([a], [e], marker=mk, c=col, s=55,
                   facecolors="none" if kind == "analytic" else col,
                   label=lab if (camp, kind) not in seen else None)
        seen.add((camp, kind))
        if spill:
            ax.annotate(f"spill: {label}", (a, e), textcoords="offset points",
                        xytext=(8, 8), fontsize=8)
    lo = min(min(a, e) for _, a, e, *_ in fig_pts) / 3
    hi = max(max(a, e) for _, a, e, *_ in fig_pts) * 3
    ax.plot([lo, hi], [lo, hi], "k--", lw=0.8, label="executed = analytic")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Analytic predicted load (max shuffle-read records)")
    ax.set_ylabel("Executed load (max per-task shuffle-read records)")
    ax.set_title("E4: analytic vs executed load (log–log)\n"
                 "analytic F points lie on the diagonal by construction")
    ax.legend(fontsize=8)
    ax.grid(True, which="both", alpha=0.25)
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(f"{FS}/figures/e4_analytic_vs_executed.{ext}", dpi=200)
    with open(f"{FS}/results/e4_figure_data.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["campaign", "analytic_load", "executed_load", "kind",
                    "label", "spill_affected_principal_stage"])
        for camp, a, e, kind, label, spill in fig_pts:
            w.writerow([camp, a, e, kind, label, spill])
    print(json.dumps({k: v for k, v in detail["sets"].items()
                      if isinstance(v, dict) and "ratio" in v}, indent=1))
    print("spill events:", detail["spill_events"])
    print("rows:", len(rows))


if __name__ == "__main__":
    main()
