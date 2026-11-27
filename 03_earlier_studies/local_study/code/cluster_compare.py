"""Local recovery of the pod comparison section (Workstream 4 follow-up).

The pod's collect_results.py produced an empty comparison section because
data/zipf_s101/predictions_main.csv was not uploaded to the pod (its default
search paths ../data, ../../data did not resolve from the pod's run dir).
Everything it would have computed is recoverable locally with no new pod
run: the Spark ground truth is the operator-reported file
results/spark_cluster/spark_ground_truth_zipf_s101.json (reconstructed
from the verbatim operator report; canonical full file on the research-ops
volume at /workspace/blind-pod/), and the arm predictions are the local
data/zipf_s101/predictions_main.csv.

Also performs the THRESHOLD AUDIT for the t_load discrepancy: the pod
report noted t_load=20000 in an auxiliary output, while the recorded
verdicts (G_B PASS at gt_load 156,387; J_B PASS at 156,541) are only
consistent with the study threshold T = 0.02 * N_fact = 200,000
(PREREGISTRATION.md S.3). pod/collect_results.py uses ONE t_load variable
(t_load = --t-load if given, else 0.02 * manifest.n_fact; run_pod.sh
passes no --t-load) for the root field, every per-pipeline t_load field,
and every verdict, so a single execution cannot pair t_load=20000 with
those PASS verdicts. This script recomputes all 9 verdicts under both
thresholds and asserts which one the recorded verdicts match.

Outputs: results/spark_cluster/cluster_comparison.json
"""
import csv, json, os, statistics, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GT_PATH = os.path.join(ROOT, "results/spark_cluster/spark_ground_truth_zipf_s101.json")
PRED_PATH = os.path.join(ROOT, "data/zipf_s101/predictions_main.csv")
OUT_PATH = os.path.join(ROOT, "results/spark_cluster/cluster_comparison.json")
PIPES = ["G_A", "G_B", "G_N", "J_A", "J_B", "J_N", "F_A", "F_B", "W_A"]
ARMS = ["A1", "A2", "A3", "A4", "A5", "A8"]
T_STUDY = 200_000.0     # 0.02 * 10,000,000, PREREGISTRATION.md S.3
T_ANOMALY = 20_000.0    # = 0.02 * 1,000,000, signature of a 1M-row invocation


def qerr(pred, gt):
    return max(pred / gt, gt / pred)


def verdict(load, t):
    return "FAIL" if load > t else "PASS"


def main():
    with open(GT_PATH) as f:
        gt_doc = json.load(f)
    spark = gt_doc["pipelines"]

    local = {}  # pid -> {"gt_load":..., "gt_verdict":..., arm -> (pred_load, pred_verdict)}
    with open(PRED_PATH) as f:
        for row in csv.DictReader(f):
            if row["pipeline"] not in PIPES:
                continue
            e = local.setdefault(row["pipeline"], {
                "gt_load": float(row["gt_load"]),
                "gt_verdict": row["gt_verdict"], "arms": {}})
            e["arms"][row["arm"]] = (float(row["pred_load"]), row["pred_verdict"])

    # ---- threshold audit ----
    audit = {"study_threshold": T_STUDY, "anomalous_value": T_ANOMALY,
             "per_pipeline": {}, "recorded_match_study": 0,
             "recorded_match_anomaly": 0}
    for pid in PIPES:
        load = float(spark[pid]["gt_load"])
        rec = spark[pid]["verdict"]
        v_study, v_anom = verdict(load, T_STUDY), verdict(load, T_ANOMALY)
        audit["per_pipeline"][pid] = {
            "gt_load": load, "recorded_verdict": rec,
            "verdict_under_200000": v_study, "verdict_under_20000": v_anom}
        audit["recorded_match_study"] += int(rec == v_study)
        audit["recorded_match_anomaly"] += int(rec == v_anom)
    assert audit["recorded_match_study"] == 9, audit
    audit["conclusion"] = (
        "All 9 recorded verdicts equal the verdicts recomputed under "
        "T = 200,000; under T = 20,000, G_B and J_B would flip to FAIL "
        "(7/9 match). The verdict path therefore applied the study "
        "threshold. Code inspection (pod/collect_results.py:129,235-238): "
        "one t_load variable feeds the root field, per-pipeline fields and "
        "verdicts; run_pod.sh passes no --t-load, so t_load = 0.02 * "
        "manifest.n_fact = 0.02 * 10,000,000 = 200,000 for the headline "
        "run (manifest n_fact written by spark_pipelines.py from "
        "fact.count()). t_load = 20000 arises exactly when n_fact = "
        "1,000,000 (a reduced-scale invocation) and cannot describe the "
        "headline verdicts. No verdict changes.")

    # ---- local simulation vs Spark ----
    sim_rows, sim_agree = {}, 0
    for pid in PIPES:
        s_load = float(spark[pid]["gt_load"])
        l_load = local[pid]["gt_load"]
        agree = int(local[pid]["gt_verdict"] == spark[pid]["verdict"])
        sim_agree += agree
        sim_rows[pid] = {
            "spark_gt_load": s_load, "local_sim_gt_load": l_load,
            "ratio_spark_over_sim": s_load / l_load,
            "q_error": qerr(l_load, s_load),
            "local_sim_verdict": local[pid]["gt_verdict"],
            "spark_verdict": spark[pid]["verdict"],
            "verdict_agrees": bool(agree),
            "gt_load_source": spark[pid]["gt_load_source"]}
    sim_block = {
        "verdict_agreement": f"{sim_agree}/9",
        "per_pipeline": sim_rows,
        "median_q_error": statistics.median(
            r["q_error"] for r in sim_rows.values()),
        "note": "F_A/F_B analytic loads from Spark-produced counts equal "
                "the local simulation ground truth exactly (ratio 1.0) "
                "because the on-pod counts are bit-identical to the local "
                "counts (SHA-256 headline check PASS). G/W Spark loads are "
                "the D2/D5 repartition upper bound (sum of group counts of "
                "colliding keys in the busiest murmur3 partition), hence "
                "ratios 1.14-2.03 vs the local max-group definition; J "
                "loads differ only by the D1 hash function (murmur3 vs "
                "splitmix64), ratios 0.89-1.03."}

    # ---- arms vs Spark ----
    arm_block = {}
    for arm in ARMS:
        agree, disagreements, qs = 0, [], []
        per = {}
        for pid in PIPES:
            pred_load, pred_verdict = local[pid]["arms"][arm]
            s = spark[pid]
            ok = pred_verdict == s["verdict"]
            agree += int(ok)
            if not ok:
                disagreements.append(pid)
            q = qerr(pred_load, float(s["gt_load"]))
            qs.append(q)
            per[pid] = {"pred_load": pred_load, "pred_verdict": pred_verdict,
                        "spark_gt_load": float(s["gt_load"]),
                        "spark_verdict": s["verdict"],
                        "verdict_agrees": bool(ok), "q_error_vs_spark": q}
        arm_block[arm] = {"verdict_agreement": f"{agree}/9",
                          "disagreements": disagreements,
                          "median_q_error_vs_spark": statistics.median(qs),
                          "per_pipeline": per}

    out = {
        "experiment": "Blind at Scale \u2014 Workstream 4: local recovery of "
                      "the pod comparison section",
        "labelling": gt_doc["labelling"],
        "dataset": "zipf_s101 (fact 10M / fact2 6M / dim 20k)",
        "spark_gt_source": "results/spark_cluster/spark_ground_truth_zipf_s101.json "
                           "(reconstructed from the operator's verbatim report; "
                           "canonical full file + event log on the research-ops "
                           "volume at /workspace/blind-pod/)",
        "predictions_source": "data/zipf_s101/predictions_main.csv (local study)",
        "threshold_audit": audit,
        "local_simulation_vs_spark": sim_block,
        "arms_vs_spark": arm_block,
        "deviations_frame": gt_doc["deviations"],
    }
    with open(OUT_PATH, "w") as f:
        json.dump(out, f, indent=1)

    print(f"threshold audit: recorded verdicts match T=200,000 on "
          f"{audit['recorded_match_study']}/9, T=20,000 on "
          f"{audit['recorded_match_anomaly']}/9")
    print(f"local sim vs spark: verdicts {sim_block['verdict_agreement']}, "
          f"median Q {sim_block['median_q_error']:.4f}")
    for arm in ARMS:
        b = arm_block[arm]
        print(f"{arm}: verdicts {b['verdict_agreement']} vs spark, "
              f"median Q {b['median_q_error_vs_spark']:.4f}, "
              f"disagreements {b['disagreements']}")
    print(f"wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
