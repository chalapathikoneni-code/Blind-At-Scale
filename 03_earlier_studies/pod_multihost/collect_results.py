"""Collect REAL Spark ground truth from the event log and compare it with
the local study's arm predictions for zipf_s101.

Inputs:
  --manifest      run_manifest.json written by spark_pipelines.py (carries
                  per-pipeline time windows, the master used, n_fact, and
                  the event-log dir).
  --predictions   local data/zipf_s101/predictions_main.csv (A1/A5/A8 rows).
  --summary       local results/summary.json (pooled arm metrics, context).
Output:
  --out           spark_ground_truth.json (default ./spark_ground_truth.json)

The load definition, and every deviation from the local simulation, are
defined in the header of spark_pipelines.py (D1-D7) and copied into the
output JSON under "load_definition"/"deviations" so the file is
self-describing. Headline labelling is copied from the manifest: a run
whose master is not spark://... is labelled NOT A CLUSTER RUN.

Event-log field names vary slightly across Spark versions/protocols, so
metric extraction accepts the known spellings (JsonProtocol names first,
REST-API names as fallbacks) and the output records, per pipeline, which
stage realised the max load -- if a future Spark renames a field and a
metric comes out 0, that is visible in the output rather than silent.
"""
import argparse, csv, glob, json, os, sys


def pick(d, *names):
    """First present key among names; fall back to case/space-insensitive."""
    if not isinstance(d, dict):
        return 0
    for n in names:
        if n in d and isinstance(d[n], (int, float)):
            return d[n]
    norm = {str(k).lower().replace(" ", "").replace("_", ""): v
            for k, v in d.items() if isinstance(v, (int, float))}
    for n in names:
        v = norm.get(n.lower().replace(" ", "").replace("_", ""))
        if v is not None:
            return v
    return 0


def task_metrics(tm):
    inp = tm.get("Input Metrics", {})
    sr = tm.get("Shuffle Read Metrics", {})
    sw = tm.get("Shuffle Write Metrics", {})
    read_records = pick(sr, "Total Records Read", "Records Read",
                        "Shuffle Records Read", "Read Records")
    read_bytes = pick(sr, "Total Bytes Read")
    if not read_bytes:
        read_bytes = (pick(sr, "Local Bytes Read") + pick(sr, "Remote Bytes Read"))
    write_records = pick(sw, "Shuffle Records Written", "Records Written")
    write_bytes = pick(sw, "Shuffle Bytes Written", "Bytes Written")
    input_records = pick(inp, "Records Read")
    return {
        "input_records": int(input_records),
        "shuffle_read_records": int(read_records),
        "shuffle_write_records": int(write_records),
        "shuffle_read_bytes": int(read_bytes),
        "shuffle_write_bytes": int(write_bytes),
        "spill_bytes": int(pick(tm, "Memory Bytes Spilled")
                           + pick(tm, "Disk Bytes Spilled")),
        "run_time_ms": int(pick(tm, "Executor Run Time")),
        "load_records": int(input_records + read_records),
        "load_records_incl_write": int(input_records + read_records
                                       + write_records),
        "shuffle_bytes": int(read_bytes + write_bytes),
    }


def load_events(eventlog_dir):
    tasks, stages = [], []
    files = sorted(glob.glob(os.path.join(eventlog_dir, "*")))
    files = [f for f in files if os.path.isfile(f)]
    if not files:
        sys.exit(f"no event log files in {eventlog_dir}")
    for path in files:
        with open(path, "rb") as fh:
            for raw in fh:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    ev = json.loads(raw)
                except Exception:
                    continue  # tail of an .inprogress log
                et = ev.get("Event", "")
                if et == "SparkListenerTaskEnd":
                    info = ev.get("Task Info", {})
                    tm = ev.get("Task Metrics")
                    if tm is None:
                        continue
                    m = task_metrics(tm)
                    m.update({"stage_id": ev.get("Stage ID"),
                              "finish_ms": info.get("Finish Time", 0),
                              "failed": bool(info.get("Failed", False))})
                    tasks.append(m)
                elif et == "SparkListenerStageCompleted":
                    si = ev.get("Stage Info", {})
                    stages.append({
                        "stage_id": si.get("Stage ID"),
                        "name": si.get("Stage Name", "")[:120],
                        "n_tasks": si.get("Number of Tasks"),
                        "submission_ms": si.get("Submission Time"),
                        "completion_ms": si.get("Completion Time"),
                    })
    return tasks, stages


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--eventlog-dir", default=None)
    ap.add_argument("--predictions", default=None,
                    help="default: search ../data and ../../data for "
                         "zipf_s101/predictions_main.csv")
    ap.add_argument("--summary", default=None)
    ap.add_argument("--out", default="spark_ground_truth.json")
    ap.add_argument("--t-load", type=float, default=None,
                    help="default: 0.02 * n_fact from the manifest "
                         "(= 200,000 at headline scale)")
    args = ap.parse_args()

    with open(args.manifest) as f:
        manifest = json.load(f)
    eventlog_dir = args.eventlog_dir or manifest["eventlog_dir"]
    n_fact = manifest.get("n_fact", 10_000_000)
    t_load = args.t_load if args.t_load is not None else 0.02 * n_fact

    tasks, stages = load_events(eventlog_dir)
    print(f"event log: {len(tasks)} task ends, {len(stages)} stage completions "
          f"from {eventlog_dir}")

    # ---- local arm predictions (zipf_s101) ----
    pred_path = args.predictions
    if pred_path is None:
        for cand in ("../data/zipf_s101/predictions_main.csv",
                     "../../data/zipf_s101/predictions_main.csv",
                     os.path.expanduser(
                         "~/workspace/blind-at-scale/data/zipf_s101/predictions_main.csv")):
            if os.path.exists(cand):
                pred_path = cand
                break
    local = {}   # pid -> {"gt_load":..., "gt_verdict":..., "arms": {...}}
    if pred_path and os.path.exists(pred_path):
        with open(pred_path) as f:
            for row in csv.DictReader(f):
                if row["arm"] not in ("A1", "A5", "A8"):
                    continue
                e = local.setdefault(row["pipeline"], {
                    "gt_load_local_sim": float(row["gt_load"]),
                    "gt_verdict_local_sim": row["gt_verdict"], "arms": {}})
                e["arms"][row["arm"]] = {
                    "pred_load": float(row["pred_load"]),
                    "pred_verdict": row["pred_verdict"]}
    else:
        print("WARNING: predictions_main.csv not found; comparison section "
              "will be empty. Pass --predictions.", file=sys.stderr)
    pooled = None
    sum_path = args.summary
    if sum_path is None:
        for cand in ("../results/summary.json", "../../results/summary.json",
                     os.path.expanduser(
                         "~/workspace/blind-at-scale/results/summary.json")):
            if os.path.exists(cand):
                sum_path = cand
                break
    if sum_path and os.path.exists(sum_path):
        with open(sum_path) as f:
            s = json.load(f)
        pooled = {a: {"recall": s["arms"][a]["recall"],
                      "precision": s["arms"][a]["precision"],
                      "spearman_rho": s["arms"][a]["spearman_rho"],
                      "median_q_error": s["arms"][a]["median_q_error"]}
                  for a in ("A1", "A5", "A8") if a in s.get("arms", {})}

    # ---- per-pipeline Spark ground truth ----
    out_pipes = {}
    for entry in manifest["pipelines"]:
        pid = entry["pipeline"]
        lo, hi = entry["start_ms"], entry["end_ms"]
        pt = [t for t in tasks if lo <= t["finish_ms"] <= hi]
        ps = [s for s in stages if s["completion_ms"] is not None
              and lo <= s["completion_ms"] <= hi]
        stage_rows = []
        for s in ps:
            st = [t for t in pt if t["stage_id"] == s["stage_id"]]
            stage_rows.append({
                **s,
                "duration_ms": (s["completion_ms"] - s["submission_ms"]
                                if s["submission_ms"] is not None else None),
                "max_task_load_records": max((t["load_records"] for t in st),
                                             default=0),
                "max_task_shuffle_read_records": max(
                    (t["shuffle_read_records"] for t in st), default=0),
            })
        if pt:
            mx = max(pt, key=lambda t: t["shuffle_read_records"])
            rec = {
                "status": entry["status"],
                "n_tasks": len(pt),
                "n_failed_tasks": sum(1 for t in pt if t["failed"]),
                "gt_load_shuffle_read": mx["shuffle_read_records"],
                "gt_load_stage_id": mx["stage_id"],
                "gt_load_incl_input": max(t["load_records"] for t in pt),
                "max_task_load_incl_write": max(t["load_records_incl_write"]
                                                for t in pt),
                "max_task_shuffle_bytes": max(t["shuffle_bytes"] for t in pt),
                "max_task_input_records": max(t["input_records"] for t in pt),
                "max_task_shuffle_read_records": max(t["shuffle_read_records"]
                                                     for t in pt),
                "spill_bytes_max_task": max(t["spill_bytes"] for t in pt),
                "spill_bytes_total": sum(t["spill_bytes"] for t in pt),
                "max_task_time_ms": max(t["run_time_ms"] for t in pt),
                "wall_seconds": entry.get("wall_seconds"),
                "stage_durations": stage_rows,
            }
        else:
            rec = {"status": entry["status"], "n_tasks": 0,
                   "warning": "no tasks attributed to this pipeline window"}
        # F pipelines (D4): task shuffle-read measures join INPUT
        # partitions (or, when skipped, the aggregate stages) -- never the
        # join output load. The verdict-bearing load is always the
        # analytic splitmix64 load computed from Spark-produced counts.
        if "analytic_load_splitmix64" in entry:
            rec["gt_load"] = entry["analytic_load_splitmix64"]
            rec["gt_load_source"] = ("analytic_splitmix64_from_spark_counts "
                                      f"(F pipeline; raw join status "
                                      f"{entry['status']}; D4)")
            rec["exact_join_output_rows"] = entry["exact_join_output_rows"]
        else:
            rec["gt_load"] = rec.get("gt_load_shuffle_read")
            rec["gt_load_source"] = "spark_event_log_tasks_shuffle_read"
        rec["verdict"] = ("FAIL" if rec["gt_load"] is not None
                          and rec["gt_load"] > t_load else
                          "PASS" if rec["gt_load"] is not None else None)
        rec["t_load"] = t_load
        for k in ("result_rows", "max_group_count", "join_output_rows",
                  "max_window_partition_rows", "error"):
            if k in entry:
                rec[k] = entry[k]
        # ---- comparison vs local arms ----
        if pid in local:
            rec["local_sim"] = {"gt_load": local[pid]["gt_load_local_sim"],
                                "gt_verdict": local[pid]["gt_verdict_local_sim"]}
            comp = {}
            for arm, a in local[pid]["arms"].items():
                q = None
                if rec["gt_load"]:
                    q = max(a["pred_load"] / rec["gt_load"],
                            rec["gt_load"] / a["pred_load"])
                comp[arm] = {**a,
                             "verdict_agrees_with_spark": a["pred_verdict"] == rec["verdict"],
                             "q_error_vs_spark_gt": q}
            rec["arm_predictions"] = comp
        out_pipes[pid] = rec

    agree = {a: {"agree": 0, "total": 0} for a in ("A1", "A5", "A8")}
    sim_agree = {"agree": 0, "total": 0}
    for rec in out_pipes.values():
        if "arm_predictions" in rec:
            for a, c in rec["arm_predictions"].items():
                agree[a]["total"] += 1
                agree[a]["agree"] += int(c["verdict_agrees_with_spark"])
        if "local_sim" in rec and rec["verdict"] is not None:
            sim_agree["total"] += 1
            sim_agree["agree"] += int(rec["local_sim"]["gt_verdict"] == rec["verdict"])

    result = {
        "experiment": "Blind at Scale -- Workstream 4: Spark cluster ground truth",
        "labelling": manifest.get("labelling"),
        "master": manifest.get("master"),
        "spark_version": manifest.get("spark_version"),
        "app_id": manifest.get("app_id"),
        "dataset": "zipf seed 101 (fact 10M / fact2 6M / dim 20k) unless n_fact says otherwise",
        "n_fact": n_fact,
        "t_load": t_load,
        "load_definition": ("GT_LOAD = max over the pipeline's tasks of "
                            "Shuffle Read Metrics.Total Records Read "
                            "(per-partition load; input/scan records are "
                            "reported separately as gt_load_incl_input and "
                            "are not verdict-bearing -- file splitting is a "
                            "storage artefact). For F pipelines GT_LOAD is "
                            "the analytic splitmix64 load from "
                            "Spark-produced per-key counts (D4). FAIL iff "
                            "GT_LOAD > T_LOAD. See spark_pipelines.py."),
        "deviations": "D1 murmur3 vs splitmix64; D2/D5 G/W realised as "
                      "repartition shuffle (upper bound of local def); D3 J "
                      "shuffle-merge, broadcast off; D4 F raw join not "
                      "materialisable -- analytic load from Spark counts; "
                      "D6 scan splits; D7 AQE off. Full text in "
                      "spark_pipelines.py.",
        "pipelines": out_pipes,
        "verdict_agreement": {
            "arms_vs_spark": agree,
            "local_simulation_vs_spark": sim_agree,
            "local_pooled_arm_metrics_all_99_instances": pooled,
        },
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=1)

    print(f"\n{'pipe':5} {'spark GT load':>16} {'verdict':>8} {'local GT':>16} "
          f"{'local v':>8}  arm verdicts (A1/A5/A8) vs spark")
    for pid, rec in out_pipes.items():
        av = "/".join(("OK" if rec.get("arm_predictions", {}).get(a, {})
                        .get("verdict_agrees_with_spark") else "XX")
                      if "arm_predictions" in rec else "--"
                      for a in ("A1", "A5", "A8"))
        print(f"{pid:5} {rec['gt_load']:>16,.0f} {str(rec['verdict']):>8} "
              f"{rec.get('local_sim', {}).get('gt_load', float('nan')):>16,.0f} "
              f"{str(rec.get('local_sim', {}).get('gt_verdict')):>8}  {av}"
              f"  src={rec['gt_load_source']}")
    print(f"\nverdict agreement: {json.dumps(result['verdict_agreement']['arms_vs_spark'])}"
          f" | local sim vs spark: {json.dumps(sim_agree)}")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
