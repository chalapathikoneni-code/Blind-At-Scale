"""E4: stream-parse both EMR Spark event logs into per-stage aggregates.

Attribution: pipelines are attributed ONLY via the verifier-recorded
gt_load_stage_id in the ground-truth JSONs (see e4_analyse.py). This
script is stage-level only; it invents no stage->pipeline mapping.
"""
import json
import os
import statistics
import sys

BASE = os.path.expanduser("~/workspace/blind-at-scale")
OUT = os.path.join(BASE, "final_scientific_strengthening/results")

LOGS = {
    "synthetic": os.path.join(
        BASE, "emr_results_synthetic/round2/eventlog/eventlog/"
        "<APPLICATION_ID>_1"),
    "realdata": os.path.join(
        BASE, "emr_results_realdata/realdata/realdata_verify_out/eventlog/"
        "<APPLICATION_ID>_1"),
}


def parse(path):
    stages = {}  # stage_id -> aggregate dict (durations kept as list)
    stage_names = {}
    n_task_end = 0
    n_failed = 0
    hosts = set()
    for line in open(path, "r", encoding="utf-8", errors="replace"):
        if '"SparkListenerTaskEnd"' in line:
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            ti = ev.get("Task Info") or {}
            tm = ev.get("Task Metrics") or {}
            sid = ev.get("Stage ID")
            n_task_end += 1
            failed = bool(ti.get("Failed")) or (
                (ev.get("Task End Reason") or {}).get("Reason") not in
                (None, "Success"))
            if failed:
                n_failed += 1
            if ti.get("Host"):
                hosts.add(ti["Host"])
            sr = tm.get("Shuffle Read Metrics") or {}
            sw = tm.get("Shuffle Write Metrics") or {}
            read_rec = sr.get("Total Records Read", 0) or 0
            read_bytes = ((sr.get("Remote Bytes Read", 0) or 0)
                          + (sr.get("Local Bytes Read", 0) or 0))
            write_bytes = sw.get("Shuffle Bytes Written", 0) or 0
            spill = ((tm.get("Memory Bytes Spilled", 0) or 0)
                     + (tm.get("Disk Bytes Spilled", 0) or 0))
            dur = tm.get("Executor Run Time", 0) or 0
            a = stages.setdefault(sid, {
                "n_tasks": 0, "n_failed": 0, "durations": [],
                "max_read_records": 0, "tot_read_records": 0,
                "tot_read_bytes": 0, "tot_write_bytes": 0,
                "spill_total": 0, "spill_tasks": 0, "spill_max_task": 0})
            a["n_tasks"] += 1
            a["n_failed"] += int(failed)
            a["durations"].append(dur)
            a["max_read_records"] = max(a["max_read_records"], read_rec)
            a["tot_read_records"] += read_rec
            a["tot_read_bytes"] += read_bytes
            a["tot_write_bytes"] += write_bytes
            a["spill_total"] += spill
            a["spill_tasks"] += int(spill > 0)
            a["spill_max_task"] = max(a["spill_max_task"], spill)
        elif '"SparkListenerStageCompleted"' in line:
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            info = ev.get("Stage Info") or {}
            stage_names[info.get("Stage ID")] = info.get("Stage Name")
    summary = {}
    for sid, a in stages.items():
        durs = sorted(a.pop("durations"))
        a["dur_max_ms"] = durs[-1] if durs else 0
        a["dur_median_ms"] = statistics.median(durs) if durs else 0
        a["dur_max_over_median"] = (
            a["dur_max_ms"] / a["dur_median_ms"] if a["dur_median_ms"] else None)
        a["stage_name"] = stage_names.get(sid)
        summary[str(sid)] = a
    return {"n_task_end": n_task_end, "n_failed_tasks": n_failed,
            "n_stages_with_tasks": len(summary),
            "n_executor_hosts": len(hosts), "stages": summary}


def main():
    for name, path in LOGS.items():
        print(f"parsing {name}: {path}", flush=True)
        res = parse(path)
        out = os.path.join(OUT, f"e4_stage_aggregates_{name}.json")
        with open(out, "w") as f:
            json.dump(res, f, indent=1)
        print(f"  task_ends={res['n_task_end']} failed={res['n_failed_tasks']} "
              f"stages={res['n_stages_with_tasks']} hosts={res['n_executor_hosts']}"
              f" -> {out}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
