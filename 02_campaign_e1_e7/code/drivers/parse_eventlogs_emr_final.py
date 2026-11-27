#!/usr/bin/env python3
"""Parse Spark event logs from the BAS final capture.

Stdlib only. For each log: app identity, executor hosts, task totals,
failed tasks + reasons, shuffle read/write, spill, per-stage summary,
executor-lost events.
"""
import json
import sys
from collections import Counter, defaultdict


def parse(path):
    r = {
        "path": path,
        "app_name": None,
        "app_id": None,
        "app_start_ms": None,
        "app_end_ms": None,
        "user": None,
        "spark_version": None,
        "executors": {},  # exec id -> host
        "executor_lost": [],
        "tasks_total": 0,
        "tasks_success": 0,
        "tasks_failed": 0,
        "task_fail_reasons": Counter(),
        "shuffle_read_bytes": 0,
        "shuffle_read_records": 0,
        "shuffle_write_bytes": 0,
        "shuffle_write_records": 0,
        "spill_mem": 0,
        "spill_disk": 0,
        "input_bytes": 0,
        "input_records": 0,
        "output_bytes": 0,
        "output_records": 0,
        "stages_completed": 0,
        "stages_failed": 0,
        "stage_info": {},
        "jobs": [],
        "app_attempt": None,
        "stage_agg": {},
    }
    with open(path, "r", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                ev = json.loads(line)
            except json.JSONDecodeError:
                continue
            et = ev.get("Event", "")
            if et == "SparkListenerLogStart":
                r["spark_version"] = ev.get("Spark Version")
            elif et == "SparkListenerApplicationStart":
                r["app_name"] = ev.get("App Name")
                r["app_id"] = ev.get("App ID")
                r["app_start_ms"] = ev.get("Timestamp")
                r["user"] = ev.get("User")
            elif et == "SparkListenerApplicationEnd":
                r["app_end_ms"] = ev.get("Timestamp")
            elif et == "SparkListenerExecutorAdded":
                eid = str(ev.get("Executor ID"))
                info = ev.get("Executor Info", {})
                r["executors"][eid] = info.get("Host")
            elif et == "SparkListenerExecutorRemoved":
                r["executor_lost"].append({
                    "id": str(ev.get("Executor ID")),
                    "reason": str(ev.get("Removed Reason"))[:200],
                    "time": ev.get("Timestamp"),
                })
            elif et == "SparkListenerTaskEnd":
                r["tasks_total"] += 1
                sid = ev.get("Stage ID")
                sa = r["stage_agg"].setdefault(
                    sid, {"tasks": 0, "failed": 0, "sr_bytes": 0,
                          "sr_rec": 0, "sw_bytes": 0, "sw_rec": 0,
                          "spill_disk": 0, "spill_mem": 0,
                          "in_rec": 0, "out_rec": 0, "max_run_ms": 0})
                sa["tasks"] += 1
                reason = ev.get("Task End Reason", {}).get("Reason", "?")
                if reason == "Success":
                    r["tasks_success"] += 1
                else:
                    r["tasks_failed"] += 1
                    r["task_fail_reasons"][reason] += 1
                m = ev.get("Task Metrics") or {}
                sr = m.get("Shuffle Read Metrics") or {}
                r["shuffle_read_bytes"] += (
                    sr.get("Remote Bytes Read", 0)
                    + sr.get("Local Bytes Read", 0))
                r["shuffle_read_records"] += sr.get("Total Records Read", 0)
                sw = m.get("Shuffle Write Metrics") or {}
                r["shuffle_write_bytes"] += sw.get("Shuffle Bytes Written", 0)
                r["shuffle_write_records"] += sw.get("Shuffle Records Written", 0)
                r["spill_mem"] += m.get("Memory Bytes Spilled", 0)
                r["spill_disk"] += m.get("Disk Bytes Spilled", 0)
                inp = m.get("Input Metrics") or {}
                r["input_bytes"] += inp.get("Bytes Read", 0)
                r["input_records"] += inp.get("Records Read", 0)
                out = m.get("Output Metrics") or {}
                r["output_bytes"] += out.get("Bytes Written", 0)
                r["output_records"] += out.get("Records Written", 0)
                if reason != "Success":
                    sa["failed"] += 1
                sa["sr_bytes"] += (sr.get("Remote Bytes Read", 0)
                                   + sr.get("Local Bytes Read", 0))
                sa["sr_rec"] += sr.get("Total Records Read", 0)
                sa["sw_bytes"] += sw.get("Shuffle Bytes Written", 0)
                sa["sw_rec"] += sw.get("Shuffle Records Written", 0)
                sa["spill_disk"] += m.get("Disk Bytes Spilled", 0)
                sa["spill_mem"] += m.get("Memory Bytes Spilled", 0)
                sa["in_rec"] += inp.get("Records Read", 0)
                sa["out_rec"] += out.get("Records Written", 0)
                sa["max_run_ms"] = max(sa["max_run_ms"],
                                       m.get("Executor Run Time", 0))
            elif et == "SparkListenerStageCompleted":
                r["stages_completed"] += 1
                si = ev.get("Stage Info", {})
                r["stage_info"][si.get("Stage ID")] = {
                    "name": si.get("Stage Name", "")[:110],
                    "num_tasks": si.get("Number of Tasks"),
                    "completion_ms": si.get("Completion Time"),
                    "submission_ms": si.get("Submission Time"),
                    "failure_reason": (si.get("Failure Reason") or "")[:160],
                }
            elif et == "SparkListenerStageSubmitted":
                si = ev.get("Stage Info", {})
                sid = si.get("Stage ID")
                if sid not in r["stage_info"]:
                    r["stage_info"][sid] = {
                        "name": si.get("Stage Name", "")[:110],
                        "num_tasks": si.get("Number of Tasks"),
                        "completion_ms": None,
                        "submission_ms": si.get("Submission Time"),
                        "failure_reason": "",
                    }
            elif et == "SparkListenerJobStart":
                props = ev.get("Properties") or {}
                r["jobs"].append({
                    "job_id": ev.get("Job ID"),
                    "app_name_prop": props.get("spark.app.name"),
                    "submit_file": props.get("spark.submit.pyFiles") or
                    props.get("spark.yarn.dist.files"),
                })
    hosts = sorted({h for h in r["executors"].values() if h})
    r["distinct_hosts"] = hosts
    r["n_executors"] = len(r["executors"])
    if r["app_start_ms"] and r["app_end_ms"]:
        r["wall_sec"] = round((r["app_end_ms"] - r["app_start_ms"]) / 1000.0, 1)
    return r


if __name__ == "__main__":
    for p in sys.argv[1:]:
        res = parse(p)
        print(json.dumps(res, indent=1, default=str))
        print("=" * 70)
