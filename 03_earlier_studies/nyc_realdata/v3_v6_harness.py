#!/usr/bin/env python3
"""V3/V6 harness for verify_realdata_run.py (build workspace, not shipped).

Builds a faithful synthetic run manifest + event log with KNOWN executed
loads, then runs the shipped verifier against them in four cases:
  1. two distinct executor hosts -> PASS (exit 0), all gates green
  2. one host only -> placement gate VOID (exit 1)
  3. no host info at all -> CANNOT DETERMINE (exit 1)
  4. study-tag refusal: the SYNTHETIC predictions file -> hard refuse
  5. manifest mismatch: tampered dataset-manifest SHA -> manifest FAIL

Executed loads are analytic loads scaled by a small per-pipeline factor
(Murmur3-vs-xxhash gap simulation), so verdicts stay FAIL except D_A.
"""
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PKG = HERE.parent / "realdata_emr_package"
WORK = HERE / "v3_v6_work"
VERIFIER = PKG / "scripts" / "verify_realdata_run.py"
CFG = PKG / "config"

INSTANCES = [
    ("C", "NYC_TLC_2026-01"), ("C", "NYC_TLC_2026-02"),
    ("C", "NYC_TLC_2026-03"), ("S", "NYC_TLC_100M"),
]
PIPELINES = ["G_A", "G_B", "G_C", "G_N", "J_A", "J_B", "J_N",
             "F_A", "F_B", "W_A", "D_A"]
KINDS = {"G_A": "group", "G_B": "group", "G_C": "group",
         "G_N": "group", "J_A": "fact_dimension",
         "J_B": "fact_dimension", "J_N": "fact_dimension",
         "F_A": "fact_fact", "F_B": "fact_fact", "W_A": "window",
         "D_A": "distinct"}
# Simulated executed-vs-analytic gap factors (Murmur3 vs xxhash).
FACTORS = {"G_A": 1.03, "G_B": 0.98, "G_C": 1.01, "G_N": 1.00,
           "J_A": 1.02, "J_B": 0.99, "J_N": 1.04, "W_A": 1.02}
GW = ["G_A", "G_B", "G_C", "G_N", "J_A", "J_B", "J_N", "W_A"]

FAILED = []


def verdict(load, threshold):
    return "FAIL" if load > threshold else "PASS"


def build_manifest_and_log(workdir, hosts, tamper_manifest=False):
    """Write data/dataset_manifest.json, output/run_manifest.json and
    output/application_0001.log into workdir. Return the entry list."""
    gt = json.loads((CFG / "analytic_gt_calibration.json").read_text())
    gt = gt["instances"]
    data_dir = workdir / "data"
    out_dir = workdir / "output"
    log_dir = workdir / "eventlog"
    data_dir.mkdir(parents=True)
    out_dir.mkdir(parents=True)
    log_dir.mkdir(parents=True)
    manifest = json.loads((CFG / "dataset_manifest.json").read_text())
    if tamper_manifest:
        # Tamper one file's SHA: the verifier's Gate 1 must fail.
        manifest["files"][0]["sha256"] = "0" * 64
    (data_dir / "dataset_manifest.json").write_text(
        json.dumps(manifest, indent=2))

    entries = []
    events = []
    t = 1_700_000_000_000  # base ms
    for host_i, host in enumerate(hosts):
        events.append({
            "Event": "SparkListenerExecutorAdded",
            "Executor Info": {
                "Executor ID": str(host_i + 1),
                "Executor Host": host,
            },
        })
    for stage, inst in INSTANCES:
        for pid in PIPELINES:
            g = gt[inst][pid]
            start = t
            end = t + 60_000
            t = end + 5_000
            n_fact = g["n_fact"]
            entry = {
                "stage": stage, "instance": inst, "pipeline": pid,
                "kind": KINDS[pid], "start_ms": start, "end_ms": end,
                "status": ("SKIPPED_NOT_MATERIALISABLE"
                           if pid in ("F_A", "F_B") else "OK"),
                "n_fact": n_fact,
                "t_rel": g["threshold_rel"],
                "t_abs": g["threshold_abs"],
                "analytic_load": g["gt_load"],
                "analytic_verdict_rel": verdict(g["gt_load"],
                                                g["threshold_rel"]),
                "analytic_verdict_abs": verdict(g["gt_load"],
                                                g["threshold_abs"]),
                "gt_load": None,
                "d8_load": None,
                "d8_verdict_rel": None,
                "d8_verdict_abs": None,
                "analytic_gate": None,
                "note": "",
            }
            if pid in GW:
                executed = int(round(g["gt_load"] * FACTORS[pid]))
                entry["d8_load"] = executed
                entry["d8_verdict_rel"] = verdict(executed,
                                                  g["threshold_rel"])
                entry["d8_verdict_abs"] = verdict(executed,
                                                  g["threshold_abs"])
                # TaskEnd events inside the window; the max shuffle-read
                # record count equals the executed load.
                for j, host in enumerate(hosts):
                    load = executed if j == 0 else executed // 2
                    events.append({
                        "Event": "SparkListenerTaskEnd",
                        "Stage ID": len(entries) + 1,
                        "Task Info": {
                            "Host": host,
                            "Finish Time": start + 10_000 + j * 1000,
                        },
                        "Task Metrics": {
                            "Executor Run Time": 40000,
                            "Input Metrics": {
                                "Records Read": n_fact // 2,
                            },
                            "Shuffle Read Metrics": {
                                "Total Records Read": load,
                            },
                            "Shuffle Write Metrics": {
                                "Records Written": n_fact // 2,
                            },
                        },
                    })
            elif pid in ("F_A", "F_B"):
                entry["gt_load"] = g["gt_load"]
                if stage == "C":
                    entry["analytic_gate"] = "PASS"
                entry["note"] = ("analytic-only; raw join never "
                                 "materialised")
            elif pid == "D_A":
                entry["gt_load"] = g["gt_load"]
            entries.append(entry)
    run_manifest = {
        "study": "NYC_TLC_100M",
        "app_id": "application_0001",
        "spark_version": "3.5.8",
        "master": "yarn",
        "stages_run": ["C", "S"],
        "executor_topology": {
            "distinct_hosts": len(set(hosts)),
            "hosts": sorted(set(hosts)),
        },
        "pipelines": entries,
    }
    (out_dir / "run_manifest.json").write_text(
        json.dumps(run_manifest, indent=2))
    with open(log_dir / "application_0001.log", "w") as fh:
        for ev in events:
            fh.write(json.dumps(ev) + "\n")
    return entries


def run_verifier(case_dir, predictions=None):
    cmd = [sys.executable, str(VERIFIER),
           "--bucket", "dummy-bucket", "--prefix", "dummy-prefix",
           "--workdir", str(case_dir),
           "--predictions", str(predictions or
                                CFG / "predictions_realdata.csv"),
           "--analytic-gt", str(CFG / "analytic_gt_calibration.json"),
           "--manifest", str(CFG / "dataset_manifest.json"),
           "--skip-download"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc


def check(name, ok, detail=""):
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
    if not ok:
        FAILED.append(name)


def main():
    if WORK.exists():
        shutil.rmtree(WORK)
    WORK.mkdir(parents=True)

    # Case 1: two hosts -> PASS.
    case1 = WORK / "case1_two_hosts"
    build_manifest_and_log(case1, ["ip-10-0-1-5", "ip-10-0-2-9"])
    p1 = run_verifier(case1)
    check("case 1 (two hosts) exit 0", p1.returncode == 0,
          f"rc={p1.returncode}")
    check("case 1 placement PASS",
          "[PASS] placement:" in p1.stdout, "")
    check("case 1 all gates PASS",
          "VERIFICATION PASSED" in p1.stdout, "")
    rep1 = json.loads((case1 / "verification_report.json").read_text())
    check("case 1 report verdict PASS",
          rep1["verdict"] == "PASS", rep1["verdict"])
    check("case 1 D8 cross-check gate green",
          rep1["gates"]["d8_cross_check"]["pass"]
          and "over 32 G/J/W entries"
          in rep1["gates"]["d8_cross_check"]["detail"],
          rep1["gates"]["d8_cross_check"]["detail"])
    check("case 1 analytic GT match 44/44",
          rep1["gates"]["analytic_gt_match"]["pass"]
          and "all 44 entries"
          in rep1["gates"]["analytic_gt_match"]["detail"],
          rep1["gates"]["analytic_gt_match"]["detail"])
    check("case 1 threshold audit D8 32/32 analytic 44/44",
          "D8 32/32, analytic 44/44" in p1.stdout, "")
    check("case 1 Stage C F gate 6/6",
          rep1["gates"]["analytic_f_gate"]["pass"]
          and "all 6 entries"
          in rep1["gates"]["analytic_f_gate"]["detail"],
          rep1["gates"]["analytic_f_gate"]["detail"])
    check("case 1 A5 agreement 40/40",
          rep1["arms_vs_executed"]["A5"]["verdict_agreement_rel"]
          == "40/40",
          rep1["arms_vs_executed"]["A5"]["verdict_agreement_rel"])
    check("case 1 A2 infinite-Q count 4 (G_N x 4 instances)",
          rep1["arms_vs_executed"]["A2"]["infinite_q_error_count"] == 4,
          str(rep1["arms_vs_executed"]["A2"]
              ["infinite_q_error_count"]))
    check("case 1 gap median ratio ~1.005 (simulated factors)",
          abs(rep1["executed_vs_analytic_gap"]["C"]
              ["median_ratio_executed_over_analytic"] - 1.005) < 0.001,
          str(rep1["executed_vs_analytic_gap"]["C"]
              ["median_ratio_executed_over_analytic"]))

    # Case 2: one host -> VOID.
    case2 = WORK / "case2_one_host"
    build_manifest_and_log(case2, ["ip-10-0-1-5"])
    p2 = run_verifier(case2)
    check("case 2 (one host) exit 1", p2.returncode == 1,
          f"rc={p2.returncode}")
    check("case 2 placement VOID",
          "VOID: executors ran on ONE host only" in p2.stdout, "")

    # Case 3: no host info -> CANNOT DETERMINE.
    case3 = WORK / "case3_no_hosts"
    build_manifest_and_log(case3, [])
    p3 = run_verifier(case3)
    check("case 3 (no hosts) exit 1", p3.returncode == 1,
          f"rc={p3.returncode}")
    check("case 3 placement CANNOT DETERMINE",
          "CANNOT DETERMINE PLACEMENT" in p3.stdout, "")

    # Case 4: study-tag refusal (synthetic predictions file).
    syn_pred = (HERE.parent / "emr_package" / "config"
                / "predictions_main.csv")
    p4 = run_verifier(case1, predictions=syn_pred)
    check("case 4 (synthetic predictions) hard refuse exit 1",
          p4.returncode == 1, f"rc={p4.returncode}")
    check("case 4 refusal message names the study tag",
          "STUDY-TAG MISMATCH" in p4.stdout, "")

    # Case 5: tampered dataset manifest -> manifest gate FAIL.
    case5 = WORK / "case5_tampered_manifest"
    build_manifest_and_log(case5, ["ip-10-0-1-5", "ip-10-0-2-9"],
                           tamper_manifest=True)
    p5 = run_verifier(case5)
    check("case 5 (tampered manifest) exit 1", p5.returncode == 1,
          f"rc={p5.returncode}")
    check("case 5 manifest gate FAIL",
          "[FAIL] manifest_match:" in p5.stdout, "")

    print(f"\nV3/V6 HARNESS: "
          f"{'ALL CHECKS PASSED' if not FAILED else str(FAILED)}")
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(main())
