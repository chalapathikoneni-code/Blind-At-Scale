"""E2 driver: symmetric information-budget frontier (FINAL_EXECUTION_PROTOCOL S.2).

Phases:
  0. ENVIRONMENT VALIDATION GATE: reproduce A9 rows of
     results/exp_a/predictions_exp_a_zipf_s101.csv exactly (0.1 rounding)
     via run_exp_a.run_a9 / classical_arms.predict_A9. Abort if it fails.
  1. Fresh recompute of every frontier point on the 9 headline datasets,
     timed, with verification against the existing per-instance CSVs.
     New runs: A9 at targets {16, 64, 256} (packets + prediction CSVs
     written under final_scientific_strengthening/results/).
  2. Uniform metric set per point + Pareto frontiers; writes
     metrics_e2.csv, rows_e2.csv, verification_e2.json, summary_e2.json.

Run with cwd = blind-at-scale/code (imports the unmodified study code),
system python3 (3.12.3, numpy 1.26.4). Writes ONLY under
final_scientific_strengthening/.
"""
import csv
import json
import os
import sys
import time

import numpy as np

CODE_DIR = os.path.expanduser("~/workspace/blind-at-scale/code")
sys.path.insert(0, CODE_DIR)

import ablation  # noqa: E402
import arms as A  # noqa: E402
import common as C  # noqa: E402
import eqinfo_arms as EQ  # noqa: E402
import run_exp_a  # noqa: E402
from classical_arms import HASH_SEED_HEADLINE, predict_A9  # noqa: E402

BASE = os.path.expanduser("~/workspace/blind-at-scale")
FSS = os.path.join(BASE, "final_scientific_strengthening")
OUT = os.path.join(FSS, "results")
os.makedirs(OUT, exist_ok=True)

DATASETS = [f"{f}_s{s}" for f in ("zipf", "lognormal", "mixed")
            for s in (101, 202, 303)]
ENV = "local, python 3.12.3/numpy 1.26.4"
SCALE = "10M fact rows per dataset"
A8_KS = (16, 64, 256, 1024)
A9_TS = (16, 64, 256, 1024)
FAM_OF = {"G": ("G_A", "G_B", "G_C", "G_N"), "J": ("J_A", "J_B", "J_N"),
          "F": ("F_A", "F_B"), "W": ("W_A",), "D": ("D_A",)}


def fmt1(x):
    return f"{x:.1f}"


def read_recorded(path, key_col, key_val):
    """{(dataset, pipeline): (pred_load_str, pred_verdict, gt_load_str)}."""
    out = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            if row[key_col] != key_val:
                continue
            out[(row["dataset"], row["pipeline"])] = (
                row["pred_load"], row["pred_verdict"], row["gt_load"])
    return out


def compare(rec, rows):
    """rows: list of dicts with dataset/pipeline/pred_load/pred_verdict.
    Returns (n_compared, n_pred_mismatch_at_0p1, n_verdict_mismatch)."""
    n = mism = vmism = 0
    for r in rows:
        key = (r["dataset"], r["pipeline"])
        if key not in rec:
            continue
        n += 1
        if fmt1(r["pred_load"]) != rec[key][0]:
            mism += 1
        if r["pred_verdict"] != rec[key][1]:
            vmism += 1
    return n, mism, vmism


def a9_loads(counts, n_fact, target):
    """run_exp_a.run_a9 body with an explicit target (identical load path)."""
    pf, pf2, pn, dimp, _size, f_contrib, packet, samples = predict_A9(
        counts, hseed=HASH_SEED_HEADLINE, target=target)
    pl = C.loads_from_counts(pf, pf2, pn, dimp, distinct_counts=None)
    for col in ("kA", "kB"):
        pl[f"F_{col[1]}"] = C.max_partition_load(f_contrib[col])
    pv = {p: C.verdict(l, p, n_fact) for p, l in pl.items()}
    return pl, pv, packet, samples


def main():
    log = []

    def say(s):
        print(s, flush=True)
        log.append(s)

    # ---------------- Phase 0: GATE ----------------
    tag0 = "zipf_s101"
    ddir0 = os.path.join(BASE, "data", tag0)
    counts0, gt_loads0, gt_verd0, n_fact0, mism0, maxdiff0 = \
        run_exp_a.gt_and_check(ddir0, tag0)
    pl0, pv0, _pkt0, _s0 = run_exp_a.run_a9(counts0, n_fact0,
                                            HASH_SEED_HEADLINE)
    rec0 = read_recorded(os.path.join(
        BASE, "results", "exp_a", "predictions_exp_a_zipf_s101.csv"),
        "arm", "A9")
    gate_rows = [{"dataset": tag0, "pipeline": pid,
                  "pred_load": pl0[pid], "pred_verdict": pv0[pid]}
                 for pid, _, _ in C.PIPELINES]
    gn, gmism, gvmism = compare(rec0, gate_rows)
    gate_ok = (gn == 11 and gmism == 0 and gvmism == 0 and mism0 == 0)
    say(f"GATE: compared={gn} pred_mismatches={gmism} "
        f"verdict_mismatches={gvmism} gt_check_mism={mism0} "
        f"-> {'PASS' if gate_ok else 'FAIL'}")
    gate = {"compared": gn, "pred_mismatches": gmism,
            "verdict_mismatches": gvmism, "gt_check_mismatches": mism0,
            "gt_check_max_diff": maxdiff0, "pass": gate_ok}
    if not gate_ok:
        with open(os.path.join(OUT, "verification_e2.json"), "w") as f:
            json.dump({"gate": gate}, f, indent=1)
        say("GATE FAILED - aborting; no new run counts.")
        return 1

    # ---------------- Phase 1: fresh recompute, all points ----------------
    # point -> {rows, bytes{tag}, time{tag}, testbed{tag}, extra{tag}}
    points = {}
    verification = {"gate": gate}

    def new_point(name):
        points[name] = {"rows": [], "bytes": {}, "time": {},
                        "testbed": {}, "extra": {}}
        return points[name]

    for k in A8_KS:
        new_point(f"A8_k{k}")
    for t in A9_TS:
        new_point(f"A9_t{t}")
    for nm in ("A5_full", "A7_full", "A7X_full"):
        new_point(nm)

    # recorded references for verification
    rec_abl = {k: read_recorded(os.path.join(
        BASE, "results", "ablation",
        "predictions_full_k1024.csv" if k == 1024
        else f"predictions_hh_k{k}.csv"), "rung",
        "full_k1024" if k == 1024 else f"hh_k{k}") for k in A8_KS}
    rec_main = {}
    rec_eq = {}
    rec_a9 = {}
    for tag in DATASETS:
        d = os.path.join(BASE, "data", tag)
        rec_main[tag] = read_recorded(os.path.join(d, "predictions_main.csv"),
                                      "arm", "A5")
        rec_eq[tag] = {}
        for arm in ("A7", "A7X"):
            rec_eq[tag][arm] = read_recorded(
                os.path.join(d, "predictions_eqinfo.csv"), "arm", arm)
        rec_a9[tag] = read_recorded(os.path.join(
            BASE, "results", "exp_a", f"predictions_exp_a_{tag}.csv"),
            "arm", "A9")

    ver_counts = {k: [0, 0, 0] for k in
                  [f"A8_k{k}" for k in A8_KS] + ["A5_full", "A7_full",
                                                 "A7X_full", "A9_t1024"]}
    packet_checks = {"A9_t1024_bytes_match_existing": 0,
                     "A7_packet_serialisation_identical": 0,
                     "A7X_packet_serialisation_identical": 0,
                     "A8_synopsis_default_dumps_eq_filesize": 0}

    for tag in DATASETS:
        ddir = os.path.join(BASE, "data", tag)
        with open(os.path.join(ddir, "meta.json")) as f:
            meta = json.load(f)
        with open(os.path.join(ddir, "synopsis.json")) as f:
            syn = json.load(f)
        counts, gt_loads, gt_verd, n_fact, mism, _md = \
            run_exp_a.gt_and_check(ddir, tag)
        assert mism == 0, f"{tag}: GT verdict mismatch vs predictions_main"
        seed = int(meta["seed"])

        def add_rows(point, pl, pv):
            for pid, _, _ in C.PIPELINES:
                points[point]["rows"].append({
                    "dataset": tag, "pipeline": pid,
                    "gt_load": gt_loads[pid], "gt_verdict": gt_verd[pid],
                    "pred_load": pl[pid], "pred_verdict": pv[pid]})

        # --- A8 rungs (ablation path: truncate_syn + arm_predictions) ---
        if len(json.dumps(syn)) == os.path.getsize(
                os.path.join(ddir, "synopsis.json")):
            packet_checks["A8_synopsis_default_dumps_eq_filesize"] += 1
        for k in A8_KS:
            dsyn = syn if k == 1024 else ablation.truncate_syn(syn, k)
            t0 = time.perf_counter()
            pf, pf2, pn, dimp, _sz = A.arm_predictions(
                "A8", counts, dsyn, seed=seed)
            pl = C.loads_from_counts(pf, pf2, pn, dimp,
                                     distinct_counts=None)
            dt = time.perf_counter() - t0
            pv = {p: C.verdict(l, p, n_fact) for p, l in pl.items()}
            P = points[f"A8_k{k}"]
            P["bytes"][tag] = len(json.dumps(dsyn))
            P["time"][tag] = dt
            P["testbed"][tag] = 0
            add_rows(f"A8_k{k}", pl, pv)

        # --- A9 targets (predict_A9 via the run_exp_a load path) ---
        for t in A9_TS:
            t0 = time.perf_counter()
            if t == 1024:
                pl, pv, packet, samples = run_exp_a.run_a9(
                    counts, n_fact, HASH_SEED_HEADLINE)
            else:
                pl, pv, packet, samples = a9_loads(counts, n_fact, t)
            dt = time.perf_counter() - t0
            blob = json.dumps(packet, separators=(",", ":"))
            P = points[f"A9_t{t}"]
            P["bytes"][tag] = len(blob)
            P["time"][tag] = dt
            P["testbed"][tag] = 0
            P["extra"][tag] = {
                "thresholds": {f"{tb}_{c}": samples[tb][c]["threshold"]
                               for tb in ("fact", "fact2")
                               for c in samples[tb]},
                "retained_total": int(sum(
                    len(samples[tb][c]["retained_keys"])
                    for tb in ("fact", "fact2") for c in samples[tb])),
                "retained_per_col": {f"{tb}_{c}": int(len(
                    samples[tb][c]["retained_keys"]))
                    for tb in ("fact", "fact2") for c in samples[tb]},
            }
            add_rows(f"A9_t{t}", pl, pv)
            if t == 1024:
                existing = os.path.getsize(os.path.join(
                    BASE, "results", "exp_a", f"packet_A9_{tag}.json"))
                if existing == len(blob):
                    packet_checks["A9_t1024_bytes_match_existing"] += 1
            else:
                # new artifacts: packet + per-instance predictions
                with open(os.path.join(
                        OUT, f"packet_A9_t{t}_{tag}.json"), "w") as f:
                    f.write(blob)
                with open(os.path.join(
                        OUT, f"predictions_A9_t{t}_{tag}.csv"), "w",
                        newline="") as f:
                    w = csv.writer(f)
                    w.writerow(["dataset", "family", "seed", "n_fact",
                                "pipeline", "gt_load", "gt_verdict", "arm",
                                "pred_load", "pred_verdict",
                                "testbed_fact_rows", "r", "packet_bytes",
                                "target"])
                    for pid, _, _ in C.PIPELINES:
                        w.writerow([tag, meta["family"], seed, n_fact, pid,
                                    fmt1(gt_loads[pid]), gt_verd[pid],
                                    "A9", fmt1(pl[pid]), pv[pid], 0, "",
                                    len(blob), t])

        # --- A5 full (run_arms seed convention: arms A1,A2,A3,A4,A5,A8,
        #     index 4 -> seed = ds*1000 + 17*4 + 5) ---
        t0 = time.perf_counter()
        pf, pf2, pn, dimp, size5 = A.arm_predictions(
            "A5", counts, syn, seed=seed * 1000 + 17 * 4 + 5, r=0.01)
        pl = C.loads_from_counts(pf, pf2, pn, dimp, distinct_counts=None)
        dt = time.perf_counter() - t0
        pv = {p: C.verdict(l, p, n_fact) for p, l in pl.items()}
        P = points["A5_full"]
        P["bytes"][tag] = len(json.dumps(syn))
        P["time"][tag] = dt
        P["testbed"][tag] = int(size5)
        add_rows("A5_full", pl, pv)

        # --- A7 / A7X full (run_eqinfo path) ---
        packets = {"A7": EQ.build_packet_A7(counts, syn),
                   "A7X": EQ.build_packet_A7X(counts, syn)}
        for idx, arm in enumerate(("A7", "A7X")):
            blob = json.dumps(packets[arm], separators=(",", ":"))
            existing_path = os.path.join(
                BASE, "results", "eqinfo", f"packet_{arm}_{tag}.json")
            with open(existing_path) as f:
                existing_blob = f.read()
            if existing_blob == blob:
                packet_checks[f"{arm}_packet_serialisation_identical"] += 1
            sd = seed * 1000 + 5000 + idx
            t0 = time.perf_counter()
            if arm == "A7":
                pf, pf2, pn, dimp, sz = EQ.predict_A7(
                    counts, packets[arm], seed=sd, r=0.01)
            else:
                pf, pf2, pn, dimp, sz = EQ.predict_A7X(
                    counts, packets[arm], seed=sd, r=0.01)
            pl = C.loads_from_counts(pf, pf2, pn, dimp,
                                     distinct_counts=None)
            dt = time.perf_counter() - t0
            pv = {p: C.verdict(l, p, n_fact) for p, l in pl.items()}
            P = points[f"{arm}_full"]
            P["bytes"][tag] = os.path.getsize(existing_path)
            P["time"][tag] = dt
            P["testbed"][tag] = int(sz)
            add_rows(f"{arm}_full", pl, pv)
        say(f"{tag}: recomputed all points")

    # verification tallies
    def tally(name, rec_by_tag):
        n = mism = vm = 0
        for tag in DATASETS:
            rows = [r for r in points[name]["rows"] if r["dataset"] == tag]
            a, b, c = compare(rec_by_tag[tag], rows)
            n += a; mism += b; vm += c
        ver_counts[name] = [n, mism, vm]

    for k in A8_KS:
        n = mism = vm = 0
        for tag in DATASETS:
            rows = [r for r in points[f"A8_k{k}"]["rows"]
                    if r["dataset"] == tag]
            a, b, c = compare(rec_abl[k], rows)
            n += a; mism += b; vm += c
        ver_counts[f"A8_k{k}"] = [n, mism, vm]
    tally("A5_full", rec_main)
    tally("A7_full", {t: rec_eq[t]["A7"] for t in DATASETS})
    tally("A7X_full", {t: rec_eq[t]["A7X"] for t in DATASETS})
    tally("A9_t1024", rec_a9)
    verification["per_point_vs_recorded_csv"] = {
        k: {"compared": v[0], "pred_mismatches_at_0p1": v[1],
            "verdict_mismatches": v[2]} for k, v in ver_counts.items()}
    verification["packet_checks"] = packet_checks
    say(f"verification: {json.dumps(verification, indent=1)}")

    # ---------------- Phase 2: uniform metrics + Pareto ----------------
    def metrics(rows):
        tp = sum(1 for r in rows if r["gt_verdict"] == "FAIL"
                 and r["pred_verdict"] == "FAIL")
        fp = sum(1 for r in rows if r["gt_verdict"] == "PASS"
                 and r["pred_verdict"] == "FAIL")
        fn = sum(1 for r in rows if r["gt_verdict"] == "FAIL"
                 and r["pred_verdict"] == "PASS")
        tn = len(rows) - tp - fp - fn
        rec = tp / (tp + fn) if tp + fn else float("nan")
        prec = tp / (tp + fp) if tp + fp else float("nan")
        f1 = (2 * rec * prec / (rec + prec)) if (rec + prec) else 0.0
        gt = np.array([r["gt_load"] for r in rows])
        pr = np.array([max(r["pred_load"], 1e-9) for r in rows])
        q = np.maximum(pr / gt, gt / pr)
        fam = {}
        for letter, pids in FAM_OF.items():
            idx = [i for i, r in enumerate(rows) if r["pipeline"] in pids]
            fam[letter] = {"median": float(np.median(q[idx])),
                           "max": float(np.max(q[idx])), "n": len(idx)}
        fail_idx = [i for i, r in enumerate(rows)
                    if r["gt_verdict"] == "FAIL"]
        worst_under = float(np.max(gt[fail_idx] / pr[fail_idx]))
        return {"tp": tp, "fp": fp, "fn": fn, "tn": tn, "recall": rec,
                "precision": prec, "f1": f1, "n_instances": len(rows),
                "q_median": float(np.median(q)), "q_mean": float(np.mean(q)),
                "q_p95": float(np.percentile(q, 95)),
                "q_max": float(np.max(q)), "n_q": int(len(q)),
                "per_family": fam, "worst_underprediction": worst_under,
                "n_gt_fail": len(fail_idx),
                "zero_pred_loads": int(sum(
                    1 for r in rows if r["pred_load"] <= 0))}

    summary = {}
    for name, P in points.items():
        m = metrics(P["rows"])
        b = np.array(list(P["bytes"].values()), dtype=float)
        tt = np.array(list(P["time"].values()), dtype=float)
        tb = np.array(list(P["testbed"].values()), dtype=float)
        m.update({"bytes_mean": float(b.mean()), "bytes_min": int(b.min()),
                  "bytes_max": int(b.max()),
                  "bytes_per_dataset": P["bytes"],
                  "pred_time_mean_s": float(tt.mean()),
                  "pred_time_per_dataset": P["time"],
                  "testbed_rows_mean": float(tb.mean())})
        if P["extra"]:
            m["a9_retained_total_mean"] = float(np.mean(
                [e["retained_total"] for e in P["extra"].values()]))
            m["a9_extra_per_dataset"] = P["extra"]
        summary[name] = m

    def pareto(key_q):
        front = []
        for name, m in summary.items():
            dominated = False
            for other, mo in summary.items():
                if other == name:
                    continue
                if (mo["bytes_mean"] <= m["bytes_mean"]
                        and mo[key_q] <= m[key_q]
                        and (mo["bytes_mean"] < m["bytes_mean"]
                             or mo[key_q] < m[key_q])):
                    dominated = True
                    break
            if not dominated:
                front.append(name)
        return sorted(front, key=lambda n: summary[n]["bytes_mean"])

    front_med = pareto("q_median")
    front_max = pareto("q_max")
    say(f"Pareto (bytes, median Q): {front_med}")
    say(f"Pareto (bytes, max Q): {front_max}")

    with open(os.path.join(OUT, "summary_e2.json"), "w") as f:
        json.dump({"points": summary, "pareto_median_q": front_med,
                   "pareto_max_q": front_max}, f, indent=1)
    with open(os.path.join(OUT, "verification_e2.json"), "w") as f:
        json.dump(verification, f, indent=1)

    # metrics_e2.csv (wide, one row per point)
    kind = {**{f"A8_k{k}": "direct" for k in A8_KS},
            **{f"A9_t{t}": "direct" for t in A9_TS},
            "A5_full": "testbed", "A7_full": "testbed",
            "A7X_full": "direct"}
    conv = {**{f"A8_k{k}": "synopsis: len(json.dumps(obj)) default separators"
               for k in A8_KS},
            **{f"A9_t{t}": "packet: compact-separator file size"
               for t in A9_TS},
            "A5_full": "synopsis: len(json.dumps(obj)) default separators",
            "A7_full": "packet: compact-separator file size",
            "A7X_full": "packet: compact-separator file size"}
    order = ([f"A8_k{k}" for k in A8_KS] + [f"A9_t{t}" for t in A9_TS]
             + ["A5_full", "A7_full", "A7X_full"])
    with open(os.path.join(OUT, "metrics_e2.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["point", "kind", "byte_convention", "bytes_mean",
                    "bytes_min", "bytes_max", "recall", "precision", "f1",
                    "tp", "fp", "fn", "tn", "q_median", "q_mean", "q_p95",
                    "q_max", "q_G_median", "q_G_max", "q_J_median", "q_J_max",
                    "q_F_median", "q_F_max", "q_W_median", "q_W_max",
                    "q_D_median", "q_D_max", "worst_underprediction",
                    "pred_time_mean_s", "testbed_rows_mean",
                    "on_pareto_median_q", "on_pareto_max_q"])
        for name in order:
            m = summary[name]
            pf = m["per_family"]
            w.writerow([name, kind[name], conv[name],
                        f"{m['bytes_mean']:.1f}", m["bytes_min"],
                        m["bytes_max"], f"{m['recall']:.4f}",
                        f"{m['precision']:.4f}", f"{m['f1']:.4f}",
                        m["tp"], m["fp"], m["fn"], m["tn"],
                        f"{m['q_median']:.6f}", f"{m['q_mean']:.6f}",
                        f"{m['q_p95']:.6f}", f"{m['q_max']:.6f}",
                        f"{pf['G']['median']:.6f}", f"{pf['G']['max']:.6f}",
                        f"{pf['J']['median']:.6f}", f"{pf['J']['max']:.6f}",
                        f"{pf['F']['median']:.6f}", f"{pf['F']['max']:.6f}",
                        f"{pf['W']['median']:.6f}", f"{pf['W']['max']:.6f}",
                        f"{pf['D']['median']:.6f}", f"{pf['D']['max']:.6f}",
                        f"{m['worst_underprediction']:.6f}",
                        f"{m['pred_time_mean_s']:.4f}",
                        f"{m['testbed_rows_mean']:.1f}",
                        "yes" if name in front_med else "no",
                        "yes" if name in front_max else "no"])

    # rows_e2.csv (long format, deliverable schema)
    art = {**{f"A8_k{k}": "results/ablation/predictions_hh_k%d.csv" % k
              if k != 1024 else
              "results/ablation/predictions_full_k1024.csv"
              for k in A8_KS},
           **{f"A9_t{t}": ("final_scientific_strengthening/results/"
                            f"predictions_A9_t{t}_<tag>.csv") for t in A9_TS},
           "A9_t1024": "results/exp_a/predictions_exp_a_<tag>.csv",
           "A5_full": "data/<tag>/predictions_main.csv",
           "A7_full": "data/<tag>/predictions_eqinfo.csv",
           "A7X_full": "data/<tag>/predictions_eqinfo.csv"}
    with open(os.path.join(OUT, "rows_e2.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["experiment", "dataset", "scale", "environment",
                    "metric", "value", "denominator", "classification",
                    "artifact", "notes"])
        w.writerow(["E2_gate", tag0, SCALE, ENV, "a9_gate_pred_mismatches",
                    gmism, "11 instance-pipelines", "simulated",
                    "results/exp_a/predictions_exp_a_zipf_s101.csv",
                    "environment validation gate: predict_A9 via "
                    "run_exp_a load path vs recorded A9 rows at 0.1 "
                    "rounding; PASS required before new runs count"])
        w.writerow(["E2_gate", tag0, SCALE, ENV, "a9_gate_verdict_mismatches",
                    gvmism, "11 instance-pipelines", "simulated",
                    "results/exp_a/predictions_exp_a_zipf_s101.csv", ""])
        metric_rows = [
            ("recall", "recall", "60 GT-FAIL instances"),
            ("precision", "precision", "predicted-FAIL instances"),
            ("f1", "f1", "99 instances"),
            ("fp", "fp_count", "39 GT-PASS instances"),
            ("fn", "fn_count", "60 GT-FAIL instances"),
            ("q_median", "q_error_median", "1089 instance-pipelines"),
            ("q_mean", "q_error_mean", "1089 instance-pipelines"),
            ("q_p95", "q_error_p95", "1089 instance-pipelines"),
            ("q_max", "q_error_max", "1089 instance-pipelines"),
            ("worst_underprediction", "worst_underprediction_gt_fail",
             "60 GT-FAIL instances"),
            ("bytes_mean", "bytes_mean", "9 datasets"),
            ("bytes_min", "bytes_min", "9 datasets"),
            ("bytes_max", "bytes_max", "9 datasets"),
            ("pred_time_mean_s", "prediction_wall_time_mean_s",
             "9 datasets"),
            ("testbed_rows_mean", "testbed_fact_rows_mean", "9 datasets"),
        ]
        for name in order:
            m = summary[name]
            for key, metric, den in metric_rows:
                w.writerow([f"E2_{name}", "pooled_9_headline", SCALE, ENV,
                            metric, f"{m[key]:.6f}" if isinstance(
                                m[key], float) else m[key], den,
                            "simulated", art[name],
                            f"kind={kind[name]}; bytes convention: "
                            f"{conv[name]}"])
            for letter in ("G", "J", "F", "W", "D"):
                pf = m["per_family"][letter]
                for stat in ("median", "max"):
                    w.writerow([f"E2_{name}", "pooled_9_headline", SCALE,
                                ENV, f"q_error_family_{letter}_{stat}",
                                f"{pf[stat]:.6f}",
                                f"{pf['n']} instance-pipelines",
                                "simulated", art[name], ""])
            for tag in DATASETS:
                w.writerow([f"E2_{name}", tag, SCALE, ENV, "bytes",
                            m["bytes_per_dataset"][tag], "1 dataset",
                            "simulated", art[name], conv[name]])
                w.writerow([f"E2_{name}", tag, SCALE, ENV,
                            "prediction_wall_time_s",
                            f"{m['pred_time_per_dataset'][tag]:.4f}",
                            "1 dataset", "simulated", art[name],
                            "prediction call + loads_from_counts, "
                            "perf_counter"])
    with open(os.path.join(OUT, "run_e2.log"), "w") as f:
        f.write("\n".join(log) + "\n")
    say("E2 driver done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
