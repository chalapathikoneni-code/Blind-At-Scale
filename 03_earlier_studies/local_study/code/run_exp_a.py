"""Driver for Experiment A arm A9 (end-biased sampling, Estan-Naughton
ICDE 2006) on the 99 headline instances + the 40M scale check.

PREREGISTRATION_EXP_A.md Sections 2-3 (locked 2026-11-24 01:30 EST,
pre-run). Reads data/<tag>/counts.npz of record (read-only) and
data/<tag>/synopsis.json size (read-only). Writes ONLY under
results/exp_a/: predictions_exp_a_<tag>.csv (headline hash H0 = 0),
predictions_exp_a_<tag>_h1.csv / _h2.csv (sensitivity hashes),
packet_A9_<tag>.json, and run_exp_a.log. Never touches data/ or
results/summary.json.

Ground truth is recomputed from counts.npz via common.loads_from_counts
with distinct_counts (as arms.run_arms does) and cross-checked against
data/<tag>/predictions_main.csv: 0 verdict mismatches required.

F_A/F_B loads use the paper's Eq. 1 per-key contributions with
common.max_partition_load (NOT the product of HT vectors); all other
loads use common.loads_from_counts on the HT vectors. A9 rows carry
testbed_fact_rows = 0 and an empty r (no row-sampling rate applies).

Usage (cwd = this code/ directory):
    python run_exp_a.py
"""
import csv, json, os, time
import numpy as np

from common import K, PIPELINES, loads_from_counts, verdict, max_partition_load
from classical_arms import (predict_A9, HASH_SEED_HEADLINE,
                            HASH_SEEDS_SENSITIVITY)

BASE = os.path.expanduser("~/workspace/blind-at-scale")
DATASETS = [f"{f}_s{s}" for f in ("zipf", "lognormal", "mixed")
            for s in (101, 202, 303)]
SCALE_TAG = "zipf_s101_40m"
ARM = "A9"


def gt_and_check(ddir, tag):
    """Ground truth from counts.npz + cross-check vs predictions_main."""
    z = np.load(os.path.join(ddir, "counts.npz"))
    counts = {k: z[k] for k in z.files}
    counts["null_count"] = int(counts["null_count"])
    fact = {c: counts[f"fact_{c}"] for c in ("kA", "kB", "kC", "kN")}
    fact2 = {c: counts[f"fact2_{c}"] for c in ("kA", "kB")}
    alldim = np.ones(K, dtype=bool)
    gt_loads = loads_from_counts(fact, fact2, float(counts["null_count"]),
                                 alldim, distinct_counts=counts["distinct_A"])
    n_fact = int(counts["fact_kA"].sum())
    gt_verd = {p: verdict(l, p, n_fact) for p, l in gt_loads.items()}
    # cross-check against the recorded predictions_main.csv
    ref_path = os.path.join(ddir, "predictions_main.csv")
    mism, max_diff = 0, 0.0
    with open(ref_path) as f:
        for row in csv.DictReader(f):
            if row["arm"] != "A1":
                continue
            pid = row["pipeline"]
            if row["gt_verdict"] != gt_verd[pid]:
                mism += 1
            max_diff = max(max_diff,
                           abs(float(row["gt_load"]) - gt_loads[pid]))
    return counts, gt_loads, gt_verd, n_fact, mism, max_diff


def run_a9(counts, n_fact, hseed):
    """A9 predicted loads + verdicts for one hash seed."""
    pf, pf2, pn, dimp, size, f_contrib, packet, samples = predict_A9(
        counts, hseed=hseed)
    pl = loads_from_counts(pf, pf2, pn, dimp, distinct_counts=None)
    for col in ("kA", "kB"):
        pl[f"F_{col[1]}"] = max_partition_load(f_contrib[col])
    pv = {p: verdict(l, p, n_fact) for p, l in pl.items()}
    return pl, pv, packet, samples


def write_preds(path, tag, meta, n_fact, gt_loads, gt_verd, pl, pv,
                syn_bytes):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["dataset", "family", "seed", "n_fact", "pipeline",
                    "gt_load", "gt_verdict", "arm", "pred_load",
                    "pred_verdict", "testbed_fact_rows", "r",
                    "synopsis_bytes"])
        for pid, _, _ in PIPELINES:
            w.writerow([tag, meta["family"], meta["seed"], n_fact, pid,
                        f"{gt_loads[pid]:.1f}", gt_verd[pid], ARM,
                        f"{pl[pid]:.1f}", pv[pid], 0, "", syn_bytes])


def main():
    outdir = os.path.join(BASE, "results", "exp_a")
    os.makedirs(outdir, exist_ok=True)
    log = open(os.path.join(outdir, "run_exp_a.log"), "w")
    info = {}
    for tag in DATASETS + [SCALE_TAG]:
        t0 = time.time()
        ddir = os.path.join(BASE, "data", tag)
        with open(os.path.join(ddir, "meta.json")) as f:
            meta = json.load(f)
        syn_bytes = os.path.getsize(os.path.join(ddir, "synopsis.json"))
        counts, gt_loads, gt_verd, n_fact, mism, max_diff = gt_and_check(
            ddir, tag)
        assert mism == 0, f"{tag}: {mism} GT verdict mismatches vs main"
        # headline hash H0
        pl, pv, packet, samples = run_a9(counts, n_fact, HASH_SEED_HEADLINE)
        ppath = os.path.join(outdir, f"packet_A9_{tag}.json")
        with open(ppath, "w") as f:
            json.dump(packet, f, separators=(",", ":"))
        pkt_bytes = os.path.getsize(ppath)
        write_preds(os.path.join(outdir, f"predictions_exp_a_{tag}.csv"),
                    tag, meta, n_fact, gt_loads, gt_verd, pl, pv, syn_bytes)
        # sensitivity hashes (predictions only; packet bytes identical
        # in expectation but recorded per hash in the log)
        sens_verdicts = {}
        for i, hs in enumerate(HASH_SEEDS_SENSITIVITY):
            pl_s, pv_s, _, _ = run_a9(counts, n_fact, hs)
            sens_verdicts[f"h{i+1}"] = pv_s
            write_preds(os.path.join(
                outdir, f"predictions_exp_a_{tag}_h{i+1}.csv"),
                tag, meta, n_fact, gt_loads, gt_verd, pl_s, pv_s, syn_bytes)
        thresholds = {f"fact_{c}": samples["fact"][c]["threshold"]
                      for c in ("kA", "kB", "kC", "kN")}
        thresholds.update({f"fact2_{c}": samples["fact2"][c]["threshold"]
                           for c in ("kA", "kB")})
        retained = {f"fact_{c}": len(samples["fact"][c]["retained_keys"])
                    for c in ("kA", "kB", "kC", "kN")}
        retained.update({f"fact2_{c}": len(samples["fact2"][c]["retained_keys"])
                         for c in ("kA", "kB")})
        n_fail = sum(1 for v in gt_verd.values() if v == "FAIL")
        n_pred_fail = sum(1 for v in pv.values() if v == "FAIL")
        line = (f"{tag}: gt_check mism={mism} max_gt_diff={max_diff:.1f} "
                f"gt_fail={n_fail}/11 pred_fail={n_pred_fail}/11 "
                f"packet={pkt_bytes}B syn={syn_bytes}B "
                f"thresholds={thresholds} retained={retained} "
                f"null_retained={samples['null_retained']} "
                f"done in {time.time()-t0:.1f}s")
        print(line)
        log.write(line + "\n")
        log.flush()
        info[tag] = {"synopsis_bytes": syn_bytes,
                     "packet_bytes": pkt_bytes,
                     "thresholds": thresholds, "retained": retained,
                     "gt_check_mismatches": mism,
                     "gt_check_max_diff": max_diff,
                     "n_gt_fail": n_fail, "n_pred_fail_h0": n_pred_fail}
    with open(os.path.join(outdir, "run_info.json"), "w") as f:
        json.dump(info, f, indent=1)
    log.close()
    print(f"wrote {outdir}/run_info.json")


if __name__ == "__main__":
    main()
