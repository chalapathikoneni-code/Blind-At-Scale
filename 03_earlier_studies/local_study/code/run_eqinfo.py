"""Driver for equal-information arms A7 / A7X (Amendment 5 Section A5.1).

For each of the 9 headline datasets:
  - loads the profiler synopsis from data/<tag>/synopsis.json (read-only),
  - builds the A7 / A7X packets and serialises them (compact JSON) to
    results/eqinfo/packet_A7_<tag>.json / packet_A7X_<tag>.json,
  - recomputes ground truth from counts.npz via common.loads_from_counts
    with distinct_counts=counts['distinct_A'] (as arms.run_arms does),
  - runs A7 (arm_index 0) and A7X (arm_index 1) with
    seed = dataset_seed * 1000 + 5000 + arm_index,
  - writes data/<tag>/predictions_eqinfo.csv with exactly the
    predictions_main.csv schema.

Usage (cwd = this code/ directory):
    python run_eqinfo.py
"""
import csv, json, os, time
import numpy as np

from common import K, PIPELINES, loads_from_counts, verdict
from eqinfo_arms import build_packet_A7, build_packet_A7X, predict_A7, predict_A7X

BASE = os.path.expanduser("~/workspace/blind-at-scale")
DATASETS = [f"{f}_s{s}" for f in ("zipf", "lognormal", "mixed")
            for s in (101, 202, 303)]
ARMS = ["A7", "A7X"]
R = 0.01


def main():
    outdir = os.path.join(BASE, "results", "eqinfo")
    os.makedirs(outdir, exist_ok=True)
    packet_info = {}
    for tag in DATASETS:
        t0 = time.time()
        ddir = os.path.join(BASE, "data", tag)
        with open(os.path.join(ddir, "meta.json")) as f:
            meta = json.load(f)
        with open(os.path.join(ddir, "synopsis.json")) as f:
            syn = json.load(f)
        syn_bytes = os.path.getsize(os.path.join(ddir, "synopsis.json"))
        z = np.load(os.path.join(ddir, "counts.npz"))
        counts = {k: z[k] for k in z.files}
        counts["null_count"] = int(counts["null_count"])
        seed_ds = int(meta["seed"])
        n_fact = int(counts["fact_kA"].sum())

        # packets (built from the profiler synopsis; fact2 HH from counts)
        packets = {"A7": build_packet_A7(counts, syn),
                   "A7X": build_packet_A7X(counts, syn)}
        packet_info[tag] = {"synopsis_bytes": syn_bytes}
        for arm, packet in packets.items():
            ppath = os.path.join(outdir, f"packet_{arm}_{tag}.json")
            with open(ppath, "w") as f:
                json.dump(packet, f, separators=(",", ":"))
            packet_info[tag][arm] = os.path.getsize(ppath)

        # ground truth (counts-level, as arms.run_arms)
        fact = {c: counts[f"fact_{c}"] for c in ("kA", "kB", "kC", "kN")}
        fact2 = {c: counts[f"fact2_{c}"] for c in ("kA", "kB")}
        alldim = np.ones(K, dtype=bool)
        gt_loads = loads_from_counts(fact, fact2,
                                     float(counts["null_count"]), alldim,
                                     distinct_counts=counts["distinct_A"])
        gt_verd = {p: verdict(l, p, n_fact) for p, l in gt_loads.items()}

        results = {}
        for arm_index, arm in enumerate(ARMS):
            seed = seed_ds * 1000 + 5000 + arm_index
            if arm == "A7":
                pf, pf2, pn, dimp, size = predict_A7(
                    counts, packets[arm], seed=seed, r=R)
            else:
                pf, pf2, pn, dimp, size = predict_A7X(
                    counts, packets[arm], seed=seed, r=R)
            pl = loads_from_counts(pf, pf2, pn, dimp, distinct_counts=None)
            results[arm] = {
                "pred_loads": pl,
                "pred_verdicts": {p: verdict(l, p, n_fact)
                                  for p, l in pl.items()},
                "testbed_fact_rows": size,
            }

        out = os.path.join(ddir, "predictions_eqinfo.csv")
        with open(out, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["dataset", "family", "seed", "n_fact", "pipeline",
                        "gt_load", "gt_verdict", "arm", "pred_load",
                        "pred_verdict", "testbed_fact_rows", "r",
                        "synopsis_bytes"])
            for pid, _, _ in PIPELINES:
                for arm in ARMS:
                    res = results[arm]
                    w.writerow([tag, meta["family"], meta["seed"],
                                meta["n_fact"], pid,
                                f"{gt_loads[pid]:.1f}", gt_verd[pid], arm,
                                f"{res['pred_loads'][pid]:.1f}",
                                res["pred_verdicts"][pid],
                                res["testbed_fact_rows"], R, syn_bytes])
        print(f"{tag}: packets A7={packet_info[tag]['A7']}B "
              f"A7X={packet_info[tag]['A7X']}B syn={syn_bytes}B "
              f"sizes A7={results['A7']['testbed_fact_rows']} "
              f"A7X={results['A7X']['testbed_fact_rows']} "
              f"done in {time.time()-t0:.1f}s -> {out}")


if __name__ == "__main__":
    main()
