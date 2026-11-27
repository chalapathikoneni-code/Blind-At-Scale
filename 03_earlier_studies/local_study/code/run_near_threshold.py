"""Driver for the near-threshold grid (PREREGISTRATION_EXP_A.md S.7).

Grid: Zipf s in {0.65, 0.68, 0.72, 0.74} x seeds {101, 202} =
8 datasets, 88 instances. For each dataset: generate with
gen_near_threshold.py (DuckDB-validated; all_exact required),
build the synopsis with the UNMODIFIED arms.build_synopsis, then run
A1/A2/A3/A4/A5/A8 via the UNMODIFIED arms.run_arms (full arm list, so
A5's seed matches the main-grid convention), A7 via the UNMODIFIED
eqinfo_arms (seed = dataset_seed * 1000 + 5000), and A9 (headline
hash H0 = 0) via classical_arms.

Writes: data/near_<tag>/ (new dataset dirs: counts.npz, parquet,
synopsis.json, meta.json, validation.json) and
results/exp_a/predictions_near_<tag>.csv (standard schema, arms
A1/A2/A3/A4/A5/A7/A8/A9), packet_A7_<tag>.json and
packet_A9_<tag>.json, and run_near_threshold.log. Existing data/ and
results/ files are never touched.

Usage (cwd = this code/ directory):
    python run_near_threshold.py
"""
import csv, json, os, time
import numpy as np

from arms import build_synopsis, run_arms
from common import K, PIPELINES, loads_from_counts, verdict, max_partition_load
from eqinfo_arms import build_packet_A7, predict_A7
from classical_arms import predict_A9, HASH_SEED_HEADLINE
from gen_near_threshold import generate

BASE = os.path.expanduser("~/workspace/blind-at-scale")
S_GRID = (0.65, 0.68, 0.72, 0.74)
SEEDS = (101, 202)
ARMS_MAIN = ["A1", "A2", "A3", "A4", "A5", "A8"]
R = 0.01


def tag_for(s, seed):
    return f"near_s{int(round(s * 1000)):03d}_s{seed}"


def main():
    outdir = os.path.join(BASE, "results", "exp_a")
    os.makedirs(outdir, exist_ok=True)
    log = open(os.path.join(outdir, "run_near_threshold.log"), "w")
    for s in S_GRID:
        for seed in SEEDS:
            tag = tag_for(s, seed)
            t0 = time.time()
            ddir = os.path.join(BASE, "data", tag)
            if not os.path.exists(os.path.join(ddir, "counts.npz")):
                generate(s, seed, ddir)
            with open(os.path.join(ddir, "meta.json")) as f:
                meta = json.load(f)
            assert meta["validation_ok"], f"{tag}: validation failed"
            z = np.load(os.path.join(ddir, "counts.npz"))
            counts = {k: z[k] for k in z.files}
            counts["null_count"] = int(counts["null_count"])
            n_fact = int(counts["fact_kA"].sum())

            syn = build_synopsis(counts)
            syn_path = os.path.join(ddir, "synopsis.json")
            with open(syn_path, "w") as f:
                json.dump(syn, f)
            syn_bytes = os.path.getsize(syn_path)

            # A1-A8 via the unmodified machinery (full list: A5 seed
            # matches the main-grid convention)
            gt_loads, gt_verd, results = run_arms(
                counts, syn, seed, ARMS_MAIN, r=R)

            # A7 (eqinfo convention: seed = ds*1000 + 5000 + 0)
            packet7 = build_packet_A7(counts, syn)
            p7path = os.path.join(outdir, f"packet_A7_{tag}.json")
            with open(p7path, "w") as f:
                json.dump(packet7, f, separators=(",", ":"))
            pf7, pf27, pn7, dimp7, size7 = predict_A7(
                counts, packet7, seed=seed * 1000 + 5000, r=R)
            pl7 = loads_from_counts(pf7, pf27, pn7, dimp7,
                                    distinct_counts=None)
            results["A7"] = {
                "pred_loads": pl7,
                "pred_verdicts": {p: verdict(l, p, n_fact)
                                  for p, l in pl7.items()},
                "testbed_fact_rows": size7,
            }

            # A9 (headline hash H0); F loads from Eq. 1 contributions
            pf9, pf29, pn9, dimp9, _, f_contrib, packet9, _ = predict_A9(
                counts, hseed=HASH_SEED_HEADLINE)
            p9path = os.path.join(outdir, f"packet_A9_{tag}.json")
            with open(p9path, "w") as f:
                json.dump(packet9, f, separators=(",", ":"))
            pl9 = loads_from_counts(pf9, pf29, pn9, dimp9,
                                    distinct_counts=None)
            for col in ("kA", "kB"):
                pl9[f"F_{col[1]}"] = max_partition_load(f_contrib[col])
            results["A9"] = {
                "pred_loads": pl9,
                "pred_verdicts": {p: verdict(l, p, n_fact)
                                  for p, l in pl9.items()},
                "testbed_fact_rows": 0,
            }

            arms_out = ARMS_MAIN + ["A7", "A9"]
            out = os.path.join(outdir, f"predictions_near_{tag}.csv")
            with open(out, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["dataset", "family", "seed", "n_fact",
                            "pipeline", "gt_load", "gt_verdict", "arm",
                            "pred_load", "pred_verdict",
                            "testbed_fact_rows", "r", "synopsis_bytes"])
                for pid, _, _ in PIPELINES:
                    for arm in arms_out:
                        res = results[arm]
                        w.writerow([tag, "near", seed, n_fact, pid,
                                    f"{gt_loads[pid]:.1f}", gt_verd[pid],
                                    arm, f"{res['pred_loads'][pid]:.1f}",
                                    res["pred_verdicts"][pid],
                                    res["testbed_fact_rows"],
                                    "" if arm == "A9" else R, syn_bytes])
            n_fail = sum(1 for v in gt_verd.values() if v == "FAIL")
            fails = [p for p, v in gt_verd.items() if v == "FAIL"]
            line = (f"{tag}: s={s} gt_fail={n_fail}/11 {fails} "
                    f"G_A={gt_loads['G_A']:.0f} J_A={gt_loads['J_A']:.0f} "
                    f"syn={syn_bytes}B pktA7={os.path.getsize(p7path)}B "
                    f"pktA9={os.path.getsize(p9path)}B "
                    f"done in {time.time()-t0:.1f}s -> {out}")
            print(line)
            log.write(line + "\n")
            log.flush()
    log.close()
    print("near-threshold grid done")


if __name__ == "__main__":
    main()
