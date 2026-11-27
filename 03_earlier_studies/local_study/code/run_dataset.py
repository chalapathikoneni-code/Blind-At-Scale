"""Per-dataset driver: synopsis + arms -> predictions CSV.

Usage: python run_dataset.py --dataset-dir D --arms A1,A2,A3,A4,A5,A8
       [--r 0.01] [--rtag main]
"""
import argparse, csv, json, os, time
import numpy as np

from arms import build_synopsis, run_arms
from common import PIPELINES


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-dir", required=True)
    ap.add_argument("--arms", required=True)
    ap.add_argument("--r", type=float, default=0.01)
    ap.add_argument("--rtag", default="main")
    args = ap.parse_args()
    d = args.dataset_dir
    with open(os.path.join(d, "meta.json")) as f:
        meta = json.load(f)
    z = np.load(os.path.join(d, "counts.npz"))
    counts = {k: z[k] for k in z.files}
    counts["null_count"] = int(counts["null_count"])
    t0 = time.time()
    syn = build_synopsis(counts)
    syn_path = os.path.join(d, "synopsis.json")
    with open(syn_path, "w") as f:
        json.dump(syn, f)
    syn_bytes = os.path.getsize(syn_path)
    arms = args.arms.split(",")
    gt_loads, gt_verd, results = run_arms(counts, syn, meta["seed"], arms, r=args.r)
    out = os.path.join(d, f"predictions_{args.rtag}.csv")
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["dataset", "family", "seed", "n_fact", "pipeline",
                    "gt_load", "gt_verdict", "arm", "pred_load",
                    "pred_verdict", "testbed_fact_rows", "r",
                    "synopsis_bytes"])
        tag = os.path.basename(d.rstrip("/"))
        for pid, _, _ in PIPELINES:
            for arm in arms:
                res = results[arm]
                w.writerow([tag, meta["family"], meta["seed"], meta["n_fact"],
                            pid, f"{gt_loads[pid]:.1f}", gt_verd[pid], arm,
                            f"{res['pred_loads'][pid]:.1f}",
                            res["pred_verdicts"][pid],
                            res["testbed_fact_rows"], args.r, syn_bytes])
    print(f"{tag} r={args.r} arms={arms}: done in {time.time()-t0:.1f}s -> {out}")


if __name__ == "__main__":
    main()
