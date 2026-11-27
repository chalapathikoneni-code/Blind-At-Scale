"""E5 driver (FINAL_EXECUTION_PROTOCOL.md S.5).

For each dataset scale, runs every phase in a fresh subprocess,
3 repetitions each, and records the raw per-repetition results to
results/e5_raw.json. Downstream phases consume the counts produced by
the scan phase when those counts validate exactly against counts.npz
for that dataset; otherwise they consume counts.npz itself (and the
report says so).
"""

import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
RESULTS = os.path.join(ROOT, "final_scientific_strengthening", "results")
SCRATCH = os.path.join(RESULTS, "e5_scratch")
PHASE_PY = os.path.join(HERE, "e5_phase.py")
PY = os.path.join(ROOT, "realdata_arm", "venv", "bin", "python")
REPS = 3

DATASETS = [
    ("outeq_200k_zipf_s101", "200K"),
    ("outeq_1m_zipf_s101", "1M"),
    ("zipf_s101", "10M"),
    ("zipf_s101_40m", "40M"),
]


def run_phase(args):
    cmd = [PY, PHASE_PY] + args
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"phase failed: {cmd}\n{p.stderr[-2000:]}")
    return json.loads(p.stdout.strip().splitlines()[-1])


def main():
    os.makedirs(SCRATCH, exist_ok=True)
    raw = {}
    for tag, scale in DATASETS:
        ddir = os.path.join(ROOT, "data", tag)
        meta = json.load(open(os.path.join(ddir, "meta.json")))
        seed = int(meta["seed"])
        entry = {"scale": scale, "seed": seed, "meta": meta, "reps": {}}

        scan_counts = os.path.join(SCRATCH, f"scan_counts_{tag}.npz")
        scans = []
        for rep in range(REPS):
            r = run_phase(["--phase", "scan", "--dataset-dir", ddir,
                           "--out-counts", scan_counts])
            scans.append(r)
            print(tag, "scan rep", rep, r["seconds"], "exact",
                  r["validation_exact"], flush=True)
        entry["reps"]["scan"] = scans
        validated = all(s["validation_exact"] for s in scans)
        entry["counts_validated"] = validated
        counts_src = scan_counts if validated else os.path.join(ddir,
                                                                "counts.npz")
        entry["counts_source"] = ("scan-derived (validated exact)"
                                  if validated else "counts.npz (fallback)")

        for phase in ("build", "serialize"):
            rs = []
            for rep in range(REPS):
                r = run_phase(["--phase", phase, "--dataset-dir", ddir,
                               "--counts", counts_src])
                rs.append(r)
                print(tag, phase, "rep", rep, r["seconds"], flush=True)
            entry["reps"][phase] = rs

        for arm in ("A8", "A5", "A9"):
            rs = []
            for rep in range(REPS):
                r = run_phase(["--phase", "predict", "--dataset-dir", ddir,
                               "--counts", counts_src, "--arm", arm,
                               "--seed", str(seed)])
                rs.append(r)
                print(tag, "predict", arm, "rep", rep, r["seconds"],
                      flush=True)
            entry["reps"][f"predict_{arm}"] = rs

        entry["fact_rows"] = int(meta["n_fact"])
        entry["fact2_rows"] = int(meta["n_fact2"])
        entry["fact_bytes"] = os.path.getsize(os.path.join(ddir,
                                                           "fact.parquet"))
        entry["fact2_bytes"] = os.path.getsize(os.path.join(ddir,
                                                            "fact2.parquet"))
        raw[tag] = entry

    out = os.path.join(RESULTS, "e5_raw.json")
    with open(out, "w") as f:
        json.dump(raw, f, indent=1)
    print("wrote", out)


if __name__ == "__main__":
    main()
