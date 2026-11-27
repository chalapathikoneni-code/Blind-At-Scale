"""Held-out extension generation for E3 (frozen protocol S.3).

Generates 4 NEW datasets with the UNMODIFIED gen_near_threshold.generate:
s in {0.70, 0.78} x seeds {404, 505}, written ONLY to
final_scientific_strengthening/data_ext/ (never into data/).
Run with the realdata_arm venv interpreter (duckdb available there):
    cwd = ~/workspace/blind-at-scale/code
    ~/workspace/blind-at-scale/realdata_arm/venv/bin/python \
        ../final_scientific_strengthening/code/gen_ext.py
"""
import os
import sys

sys.path.insert(0, os.path.expanduser("~/workspace/blind-at-scale/code"))

from gen_near_threshold import generate  # noqa: E402

BASE = os.path.expanduser(
    "~/workspace/blind-at-scale/final_scientific_strengthening/data_ext")

COMBOS = [(0.70, 404), (0.70, 505), (0.78, 404), (0.78, 505)]


def tag_for(s, seed):
    return f"near_s{int(round(s * 1000)):03d}_s{seed}"


def main():
    for s, seed in COMBOS:
        tag = tag_for(s, seed)
        outdir = os.path.join(BASE, tag)
        if os.path.exists(os.path.join(outdir, "counts.npz")):
            print(f"{tag}: already exists, skipping generation")
            continue
        ok = generate(s, seed, outdir)
        print(f"{tag}: generate returned validation_ok={ok}", flush=True)
    print("extension generation done")


if __name__ == "__main__":
    main()
