#!/bin/bash
# Full experimental grid (PREREGISTRATION.md). Sequential per dataset so
# each gen_dataset.py runs in a fresh process (memory hygiene on 7 GB).
set -u
PY=~/workspace/ieee-access-stress-test/venv/bin/python
cd ~/workspace/blind-at-scale/code
D=~/workspace/blind-at-scale/data
for fam in zipf lognormal mixed; do
  for seed in 101 202 303; do
    tag=${fam}_s${seed}
    [ -f "$D/$tag/validation.json" ] || \
      $PY gen_dataset.py --family $fam --seed $seed --n-fact 10000000 \
         --n-fact2 6000000 --outdir "$D/$tag" >> "$D/grid.log" 2>&1
    $PY run_dataset.py --dataset-dir "$D/$tag" \
        --arms A1,A2,A3,A4,A5,A8 --rtag main >> "$D/grid.log" 2>&1
  done
done
# scale spot-check: zipf seed 101 at 40M fact rows, arms A1/A5/A8
tag=zipf_s101_40m
[ -f "$D/$tag/validation.json" ] || \
  $PY gen_dataset.py --family zipf --seed 101 --n-fact 40000000 \
     --n-fact2 24000000 --outdir "$D/$tag" >> "$D/grid.log" 2>&1
$PY run_dataset.py --dataset-dir "$D/$tag" \
    --arms A1,A5,A8 --rtag main >> "$D/grid.log" 2>&1
# reduction spot-check: r=0.001 on zipf seed 101, arms A1/A5
$PY run_dataset.py --dataset-dir "$D/zipf_s101" \
    --arms A1,A5 --r 0.001 --rtag r0001 >> "$D/grid.log" 2>&1
echo "GRID DONE" >> "$D/grid.log"
