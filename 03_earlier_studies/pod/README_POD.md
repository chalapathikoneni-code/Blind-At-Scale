# Blind at Scale — Workstream 4: Spark cluster ground truth (pod package)

Self-contained package that produces **real Spark execution ground truth**
(actual shuffle / spill / task metrics from Spark event logs) for the
headline pipeline families **G** (group-by), **J** (fact-join-dim) and
**F** (fact-fact join), plus **W_A**, on the **zipf seed-101** dataset at
**10M-row fact scale** — replacing, for these pipelines, the local study's
single-machine hash-partition *simulation*.

## Honest labelling (binding)

The cluster is a **Spark standalone cluster with 1 master + 2 worker
processes on a single host** — all shuffle traffic is loopback, there is
no cross-host network and no multi-node fault behaviour. Unless multiple
hosts are genuinely used, every number from this package must be cited
as: *"multi-worker standalone cluster on a single host"*. `collect_results.py`
copies the master/labelling string from the run manifest into
`spark_ground_truth.json`; a run submitted with `--master local[*]`
(smoke test only) is labelled `NOT A CLUSTER RUN` and must never be
quoted as cluster ground truth.

## What "ground-truth load" means here

Defined precisely, with all deviations from the local simulation
(murmur3 vs splitmix64, partial aggregation, the non-materialisable
F join, AQE off, …), in the header of `spark_pipelines.py` (D1–D7).
Short form: per pipeline, GT load = max over its tasks of
**shuffle-read records** from the event log (the per-partition load;
scan/input records are reported separately — they are governed by
parquet file splitting, a storage artefact, and in the local smoke test
one scan task read all 500,000 rows while the busiest shuffle partition
held ~50k). For F, the verdict-bearing load is the analytic splitmix64
load computed from Spark-produced per-key counts (the raw join output
is not a shuffle; see D4). Verdict = FAIL iff GT load > T_load =
200,000 (= 0.02 × 10M, PREREGISTRATION.md §3).

## Files

| File | Purpose |
|---|---|
| `setup.sh` | Install OpenJDK + Spark 3.5.3, event logging on, start master + 2 workers |
| `gen_on_pod.py` | zipf seed-101 dataset as parquet; per-key counts bit-identical to the local study (validated against local G_A/G_B/G_C/G_N maxima) |
| `spark_pipelines.py` | G_A, G_B, G_N, J_A, J_B, J_N, F_A, F_B, W_A at `spark.sql.shuffle.partitions=128`; writes `run_manifest.json` |
| `collect_results.py` | Event log → `spark_ground_truth.json` + comparison vs local A1/A5/A8 predictions for zipf_s101 |
| `run_pod.sh` | Steps 2–4 below in one command (`bash run_pod.sh`, or `bash run_pod.sh smoke`) |

## Exact run sequence

```bash
# 0. On the pod (Ubuntu 22.04/24.04, >= 8 vCPU, >= 16 GB RAM, >= 40 GB disk):
#    upload this pod/ directory, e.g. scp -P <port> -r pod/ root@<pod-host>:~/pod
#    or runpodctl send pod/  — then:
cd ~/pod

# 1. Install + start the cluster            (~5-10 min, mostly downloads)
bash setup.sh
#    verify: curl -s http://127.0.0.1:8080/json/  -> "aliveworkers": 2

# 2-4. Generate data, run pipelines, collect results
bash run_pod.sh
#    (equivalently, by hand:)
#    python3 gen_on_pod.py --outdir ~/bas-data/zipf_s101
#    $SPARK_HOME/bin/spark-submit --master spark://127.0.0.1:7077 \
#        --driver-memory 2g --total-executor-cores <pod cores> \
#        spark_pipelines.py --data ~/bas-data/zipf_s101 \
#        --manifest ~/bas-spark/run_manifest_zipf_s101.json
#    python3 collect_results.py --manifest ~/bas-spark/run_manifest_zipf_s101.json \
#        --predictions ../data/zipf_s101/predictions_main.csv \
#        --summary ../results/summary.json \
#        --out results_out/spark_ground_truth_zipf_s101.json
#    (the two local-study paths exist only if the whole blind-at-scale/
#     tree was uploaded; collect_results.py falls back to searching
#     ~/workspace/blind-at-scale/ and otherwise omits the comparison.)

# 5. Download results_out/spark_ground_truth_zipf_s101.json
#    (and results_out/*.log) OFF the pod.

# 6. TERMINATE THE POD. Verify termination. Record the account balance;
#    spend for this workstream = balance_before - balance_after, cap $10.
```

## Expected runtime (8–16 vCPU pod)

| Step | Expected |
|---|---|
| setup.sh | 5–10 min |
| gen_on_pod.py (10M/6M) | 1–3 min |
| spark_pipelines.py (9 pipelines) | 10–30 min (G/J/W are minutes each; F runs its aggregate form only — the raw F join is *by design* never attempted, see D4) |
| collect_results.py | < 1 min |
| **Total pod wall time** | **≈ 20–45 min** (budget 1 h) |

At typical community-cloud CPU-pod prices this is far under the $10 cap;
the cap is governed by the balance difference, so record the balance
before deploying and after terminating.

## Smoke test

`bash run_pod.sh smoke` runs the identical code path at 500k fact rows
(seed 999, the local study's smoke seed). Its verdicts use T = 10,000 and
are **not** headline ground truth — they only prove the package executes.
