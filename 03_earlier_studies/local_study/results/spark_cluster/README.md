# Spark cluster ground truth — zipf_s101 (Workstream 4, executed 2026-11-06)

**Labelling (binding): multi-worker standalone cluster on a single host.**
Spark 3.5.3 standalone, 1 master + 2 worker processes on one RunPod host
(pod `blind-at-scale-gt`, EU-RO-1), shuffle traffic on loopback. Not a
multi-host run; no cross-host network or fault behaviour is exercised.

## Files

| File | What it is |
|---|---|
| `spark_ground_truth_zipf_s101.json` | The 9 headline pipelines' Spark ground truth (loads, verdicts, sources), **reconstructed from the pod operator's verbatim verified report** (see its `_provenance` field). The canonical full file (stage durations, task counts, byte metrics), the Spark event log and all run logs are on the research-ops volume at `/workspace/blind-pod/` (186 MB). |
| `cluster_comparison.json` | Local recovery of the comparison section that was empty on the pod (predictions file not uploaded): arm verdicts vs Spark GT, local-simulation vs Spark agreement, and the threshold audit. Computed by `code/cluster_compare.py` from the file above + `data/zipf_s101/predictions_main.csv`. |
| `POD_RUN_RECORD.md` | Execution path as run: pod spec, template/volume, preflight, command sequence, the two no-patch execution fixes, spend, termination. |
| `smoke_local2_spark_ground_truth.json` | The earlier local-mode smoke check. **NOT A CLUSTER RUN** (labelled in-file); kept unchanged as the smoke record. |

## Headline outcomes

- Threshold applied: **T = 200,000** (= 0.02 × N_fact; audit in
  `cluster_comparison.json` — all 9 recorded verdicts recompute exactly
  under 200,000; the 20,000 figure in an auxiliary pod output is the
  signature of a 1M-row invocation and never entered the verdict path).
- Local simulation vs Spark: **verdicts 9/9**, median load Q-error 1.124.
  F_A/F_B analytic loads from Spark-produced counts equal the local
  ground truth **exactly** (counts bit-identical; SHA-256 check PASS).
  G/W Spark loads are the D2/D5 repartition upper bound (ratios
  1.14–2.03 vs the local max-group definition); J loads differ only by
  the D1 hash function (ratios 0.89–1.03).
- Arms vs Spark GT (9 pipelines): A1 9/9, A2 9/9, A4 9/9, **A5 9/9,
  A8 9/9**, A3 4/9 (misses G_A, G_N, J_A, J_N, W_A — the same
  hot-key blindness as in the simulated study). Median pred-load
  Q-errors vs Spark: A5 1.080, A8 1.087, A1 1.136, A2 1.142,
  A4 1.154, A3 6.776.
- F_A / F_B raw joins not materialised by design (D4):
  SKIPPED_NOT_MATERIALISABLE; exact output sizes 898,098,021,014 and
  17,566,403,854 rows from Spark-produced counts.
- Zero failed tasks, zero spill bytes across all 9 pipelines.
- Spend: $0.16 (balance $14.16 → $14.00); pod terminated and deleted.
