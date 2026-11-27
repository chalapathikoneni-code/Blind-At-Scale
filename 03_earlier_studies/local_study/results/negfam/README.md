# Negative family (Amendment 4) — results

Pre-registration: `PREREGISTRATION.md` Amendment 4, written
**2026-11-06 11:46 UTC, before any negative-family run**. All thresholds,
pipeline definitions, arm mappings, and qualitative expectations were
locked there. No threshold or mapping was changed after seeing results.

## Design recap (as run)

- fact N = 5,000,000 rows; key `k` over 20,000 keys, Zipf s = 0.6
  (realised max per-key row count: 38,420 seed 101; 38,654 seed 202 —
  below the 100,000-row main-study threshold, as designed).
- `cat` WIDE with p = 0.02 (realised 100,150 / 100,152 rows), independent
  of `k`. Payload length: NORMAL U[20,100] chars, WIDE U[5,000,20,000]
  chars. Total payload bytes: 1,548,204,724 (s101) / 1,547,118,995 (s202).
- `sprefix` = 'WIDE' for all wide rows; 'p'||code (10,000 codes) otherwise.
- dim: 20,000 keys, 48-char payload. No fact2 (Amendment 4 §A4.1).

## Pipelines and thresholds (Amendment 4 §A4.2)

| Pipeline | Load statistic | FAIL threshold |
|---|---|---|
| V1 GROUP BY bytes | max over k of SUM(LENGTH(payload)) | > 4,000,000 bytes |
| V2 fact⋈dim join bytes | max hash-partition (P=128) of per-key join output bytes | > 10,000,000 bytes |
| V3 string-partition spill proxy | max over sprefix of SUM(LENGTH(payload)) | > 50,000,000 bytes |
| V4 UDF cost | max hash-partition (P=128) of per-key SUM(LENGTH(payload)^2) | > 50,000,000,000 |

Ground truth: real DuckDB SQL (GROUP BY k / GROUP BY sprefix / JOIN +
GROUP BY) over the materialised parquet; per-key counts and byte totals
validated exactly against drawn arrays (`data/negfam_s*/validation.json`,
all diffs 0). Hash partitioning uses the study's splitmix64/P=128 model.

## Ground truth: 8/8 FAIL (as pre-registered)

| Seed | V1 bytes | V2 bytes | V3 bytes (argmax 'WIDE') | V4 cost |
|---|---|---|---|---|
| 101 | 12,184,965 | 32,070,756 | 1,254,205,218 | 319,531,768,216 |
| 202 | 12,393,566 | 32,097,836 | 1,253,197,021 | 317,494,192,998 |

## Headline metrics (pooled, 8 instances; FAIL = positive class)

| Arm | Recall | Precision | F1 | TP/FP/FN/TN |
|---|---|---|---|---|
| A5-NF (synopsis testbed) | 0.000 | undefined (no FAIL predicted) | 0.000 | 0/0/8/0 |
| A8-NF (synopsis direct) | 0.000 | undefined (no FAIL predicted) | 0.000 | 0/0/8/0 |
| A1-NF (1% sample, payload retained) | 1.000 | 1.000 | 1.000 | 8/0/0/0 |
| A4-NF (marginal synthesis) | 0.750 | 1.000 | 0.857 | 6/0/2/0 |

Per pipeline (identical pattern on both seeds): synopsis arms recall 0.0
on V1–V4; A1-NF recall 1.0 on V1–V4; A4-NF recall 1.0 on V1, V2, V4 and
0.0 on V3 — the correlational pipeline, exactly the miss pre-registered
for it (its predicted V3 loads were 31,528,229 and 31,220,504 bytes vs
the 50,000,000 threshold, while the true WIDE partition is ~1.25×10^9).

Synopsis predicted row loads were 38,400–38,700 (max group) and
85,800–86,200 (max partition), all below T_row = 100,000 → PASS on every
instance, while every byte/cost ground-truth load exceeded its threshold.
**Interpretation (as pre-registered): the synopsis, which stores only key
counts / heavy hitters / fan-out, is blind to value-driven failures. This
is the method's boundary, reported as a negative result (R1#4); it was
not tuned away.**

Full numbers: `results/negfam/summary.json`, `results/negfam/metrics_by_arm.csv`;
raw per-instance predictions: `data/negfam_s{101,202}/predictions_negfam.csv`;
ground-truth details: `data/negfam_s{101,202}/gt_negfam.json`.

## Deviations and incidents (all declared)

1. Declared in Amendment 4 itself, pre-run: N at the 5M lower end of the
   brief's range; WIDE length cap 20 KB (brief example 50 KB) for the
   ~7 GB machine; payload content is a repeated filler character (length
   is the statistic); A1 uses a systematic uniform 1-in-100 sample over
   shuffled row order (inclusion probability exactly 0.01) rather than
   Bernoulli sampling; for V3 the synopsis arm is credited with its
   largest available row statistic (max key-group count) since sprefix
   is not stored.
2. Implementation incident, pre-result (2026-11-06 ~11:48 UTC): the first
   seed-101 generation invocation aborted during its validation step
   (DuckDB view for `dim` not created in the validation block). No
   parameter, data, or result was affected; the partial dataset directory
   was deleted, the view line was added, and generation was re-run from
   scratch. Validation then passed exactly on both seeds.
3. A4-NF (optional in the brief) was included.

## Files

- Code: `code/negfam/gen_negfam.py`, `code/negfam/run_negfam.py`,
  `code/negfam/score_negfam.py`
- Data: `data/negfam_s101/`, `data/negfam_s202/` (fact.parquet with the
  VARCHAR payload, dim.parquet, fact_int.parquet sidecar, counts.npz,
  marginals.npz, negfam_synopsis.json — key-counts only, by construction)
- Results: `results/negfam/summary.json`, `results/negfam/metrics_by_arm.csv`
- Runs appended to `RUN_LOG.md`.
