# Real-Data EMR Arm — Design + Feasibility (Blind at Scale)

**Date:** 2026-11-23. **Status:** design + feasibility only; full build follows the
synthetic 100M package. **No spend. No AWS calls. No contact.**

The synthetic 100M EMR arm validates the paper's claims on multi-node. This arm
executes the SAME synopsis method on genuine real-world data on the same EMR
setup: the multi-node executed version of the co-author's Nov 21 analytic
real-world study (notebook `~/workspace/blind-at-scale-review/Coauthor_Notebook_20261121.ipynb`).

## 1. Verdicts

| Dataset | Verdict | Basis |
|---|---|---|
| NYC TLC taxi (yellow + green, 27 months) | **GO** | Public parquet live and verified; 1-month sample reproduced the partner's analytic ground truth exactly (11/11 loads); scale math closes at ~102M rows |
| Stack Exchange (public dump) | **NO-GO via public download** | 33.2 GB compressed, ~120 GB extraction scratch, multi-hour XML conversion, live-dump vintage unverified |
| Stack Exchange (partner's prepared parquet) | **CONDITIONAL GO** | His converted parquet totals 5.6 GB — the only practical route; requires him to deliver it (not in the current asks). Value-add over NYC is narrow (same 11 pipeline shapes; distinctive only in the 54M-key external-memory regime, already shown analytically). Recommend NYC-only for this arm |

## 2. NYC TLC feasibility (verified, not assumed)

**Source (from notebook cell 2, verified live 2026-11-23):**
`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
(same pattern for `green_`), plus `.../misc/taxi_zone_lookup.csv`.
HEAD sweep: every yellow month 2024-01..2026-09 returns HTTP 200; 2026-10
returns 403 (not yet published — ~2-month lag). All 33 green months also 200.

**Recommended corpus — window 2024-07..2026-09 (27 months):**
estimated ~102.2M yellow rows, 1.76 GB yellow + ~30 MB green + 12 KB lookup.
Includes the partner's 3 months (2026-01..03), so the calibration stage needs
no extra download. Row estimates use 17.22 bytes/row, calibrated on the three
verified months (17.18–17.26); validated on 2024-07 at +1.29% error, so the
window clears 100M with margin at the low end. **The exact row count is
established by the staging gate (parquet footer counts), never asserted here.**

**Sample validation — yellow 2026-01 (downloaded 2026-11-23):**
- Size 64,165,080 B — byte-identical to the partner's file (his cell 24).
- SHA-256 `8b3933fe6f0d7b6d8826613c0dd724edc680ff7c49e2bd4c7635c05102728637`
- Green 2026-01: 991,656 B, SHA-256
  `b0b188cd9ded91a9dfc931d9250aaca09fe388355d87802422707c6fc14762e0`
- Zone lookup: 12,331 B, SHA-256
  `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed`
- Yellow 2024-07 (schema-drift check): 52,299,432 B, SHA-256
  `af12d6b04daeb78550799ae99933d495b27d715b6dbb0fad9bb750a4537d3070`,
  3,076,903 rows.
- Files kept at `~/workspace/blind-at-scale/realdata_arm/sample/`.

**Replication of his analytic ground truth (script
`replicate_nyc_jan.py`, his exact model from notebook cells 28 + 32):**
all 11 NYC 2026-01 ground-truth loads reproduced EXACTLY from the public file
(G_A 199,975; G_B 191,226; G_C 10,596; G_N 2,390,495; J_A 199,975; J_B 191,226;
J_N 10,596; F_A 173,798,004; F_B 330,477,337; W_A 199,975; D_A 259) and all
thresholds exact (F_A 10,038,957.86 / F_B 24,985,055.67 — the notebook displays
them rounded). A5 predictions reproduced exactly (G_A 199,730; G_B 191,447).
**A1/A2/A3/A4 do NOT reproduce exactly — see §5, finding F2.**

**Skew (measured on the 2026-01 sample, all matching his cell-24 profile):**
top pickup zone 4.30% (160,343 rows); top-10 zones 33.48%; top-64 zones
92.56% (the synopsis head at k=64 covers nearly all mass); RatecodeID top
value 64.2% of rows with 29.2% NULL; zone join 100% lossless (0 keys/rows
outside the 265-zone lookup). Skew confirmed genuine and in older months too
(2024-07 top zone 5.9%).

**F_A/F_B are analytic-only at 100M (measured):** 1-month exact join outputs
F_A 642,493,303 / F_B 1,599,043,563 rows (exact match to his totals);
extrapolated ×729 for 27 months: F_A ~468B / F_B ~1.17T rows — never
materialised. Same treatment as the synthetic job (SKIPPED_NOT_MATERIALISABLE).

**Schema drift (measured):** 2026-01 yellow has 20 columns, 2024-07 has 19
(no `cbd_congestion_fee`). All four needed columns
(PULocationID, DOLocationID, RatecodeID, tpep_pickup_datetime) are present in
both. The job reads ONLY those columns (projection pushdown); the staging gate
verifies them in every file's footer.

## 3. Stack Exchange assessment (no download)

Public dump (Internet Archive item `stackexchange`, HEAD-verified 2026-11-23
after following redirects):
Posts.7z 23,026,928,274 B (23.0 GB); Comments.7z 6,997,473,057 B (7.0 GB);
Users.7z 990,180,949 B (0.99 GB); Votes.7z 2,202,885,100 B (2.2 GB);
**core-4 total ~33.2 GB compressed.** The notebook itself requires 120 GB free
for py7zr extraction (cell 2, `MIN_FREE_GB_FOR_PY7ZR_EXTRACTION = 120`), and
XML→parquet conversion of ~390M rows is a multi-hour job. The live dump's
vintage could NOT be verified from here (archive.org metadata API returned an
empty body via curl; the browser fetch tool failed) — a public-download build
would have to record the vintage at download time and check it against his
2024-04 study, or comparability breaks silently.
Partner's converted parquet (his cell 24): Posts 1.49 GB, Comments 1.68 GB,
Votes 2.23 GB, Users 0.23 GB — **5.6 GB total**, the only practical route.

## 4. Pipeline mapping (notebook cell 30 definitions)

Corpus: 27 months yellow (fact) + 27 months green (fact2) + zone lookup (dim).
Two stages in one EMR job:
- **Stage C (calibration):** his exact 3 months (2026-01..03) × 11 pipelines,
  executed. Measures the executed-vs-analytic gap on identical data (Spark
  uses Murmur3 hash partitioning; his analytic model uses xxhash64+splitmix64
  — the loads will differ somewhat; measuring the gap is a result, not a bug).
- **Stage S (scale):** the 100M corpus (27 months unioned) × 11 pipelines.

| Pipe | Kind (cell 30) | Tables / keys | Synopsis input (driver) | GT measured on EMR | Notes |
|---|---|---|---|---|---|
| G_A | group | yellow.PULocationID | per-key counts (262 keys) | max per-task shuffle-read (do_group) | Top zone alone exceeds the 2× threshold |
| G_B | group | yellow.DOLocationID | per-key counts (260) | as G_A | |
| G_C | group | green.PULocationID | per-key counts (~235) | as G_A | Small table (~1.1M rows / 27 mo) |
| G_N | group | yellow.RatecodeID | per-key counts (8 incl NULL) | as G_A | **Retain NULLs** (29% of rows); NULL hashes to one partition in both models; do not filter |
| J_A | fact_dimension | yellow.PU ⨝ zones.LocationID | fact counts ∩ dim keys | max per-task shuffle-read (do_join) | **SHUFFLE_MERGE hint mandatory** — the 265-row dim would otherwise broadcast and there would be zero shuffle to measure |
| J_B | fact_dimension | yellow.DO ⨝ zones | as J_A | as J_A | |
| J_N | fact_dimension | green.PU ⨝ zones | as J_A | as J_A | |
| F_A | fact_fact | yellow.PU × green.PU | product counts per shared key | **Analytic only** (splitmix64 max load from Spark counts) | 468B-row output; SKIPPED_NOT_MATERIALISABLE; raw-join cap logic reused from synthetic job |
| F_B | fact_fact | yellow.DO × green.DO | as F_A | Analytic only | 1.17T-row output |
| W_A | window | yellow.PULocationID | per-key counts | Real window execution (row_number over PU, ordered by tpep_pickup_datetime) + D8 partition counts | His design calls W_A "a partition-size proxy"; the EMR job runs a REAL window — stronger; record as a deviation from his §IV.C framing |
| D_A | distinct (diagnostic) | yellow PU → distinct DO | distinct counts per key | Spark groupBy + countDistinct (265 groups) | Diagnostic only, TDISTINCT=50,000, excluded from scoring — as in his design |

Measurement reuse from the synthetic job (`emr_package/code/spark_job.py`):
`do_group` / `do_join` / `do_factfact` / `do_window` implementations,
P_PARTITIONS = 128, event-log GT (max per-task shuffle-read records),
statusTracker topology gate (executors on ≥2 distinct hosts or VOID), D8
per-partition cross-check. Top-k = 64 (same as his study).

**Threshold rule — recommendation:** PRIMARY = relative 2× mean (his rule,
notebook cell 2 `LOAD_THRESHOLD_MULTIPLIER = 2.0`; comparability with his
analytic study). SECONDARY = absolute 0.02 × N (the synthetic study's rule;
comparability with the synthetic arm), reported alongside at zero extra cost —
both derive from the same measured loads. At ~102M rows: relative ≈ 1.59M,
absolute ≈ 2.04M; both far below the top-key contribution (~4.4M), so verdicts
agree on NYC, but reporting both is cheap insurance.

**Label disambiguation (mandatory):** his A1/A2/A3/A5 mean different methods
from the synthetic study's (his A1 = contribution-map sampling; synthetic A1 =
testbed-from-row-sample; his A2 = hash-key sampling; A3 = random-key sampling;
A5 = sampled-tail estimator). Every output row/file of this arm carries
`study="NYC_TLC_100M"` (per-instance for Stage C) and arm labels in HIS
vocabulary; the verify script must REFUSE a synthetic predictions file
(study-tag mismatch = hard error). The manuscript must never quote RW-A1 and
synthetic-A1 side by side without the qualifier.

## 5. Findings the build must honour

- **F1 — A1/A2/A3/A4 are not seed-pinned in his notebook.** His `predict_arm`
  (cell 32) consumes RNG in dict/set iteration order, which follows DuckDB's
  GROUP BY output order — not deterministic across environments. Proven here:
  same seed (42), same data, four dict orders → A1 G_A = 195,000 / 200,200 /
  202,700 / 202,600 — all different, none equal to his 197,100. A5 reproduces
  exactly because it sorts items before sampling. **Build requirement: sort
  keys canonically (by str) before ANY RNG consumption in the EMR job.**
  Consequence for the paper: his A1/A2/A3/A4 exact values are
  environment-sensitive; only verdicts (robust — all NYC workloads are
  high-skew by wide margins) and A5/A8/B* values are stable. The EMR arm's
  predictions are the canonical ones going forward.
- **F2 — J pipelines need the shuffle-merge hint.** See table above.
- **F3 — W_A is a real window on EMR, a proxy in his design.** Record the
  deviation; do not silently re-label his proxy as executed.
- **F4 — F_A/F_B never execute the raw join at this scale.** Analytic loads
  from Spark-produced counts (deviation D4 pattern, as in the synthetic job).
- **F5 — No seeded SHA exists for real data.** The integrity gate is
  reproducibility + completeness + schema (§6), not a hash anchor.

## 6. Dataset-integrity gate design (replaces the seeded SHA)

The data is not seeded, so the gate proves the staged data IS the public data
and is complete — it cannot prove it against a precomputed hash:
1. **Per-file SHA-256**, recorded in `dataset_manifest.json` at first download
   and re-verified on every re-download / staging. First-download trust is the
   residual (TLC publishes no checksums); the manifest is the durable record.
2. **Exact row counts from parquet footers** (no scan) for all 54 files
   (27 yellow + 27 green), summed and reported as THE corpus size. No
   estimated counts enter any result.
3. **Required-columns check** per file footer: PULocationID, DOLocationID,
   RatecodeID, tpep_pickup_datetime (yellow); PULocationID, DOLocationID
   (green); LocationID (lookup).
4. **Key-range checks:** PU/DO values ⊆ zone lookup IDs (100% in the sample);
   RatecodeID domain {1..6, 99, NULL}; no negative IDs.
5. **Month-pairing check:** every month present for BOTH yellow and green;
   abort on a missing month (a half corpus would silently change the scale).
6. The Spark job re-reads `dataset_manifest.json` at startup and aborts if
   staged files fail any check. The manifest (with SHAs + counts) ships in the
   reproduction package.

## 7. Dry-run feasibility (VM ~6 GB RAM, no Spark)

- **Done in this task:** 1-month end-to-end analytic replication, exact
  (§2). Proves source identity, schema, key columns, skew, pipeline
  definitions, threshold math, F analytic outputs.
- **Build dry run CAN cover the full 100M scale locally:** download all 27
  months (1.76 GB, ~10 min at observed speeds), then DuckDB counts + analytic
  GT + all 9 arm predictions. Group-by state is tiny (265 keys); the work is
  scan-bound (the 1-month replication took ~12 s; 27 months ≈ 5 min). Disk is
  fine (49 GB free). The entire non-Spark path — staging gate, counts,
  analytic GT, predictions, verify logic — is dry-runnable here at full scale.
- **Cannot be validated until EMR:** Spark execution itself (shuffle
  measurement, executor placement, event-log capture), the executed-vs-analytic
  gap, wall time / cost, S3 read throughput, bootstrap numpy install. Validated
  by API audit + code reuse from the synthetic job, same as the other package.

## 8. Build plan (mirror `emr_package/` layout)

```
realdata_emr_package/
  code/
    spark_job_realdata.py     # Stage C + Stage S; reuses synthetic do_group/do_join/
                              # do_factfact/do_window with real columns; sorted-key RNG (F1);
                              # SHUFFLE_MERGE hint on J (F2); real window (F3); F analytic (F4)
    bootstrap_action.sh       # pinned numpy (same as synthetic job)
    stage_data.py             # download 27mo + lookup from CloudFront, integrity gate (§6),
                              # write dataset_manifest.json; resumable (his download_resumable
                              # pattern, notebook cell 8)
  config/
    fleet.json / instance_groups.json   # copy from synthetic package (same ~20-node fleet)
    predictions_realdata.csv  # Stage C + S arm predictions (study-tagged; computed in dry run)
    analytic_gt_calibration.json  # his 3-month analytic GT (recomputed in dry run, §2 values)
    dataset_manifest.json     # SHAs + exact counts + schema (§6); generated by stage_data.py
  scripts/
    upload_to_s3.sh           # same layout as synthetic (code/ config/ scripts/ docs/ + data/ prefix)
    add_job_step.sh           # spark-submit, --master yarn, event log to S3
    emr_create_cluster.sh     # same as synthetic (shared cluster shape)
    verify_realdata_run.py    # placement gate (≥2 hosts) + verdicts at BOTH thresholds (§4) +
                              # executed-vs-analytic comparison (Stage C) + study-tag enforcement
  docs/
    README_RUNBOOK.md         # user's workflow: Drive -> S3 -> cluster -> add step -> verify -> terminate
    VALIDATION_REPORT.md      # V1-V7 results (below)
```

**Validation checklist (mirrors the synthetic package's V1–V6):**
- V1 staging gate: all 54 files + lookup downloaded, SHAs recorded, exact row
  counts summed, columns + key ranges + month pairing checked.
- V2 full-scale local analytic replication: 27-month counts + analytic GT +
  all arm predictions via DuckDB; Stage C values must equal the §2 replication
  for the overlapping months.
- V3 1-month + 3-month dry run of the verify/collect logic against the
  analytic GT (verdicts at both thresholds; F analytic cross-check).
- V4 Spark 3.5 API audit of `spark_job_realdata.py` (same as synthetic V3).
- V5 shellcheck + py_compile on all scripts; `aws emr` commands validated by
  construction (no live calls).
- V6 placement-gate logic tested against the synthetic run's event log
  (reuse) — must fail LOUD on missing placement evidence, never pass silent.
- V7 RNG-order audit: prove predictions are invariant to input dict order
  (sorted-key consumption, F1) by re-running predictions under shuffled input
  order and asserting identical outputs.

**Inputs the build needs from the synthetic package:** `spark_job.py`
(measurement implementations), `fleet.json` / `instance_groups.json`,
`verify_emr_run.py` (placement-gate logic). **Nothing needed from the partner**
for the NYC arm; SE needs his 5.6 GB parquet if ever pursued.

## 9. Work artifacts (this task)

- `~/workspace/blind-at-scale/realdata_arm/REALDATA_ARM_DESIGN.md` (this file)
- `~/workspace/blind-at-scale/realdata_arm/replicate_nyc_jan.py` (exact replication)
- `~/workspace/blind-at-scale/realdata_arm/sample/` (yellow+green 2026-01,
  yellow 2024-07, zone lookup; SHAs in §2)
- `~/workspace/blind-at-scale/realdata_arm/venv/` (pyarrow 26.0.1, duckdb
  2.0.1, pandas 3.1.1, numpy 2.5.5, xxhash)
