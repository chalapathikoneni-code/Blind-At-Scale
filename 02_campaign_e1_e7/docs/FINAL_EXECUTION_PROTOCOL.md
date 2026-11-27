# FINAL EXECUTION PROTOCOL — Blind at Scale, Final Scientific Strengthening

Frozen 2026-11-25 (America/New_York), BEFORE any new outcome in this
campaign was examined. This campaign is **follow-up validation informed
by earlier results**. It is NOT part of the original preregistration
(`PREREGISTRATION.md`, 2026-11-06) or of `PREREGISTRATION_EXP_A.md`
(2026-11-24), and nothing here retroactively changes either. The
original failed hypotheses are preserved exactly as recorded: the
recall-superiority kill clause fired (A4 ties A5 on the 99), and
Experiment A's Branch 1 fired (A9 matches A5 → claim narrowed to the
heavy-hitter *information*). The user submits the manuscript on
2026-11-26; this campaign's local experiments must complete and be
verified today (2026-11-25) or be reported NOT EXECUTED with the reason.

Hard rules (all experiments): no existing artifact is overwritten; new
code lives in `final_scientific_strengthening/code/`, new outputs in
`final_scientific_strengthening/results/` and `.../figures/`; analytic
calculations, simulated outcomes, measured execution metrics, and
actual execution failures are always distinguished; a threshold
violation is never described as an observed execution failure; no
privacy claim is made from row-free representations; no
universal-superiority or theoretical-minimum claim is made.

## 0. Already-satisfied audit (do not duplicate)

| Req. | Status before this campaign | What is actually missing |
|---|---|---|
| E2 frontier | A8 k∈{16,64,256,1024}, A5/A7/A7X/A9 @1024 points exist on the headline 99 with bytes (`results/cost_frontier/frontier.csv`); per-instance A8 rung predictions exist (`results/ablation/predictions_hh_k*.csv`) | A9 depth sweep (targets 16/64/256); ONE uniform metric set for every point (FP/FN, mean/p95/max Q, per-family, worst underprediction, prediction time); the coordinated-sampling determination |
| E3 near-threshold depth | Grid (8 datasets, 88 instances, 62 GT FAIL), GT labels, A8@1024, A9@1024, A5, A7 results exist (`results/exp_a/predictions_near_*.csv`, `summary_near.json`) | A8 k∈{16,64,256} and A9 targets {16,64,256} on the grid; F_B and J_B investigations; held-out extension grid |
| E4 physical validation | Stage C executed-vs-analytic ratios exist for the real-data leg (30 comparisons); both EMR verifier outputs exist | Systematic re-derivation from BOTH raw event logs incl. task durations, shuffle bytes, spill per pipeline; the separated 24 non-F / 6 F calibration analysis; figure + machine-readable data |
| E5 profiling overhead | ONE prior measurement (Zipf-101, from pre-loaded counts; REMAINING_QUESTIONS #26) | Everything else: scan/count from raw parquet, build, serialisation, prediction, memory, at multiple scales |
| E7 real-data A8 depth | Nothing exists (REMAINING_QUESTIONS #22) | Synthetic-definition A8 at k∈{16,64,256,1024} on a real-data instance set, or a documented NOT EXECUTED |
| E1 / E6 | Nothing exists | EMR execution by the user (fast-lane package); here: PENDING USER RUN reports + the dataset gate (§1) |

## 1. E1 dataset gate (cross-check record; the fast lane's pragmatic
choice is checked against this analysis, it does not block it)

Candidates considered, with evidence:

1. **Partner's Stack Exchange prepared parquet — UNAVAILABLE.** Never
   delivered (FINAL_EVIDENCE_HANDOFF §3). The public dump route was
   already judged NO-GO in `realdata_arm/REALDATA_ARM_DESIGN.md`:
   Posts.7z 23.0 GB + Comments 7.0 + Users 0.99 + Votes 2.2 ≈ 33.2 GB
   compressed, ~120 GB extraction, multi-hour XML conversion (all-sites
   Stack Overflow dump).
2. **OpenAlex snapshot — FEASIBLE ON EMR, NOT BOUNDABLE BY A STAGING
   GATE.** Public S3 bucket `openalex` (us-east-1, CC0), Parquet format
   at `data/parquet/`; ~324M works total (Aug 2026 published count);
   files are partitioned by `updated_date`, NOT by publication year, so
   a year/type slice requires scanning essentially the whole works
   entity (works JSONL alone ≈ 615 GB). A Spark full-entity scan with a
   filter is executable on the user's EMR, but no manifest/SHA staging
   gate can bound it the way the previous packages were bounded, and
   exact slice cardinalities could not be re-verified in this audit
   (api.openalex.org returned HTTP 429). Retained as the documented
   alternate.
3. **Single-site Stack Exchange dump (Internet Archive item
   `stackexchange`, Apr 2024 vintage) — RECOMMENDED PRIMARY.**
   `mathoverflow.net.7z` is a single 510,436,127 B file, md5
   `01ae82cb94d3ab41466629b5eb3f3d78` (archive.org metadata, retrieved
   2026-11-25). Natural multi-table relational schema (Posts, Users,
   Comments, Votes, PostLinks), naturally occurring skew (prolific
   users, viral posts), a dimension table of ~10^5 users (not a 265-row
   toy), and single-file MD5-verifiable staging in the established
   package pattern. Fallback (frozen): if the frozen workload set
   screens degenerate on MathOverflow (all-PASS or all-FAIL ground
   truth), the identical site-agnostic workload definitions are run on
   one additional single site (CrossValidated,
   `stats.stackexchange.com.7z`; its size is recorded by the staging
   gate at run time).
4. **TPC-DS with injected skew — supplementary controlled benchmark
   only.** Never classified as naturally occurring. Not required for
   this campaign's verdict.

**Frozen E1 workload definitions** (site-agnostic; P = 128 shuffle
partitions; measured load = max per-task shuffle-read records of the
principal group stage, as in both prior EMR legs; verdict threshold
T = 0.02 × N, N = grouped input rows; FAIL iff measured max load > T):

- `G_POSTS_USER` — GROUP Posts BY OwnerUserId (single-table baseline).
- `G_COMMENTS_POST` — GROUP Comments BY PostId (single-table baseline).
- `J_POSTS_USERS` — Posts ⋈ Users ON OwnerUserId = Users.Id
  (SHUFFLE_MERGE hint; broadcast disabled), GROUP BY OwnerUserId.
- `F_EMERGENT` — Comments ⋈ Posts ON Comments.PostId = Posts.Id,
  GROUP BY Posts.OwnerUserId (comments received per author: alignment
  of comment skew with post ownership; the fact–fact-style emergent
  candidate).

**Frozen E1 arms:** full-data execution (ground truth); uniform
sampling of inputs r = 0.01 with Horvitz–Thompson rescaling;
foreign-key-aware sampling (consistent key-space sampling across
tables); coordinated hash-consistent join-key sampling with
heavy-hitter retention; classical end-biased sampling (the study's A9
design); heavy-hitter synopsis direct prediction (the study's A8
design). **Frozen metrics:** input/output cardinalities; predicted vs
measured max partition load; shuffle read/write bytes; task-duration
distribution (max, median, max/median); spill bytes and affected tasks;
peak execution memory where available; completion status; prediction
errors and verdict outcomes.

**Frozen E1 decision rule.** E1 supports join-emergent resource
pressure on naturally occurring data iff at least one joined workload
(J_POSTS_USERS or F_EMERGENT) is GT FAIL while its joined max load is
≥ 1.5× the larger of its input-side single-table max loads
(G_POSTS_USER, resp. G_COMMENTS_POST and G_POSTS_USER), AND at least
one reduction arm conceals it (predicts PASS, or under-predicts the
joined max load with Q-error ≥ 2). If no joined workload meets the
rule, the result is a NULL and is reported as a null. Materialisability
cap: any join whose screening output cardinality exceeds 5×10^8 rows is
dropped, with the screening number recorded (none is expected at
MathOverflow scale). Do not assume join-emergent pressure exists.

**Frozen E6 design.** Subset {J_POSTS_USERS, F_EMERGENT, G_POSTS_USER
(control)} from the E1 staging; identical inputs and settings with
Spark AQE OFF vs ON; document from the event log / plan whether
skew-join optimisation actually triggered; measure partition loads,
task durations, shuffle activity, spill, completion status. AQE is a
runtime mitigation, not a prediction substitute: the question is
whether the pre-execution predicted pressure remains operationally
visible under AQE (elevated max task duration / spill on the
predicted-hot workloads vs the control).

## 2. E2 — Symmetric information-budget frontier (headline 99; LOCAL)

Points: A8 at k ∈ {16, 64, 256, 1024}; A9 at expected retained counts
(target) ∈ {16, 64, 256, 1024}; A5, A7, A7X at their existing full
operating points. Existing per-instance predictions are reused wherever
they exist (ablation rung CSVs for A8; `results/exp_a/` for A9@1024;
`results/summary.json` / `results/eqinfo/` for A5/A7/A7X). NEW runs:
A9 at targets {16, 64, 256} on the 9 headline datasets, via the
unmodified `classical_arms.predict_A9(counts, target=t)` and the
`run_exp_a.py` load path.

**Environment validation gate (must pass before any new run counts):**
reproduce the A9 rows of `results/exp_a/predictions_exp_a_zipf_s101.csv`
exactly (pred_load equal to the recorded 0.1-rounded values) with the
campaign environment (system python3 3.12.3, numpy 1.26.4 — the
environment of the Nov 24 runs, per `code/__pycache__` cpython-312).
A9 is hash-determined and A8 is deterministic (its r=None path consumes
no RNG), so exact reproduction is required, not approximate.

**Byte accounting (frozen convention = the study's existing one):**
synopsis-family bytes = `len(json.dumps(obj))` with default separators
(the on-disk synopsis format, verified byte-identical 9/9 in the cost
frontier derivation); packet-family bytes (A7/A7X/A9) = on-disk file
sizes of the compact-separator packet JSONs. The two conventions differ
and the difference is reported, never hidden. A9 packet bytes at new
targets are measured by serialising `predict_A9`'s packet with the same
compact separators as the existing packet files. Report mean/min/max
across the 9 datasets. Account, per point, for: heavy-hitter identities
and counts, tail statistics, pair/alignment information, dimension
information, hash seed / target metadata, and any retained rows (A9
retains key+count pairs, no rows; testbed arms' retained rows are
reported separately and never conflated with side-information bytes).

**Metrics (frozen):** per point — verdict recall, precision, F1
(denominator: 99 instances; GT FAIL = 60, GT PASS = 39); FP and FN
counts; Q-error over the 99 × 11 = 1,089 instance-pipelines: median,
mean, p95, max; per-family (G, J, F, W, D) median and max Q;
worst-case underprediction = max over GT-FAIL instances of GT/pred
load; actual bytes; prediction wall time per dataset (environment
stated). Pareto frontier over (bytes ↓, median Q ↓) and (bytes ↓,
max Q ↓): a point is on a frontier iff no other point is ≤ on both
axes and < on one. Verdict fidelity and load fidelity are reported
separately; runnable testbeds (A5, A7) and direct predictors (A8, A9,
A7X) are never merged into one ranking without the distinction being
stated. No dominance claim from asymmetric operating points or pooled
medians alone.

**Coordinated-sampling determination (frozen method):** inspect
`classical_arms.build_sample` / `build_packet` / `predict_A9` for three
properties — (i) one hash function/seed shared across all tables and
columns (coordination), (ii) exact retention of values with
f_v ≥ threshold (heavy-hitter retention), (iii) hash-probabilistic
retention of the tail. If all three hold, A9 IS the coordinated
hash-consistent join-key sampling baseline with heavy-hitter retention
and no redundant baseline is built; the determination and code
citations go in the E2 report either way.

## 3. E3 — Near-threshold depth sensitivity (LOCAL)

Grid: the existing 8 near-threshold datasets (Zipf s ∈ {0.65, 0.68,
0.72, 0.74} × seeds {101, 202}; 88 instances; 62 GT FAIL, 26 GT PASS).
Ground-truth loads, verdicts, and thresholds are taken from the
existing `results/exp_a/predictions_near_*.csv` UNCHANGED; a
consistency check recomputing GT verdicts from counts must match them
exactly or the experiment stops. The grid is a follow-up benchmark
developed after the headline experiment; it is never described as an
independent external holdout.

NEW runs on the grid: A8 at k ∈ {16, 64, 256} via the unmodified
`ablation.truncate_syn` on each dataset's `synopsis.json` + the
unmodified `arms.arm_predictions("A8", ...)` (deterministic; the seed
argument is immaterial and this is stated); A9 at targets {16, 64, 256}
via unmodified `predict_A9`. Existing A8@1024 / A9@1024 / A5 / A7 / A1
rows are reused.

**Metrics (frozen):** per arm-point — recall (denominator 62),
specificity (denominator 26), full confusion matrix, FN/FP listed by
pipeline, per-error GT-load/threshold ratio, per-family median and max
Q-error, worst-case Q-error, bytes (conventions of §2).
**F_B investigation (frozen):** F_B Q-error by depth on BOTH grids
(headline per-family numbers come from E2) — on the near grid report
F_B specifically per depth; state plainly that the near grid is
Zipf-only, so the headline *mixed-family* F_B underprediction
(worst-case Q 6.41 @ k=16 / 3.17 @ k=64) cannot be re-tested there and
is instead characterised on the headline grid in E2.
**J_B hash sensitivity (frozen):** A9 at target 1024 and target 64
under the two sensitivity hashes (`classical_arms.HASH_SEEDS_SENSITIVITY`,
Amendment 1 of the Exp A preregistration) on the near grid: verdict
flips overall and on J_B specifically, reported per hash; the
sensitivity hashes are correlated by construction (h_{H+1}(v) =
h_H(v+1)) and are not independent replications — stated, as before.
**Held-out extension grid (frozen, cheap):** 4 NEW datasets from the
unmodified `gen_near_threshold.py`, s ∈ {0.70, 0.78} × seeds {404, 505},
written to `final_scientific_strengthening/data_ext/` (never into
`data/`), DuckDB-validated (`validation_ok` required, all-exact as in
the original driver). Frozen before generation: these s/seeds, the arm
set {A1, A5, A8@1024, A8@64, A9@1024, A9@64}, and the same metrics.
Reported separately as a held-out extension (same generator family —
independent instances, not an independent generating process). If
generation or validation fails in the available environments, the
extension is dropped and the reason reported; the main E3 result must
not be blocked by it.

## 4. E4 — Physical execution validation from existing EMR logs (LOCAL)

Logs: synthetic EMR event log (<APPLICATION_ID>, in
`emr_results_synthetic/round2/eventlog/`) and real-data EMR event log
(<APPLICATION_ID>, in
`emr_results_realdata/realdata/realdata_verify_out/eventlog/`), plus the
two verifier ground-truth outputs. Per pipeline: analytic predicted
load (ground-truth JSONs), executed max shuffle-read load,
principal-stage task durations (max, median, max/median), stage shuffle
read/write bytes, spill (tasks affected, bytes).

**Frozen analysis sets:** (a) PRIMARY calibration set = real-data Stage
C: 24 non-F physical comparisons (3 months × G_A, G_B, G_C, G_N, J_A,
J_B, J_N, W_A) reported separately from the 6 analytic F comparisons
(F_A, F_B × 3 months), which are never pooled with physical ones;
(b) synthetic set reported separately: 7 physical (G_A, G_B, G_N, J_A,
J_B, J_N, W_A) + 2 analytic F. Associations are computed WITHIN a
campaign only (Spearman ρ and Pearson on log10 loads), with n stated;
no pooling across campaigns, operators, or cluster configurations.
Spill events are re-derived from the logs and reported with operational
consequences (all spilled tasks succeeded). The verified fact is
retained verbatim: **no resource-induced job non-completion was
observed in either campaign**; the observed failures (missing event-log
S3 prefix; ApplicationMaster 100 s startup timeout; three
bootstrap-dead clusters) are infrastructure failures, not data-induced
failures. Deliverable figure: log–log scatter, analytic vs executed
load, physical vs analytic points distinguished, spill-affected
pipelines annotated; underlying data as CSV.

## 5. E5 — Profiling and deployment overhead (LOCAL)

Scales (datasets on disk): `outeq_200k_*` (~200K fact rows),
`outeq_1m_*` (1M), `zipf_s101` (10M), `zipf_s101_40m` (40M). One family
per scale (the first available tag, stated). Environment: the
`realdata_arm/venv` interpreter (Python 3.12.3, numpy 2.5.5, pyarrow
26.0.1, duckdb) because raw-parquet scanning requires pyarrow/duckdb;
timings only — no value from E5 enters any fidelity claim, and the
numpy-version difference vs the study runs is stated. Measurements,
3 repetitions each, median reported, each repetition in a fresh
process (page-cache state stated, not controlled): (a) profiler scan —
per-key counts for the fact table from the raw parquet via DuckDB
GROUP BY, with rows and bytes scanned (parquet file bytes); (b)
heavy-hitter extraction + synopsis construction (`arms.build_synopsis`)
from counts; (c) serialisation (JSON dump time and bytes); (d)
prediction time per arm (A8, A5, A9) per dataset; (e) testbed
construction at counts level (A5 generation path) — materialisation of
a testbed to disk is NOT part of the study machinery and is reported as
not measured, never estimated; (f) peak memory per phase
(`resource.getrusage` in-process delta); (g) end-to-end elapsed (a–d).
Report hardware (this VM: AMD EPYC 9D25, 4 visible CPUs, shared),
software versions, repetition counts. Measured overhead and estimated
deployment cost are kept in separate sections; **no refresh-efficiency
claim** (refresh is not measured).

## 6. E7 — Real-data information efficiency (LOCAL, bounded)

Instance set: NYC TLC **January 2026** (yellow + green parquet already
local in `realdata_arm/sample/`, SHA-verified in the Nov 23 staging),
the 11 real-world pipelines as defined in
`realdata_emr_package/code/realdata_model.py`, ground truth = that
model's analytic loads (consistency check: they must reproduce
`realdata_emr_package/config/analytic_gt_calibration.json` for 2026-01
exactly, as they did in the Nov 23 design validation). Estimator: the
synthetic-study A8 definition — exact heavy-hitter counts + power-law
tail reconstruction (`arms._fit_tail` / `arms._tail_expected`) — applied
per column at k ∈ {16, 64, 256, 1024}, loads computed with the
real-world model's OWN load function (xxhash64 seed 42 → splitmix64 →
% 128), byte accounting = serialised truncated synopsis under the §2
synopsis convention. The partner's differently defined RW-A8 is never
substituted. **Frozen consistency rule:** if applying the estimator
requires changing its tail model or the load function, STOP and write
NOT EXECUTED with the precise reason and the consequence for claims.
Saturated workload groups (all-FAIL or all-PASS under the model's
thresholds) are identified explicitly. A9 operating points on the same
instance set are included only if the same adapter generalises without
definition changes; otherwise their absence is stated.

## 7. Statistics (all new experiments)

The independent experimental unit is the DATASET (headline 9; near 8;
extension 4; E7: one month = one dataset, n = 1, descriptive only).
Instance- and pipeline-level figures are descriptive statistics over
correlated instances and are never presented as independent samples.
Denominators are stated for every rate. Where an interval is reported
it is a dataset-cluster bootstrap (1,000 resamples, seed 20261125); a
degenerate interval is reported as degenerate and is never read as
proof of equivalence. Equivalence language is used only descriptively
(e.g., "0 discordant instances in 99") unless a margin was frozen in
advance — the only frozen margin is E1's emergence rule (§1). Verdict
fidelity and load fidelity are always reported separately.

## 8. Pending-EMR slots

E1, E6, and A9-at-100M are executed by the user on EMR from the
fast-lane package and are PENDING USER RUN at the time this protocol
was frozen. Their reports in this campaign state the frozen protocol
(§1) and the pending status; `FINAL_SCIENTIFIC_VERDICT.md` is
structured so that only the E1 / E6 / A9-100M slots need filling once
the returned event logs and outputs are verified (verification =
the established two-gate pattern: placement ≥ 2 distinct executor
hosts from the event log, and the package verifier's gates green).
