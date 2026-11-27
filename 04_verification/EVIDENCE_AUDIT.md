# EVIDENCE AUDIT — Blind at Scale (IEEE Access)

Deep audit of the entire study, 2026-11-25, in preparation for the
one final revision that will combine Kiro's self-join measurement
return with these findings. Method: every load-bearing number was
read from, or recomputed from, raw artifacts (JSON / CSV / event-log
parses / notebook cell outputs). Reports and summaries — including
our own — were treated as pointers, never as evidence. Two strands
(local + cloud; partner study) were audited by delegated agents
working from raw artifacts only; their findings are integrated and
marked where they are recomputations.

**Headline: no headline number in the final manuscript failed
verification.** Every quantitative claim checked was found in, or
recomputed exactly from, a raw artifact. The gaps that remain are
archival and scope gaps — missing as-run code, missing raw records,
one unmeasured flagship case — and in every case the manuscript's
text is already scoped to what the artifacts support. One minor
numeric imprecision was found in the manuscript (G07, E5 peak
memory). Details: `GAP_REGISTER.csv`; recomputations banked in
`RECOVERED_RESULTS.md/.csv`; the revision plan is
`FINAL_REVISION_CHECKLIST.md`.

---

## 1. Claim-by-claim status (C1–C30, as the claims now stand in manuscript_final)

Of the 30 claims in the updated matrix: **25 are fully verified at
their stated scope** (including C3 verified-as-negated, C12 verified
as a weak/negative result, C20/C21/C22 verified as limitations, C24
discharged). **2 remain PARTIAL**: C25 (reproduction — November
as-run code never retrieved) and C28 (E1 decision rule — no measured
self-join). **3 are non-evidence by design**: C13 (agent framing =
motivation only, fenced in text), C16 (privacy — disclaimed, never
claimed), C18 (cited literature).

### The handoff's 7 "no adequate evidence" items — where they stand now

| # | Item (2026-11-25 handoff) | Status at audit |
|---|---|---|
| 1 | LLM/agent behaviour claims (C13) | **Still open, by design.** No LLM was run; the manuscript fences the surrogate framing (abstract: "No language model was run"). Not fixable by evidence; must never be claimed |
| 2 | Kilobyte-depth (k=16/64) near threshold / on real data (C20, C22) | **RESOLVED by E3/E7** — as limiting evidence: A8 recall 0.871@k16 → 0.968@k1024 near threshold (specificity 1.000); NYC group saturated, Q-error discriminates (3.839@k16 → 1.000@k≥256). All values recomputed from `e3_summary.json` / `e7_summary.json` in this audit |
| 3 | Snowflake beyond "weak at 10M" (C12) | **Still open; claim correctly capped.** Raw check: 0 spill in all 36 pipeline records; Spearman unstable 0.243–0.485 across 4 measurements; A1 beats A5 on Spearman in all 4. No stronger claim is made |
| 4 | A9 at 100M (C5) | **RESOLVED** — ran (EMR Step 5) and verified: 9/9 vs Nov 23 executed GT, packet 78,447 B, median Q 1.139873 (executed basis), from `a9_100m_scoring.json`. G_C unscored — disclosed in the manuscript in 3 places |
| 5 | Privacy safety (C16) | **Still disclaimed, never claimed.** Correct as-is |
| 6 | Partner "18 Py4JJavaError" | **Confirmed OUT.** 0 occurrences in `manuscript_final/` (recursive grep, this audit + delegated re-check). The string exists only in the partner's own two manuscripts (1× each); his notebooks contain 0 — the claim is unsourced in his own artifacts and stays out |
| 7 | Partner instance-level claims beyond notebook tables (C14) | **Still open.** Prediction CSVs (`real_world_analysis_v2/`) confirmed never delivered (filesystem search, both trees). BUT: the delegated audit re-verified **13/13** quantitative claims in §VI-K against the notebook's displayed tables — all traceable, the tie presented as a tie (pooled recomputation banked as RECOVERED R5/R6) |

## 2. Strand-by-strand inventory

**Nov 6 local study (99 instances).** Artifacts complete locally:
`results/` (summary, per-arm CSVs, feedback, negfam, outeq, eqinfo,
exp_a, ablation), `data/` (51 dataset dirs with prediction CSVs),
`code/`. Verified from raw: A5/A8 60/60 FAIL-recall (99/99 total),
median Q 1.0015/1.00003; A1 join recall 0.611; FB-SYN 51/51, margin
0.274510; outeq 48/48 EQUAL; negfam recall 0.000 (0/8); A7 ≤0.3368×
bytes with verdict match (note: "matches" is verdict-level — A7
median Q is 1.015 vs A5's 1.001); Exp A A9 1.00155 vs A5 1.00147,
bytes 0.639–0.648×, 40M 8/8 Q 1.00003, F margin 1.00049 vs 1.00672
(sub-rule honestly against A9). The two abstract figures 24.7× and
48.4× exist **only** as recomputations over the distributed
prediction CSVs — recomputed in this audit (24.7034 / 48.3554) and
banked (RECOVERED R1/R2).

**Single-host pod Spark GT.** 9/9 agreement confirmed in
`cluster_comparison.json`; topology honestly labelled "multi-worker
standalone cluster on a single host" in all three record files.
**Reproducibility break (G01):** the canonical GT file, event log
and run logs are not in this workspace — the local GT is explicitly
a reconstruction from the operator's verbatim report; the raw
record is stated to live on the research-ops volume
`/workspace/blind-pod/` (~186 MB). F_A/F_B in this leg are analytic
from Spark counts, not executed joins (disclosed in the artifact).

**Databricks leg.** A5 median Q ladder recomputed from the three
comparison JSONs: 1.0813 / 1.0814 / 1.0850 at 10M/40M/100M, 9/9 at
each rung — matches. Topology gate FAILED as recorded (exactly 1
executor, hostnames masked) — no multi-node evidence, none claimed.
D-FB 7/7 (record artifact: `feedback_manifest.json`, attempt 2;
the sibling `feedback_task_output.json` is the FAILED attempt-1 —
G15). D-NEG 0/4 both arms. Spend: <$8.82 worst-case **bound**
(billing table returned 0 rows; no measured-DBU artifact exists —
G18, internal only). Serverless event logs never landed; GT rests
on the D8 partition-id measurement, documented in the manifest
(G20).

**Snowflake leg.** Fully verified as a weak/negative result (see
§1 item 3). AUP advance-sharing **discharged** 2026-11-25 (Gmail
message id 1a213ce63aa66279) — REMAINING_QUESTIONS item 13 closed.

**Synthetic EMR 100M.** Verification report stands on its own
event-log parse: 18 distinct executor hosts, 5,036 tasks, 0 failed,
counts SHA-256 3549c275…dd8f = Databricks anchor, spill
748,309,212 B (one J_N task, succeeded). The multi-node claim
(C11) rests on this run and the real-data run — both verified.

**Real-data EMR (NYC TLC).** Own event-log parse: 18 distinct
hosts, 25,377 tasks, 0 failed; corpus exact (103,139,628 yellow +
1,295,576 green rows, 1,791,549,940 B, 55 files); Stage C median
executed/analytic 0.9961 (non-F 0.9912); Stage S saturation — all
40 scored executed verdicts FAIL — with the caveat carried in the
manuscript in at least 3 places including the appendix table
caption. Spill 4,410,001,438 B across 4 tasks, all succeeded.

**Experiment A + near grid + frontier (pre-campaign).** All Exp A
numbers verified from `summary_exp_a.json` (see local study above).
Near grid: A8 ran at k=1024 only pre-campaign (misses 2/62) — the
gap E3 later filled. Frontier bytes for A1/A2/A3 never measured —
manuscript marks them NOT MEASURED with a star (G09).

**Partner study (§VI-K).** Delegated raw audit: the two notebooks
are byte-identical (SHA-256 b6bf99ed…). 13/13 manuscript claims
traceable to displayed tables at stated rounding; pooled tie
recomputed (both arms TP39/FP1/FN0); A1 wins median Q in 2 of 3
groups; his arm definitions differ from the main study's for every
shared label, and the manuscript discloses this in both places;
his k-sweep is his A8 power-law arm on G_A (Q 1.1568→1.0000), no
per-arm or sweep byte accounting. One correction to our own
records (G17): synopsis-only `serialized_bytes` at k=64 **does**
exist (cell 35, output 4) — the handoff matrix's "no byte
accounting" is right only in the per-arm/sweep sense; the
manuscript makes no partner byte claim, so nothing in the paper
changes. The study carries ~4% of the results text and is
scope-limiting, not load-bearing.

**Campaign local E2/E3/E4/E5/E7.** All recomputed or read from raw
result files in this audit:
- E2 (`metrics_e2.csv`): corrected values confirmed — A9_t16
  precision 0.6061 / FP 39; A9_t64/t256 precision 0.8696 / FP 9;
  A9 worst-case Q ≥ 16.72 at every depth; A8_k16 max Q 6.4097;
  Pareto A9_t16 → A8_k16 → A8_k64 → A8_k1024. The erratum'd prose
  values appear nowhere in the final manuscript.
- E3 (`e3_summary.json`): A8 recall ladder 0.871/0.903/0.903/0.968,
  specificity 1.000 throughout; consistency gate 0 GT mismatches.
  Extension (`e3_extension_summary.json`): A1 recall 0.647, GT
  34 FAIL/10 PASS — confirms C30.
- E4 (`e4_figure_data.csv`): independently recomputed — Stage C
  n=24, median 0.9912, range 0.8574–1.0353, Spearman 0.9705,
  Pearson-log 0.9994 (RECOVERED R3). Synthetic ρ recomputed
  0.9535 vs reported 0.953 (G16, cosmetic truncation).
- E5 (`e5_raw.json`): scan medians and the 84–98% share confirmed
  under the report's stated end-to-end definition (RECOVERED R7).
  **One imprecision found (G07/R8): "peak scan memory 715.5 MB"
  is the median rep; observed peak is 725.1 MB.**
- E7 (`e7_summary.json`): saturation confirmed (10/10 FAIL under
  both thresholds), agreement 10/10 at every k, max Q 3.8386@k16
  → 1.0107@k64 → 1.000@k≥256 — confirms the manuscript's figures.

**Campaign EMR, first capture.** Verified at the time from raw
JSONs + the 5 captured event logs (18 hosts, 0 failed, 0 spill on
completed apps): A9@100M (above); join_dim at 4 parts (max 51,961;
synopsis Q 1.0217; end-biased 1.0519; uniform FP Q 2.3094); E6
join_dim + synthetic pairs bit-identical off/on. Capture gaps
(disclosed): failed apps' event logs absent (G04); November
retrieval absent (G02).

**Recovery + final return, second capture.** Verified from raw:
8 recovery apps (18 hosts, 0 failed, 0 spill); join_meta measured
(max 43,348; synopsis Q 1.0037; end-biased 1.1059; uniform FP #3
Q 2.5376); join_source_dim measured FAIL in a second natural
family (max 171,026; synopsis Q 1.0117; end-biased 1.0063);
join_dim_s8 event-log verified (stage-18 shuffle records
10,218,360 = JSON output rows); self-join SKIPPED_GUARD at both
scales with exact predictions; Step 6 failed at the s8 self-join
(4th observed failure); s12 never ran; patched scripts and
November retrieval absent again (G02/G03). C29 filled for all
executable scope; **C28 PARTIAL — final** unless Kiro's in-flight
measurement runs land.

## 3. Prose-vs-raw discrepancies

In the **manuscript**: exactly one — G07 (E5 peak memory, median
reported as peak, 1.3% understatement). No other manuscript number
disagreed with its raw artifact anywhere in this audit.

In **internal records** (G15, none reaches the manuscript):
NUMBER_PROVENANCE "6.85" for A3's 100M median Q (exact 6.847;
manuscript correct); cloud EXECUTION_REPORT body superseded by
its own addendum on Snowflake; d-fb task-output file is the
failed attempt-1; COST_FRONTIER_REPORT §5 still labels
Experiment A "PENDING"; EXP_A_REPORT's "0.64 on every dataset"
is a mean (range 0.639–0.648). The E2 report's original prose
error (precision/FP) was already erratum'd and the corrected
values are what the manuscript carries — re-confirmed here.

## 4. Tier mismatches

None found in the manuscript. Every claim sits at or below the
tier its artifacts support: bases are labelled and never mixed
(local ~1.001 / Databricks 1.085 / executed-EMR 1.139873);
join_dim_s8's tier flag was removed only after its event log
verified; the self-join is stated as predictions + observed
failures with "no completed measurement exists"; failure
classification for the crashed apps rests on step logs and is
not dressed as event-log evidence; the partner section discloses
its displayed-tables-only basis in its own text.

## 5. Reproducibility completeness

Reproducible from the archive: all local legs (code + data +
results + staged package with MANIFEST.sha256; generated binaries
regenerable from the packaged generator, inventoried by SHA —
G19); Snowflake legs; campaign local E2–E7 (code + raw results in
`final_scientific_strengthening/`); campaign EMR legs (captured
step commands + output JSONs + most event logs).
Chain breaks: pod GT raw record outside the workspace (G01);
November EMR as-run code (G02); campaign patched code (G03);
failed-app event logs (G04); partner CSVs (G06); Databricks
serverless event logs + measured billing (G18/G20). The
manuscript's reproduction sentence is already qualified for the
EMR exception (C25 PARTIAL).

## 6. REMAINING_QUESTIONS reconciliation (36 items)

- **Closed (13):** 13 (Snowflake AUP — discharged), 15 (NYC TLC
  practice documented), 21 (E3 ran the depths), 22 (E7 ran
  synthetic A8 on real data), 23 (E2 swept A9), 24 (A9@100M ran),
  29 (Table `near` caption fixed — "discriminating benchmark"
  absent from the final text), 31 (= item 2, title — RESOLVED in
  v2/final: "Information Sufficiency…" title is in the built PDF),
  34 (saturation caveat adjacency verified, ≥3 places), plus 2
  (=31), and partially 26 (E5 measured local profiling 200K–40M;
  refresh/deployment remain not-measured by design).
- **Superseded (3):** 19 (direction decision — the bounded merge
  shipped in the final manuscript), 25 (Snowflake 100M — author
  closed Snowflake work; claim capped), 36 (Snowflake cited only
  as "weak", so item 13 was discharged anyway).
- **Still open (20):** 1 (B1 authorship — BLOCKER, author), 3
  (M11 disclosure wording — placeholder in place), 4 (m18
  footnote — placeholder still in main.tex:38), 5 (m16 length —
  accepted de facto: 45 pp total), 6–10 (M9 as-run files, G02),
  11 (counts.npz — partially mitigated: counts data exists locally
  from the Nov return incl. fact2_counts.npz; the formal S3
  retrieval item remains open), 12 (app-0002 stdout, non-blocking),
  14 (Databricks AUP clause re-verification — calendared, minor),
  16 (partner CSVs, G06), 17 (Py4J — stays out; verified out),
  18 (partner re-attempt — backup only, nothing depends on it),
  20 (partner increments not merged — editorial, non-blocking),
  27 (A1/A2/A3 bytes, G09), 28 (equivalence at 10M, G10),
  30 (record hygiene, G15 — the "6.85" instance confirmed still
  present), 32/33 (depend on items 1/3), 35 (appendix-evidence
  check — R6 confirmed none; final's appendix campaign tables
  support main-text claims that also appear in the main text).

## 7. Bottom line for the final revision

The evidence base is submission-grade except for the items the
manuscript already fences. The final revision needs: Kiro's
self-join outcome (checklist branch a or b), the 725.1 MB fix,
the abstract cut, and the three author-supplied items
(authorship, disclosure wording, footnote). Nothing else in the
text needs to move, and the must-not-change list in the checklist
protects the fences that make the paper honest.
