# FINAL SCIENTIFIC VERDICT — Blind at Scale (IEEE Access)

Date: 2026-11-25. Scope: the Final Scientific Strengthening campaign's
**completed** experiments (E2, E3 + extension, E4, E5, E7), on top of the
verified evidence base in `final_evidence_handoff/`. This verdict covers
experimental evidence only. The manuscript is ChatGPT's and the author's
responsibility; authorship (B1), title (M1), and AI-use disclosure (M11)
are author decisions outside this verdict.

## Classification for the completed scope: **CONDITIONAL GO**

The completed campaign strengthened the evidence base and — equally
important — replaced the two most exploitable evidence gaps with
measured, limiting results. Nothing in the completed scope produced a
finding that negates the paper's narrowed central claim (heavy-hitter
information at kilobyte scale suffices for verdict-faithful prediction
on the headline grid, with a measured budget–fidelity frontier). Several
findings **narrow** what may be claimed; the conditions below are those
narrowings. A GO without conditions would not be honest: the EMR
slots have since been filled from the author's verified returns
(2026-11-25, below) — A9-at-100M and E6 verified without
contradiction, E1 partially — but E1's formal decision rule, the
recovery batch, and the C25 retrieval remain open, and the verdict
says so wherever it matters.

## What the completed scope established

- **E2 (P0) — frontier, fair budgets.** A9 determined to *be* the
  coordinated hash-consistent sampler with heavy-hitter retention (no
  redundant baseline). Swept frontier (11 points, actual bytes): A9
  never beats A8 at comparable bytes; A8_k16 dominates A9_t64+.
  Verdicts tie 99/99 for eight points; A9's low depths fail on
  verdicts as well (t16 degenerate, FP = 39; t64/t256 FP = 9 each) —
  corrected during coordinator verification against the raw artifacts
  (E2 report carries a dated erratum).
- **E3 (P0) — near-threshold depth.** A8 recall 0.871/0.903/0.903 at
  k=16/64/256 (specificity 1.000); k=1024 0.968. A9_t16 degenerate
  (specificity 0.000); A9@1024 not hash-robust (7/88 flips, hash1).
  F_B tail is mixed-family-specific. Extension set (separately frozen,
  44 instances) reproduces the pattern, incl. A1 join blindness (0.647).
- **E4 (P1) — physical validation.** Analytic↔executed correspondence
  re-derived from raw logs: Spearman 0.971, Pearson-log 0.9994 (n = 24
  non-F physical); 31/31 match to prior verifier; spill byte-exact; no
  resource-induced non-completion.
- **E5 (P1) — profiling overhead.** Scan-dominated (84–98% of
  end-to-end); construction+prediction < 0.6 s combined at all scales to
  40M; counts validated exactly. Refresh NOT MEASURED.
- **E7 (P2) — real-data budget.** Synthetic-definition A8 on NYC Jan
  2026 at 4 depths; group saturated (agreement ≡ recall); Q-error
  discriminates (max 3.84 @k16 → 1.000 @k≥256, near-transcription).

## Conditions (claim-level; for the manuscript pass)

1. Kilobyte-faithfulness (C19) stays scoped to the headline 99; the
   near-threshold depth results (C20) and their limits are reported,
   not omitted.
2. A9 is presented via the swept frontier (C23), including its
   worst-case Q and E3 hash sensitivity — not via the single
   Experiment A operating point.
3. NYC verdict agreement is never presented as discrimination (C15/C22
   saturation); Q-error is the real-data metric.
4. Profiling cost is stated as E5 measured it (scan-dominated; refresh
   unmeasured); the isolated "0.009 s build" figure does not appear
   without that context.
5. Snowflake stays "weak at 10M" (C12); privacy stays disclaimed (C16);
   the partner's Py4JJavaError assertion stays out; no refresh,
   universal-superiority, or theoretical-minimum claims anywhere.
6. The five limitation/framing sentences specified in
   `FINAL_HOSTILE_REVIEW.md` (Risks 1–5) appear in the manuscript in
   substance — they are the priced cost of the risks the campaign did
   not close (Risk 3 partially evidenced after the E1 return —
   natural-data emergent pressure observed in execution, formal rule
   closure pending the recovery batch; Risks 1, 2 partially;
   Risk 4 presentational; Risk 5 positional).

## PENDING slots — FILLED 2026-11-25 from the verified EMR capture

Fill rules as frozen in advance (unchanged, governing); outcomes
verified against raw artifacts in `EMR_FINAL_CAPTURE_VERIFICATION.md`:

| Slot | Source | Fill rule (frozen in advance) | **Outcome (verified)** |
|---|---|---|---|
| **E1 — natural join emergence (C28)** | Fast-lane package Steps 2 + 6 (OpenAlex; EM_Results) | Verify event-log placement (≥ 2 hosts) + verifier gates before reading outcomes. Supported iff ≥ 1 joined workload GT FAIL with joined max load ≥ 1.5× the larger input-side load AND ≥ 1 reduction arm conceals it; otherwise the NULL is recorded and C28 closes as NO EVIDENCE → real-data claims stay at C14/C15/C22 level. My MathOverflow gate analysis stands as the cross-check record | **PARTIALLY FILLED — rule NOT formally closed.** Placement gates passed for every captured application (18 hosts). The natural self-join produced an **observed execution failure in 3 applications** (resource/materialisation), with predicted FAIL/FAIL on proven bounds and the ratio component exceeded ≥ 2,352× (requirement 1.5×). But the rule's letter requires a *measured* joined max load (the failure prevented it at this scale) and a concealment observation (the arms' selfjoin predictions were never persisted) — both await the recovery batch (1-part measured selfjoin + guard record). C28 is therefore **not** "supported" and **not** NULL: natural-data emergent pressure is now observed in execution, and real-data claims may say exactly that, no more. Step 6 remainder (s8/s12, works×sources) also pending |
| **E6 — AQE robustness (C29)** | Steps 3/4 + 7 | Record whether skew-join optimisation actually triggered. If AQE-on erases the executed failures, the necessity claim shrinks per the frozen interpretation in the E6 report; if failures persist under AQE, the prediction claim strengthens. Either outcome is reportable; neither is assumed | **FILLED for the completed scope — the strengthening branch.** Skew-join optimisation never triggered (plan evidence); AQE-on is execution-identical to AQE-off on every completed case (join_dim max 51,961 both; J_A/J_N maxima 10,873,845 / 10,442,419 both, FAILs persist, shuffle totals identical to the byte); the natural selfjoin failed under BOTH conditions — AQE did not erase it. The prediction claim strengthens per the frozen interpretation. Open sub-slot: the 1-part selfjoin AQE pair (recovery R3) |
| **A9 at 100M (C5)** | Step 5 | A9 port already proven exact vs `classical_arms.py` (20k keys). If A9 matches A5 at 100M as at 10M/40M, C5 closes VERIFIED at all scales; if not, C5 stays scoped to ≤ 40M and the abstract wording must reflect that | **FILLED — C5 closes VERIFIED at all scales.** Verdicts 9/9 vs the Nov 23 executed GT (recall 7/7, precision 7/7, specificity 2/2); packet 78,447 B; median Q 1.140 on the executed-load basis (G exact vs its max-group statistic; definitional note in the provenance record). G_C unscored at 100M (no executed GT) — stated, not smoothed |
| **Reproducibility retrieval (C25)** | Author S3 pull per `REPRODUCIBILITY_CLOSURE_REPORT.md` | Closes the archival half of M9 only when the as-run files are in hand and hash-checked; until then the manuscript's reproduction qualification stays | **STILL OPEN.** The Nov 25 capture did not include the retrieval items (checked file-by-file: no November as-run scripts, no counts.npz). The qualification stays |

**Finalisation rule:** when the three EMR slots are verified, this
verdict is re-issued with the slots filled and the classification
revisited *only* on their evidence: E1 NULL or an AQE-erasure result
would move the classification's conditions (narrower claims), not
retroactively invalidate the completed scope; an E1 positive with
concealment would remove the largest remaining hostile-review exposure
(Q3).

**Revisit on the 2026-11-25 evidence (per the rule above):** no
AQE-erasure occurred (the opposite branch fired); E1 is neither NULL
nor formally positive, so its condition stays, narrowed as recorded;
A9 verified at all scales, removing that scope limit. The
classification therefore remains **CONDITIONAL GO**, with conditions
1–6 unchanged except Condition 6's Risk-3 wording (updated above).
Full finalisation still awaits the recovery batch (E1 formal rule +
E6 1-part pair), the Step 6 remainder, and the C25 retrieval.

## Confidence (reported as required: conditional and unconditional)

- Conditional on the pending slots resolving *without* contradiction
  (E1 either way counts as resolution; contradiction = e.g. A9 failing
  at 100M in a way that breaks the ≤40M pattern, or AQE results that
  undermine the prediction framing beyond the frozen shrink): the
  evidence base supports submission — my acceptance estimate for the
  paper as a whole remains in the previously stated band (60–70%
  conditional on the author decisions and manuscript pass being done
  well), unchanged by this campaign in either direction: the campaign
  removed reviewer-findable gaps but also removed two comfortable
  ambiguities. **Update 2026-11-25:** the A9 and E6 slots have now
  resolved without contradiction (A9 verified at 100M; AQE
  strengthened rather than shrank the prediction claim), and E1
  partially resolved in the favourable direction without formally
  closing. The band is unchanged: what keeps it from rising is the
  still-open E1 formal rule, the open C25 archival half, and the
  author decisions — not the filled slots.
- Unconditional (all author decisions, manuscript integration, and
  pending slots unresolved): 45–55%, as previously stated. The campaign
  does not justify raising it before the slots fill.

## Not a verdict on

Authorship, title, disclosure, venue choice, or submission timing —
the author's decisions. This document judges evidence only.

---

## FINAL RETURN UPDATE — 2026-11-26 (COMPLETE capture, verified)

Full record: `VERIFICATION_REPORT_FINAL_RETURN.md` in this folder.
The recovery batch and Step 6 remainder have now been verified from
raw artifacts. Outcomes under the frozen fill rules:

- **E1 / C28: the formal rule STILL DOES NOT CLOSE — PARTIAL, final.**
  The guard-record half completed exactly (self-join Σcount² =
  795,242,814 at 4 parts, 262,464,138 at 1 part; predicted max loads
  122,469,088 / 43,344,257; all arms FAIL/FAIL), but the 1-part
  self-join was SKIPPED_GUARD rather than measured — its predicted
  max load exceeds the 25M cap even at 1 part — so the rule's
  measured component and concealment component remain unevaluated.
  In exchange, the return added *measured* natural-data evidence the
  rule did not anticipate needing: join_meta PASS control (uniform
  false positive #3) and a **measured join-emergent FAIL in a second
  natural family (join_source_dim), called correctly by both
  heavy-hitter arms at Q ≤ 1.012**. join_dim_s8 is now event-log
  verified. Step 6 failed at the s8 self-join (fourth observed
  failure); s12 never ran and is recorded as not run.
- **E6 / C29: FILLED for all executable scope** (join_meta pair
  identical under AQE off/on; self-join pair guard-skipped under both
  conditions — nothing executable remains).
- **C25: STILL OPEN.** The COMPLETE capture contains neither the
  November as-run retrieval nor Kiro's patched scripts; the archival
  half of M9 and the recovery code diff both remain undone, and the
  manuscript's reproduction qualification stays.

**Classification remains CONDITIONAL GO; confidence unchanged
(conditional 60–70% / unconditional 45–55%).** No pending evidence
slot remains that the author has run: what is open (C28's measured
self-join, C25's retrieval) is open because the runs/retrieval did
not produce it, and the record now says so in final form.

---

## SELF-JOIN MEASUREMENT UPDATE — 2026-11-26 (verified)

Full record: `VERIFICATION_REPORT_SELFJOIN.md` (in
`emr_results_selfjoin_measured/`). The author ordered both
self-join scales measured with the guard overridden and the
patched streaming counter. **Both jobs completed** (event-log
verified: 18 hosts, 0 failed tasks, 0 spill):

- **C28: CLOSED — VERIFIED.** Measured output totals equal the
  predicted Σcount² exactly at 1 part (262,464,138) and 4 parts
  (795,242,814); measured max partition loads 43,340,706
  (Q 1.000082) and 123,158,197 (Q 0.994405) — FAIL/FAIL as
  measured at both scales; no arm conceals the load (uniform
  understates the 4-part max, Q 0.727114, but verdicts FAIL).
  The decision rule's measured and concealment components are
  both evaluated; nothing in C28 remains partial.
- **G03: CLOSED.** The patched scripts arrived with this capture;
  the diff confirms streaming counting, guard extension +
  recording, case selection, and the force override only —
  prediction math unchanged.
- **C25: STILL OPEN.** The November as-run code remains
  undelivered; the manuscript's reproduction qualification stays.

**Classification remains CONDITIONAL GO; confidence unchanged
(conditional 60–70% / unconditional 45–55%).** The condition no
longer rests on any evidence slot: every campaign claim slot is
now filled at its stated tier. What still gates submission is
author-side (authorship B1, disclosure wording M11, footnote
m18, the abstract length) plus C25's archival qualification,
which the text already carries.
