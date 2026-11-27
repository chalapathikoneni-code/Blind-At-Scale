# REPRODUCIBILITY CLOSURE REPORT — Blind at Scale Final Campaign

State as of 2026-11-25. Muse has **no AWS access**; every item in §1
can be retrieved only by the author or his laptop assistant from his S3
bucket `YOUR_BUCKET`. Until they land, the manuscript's reproduction
sentence must keep its current qualification (the packaged v2.1 setup
reaches the same verified pinned end state by a different route than
the bootstrap that actually ran), and **no text may claim byte-identical
reproducibility of the EMR legs**.

## 1. Exact retrieval checklist (from REMAINING_QUESTIONS items 6–12)

Pull into the reproduction package (and, for the new campaign, into the
fast-lane return folder):

**Synthetic EMR campaign (Nov 23):**
- [ ] `s3://YOUR_BUCKET/Access/synthetic/code/bootstrap_action.sh` — the
  hand-fix bootstrap that actually ran (venv `/usr/local/bas-venv`;
  numpy 1.23.5 + pyarrow 14.0.2 from PyPI; v2.1 markers 0/20 nodes).
- [ ] `s3://YOUR_BUCKET/Access/synthetic/code/spark_job.py` — for
  completeness; no evidence it differs from v2.1 (the line-410 collect
  matches), byte-identity unproven.
- [ ] `s3://YOUR_BUCKET/Access/synthetic/data/counts.npz` (+
  `counts_summary.json`, `meta.json`) — never delivered; blocks direct
  recomputation of the counts SHA from the EMR run's own count vectors
  (verifier sub-gate SKIP). Dataset identity is settled by other
  evidence; this is a completeness gap, not an evidential one.
- [ ] Synthetic app-0002 driver stdout — not delivered (app-0001's
  stdout covers the same cluster's Phase 0). Non-blocking.

**Real-data EMR campaign (Nov 23–24):**
- [ ] `s3://YOUR_BUCKET/Access/realdata/code/bootstrap_action.sh` — the
  hand-fix bootstrap that actually ran (numpy 1.26.4 + pyarrow 18.0.0 +
  xxhash 4.1.0 from PyPI; v2.1 markers 0/20 nodes).
- [ ] `s3://YOUR_BUCKET/Access/realdata/code/spark_job_realdata.py` —
  provably NOT the packaged v2.1 file (event-log collect at line 266
  vs v2.1's line 300; no `staging_mode` recorded).
- [ ] `s3://YOUR_BUCKET/Access/realdata/code/realdata_model.py` and
  `s3://YOUR_BUCKET/Access/realdata/code/stage_data.py` — submitted with
  the job; cannot be assumed identical to v2.1.

**New fast-lane campaign (Nov 25–26):** the as-run bootstrap, job
code, fleet/cluster configuration, and step definitions for the
E1 / E6 / A9-at-100M runs, plus the E1 staging-gate record (file hashes
vs source metadata, per-table row counts, join screening
cardinalities). The lesson of the first two campaigns is frozen into
this checklist: **the code that ran is archived at run time, not
reconstructed afterwards.**

## 2. What happens after retrieval (Muse, on receipt)

1. SHA-256 every retrieved file; record hashes in the updated
   provenance.
2. Diff each file against its packaged v2.1 counterpart; document
   every difference (expected: bootstrap route; real-data job collect
   line and `staging_mode`; others unknown until seen).
3. Archive the actual executed files in the reproduction package as
   the as-run set (packaged versions retained alongside, labelled).
4. Re-run the applicable verifier gates (synthetic counts-SHA
   recomputation from counts.npz — the one SKIP gate — closes here).
5. Only then may M9 be marked fully RESOLVED.

## 3. Current status

- M9 evidential half: **CLOSED** (both runs re-derived from raw event
  logs in the evidence handoff: 18 distinct executor hosts each, 0
  failed tasks, gates exact).
- M9 archival half: **OPEN — PENDING AUTHOR RETRIEVAL** (this
  checklist). Nothing in the final campaign's local experiments depends
  on it. **Status check 2026-11-25 (evening): the author's final EMR
  capture (`bas_final_capture_20261125-192309Z.zip`, verified in
  `EMR_FINAL_CAPTURE_VERIFICATION.md`) was inventoried file-by-file
  and does NOT contain the §1 retrieval items — no November as-run
  scripts, no counts.npz. The archival half therefore remains OPEN,
  unchanged.** What the capture does close on the fast-lane side: the
  campaign package's own as-run step commands and confs are preserved
  in the captured controller logs, so the Nov 25 legs' execution
  configuration is archived even though the November legs' is not.
- Local reproducibility (this campaign): every new experiment ships
  its driver code (`final_scientific_strengthening/code/`), raw
  per-instance outputs (`.../results/`), and a SHA-256 manifest
  (`MANIFEST.sha256` in the campaign folder), so the local legs are
  reproducible without any retrieval step.
