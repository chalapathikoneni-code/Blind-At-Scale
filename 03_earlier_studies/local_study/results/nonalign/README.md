# Item 9 — Non-aligned fact–fact corpora (Amendment 5 §A5.2)

Generated: 2026-11-06T12:21:17.263719+00:00 (scorer run). All numbers below are read from `summary.json` written by the same run of `code/score_nonaligned.py`.

## Design recap (PREREGISTRATION.md Amendment 5 §A5.2, locked 2026-11-06 12:15 UTC)

Generator `code/gen_nonaligned.py` is identical to `code/gen_dataset.py` in sizes (fact 10,000,000; fact2 6,000,000; K = 20,000), materialisation, and DuckDB ground-truth cross-validation, EXCEPT that fact2's popularity pmf is permuted across key ids before drawing: pmf2[k] = pmf[pi(k)], with an independent pi per pair column (kA: rng seeded seed+31337; kB: seed+31338), and fact2 counts then drawn multinomially from the permuted pmf with the seed+777 fact2 rng stream. NA1: pi permutes a uniformly random subset of phi = 0.5 of the K key ids among themselves, identity elsewhere. NA2: pi is a uniform random permutation of all K key ids. Datasets: families {zipf, lognormal, mixed} × seeds {101, 202} × levels {NA1, NA2} = 12. Arms A1, A4, A5, A8 run via the unmodified `arms.py` / `run_dataset.py` machinery (rtag `nonaligned`; synopsis built by the unmodified profiler from the realised, misaligned, data). Scoring pooled per level (66 instances): recall / precision / F1 of FAIL prediction, Spearman rho on log10 loads, median Q-error — overall, F family only (F_A, F_B; 12/level, primary), and join-emergent (J + F; 30/level, secondary). The aligned reference is seeds-matched: predictions_main.csv restricted to seeds {101, 202} × the same 3 families (66 instances).

Validation: all 12 datasets must have every DuckDB cross-check exactly 0. Status: ALL 12 all_exact = true.

Fact-count identity sanity check (fact counts of each non-aligned dataset vs the aligned dataset of the same family/seed must be bit-identical, since only the fact2 pmf changed): ALL 12 identical (fact_kA/kB/kC/kN + null_count; distinct_A also identical).

## Realised alignment (Spearman rho, fact vs fact2 per-key counts)

| dataset | level | family | seed | rho_kA | rho_kB |
|---|---|---|---|---|---|
| nonaligned_NA1_zipf_s101 | NA1 | zipf | 101 | 0.4617 | 0.4764 |
| nonaligned_NA1_zipf_s202 | NA1 | zipf | 202 | 0.4756 | 0.4791 |
| nonaligned_NA1_lognormal_s101 | NA1 | lognormal | 101 | 0.4881 | 0.4967 |
| nonaligned_NA1_lognormal_s202 | NA1 | lognormal | 202 | 0.4918 | 0.4908 |
| nonaligned_NA1_mixed_s101 | NA1 | mixed | 101 | 0.0458 | 0.0337 |
| nonaligned_NA1_mixed_s202 | NA1 | mixed | 202 | 0.0429 | 0.0426 |
| nonaligned_NA2_zipf_s101 | NA2 | zipf | 101 | -0.0015 | -0.0009 |
| nonaligned_NA2_zipf_s202 | NA2 | zipf | 202 | -0.0090 | 0.0106 |
| nonaligned_NA2_lognormal_s101 | NA2 | lognormal | 101 | 0.0038 | 0.0136 |
| nonaligned_NA2_lognormal_s202 | NA2 | lognormal | 202 | 0.0027 | 0.0012 |
| nonaligned_NA2_mixed_s101 | NA2 | mixed | 101 | 0.0026 | 0.0038 |
| nonaligned_NA2_mixed_s202 | NA2 | mixed | 202 | 0.0026 | 0.0029 |

## Results — NA1 (pooled, 66 instances, 40 ground-truth FAIL)

| arm | scope | recall | precision | F1 | Spearman rho (log10 loads) | median Q-error | n |
|---|---|---|---|---|---|---|---|
| A1 | overall | 0.750 | 1.000 | 0.857 | 0.830 | 1.044 | 66 |
| A1 | F | 1.000 | 1.000 | 1.000 | 0.979 | 1.038 | 12 |
| A1 | J+F | 0.583 | 1.000 | 0.737 | 0.934 | 4.505 | 30 |
| A4 | overall | 1.000 | 1.000 | 1.000 | 0.998 | 1.017 | 66 |
| A4 | F | 1.000 | 1.000 | 1.000 | 0.769 | 28.444 | 12 |
| A4 | J+F | 1.000 | 1.000 | 1.000 | 0.984 | 1.018 | 30 |
| A5 | overall | 1.000 | 1.000 | 1.000 | 0.999 | 1.001 | 66 |
| A5 | F | 1.000 | 1.000 | 1.000 | 0.993 | 1.001 | 12 |
| A5 | J+F | 1.000 | 1.000 | 1.000 | 0.998 | 1.005 | 30 |
| A8 | overall | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 66 |
| A8 | F | 1.000 | 1.000 | 1.000 | 0.993 | 1.001 | 12 |
| A8 | J+F | 1.000 | 1.000 | 1.000 | 0.999 | 1.006 | 30 |

## Results — NA2 (pooled, 66 instances, 40 ground-truth FAIL)

| arm | scope | recall | precision | F1 | Spearman rho (log10 loads) | median Q-error | n |
|---|---|---|---|---|---|---|---|
| A1 | overall | 0.700 | 1.000 | 0.824 | 0.771 | 1.085 | 66 |
| A1 | F | 1.000 | 1.000 | 1.000 | 0.930 | 1.100 | 12 |
| A1 | J+F | 0.500 | 1.000 | 0.667 | 0.925 | 12.877 | 30 |
| A4 | overall | 1.000 | 1.000 | 1.000 | 0.998 | 1.016 | 66 |
| A4 | F | 1.000 | 1.000 | 1.000 | 0.818 | 1.183 | 12 |
| A4 | J+F | 1.000 | 1.000 | 1.000 | 0.987 | 1.015 | 30 |
| A5 | overall | 1.000 | 1.000 | 1.000 | 0.999 | 1.003 | 66 |
| A5 | F | 1.000 | 1.000 | 1.000 | 0.930 | 1.036 | 12 |
| A5 | J+F | 1.000 | 1.000 | 1.000 | 0.994 | 1.012 | 30 |
| A8 | overall | 1.000 | 1.000 | 1.000 | 0.999 | 1.001 | 66 |
| A8 | F | 1.000 | 1.000 | 1.000 | 0.937 | 1.045 | 12 |
| A8 | J+F | 1.000 | 1.000 | 1.000 | 0.996 | 1.015 | 30 |

## Aligned reference — seeds-matched (66 instances, 40 ground-truth FAIL)

| arm | scope | recall | precision | F1 | Spearman rho (log10 loads) | median Q-error | n |
|---|---|---|---|---|---|---|---|
| A1 | overall | 0.750 | 1.000 | 0.857 | 0.797 | 1.044 | 66 |
| A1 | F | 1.000 | 1.000 | 1.000 | 0.979 | 1.019 | 12 |
| A1 | J+F | 0.583 | 1.000 | 0.737 | 0.905 | 7.669 | 30 |
| A4 | overall | 1.000 | 1.000 | 1.000 | 0.997 | 1.019 | 66 |
| A4 | F | 1.000 | 1.000 | 1.000 | 0.858 | 48.355 | 12 |
| A4 | J+F | 1.000 | 1.000 | 1.000 | 0.988 | 1.019 | 30 |
| A5 | overall | 1.000 | 1.000 | 1.000 | 0.999 | 1.001 | 66 |
| A5 | F | 1.000 | 1.000 | 1.000 | 1.000 | 1.001 | 12 |
| A5 | J+F | 1.000 | 1.000 | 1.000 | 0.999 | 1.005 | 30 |
| A8 | overall | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 66 |
| A8 | F | 1.000 | 1.000 | 1.000 | 1.000 | 1.001 | 12 |
| A8 | J+F | 1.000 | 1.000 | 1.000 | 1.000 | 1.006 | 30 |

## F family (primary split) — per level vs aligned reference

| arm | aligned F recall | NA1 F recall | NA2 F recall | aligned F median Q | NA1 F median Q | NA2 F median Q | aligned F rho | NA1 F rho | NA2 F rho |
|---|---|---|---|---|---|---|---|---|---|---|
| A1 | 1.000 | 1.000 | 1.000 | 1.019 | 1.038 | 1.100 | 0.979 | 0.979 | 0.930 |
| A4 | 1.000 | 1.000 | 1.000 | 48.355 | 28.444 | 1.183 | 0.858 | 0.769 | 0.818 |
| A5 | 1.000 | 1.000 | 1.000 | 1.001 | 1.001 | 1.036 | 1.000 | 0.993 | 0.930 |
| A8 | 1.000 | 1.000 | 1.000 | 1.001 | 1.001 | 1.045 | 1.000 | 0.993 | 0.937 |

Full-grid aligned values from `results/summary.json` (99 instances incl. seed 303; pooled and join-emergent) are copied into `summary.json` under `aligned_reference_full_grid_from_summary_json` for context; the seeds-matched reference above is the apples-to-apples comparison.

## Pre-registered expectation — did it hold?

Pre-registered (Amendment 5 §A5.2): “A5/A8 F-family load fidelity degrades as alignment falls — the synopsis's HH pair counts remain exact (they are profiled from the realised data), so the comonotone tail model is the expected failure point; A4 and A1 are unchanged in distribution (A4 already breaks alignment by construction; A1 samples rows).” Load fidelity is judged here by the F-family median Q-error (and Spearman rho); F recall is reported alongside.

- A5: F median Q-error aligned 1.001 → NA1 1.001 (+0.02% relative) → NA2 1.036 (+3.50% relative): degraded at both NA1 and NA2 vs aligned. F Spearman rho aligned 1.000 → NA1 0.993 → NA2 0.930. F recall aligned 1.000 → NA1 1.000 → NA2 1.000.
- A8: F median Q-error aligned 1.001 → NA1 1.001 (+0.00% relative) → NA2 1.045 (+4.47% relative): degraded at both NA1 and NA2 vs aligned. F Spearman rho aligned 1.000 → NA1 0.993 → NA2 0.937. F recall aligned 1.000 → NA1 1.000 → NA2 1.000.
- A1 (expected unchanged in distribution): F median Q-error aligned 1.019 → NA1 1.038 → NA2 1.100; F recall aligned 1.000 → NA1 1.000 → NA2 1.000.
- A4 (expected unchanged in distribution): F median Q-error aligned 48.355 → NA1 28.444 → NA2 1.183; F recall aligned 1.000 → NA1 1.000 → NA2 1.000.
- Note on A4: its prediction procedure is unchanged, but its F-family error is measured against a ground truth whose alignment falls — as alignment falls the truth moves toward A4's own broken-alignment model, so A4's F median Q-error IMPROVES sharply (aligned 48.355 → NA1 28.444 → NA2 1.183). This is a change in the target, not in A4.
- Overall: the expectation HELD IN DIRECTION but with small magnitude. A5/A8 F median Q-error is worse than aligned at both levels for both arms, but the NA1 degradation is at most 0.02% relative (negligible) and the NA2 degradation at most 4.5% relative; the clearer fidelity loss is in F Spearman rho (see per-arm lines). F verdicts are unaffected: all 12 F instances per level are ground-truth FAIL and every arm predicts FAIL on all of them (F recall 1.000 at both levels and in the aligned reference), so the comonotone tail model did NOT become a material failure point for verdicts, nor for median load fidelity at these scales — the degradation is real but confined mainly to rank fidelity at full misalignment.

## Deviations

- [2026-11-06T12:21:17.263719+00:00] None. Generation, validation, arm runs, and scoring followed Amendment 5 §A5.2 as written.
