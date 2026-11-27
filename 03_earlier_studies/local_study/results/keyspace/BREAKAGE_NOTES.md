# Key-space scaling — breakage notes (WORKSTREAM 3, 2026-11-06)

All numbers from executed outputs: `data/keyspace_*/predictions_keyspace.csv`,
scored in `keyspace_summary.json` / `keyspace_table.md` (this folder).
Ground truth for every dataset cross-validated against DuckDB SQL on the
materialised parquet: `validation.json` all_exact = true for all 4 datasets
(all 10 checks at max-abs-diff 0, including the K = 1,000,000 run).

## What was run

| config | K | k/K | fact / fact2 rows | datasets | synopsis bytes |
|---|---|---|---|---|---|
| headline (reference, main program) | 20,000 | 5.12% | 10M / 6M | 9 datasets, 99 inst. | 91,379 |
| K200 | 200,000 | 0.512% | 10M / 6M | zipf s101, mixed s101, zipf s202 (33 inst.) | 88,049 / 89,349 / 88,080 |
| K1M | 1,000,000 | 0.1024% | 5M / 3M (reduced) | zipf s101 (11 inst.) | 85,042 |

Synopsis size does NOT grow with K (k = 1024 fixed; it slightly shrinks
because heavy-hitter counts get smaller as mass spreads). Tail mass outside
the top-1024 grows: kA tail = 41.2% of rows at K200-zipf, 47.8% at K1M-zipf.

Threshold views: at K200 the fixed (200,000) and normalised (0.02·N_fact)
views are literally identical (N_fact = 10M). At K1M they also produce
identical verdicts for every arm — verified from the loads, not assumed:
the largest ground-truth PASS load is 48,912 (J_B) and the smallest
ground-truth FAIL load is 333,282 (G_N), so the (100k, 200k] band that
separates the two views is empty, and no arm prediction crosses it
differentially either.

## Where fidelity breaks — and where it does not

**A5 / A8 (synopsis): no verdict breakage at either scale.** Recall,
precision, F1 = 1.000 on all 44 instances under both views; Spearman
ρ = 0.999 (K200 pooled) and 1.000 (K1M); median Q-error 1.001 / 1.000.
Predicted F_A loads stay within ~0.1% of ground truth (e.g. K200-zipf
GT 3.675e11, A5 3.675e11). Honest scope note: every pipeline load in this
program is a MAX statistic, and the maxima are set by the hottest keys,
which remain inside the top-1024 at every K tested. These runs therefore
show that max-load verdict fidelity survives k/K shrinking to 0.10%;
they do NOT probe tail fidelity (a tail-sensitive statistic could degrade
even though these verdicts do not).

**A4 (marginal synthesis): verdicts survive, magnitudes collapse on F.**
Recall stays 1.000 everywhere, but the fact–fact predicted loads fall
~4 orders of magnitude below ground truth and worsen with K — F_A
predicted 2.4e7 / 2.2e6 (K200 zipf/mixed) and 1.6e6 (K1M) vs GT
3.7e11 / 3.7e10 / 7.2e10; F_B predicted 5.9e6 / 5.6e6 / 2.7e5 vs GT
5.7e8 / 4.2e8 / 4.0e7. The verdict is still FAIL only because even the
alignment-destroyed estimate clears the threshold. A4's Spearman ρ drops
to 0.863 at K1M (headline: 0.995). This is a latent breakage: any
threshold between the collapsed estimate and the true load would flip it.

**A1 (uniform sample): J_A / J_N missed in all 4 datasets.** The dim
sample (Bernoulli r = 0.01) retains ~2,000 keys at K200 and ~10,000 at
K1M, but J load is max-dominated and the hottest join key is retained
only with probability r — it was missed in every dataset run (predicted
J_A 176k/29.6k/96.9k/3.0k vs GT 394k/875k/876k/393k). F (no dim involved)
is still caught, so pooled J/F recall = 0.500 at both scales. A1 ρ falls
to 0.786 (K200) / 0.653 (K1M) vs 0.809 headline pooled.

**A2 (key-hash strata): a seed lottery on the hottest key.** zipf s101:
perfect. zipf s202: the hot keys fall outside the selected 10% of strata
and G_A, G_N, W_A, J_A, J_N are ALL missed. mixed s101: G_N (NULL-group
bucket), J_A, J_N missed. K1M s101 happens to win the lottery (recall
1.000 on that single dataset) — do not read it as scaling behaviour;
pooled K200 recall is 0.636.

**A3 (FK-preserving subsetting): worst degradation.** Pooled recall
0.364 (K200) / 0.375 (K1M), median Q-error ≈ 6.0 / 5.9 (headline 2.906).
Whenever the hottest key is outside the 10% selected key set, G_A / G_N /
W_A collapse ~6–13× (e.g. K1M G_A predicted 58,950 vs GT 346,734) and
D_A also flips to a miss at zipf s202 (41,157 vs T = 50,000) and K1M
(44,540 vs 50,000).

**No arm produced a false positive anywhere** (precision 1.000 for all
arms, both configs, both views) — all breakage is missed failures.

## Caveats

- K1M is ONE dataset (zipf, seed 101, 11 instances) at reduced fact sizes
  (5M/3M); K200 is 33 instances. Small-n caution applies, especially to
  A2's K1M recall of 1.000.
- K1M ran within the mandated envelope (DuckDB threads = 2,
  memory_limit = '2GB'; gen 6.1s + ground-truth SQL 5.2s, arms 19.5s) —
  no OOM or failure occurred, so no fallback was needed.
- Headline reference numbers are from the main program's
  `results/summary.json` (K = 20,000, 99 instances), not re-run here.
