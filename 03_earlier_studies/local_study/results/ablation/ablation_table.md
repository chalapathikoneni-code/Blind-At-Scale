# Synopsis-information ablation (A8-style direct prediction, 99 instances)

Pooled over the 9-dataset grid. FAIL is the positive class. full_k1024 is the unmodified synopsis (headline A8 control).

| Rung | Recall | Precision | F1 | Spearman rho | Median Q-error | TP | FP | FN | TN |
|---|---|---|---|---|---|---|---|---|---|
| full_k1024 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 60 | 0 | 0 | 39 |
| hh_k16 | 1.000 | 1.000 | 1.000 | 0.997 | 1.001 | 60 | 0 | 0 | 39 |
| hh_k64 | 1.000 | 1.000 | 1.000 | 0.999 | 1.000 | 60 | 0 | 0 | 39 |
| hh_k256 | 1.000 | 1.000 | 1.000 | 0.999 | 1.000 | 60 | 0 | 0 | 39 |
| no_aligned_pairs | 1.000 | 1.000 | 1.000 | 0.999 | 1.001 | 60 | 0 | 0 | 39 |
| no_join_conditioning | 1.000 | 1.000 | 1.000 | 0.998 | 1.001 | 60 | 0 | 0 | 39 |
| noise_eps8 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 60 | 0 | 0 | 39 |
| noise_eps1 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 60 | 0 | 0 | 39 |
| uniform_tail | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 60 | 0 | 0 | 39 |
| stored_only | 1.000 | 1.000 | 1.000 | 0.996 | 1.000 | 60 | 0 | 0 | 39 |

## Per-pipeline-family recall

| Rung | G/D direct (n=45) | J (n=27) | F (n=18) | W (n=9) |
|---|---|---|---|---|
| full_k1024 | 1.000 | 1.000 | 1.000 | 1.000 |
| hh_k16 | 1.000 | 1.000 | 1.000 | 1.000 |
| hh_k64 | 1.000 | 1.000 | 1.000 | 1.000 |
| hh_k256 | 1.000 | 1.000 | 1.000 | 1.000 |
| no_aligned_pairs | 1.000 | 1.000 | 1.000 | 1.000 |
| no_join_conditioning | 1.000 | 1.000 | 1.000 | 1.000 |
| noise_eps8 | 1.000 | 1.000 | 1.000 | 1.000 |
| noise_eps1 | 1.000 | 1.000 | 1.000 | 1.000 |
| uniform_tail | 1.000 | 1.000 | 1.000 | 1.000 |
| stored_only | 1.000 | 1.000 | 1.000 | 1.000 |

## Per-pipeline-family median Q-error (load fidelity)

Verdict metrics saturate on this grid (F ground-truth loads exceed the threshold by orders of magnitude), so Q-error is the discriminating metric for the pair rungs.

| Rung | G/D direct | J | F | W | F median pred/GT |
|---|---|---|---|---|---|
| full_k1024 | 1.000 | 1.009 | 1.001 | 1.000 | 1.000 |
| hh_k16 | 1.000 | 1.061 | 1.028 | 1.000 | 0.973 |
| hh_k64 | 1.000 | 1.039 | 1.004 | 1.000 | 0.996 |
| hh_k256 | 1.000 | 1.043 | 1.005 | 1.000 | 0.995 |
| no_aligned_pairs | 1.000 | 1.009 | 23.609 | 1.000 | 0.042 |
| no_join_conditioning | 1.000 | 1.009 | 56.819 | 1.000 | 0.018 |
| noise_eps8 | 1.000 | 1.009 | 1.001 | 1.000 | 1.000 |
| noise_eps1 | 1.000 | 1.009 | 1.001 | 1.000 | 1.000 |
| uniform_tail | 1.000 | 1.001 | 1.000 | 1.000 | 1.000 |
| stored_only | 1.000 | 1.139 | 1.003 | 1.000 | 0.997 |

Rung definitions, noise mechanism (per-count sensitivity 1, Laplace scale 1/eps, clip >= 0, totals renormalised, seeds in script header) and the uniform-tail coarsening are documented in code/ablation.py. DP-style noise only; no formal DP claim for the synopsis as a whole.
