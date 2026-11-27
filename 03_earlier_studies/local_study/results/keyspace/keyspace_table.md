# Key-space scaling — headline table (WORKSTREAM 3)

Synopsis k = 1024 fixed. Threshold views: **fixed200k** T_load = 200,000 (preregistration absolute); **norm002** T_load = 0.02·N_fact (identical to fixed200k at N_fact = 10M; 100,000 at the reduced K1M N_fact = 5M). T_distinct = 50,000 in both views. FAIL is the positive class.

## Config K200 (K = 200,000, k/K = 0.5120%)

Datasets: keyspace_K200_mixed_s101, keyspace_K200_zipf_s101, keyspace_K200_zipf_s202; instances: 33; synopsis bytes: keyspace_K200_mixed_s101=89,349, keyspace_K200_zipf_s101=88,049, keyspace_K200_zipf_s202=88,080

### View: fixed200k (ground-truth FAILs: 22/33)

| arm | recall | precision | F1 | Spearman ρ | median Q-err | recall J/F | recall G/D/W |
|---|---|---|---|---|---|---|---|
| A1 | 0.727 | 1.000 | 0.842 | 0.786 | 1.044 | 0.500 | 1.000 |
| A2 | 0.636 | 1.000 | 0.778 | 0.886 | 2.000 | 0.667 | 0.600 |
| A3 | 0.364 | 1.000 | 0.533 | 0.780 | 6.014 | 0.500 | 0.200 |
| A4 | 1.000 | 1.000 | 1.000 | 0.991 | 1.024 | 1.000 | 1.000 |
| A5 | 1.000 | 1.000 | 1.000 | 0.999 | 1.001 | 1.000 | 1.000 |
| A8 | 1.000 | 1.000 | 1.000 | 0.999 | 1.000 | 1.000 | 1.000 |

### View: norm002 (ground-truth FAILs: 22/33)

| arm | recall | precision | F1 | Spearman ρ | median Q-err | recall J/F | recall G/D/W |
|---|---|---|---|---|---|---|---|
| A1 | 0.727 | 1.000 | 0.842 | 0.786 | 1.044 | 0.500 | 1.000 |
| A2 | 0.636 | 1.000 | 0.778 | 0.886 | 2.000 | 0.667 | 0.600 |
| A3 | 0.364 | 1.000 | 0.533 | 0.780 | 6.014 | 0.500 | 0.200 |
| A4 | 1.000 | 1.000 | 1.000 | 0.991 | 1.024 | 1.000 | 1.000 |
| A5 | 1.000 | 1.000 | 1.000 | 0.999 | 1.001 | 1.000 | 1.000 |
| A8 | 1.000 | 1.000 | 1.000 | 0.999 | 1.000 | 1.000 | 1.000 |

## Config K1M (K = 1,000,000, k/K = 0.1024%)

Datasets: keyspace_K1M_zipf_s101; instances: 11; synopsis bytes: keyspace_K1M_zipf_s101=85,042

### View: fixed200k (ground-truth FAILs: 8/11)

| arm | recall | precision | F1 | Spearman ρ | median Q-err | recall J/F | recall G/D/W |
|---|---|---|---|---|---|---|---|
| A1 | 0.750 | 1.000 | 0.857 | 0.653 | 1.029 | 0.500 | 1.000 |
| A2 | 1.000 | 1.000 | 1.000 | 0.973 | 1.033 | 1.000 | 1.000 |
| A3 | 0.375 | 1.000 | 0.545 | 0.881 | 5.882 | 0.500 | 0.250 |
| A4 | 1.000 | 1.000 | 1.000 | 0.863 | 1.027 | 1.000 | 1.000 |
| A5 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| A8 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |

### View: norm002 (ground-truth FAILs: 8/11)

| arm | recall | precision | F1 | Spearman ρ | median Q-err | recall J/F | recall G/D/W |
|---|---|---|---|---|---|---|---|
| A1 | 0.750 | 1.000 | 0.857 | 0.653 | 1.029 | 0.500 | 1.000 |
| A2 | 1.000 | 1.000 | 1.000 | 0.973 | 1.033 | 1.000 | 1.000 |
| A3 | 0.375 | 1.000 | 0.545 | 0.881 | 5.882 | 0.500 | 0.250 |
| A4 | 1.000 | 1.000 | 1.000 | 0.863 | 1.027 | 1.000 | 1.000 |
| A5 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
| A8 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 |
