# Output-equivalence execution of the feedback fixes — results

Study design: PREREGISTRATION.md Amendment 5 §A5.3 (locked 2026-11-06 12:15 UTC), executing the feedback-study fixes of §5 whose revised loads were previously computed analytically (Amendment 2). Fix types: J_* and F_* = pre-aggregation before the join; G_A / G_N / W_A = salting with 16 salts.

Run: 2026-11-06T12:17:58.395072+00:00 → 2026-11-06T12:19:19.517330+00:00 (UTC). Code: code/outeq_check.py (driver) + code/outeq_runner.py (one subprocess per query). Envelope: DuckDB threads=2, memory_limit='2GB', per-query wall-clock cap 900 s; an over-envelope query is recorded INFEASIBLE and not retried under weaker settings. Comparison method: both result relations fetched to parquet, sorted by key, compared exactly (key-set equality; COUNT and SUM compared per key; all measures integers, SUM over int32 column s accumulated as BIGINT). Corpus validation: all 12 corpora show validation.json all_exact=true (asserted before checking). kN grouping is on the raw column, including the -1 NULL-sentinel group stored in the parquet.

## Exact SQL pairs (column name substituted per pipeline)

### F_A (key kA, fix ff)
- orig: `SELECT f1.kA AS key, COUNT(*) AS c, CAST(SUM(f1.s) AS BIGINT) AS ss FROM fact f1 JOIN fact2 f2 ON f1.kA = f2.kA GROUP BY f1.kA`
- rep:  `SELECT a.key AS key, a.c1 * b.c2 AS c, a.ss1 * b.c2 AS ss FROM (SELECT kA AS key, COUNT(*) AS c1, CAST(SUM(s) AS BIGINT) AS ss1 FROM fact GROUP BY kA) a JOIN (SELECT kA AS key, COUNT(*) AS c2 FROM fact2 GROUP BY kA) b ON a.key = b.key`

### F_B (key kB, fix ff)
- orig: `SELECT f1.kB AS key, COUNT(*) AS c, CAST(SUM(f1.s) AS BIGINT) AS ss FROM fact f1 JOIN fact2 f2 ON f1.kB = f2.kB GROUP BY f1.kB`
- rep:  `SELECT a.key AS key, a.c1 * b.c2 AS c, a.ss1 * b.c2 AS ss FROM (SELECT kB AS key, COUNT(*) AS c1, CAST(SUM(s) AS BIGINT) AS ss1 FROM fact GROUP BY kB) a JOIN (SELECT kB AS key, COUNT(*) AS c2 FROM fact2 GROUP BY kB) b ON a.key = b.key`

### G_A (key kA, fix salt)
- orig: `SELECT kA AS key, COUNT(*) AS c, CAST(SUM(s) AS BIGINT) AS ss FROM fact GROUP BY kA`
- rep:  `SELECT key, CAST(SUM(c) AS BIGINT) AS c, CAST(SUM(ss) AS BIGINT) AS ss FROM (SELECT kA AS key, s % 16 AS salt, COUNT(*) AS c, CAST(SUM(s) AS BIGINT) AS ss FROM fact GROUP BY kA, s % 16) GROUP BY key`

### G_N (key kN, fix salt)
- orig: `SELECT kN AS key, COUNT(*) AS c, CAST(SUM(s) AS BIGINT) AS ss FROM fact GROUP BY kN`
- rep:  `SELECT key, CAST(SUM(c) AS BIGINT) AS c, CAST(SUM(ss) AS BIGINT) AS ss FROM (SELECT kN AS key, s % 16 AS salt, COUNT(*) AS c, CAST(SUM(s) AS BIGINT) AS ss FROM fact GROUP BY kN, s % 16) GROUP BY key`

### J_A (key kA, fix join)
- orig: `SELECT f.kA AS key, COUNT(*) AS c, CAST(SUM(f.s) AS BIGINT) AS ss FROM fact f JOIN dim d ON f.kA = d.dim_key GROUP BY f.kA`
- rep:  `SELECT p.key AS key, p.c AS c, p.ss AS ss FROM (SELECT kA AS key, COUNT(*) AS c, CAST(SUM(s) AS BIGINT) AS ss FROM fact GROUP BY kA) p JOIN dim d ON p.key = d.dim_key`

### J_B (key kB, fix join)
- orig: `SELECT f.kB AS key, COUNT(*) AS c, CAST(SUM(f.s) AS BIGINT) AS ss FROM fact f JOIN dim d ON f.kB = d.dim_key GROUP BY f.kB`
- rep:  `SELECT p.key AS key, p.c AS c, p.ss AS ss FROM (SELECT kB AS key, COUNT(*) AS c, CAST(SUM(s) AS BIGINT) AS ss FROM fact GROUP BY kB) p JOIN dim d ON p.key = d.dim_key`

### J_N (key kN, fix join)
- orig: `SELECT f.kN AS key, COUNT(*) AS c, CAST(SUM(f.s) AS BIGINT) AS ss FROM fact f JOIN dim d ON f.kN = d.dim_key GROUP BY f.kN`
- rep:  `SELECT p.key AS key, p.c AS c, p.ss AS ss FROM (SELECT kN AS key, COUNT(*) AS c, CAST(SUM(s) AS BIGINT) AS ss FROM fact GROUP BY kN) p JOIN dim d ON p.key = d.dim_key`

### W_A (key kA, fix salt)
- orig: `SELECT kA AS key, COUNT(*) AS c, CAST(SUM(s) AS BIGINT) AS ss FROM fact GROUP BY kA`
- rep:  `SELECT key, CAST(SUM(c) AS BIGINT) AS c, CAST(SUM(ss) AS BIGINT) AS ss FROM (SELECT kA AS key, s % 16 AS salt, COUNT(*) AS c, CAST(SUM(s) AS BIGINT) AS ss FROM fact GROUP BY kA, s % 16) GROUP BY key`

W_A uses the same two SQL forms as G_A on kA (declared equivalence target: the per-key (COUNT(*), SUM(s)) relation that a partition-aggregate window broadcasts).

## Pooled results

Checked 48 instances: **48 EQUAL**, 0 MISMATCH, 0 INFEASIBLE.

| scope | checked | EQUAL | MISMATCH | INFEASIBLE |
|---|---|---|---|---|
| pooled | 48 | 48 | 0 | 0 |
| G_A | 6 | 6 | 0 | 0 |
| G_N | 6 | 6 | 0 | 0 |
| W_A | 6 | 6 | 0 | 0 |
| J_A | 6 | 6 | 0 | 0 |
| J_B | 6 | 6 | 0 | 0 |
| J_N | 6 | 6 | 0 | 0 |
| F_A | 6 | 6 | 0 | 0 |
| F_B | 6 | 6 | 0 | 0 |
| EQ1M | 36 | 36 | 0 | 0 |
| EQ200K | 12 | 12 | 0 | 0 |

Ground-truth verdict at check scale (context, from counts.npz via common.loads_from_counts + verdict, T_load = 0.02 * n_fact): FAIL=34, PASS=14.

## Per-instance results

| dataset | pipeline | GT verdict @scale | status | n_keys | n_mismatch | max|dC| | max|dSS| | wall orig (s) | wall rep (s) |
|---|---|---|---|---|---|---|---|---|---|---|
| outeq_1m_zipf_s101 | G_A | FAIL | EQUAL | 19980 | 0 | 0 | 0 | 0.291 | 0.2 |
| outeq_1m_zipf_s101 | G_N | FAIL | EQUAL | 19969 | 0 | 0 | 0 | 0.087 | 0.219 |
| outeq_1m_zipf_s101 | W_A | FAIL | EQUAL | 19980 | 0 | 0 | 0 | 0.104 | 0.131 |
| outeq_1m_zipf_s101 | J_A | FAIL | EQUAL | 19980 | 0 | 0 | 0 | 0.119 | 0.085 |
| outeq_1m_zipf_s101 | J_B | PASS | EQUAL | 20000 | 0 | 0 | 0 | 0.113 | 0.101 |
| outeq_1m_zipf_s101 | J_N | FAIL | EQUAL | 19968 | 0 | 0 | 0 | 0.1 | 0.15 |
| outeq_200k_zipf_s101 | F_A | FAIL | EQUAL | 12361 | 0 | 0 | 0 | 10.275 | 0.284 |
| outeq_200k_zipf_s101 | F_B | FAIL | EQUAL | 19295 | 0 | 0 | 0 | 0.314 | 0.104 |
| outeq_1m_zipf_s202 | G_A | FAIL | EQUAL | 19971 | 0 | 0 | 0 | 0.231 | 0.118 |
| outeq_1m_zipf_s202 | G_N | FAIL | EQUAL | 19969 | 0 | 0 | 0 | 0.104 | 0.152 |
| outeq_1m_zipf_s202 | W_A | FAIL | EQUAL | 19971 | 0 | 0 | 0 | 0.093 | 0.159 |
| outeq_1m_zipf_s202 | J_A | FAIL | EQUAL | 19971 | 0 | 0 | 0 | 0.113 | 0.092 |
| outeq_1m_zipf_s202 | J_B | PASS | EQUAL | 20000 | 0 | 0 | 0 | 0.159 | 0.091 |
| outeq_1m_zipf_s202 | J_N | FAIL | EQUAL | 19968 | 0 | 0 | 0 | 0.099 | 0.113 |
| outeq_200k_zipf_s202 | F_A | FAIL | EQUAL | 12501 | 0 | 0 | 0 | 9.112 | 0.062 |
| outeq_200k_zipf_s202 | F_B | FAIL | EQUAL | 19339 | 0 | 0 | 0 | 0.228 | 0.082 |
| outeq_1m_lognormal_s101 | G_A | PASS | EQUAL | 19003 | 0 | 0 | 0 | 0.116 | 0.209 |
| outeq_1m_lognormal_s101 | G_N | FAIL | EQUAL | 18879 | 0 | 0 | 0 | 0.109 | 0.162 |
| outeq_1m_lognormal_s101 | W_A | PASS | EQUAL | 19003 | 0 | 0 | 0 | 0.102 | 0.166 |
| outeq_1m_lognormal_s101 | J_A | FAIL | EQUAL | 19003 | 0 | 0 | 0 | 0.103 | 0.093 |
| outeq_1m_lognormal_s101 | J_B | PASS | EQUAL | 19985 | 0 | 0 | 0 | 0.096 | 0.105 |
| outeq_1m_lognormal_s101 | J_N | FAIL | EQUAL | 18878 | 0 | 0 | 0 | 0.108 | 0.101 |
| outeq_200k_lognormal_s101 | F_A | FAIL | EQUAL | 12594 | 0 | 0 | 0 | 1.447 | 0.253 |
| outeq_200k_lognormal_s101 | F_B | FAIL | EQUAL | 17830 | 0 | 0 | 0 | 0.384 | 0.194 |
| outeq_1m_lognormal_s202 | G_A | PASS | EQUAL | 18975 | 0 | 0 | 0 | 0.113 | 0.281 |
| outeq_1m_lognormal_s202 | G_N | FAIL | EQUAL | 18941 | 0 | 0 | 0 | 0.208 | 0.188 |
| outeq_1m_lognormal_s202 | W_A | PASS | EQUAL | 18975 | 0 | 0 | 0 | 0.115 | 0.214 |
| outeq_1m_lognormal_s202 | J_A | FAIL | EQUAL | 18975 | 0 | 0 | 0 | 0.211 | 0.12 |
| outeq_1m_lognormal_s202 | J_B | PASS | EQUAL | 19986 | 0 | 0 | 0 | 0.207 | 0.121 |
| outeq_1m_lognormal_s202 | J_N | FAIL | EQUAL | 18940 | 0 | 0 | 0 | 0.2 | 0.103 |
| outeq_200k_lognormal_s202 | F_A | FAIL | EQUAL | 12579 | 0 | 0 | 0 | 0.639 | 0.088 |
| outeq_200k_lognormal_s202 | F_B | FAIL | EQUAL | 17904 | 0 | 0 | 0 | 0.166 | 0.106 |
| outeq_1m_mixed_s101 | G_A | PASS | EQUAL | 20000 | 0 | 0 | 0 | 0.117 | 0.415 |
| outeq_1m_mixed_s101 | G_N | FAIL | EQUAL | 20001 | 0 | 0 | 0 | 0.069 | 0.384 |
| outeq_1m_mixed_s101 | W_A | PASS | EQUAL | 20000 | 0 | 0 | 0 | 0.116 | 0.367 |
| outeq_1m_mixed_s101 | J_A | FAIL | EQUAL | 20000 | 0 | 0 | 0 | 0.38 | 0.128 |
| outeq_1m_mixed_s101 | J_B | PASS | EQUAL | 20000 | 0 | 0 | 0 | 0.146 | 0.205 |
| outeq_1m_mixed_s101 | J_N | FAIL | EQUAL | 20000 | 0 | 0 | 0 | 0.089 | 0.094 |
| outeq_200k_mixed_s101 | F_A | FAIL | EQUAL | 16135 | 0 | 0 | 0 | 3.302 | 0.057 |
| outeq_200k_mixed_s101 | F_B | FAIL | EQUAL | 19478 | 0 | 0 | 0 | 0.232 | 0.116 |
| outeq_1m_mixed_s202 | G_A | PASS | EQUAL | 20000 | 0 | 0 | 0 | 0.161 | 0.205 |
| outeq_1m_mixed_s202 | G_N | FAIL | EQUAL | 20001 | 0 | 0 | 0 | 0.554 | 0.309 |
| outeq_1m_mixed_s202 | W_A | PASS | EQUAL | 20000 | 0 | 0 | 0 | 0.297 | 0.28 |
| outeq_1m_mixed_s202 | J_A | FAIL | EQUAL | 20000 | 0 | 0 | 0 | 0.298 | 0.093 |
| outeq_1m_mixed_s202 | J_B | PASS | EQUAL | 20000 | 0 | 0 | 0 | 0.191 | 0.079 |
| outeq_1m_mixed_s202 | J_N | FAIL | EQUAL | 20000 | 0 | 0 | 0 | 0.086 | 0.11 |
| outeq_200k_mixed_s202 | F_A | FAIL | EQUAL | 16183 | 0 | 0 | 0 | 3.319 | 0.086 |
| outeq_200k_mixed_s202 | F_B | FAIL | EQUAL | 19518 | 0 | 0 | 0 | 0.242 | 0.092 |

## Non-EQUAL instances

None. Every checked (dataset, pipeline) instance compared exactly equal (0 mismatched keys).

## Deviations

None. Corpora were generated with the unmodified code/gen_dataset.py at the pre-registered scales; the feasible set, SQL pairs, envelope, and exact-equality criterion are exactly those of Amendment 5 §A5.3 and the workstream specification. Timeout enforcement uses the subprocess approach (one runner subprocess per query, 900 s driver timeout), as permitted by the specification.

## Implementation note (not a design deviation)

A first driver invocation on 2026-11-06 (~12:16 UTC, before the recorded
run above) crashed on its first comparison because the venv has no
pyarrow/fastparquet engine for pandas.read_parquet; no instance result
was recorded from that invocation. The comparison read in
code/outeq_check.py was switched to a DuckDB-native parquet read
(.df(), sorted by key — the same fetch-into-pandas, exact-compare
method), and the full 48-instance grid was then executed in the single
recorded run above. No corpus, scale, SQL pair, envelope setting, or
equivalence criterion was changed.
