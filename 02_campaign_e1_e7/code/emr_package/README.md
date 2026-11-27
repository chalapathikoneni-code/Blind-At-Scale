# Blind at Scale — EMR package (FINAL, consolidated)

One package, one cluster, one runbook: `docs/RUNBOOK.md`.

Experiments: E1 real join validation (OpenAlex, natural skew, three
scales + a second join family), E6 AQE robustness (OpenAlex and
synthetic), and A9 at 100M (synthetic). Steps 0, 1, 2, 3, 4, 5, 6,
7a, 7b — all in the single `scripts/add_steps.sh` sequence.
Steps 5 and 7 are the synthetic extras and can be dropped without
affecting the rest; Step 5 is recommended, Step 7 is optional.

Design rules (learned from the failed November attempts):
- NO bootstrap action, NO pip installs — pyspark + stdlib only.
- Step 0 is a 2-minute smoke test that proves the path first.
- Each step is independent (ActionOnFailure=CONTINUE).

Layout: code/ (jobs), config/ (cluster shape), scripts/ (upload,
create cluster, add steps), docs/RUNBOOK.md.
