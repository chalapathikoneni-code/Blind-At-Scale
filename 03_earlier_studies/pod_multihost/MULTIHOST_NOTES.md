# MULTIHOST_NOTES — for the manuscript team

What the multi-host run (package in this directory) is designed to
establish, and the exact language to use about it.

## Exact honest label (use verbatim in the paper)

RunPod execution:

> **Spark standalone cluster across 3 hosts (1 master/driver + 2 workers), RunPod EU-RO-1**

Generic-hosts execution: drop the trailing ", RunPod EU-RO-1".

## What it establishes (if the gates pass)

- The 9 headline pipelines' ground-truth loads come from Spark task
  metrics of a cluster whose **workers are separate hosts**: executor
  registration, task scheduling and **shuffle block fetch all cross
  real host boundaries** over the pods'/hosts' network endpoints. The
  run script enforces this with a hard gate: the event log must show
  executors on **both** worker hosts, or the run must not be cited as
  multi-host.
- The dataset is bit-identical to the local study and the single-host
  run (SHA-256 counts gate, `f9f3fa8b…28f`, enforced before any
  pipeline runs), so the three-way comparison (local simulation /
  single-host Spark / multi-host Spark) and the arm comparison
  (A1/A5/A8, now populated on the pod via `--predictions`) are
  apples-to-apples.
- Verdicts use the study threshold T = 200,000, passed explicitly
  (`--t-load 200000`); the single-host run's 20,000 display anomaly
  (traced in the R4 integration to a reduced-scale collect invocation's
  signature in a recorded field, never applied to any verdict) cannot
  recur silently because the applied value is on the command line and
  in the collect log.

## What it does NOT establish

- **Not a multi-datacenter / WAN test.** All 3 hosts are in one
  region/datacenter (EU-RO-1 on RunPod); cross-host latency is
  LAN-class. Nothing is shown about geo-distributed execution.
- **Not a scale-out result.** 3 hosts, 8 executor cores total, one
  dataset (zipf seed-101, 10M fact rows). No claim about larger
  clusters, other distributions, or other seeds.
- **Storage is not distributed on the RunPod path:** input parquet is
  read from the shared research-ops volume mounted on all pods
  (compute and shuffle are cross-host; input storage is shared).
  On the generic-hosts path the parquet may instead be identical
  local copies — disclose whichever was used.
- **F_A/F_B remain analytic (deviation D4):** their loads are computed
  from Spark-produced per-key counts (the counts themselves now come
  from cross-host aggregate shuffles), not from materialised joins.
- **No performance/timing claims.** Loads and verdicts only; wall
  times are operational metadata.
- **No fault-tolerance evidence.** No host is killed mid-run; nothing
  is shown about failure behaviour of the cluster itself.

## If the run cannot be executed

Both runbooks end in the same hard rule: if cross-host registration or
shuffle fetch cannot be established on the platform, the attempt is
reported as a platform block with logs preserved — it is **not**
evidence, and the paper keeps R4's "adequately fenced, not earned"
status for the cluster item. Do not cite the single-host run, a
`local-cluster` pseudo-cluster, or multiple worker processes on one
host as multi-host evidence under any circumstances.
