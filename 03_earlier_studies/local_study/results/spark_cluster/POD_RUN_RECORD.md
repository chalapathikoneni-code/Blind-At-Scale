# POD RUN RECORD — Blind at Scale cluster ground truth (Workstream 4)

Executed 2026-11-06, run window 11:10–11:50 AM EST, by the browser-console
operator under the pod-ops standard (`~/workspace/pod-ops/POD_RUN_CHECKLIST.md`).
This record is written from the operator's verified report; the canonical raw
record (full `spark_ground_truth.json`, Spark event log, all logs, generated
data) remains on the research-ops network volume at `/workspace/blind-pod/`
(186 MB: `results_out/`, `eventlog/`, `spark-logs/`, `bas-data/`).

## Pod

| Item | Value |
|---|---|
| Pod name | blind-at-scale-gt |
| Spec | 8 vCPU AMD EPYC 9575F, 16 GB RAM, 80 GB disk |
| Region | EU-RO-1 |
| Rate | $0.29/hr |
| Template | research-standard (kya0m00ekg), image runpod/base:1.5.0-ubuntu2204 |
| Volume | research-ops (lhxc1nip98) mounted at /workspace |
| Preflight | PASS (`pod-ops/preflight.sh`) |
| Balance before / after | $14.16 / $14.00 |
| **Spend** | **$0.16 total** |
| Termination | Pod terminated and deleted 2026-11-06 11:48:03 EST, audit-confirmed; no pod running afterwards |

Note on spend: the $0.16 covers two deployments — a first pod deployed
without Jupyter/ports was terminated and redeployed with console overrides
(Start Jupyter checked, port 8888 exposed, disk 80 GB).

## Environment (on the pod)

Spark 3.5.3 standalone, master `spark://127.0.0.1:7077`, 1 master + 2 workers
(`aliveworkers = 2` verified via the master JSON), app id
`app-20261106164103-0000`, `spark.sql.shuffle.partitions = 128`, AQE off (D7),
OpenJDK 17.0.20.1. Pod Python venv 3.13.18, numpy 2.5.4 (pinned — counts are
bit-identical to the local study only under this version), pyarrow 26.0.1.
Binding label: **multi-worker standalone cluster on a single host**.

## Execution path (as run)

1. Deploy pod from template research-standard + console overrides above.
2. Preflight gate on the pod: PASS.
3. Upload `pod/` package to the pod run directory (`/workspace/blind-pod/`).
4. `bash setup.sh` — installs OpenJDK + Spark 3.5.3, starts master + 2
   workers, event logging on. Master JSON verified `aliveworkers: 2`.
5. `bash run_pod.sh` — `gen_on_pod.py` (zipf seed 101, fact 10,000,000 /
   fact2 6,000,000, p_null 0.04) → `spark_pipelines.py` (9 pipelines) →
   `collect_results.py` (event log → `spark_ground_truth.json`).
6. Dataset validation on the pod: per-key counts SHA-256
   `f9f3fa8b29356e870df37e292f755a03d20fdcd016484d79dbcced148dcb528f`,
   `headline_expected_check` PASS (bit-identical to the local study).
7. Results read back from `results_out/`; STATUS.txt maintained at phase
   transitions during the run (contents as reported by the operator:
   phases setup → generation → pipelines → collection → termination, all
   completed; the two execution fixes below recorded in-flight).
8. Pod terminated and deleted (see above); results preserved on the volume.

## Execution fixes applied (NO package files patched)

1. **Interpreter pin.** `setup.sh`'s bare `python3` resolved to the pod's
   system Python 3.10, under which numpy 2.5.4 cannot install (needs
   ≥ 3.12). The operator pinned the venv interpreter (Python 3.13.18) via a
   PATH shim so all package scripts ran under the venv, and installed
   pyarrow 26.0.1 into the venv.
2. **SPARK_HOME preset.** `run_pod.sh`'s SPARK_HOME auto-detection
   (`ls -d /opt/spark-*-bin-hadoop3 ... | head -1`) aborted under
   `set -e`/`pipefail` when one glob missed (ls exit 2). The operator
   preset `SPARK_HOME=/opt/spark-3.5.3-bin-hadoop3` — the override the
   script itself supports — and the run proceeded unmodified otherwise.

## Known output artefacts / discrepancies

- `collect_results.py`'s comparison section is **empty in the on-pod
  output**: `predictions_main.csv` was not uploaded to the pod and its
  default search paths did not resolve from the pod run directory. The
  comparison was recovered locally with no new pod run:
  `results/spark_cluster/cluster_comparison.json` (code/cluster_compare.py).
- An auxiliary pod output recorded `t_load: 20000`. Audit (see
  NUMBER_PROVENANCE.md and cluster_comparison.json `threshold_audit`):
  the verdict path applied **T = 200,000** — all 9 recorded verdicts
  recompute exactly under 200,000 and two of them (G_B, J_B) would flip
  under 20,000. 20,000 = 0.02 × 1,000,000 is the signature of a
  reduced-scale invocation; collect_results.py uses a single `t_load`
  variable for the root field, per-pipeline fields and verdicts, and
  with the headline manifest (n_fact = 10,000,000) that variable is
  200,000. No verdict is affected.
