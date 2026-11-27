# ACCESS BLOCKER — Workstream 4, Part B (RunPod cluster run)

**Status (2026-11-06): BLOCKED on access route. $0 spent. No pod created.
The pod package in this directory is complete, locally smoke-tested, and
ready to run the moment a browser session deploys a pod (steps below).**

## What route is missing

There is **no non-browser RunPod access route** on this machine. Checked
2026-11-06 (presence/absence only; no secret values read or printed):

| Route | Result |
|---|---|
| `RUNPOD_API_KEY` (or any `*RUNPOD*`) environment variable | **absent** |
| `runpodctl` / `runpod` binary on PATH or in `/opt/hatch/bin` | **absent** |
| `~/.runpod/` config directory | **absent** |
| RunPod connector under `/opt/hatch/connectors/` | **absent** |
| API key in workspace config files (searched `~/workspace` for `runpod` across `.json/.yaml/.yml/.toml/.env/.md/.txt/.sh` and for `*runpod*` filenames) | **none found** — every hit is prose in project/memory documents noting that previous pods (HF1067, Rebase) were handled through the **browser console with a login the agent never performed** |

Without an API key or `runpodctl`, the only remaining route is the
RunPod web console, which requires a **live-browser login**. A generic
(non-browser) agent cannot perform that login, and no credential
workaround was attempted (per workstream constraints).

## Exact steps for a browser session (est. total ≈ 1 h)

1. **Record the starting balance** (spend = balance difference; cap $10,
   NO deposits). RunPod console → Billing → note current balance.
2. **Deploy the cheapest qualifying CPU pod**: Pods → Deploy → CPU pod,
   Community Cloud, On-Demand, **≥ 8 vCPU / ≥ 16 GB RAM** (16 vCPU /
   32 GB preferred for headroom — still far under the cap at these
   runtimes), Ubuntu 22.04/24.04 template, ≥ 40 GB volume, SSH exposed.
3. **Upload this `pod/` directory** to the pod (SSH/scp on the pod's
   exposed port, RunPod file upload, or `runpodctl send` if installed
   in that session).
4. **Run the README sequence** on the pod:
   `bash setup.sh` → `bash run_pod.sh`
   (setup ≈ 5–10 min; generation ≈ 1–3 min; the 9 Spark pipelines
   ≈ 10–30 min; collect < 1 min; **total ≈ 20–45 min**).
5. **Download results**:
   `pod/results_out/spark_ground_truth_zipf_s101.json` (+ the three
   `results_out/*.log` files) → save locally as
   `~/workspace/blind-at-scale/results/spark_cluster/spark_ground_truth_zipf_s101.json`.
6. **TERMINATE the pod** (not just stop), verify in the console that it
   no longer exists, and **record the final balance + actual spend**
   in `~/workspace/blind-at-scale/RUN_LOG.md`.
   **Abort rule:** if the projected spend approaches $10, terminate
   immediately and keep whatever partial results exist.

## What is already proven (so the browser session is turnkey)

- `gen_on_pod.py` reproduces the local study's per-key counts
  **bit-identically** (SHA-256 checked in-script; numpy pinned to 2.5.4
  in `setup.sh` — the multinomial algorithm differs under numpy 1.26).
- `spark_pipelines.py` + `collect_results.py` were smoke-tested locally
  (Spark 3.5.3 tarball, `local[2]`, gen_on_pod's 500k-row smoke data):
  all 9 pipelines executed; Spark's per-key counts reproduced the local
  smoke maxima exactly; F_A correctly skipped (exact output 2.24e9 rows
  even at smoke scale), F_B raw join count matched the analytic total
  exactly (44,243,713); event-log extraction produced verdicts agreeing
  with the local simulation 9/9 at smoke scale (A5/A8 also 9/9, A1 7/9).
  The smoke output is preserved at
  `results/spark_cluster/smoke_local2_spark_ground_truth.json` and is
  labelled inside the file as NOT A CLUSTER RUN — it is a code-path
  test, not ground truth. The smoke test also caught and fixed a real
  definition bug (scan-task input records swamping partition loads —
  see deviation D6 in `spark_pipelines.py`).
- NOT smoke-tested locally: `setup.sh`'s standalone master/worker
  startup (this machine has 4 cores / ~8 GB RAM and could not host a
  representative run); it follows the standard Spark 3.5 standalone
  scripts and health-checks `aliveworkers` before declaring success.
- The F_A/F_B raw joins are **never attempted** (exact output
  ≈ 5.5×10¹¹ rows for F_A — non-materialisable by design, deviation D4
  in `spark_pipelines.py`); the pod cannot burn the budget on them.
