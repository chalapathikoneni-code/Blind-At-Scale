# OPERATOR RUNBOOK — Blind at Scale multi-host cluster ground truth (RunPod)

For the browser operator executing this run. The single-host run's
runbook discipline applies throughout (preflight gate, one pinned
interpreter, checkpoint visibility, terminate in the same session,
spend by balance difference). This run is the ONE sanctioned exception
to "pods one at a time": it deploys **3 pods simultaneously by design**
— but no OTHER session's pod may be running (Phase 0), and the **$10
total cap covers all 3 pods combined**.

**Topology.** Spark 3.5.3 standalone across 3 pods in EU-RO-1, all from
template `research-standard` (`kya0m00ekg`) with the shared
`research-ops` volume at `/workspace`:

| Pod | Name | Role | Daemons |
|---|---|---|---|
| A | `blind-mh-master` | master + driver | Spark master, spark-submit driver |
| B | `blind-mh-worker-b` | worker | 1 worker (4 cores / 6 GB) |
| C | `blind-mh-worker-c` | worker | 1 worker (4 cores / 6 GB) |

**Honest label** (goes into the ground-truth JSON via `cluster.env`):
"Spark standalone cluster across 3 hosts (1 master/driver + 2 workers),
RunPod EU-RO-1". Compute and shuffle are cross-host; input parquet sits
on the shared volume — that storage fact is disclosed, not hidden.

## Phase 0 — exclusivity + balance

1. Sign in to the RunPod console (secure login card is expected).
2. Pods page: if ANY pod is running that is not one of this run's three,
   STOP and report it. Do not deploy alongside another session's pod.
3. Record the starting balance exactly.

## Phase 1 — deploy 3 pods

For EACH pod: deploy from template `research-standard` (volume
`research-ops` mounts at `/workspace` automatically). Use **Set
overrides** (the template has no Jupyter/ports by default — the
single-host run proved this):

- **Start Jupyter notebook = checked**, HTTP port **8888** exposed.
- **Expose TCP ports** — enter exactly:
  - Pod A: `7077,8080,41000,42000`
  - Pod B: `7177,8081,42000`
  - Pod C: `7177,8081,42000`
- Container disk: template default (≥40 GB). Spec: ≥8 vCPU / 16 GB,
  cheapest qualifying in EU-RO-1 (≈$0.29/hr each, ≈$0.87/hr for three).
- Names: `blind-mh-master`, `blind-mh-worker-b`, `blind-mh-worker-c`.

Why each port: 7077 master RPC (workers→A) · 41000 driver RPC
(executors→A) · 42000 block manager (shuffle fetch, all pods) ·
7177 worker daemon (A→workers) · 8080/8081 web UIs (inspection only).

## Phase 2 — record endpoints; the 1:1 port gate

From each pod's Connect panel, record: **public IP** and the **TCP
port-mapping table** (external → internal). Fill the table:

| Pod | Public IP | 7077→ | 41000→ | 42000→ | 7177→ | 8080/8081→ |
|---|---|---|---|---|---|---|
| A | | | | | — | |
| B | | — | — | | | |
| C | | — | — | | | |

**GATE:** for the **driver port (41000 on A)**, the **block-manager
port (42000 on B and C)** and the **worker daemon ports (7177 on B and
C)**, the external number MUST equal the internal number (1:1). Spark
binds and advertises a single port number for these services; a shifted
mapping cannot be bridged by configuration, and `setup_multihost.sh`
will refuse to start (port guard). If a pod's mapping is shifted for
one of these ports: terminate and redeploy that pod (assignment varies
by host machine) — at most **2 redeploys per pod**. If it is still not
1:1, **STOP**: preserve any logs on the volume, terminate all pods,
record the balance, and report the platform block precisely (which pod,
which port, assigned mapping). The master RPC port (7077) is exempt —
workers are handed `spark://PUBLIC_IP:EXT` explicitly, so a shifted
7077 is usable; record it in `EXT_MASTER_PORT`.

## Phase 3 — stage the package + cluster.env per pod

1. Upload the package **once** to the volume: `/workspace/blind-multihost/pkg/`
   (all of `pod_multihost/`, including `staging/`).
2. On EACH pod (JupyterLab terminal): make a pod-local working copy and
   place that pod's `cluster.env` in it (the volume copy is shared —
   `cluster.env` differs per pod, so it must live in the local copy):
   ```bash
   mkdir -p ~/bas-mh && cp -r /workspace/blind-multihost/pkg/* ~/bas-mh/
   cd ~/bas-mh && cp cluster.env.template cluster.env
   ```
3. Edit `cluster.env` on each pod: identical contents EXCEPT `ROLE`
   (`master` / `worker-b` / `worker-c`). Fill the three `*_PUBLIC_HOST`
   values (public IPs from Phase 2) and any shifted `EXT_MASTER_PORT` /
   UI ports. `BAS_LABEL` stays the RunPod line.

## Phase 4 — setup, in order A → B → C

On each pod's terminal, in its `~/bas-mh/`:

- A: `bash setup_multihost.sh` → expect preflight PASS, port guard OK,
  master started.
- B: `bash setup_multihost.sh` → expect master-RPC probe OK, worker started.
- C: same as B.

**Gates (all must hold before Phase 5):**
- Preflight PASS on all 3 pods.
- On A: `curl -s http://127.0.0.1:8080/json/` shows `aliveworkers: 2`
  with workers on **distinct hosts** (the two worker public hosts / two
  different container hostnames — not two processes on A).
- On B and C: `bash run_worker.sh` prints `registered: ... state=ALIVE`.

## Phase 5 — run (on pod A only)

```bash
cd ~/bas-mh && bash run_master.sh 2>&1 | tee ~/bas-mh/run_master_console.log
```

`run_master.sh` checkpoints `/workspace/blind-multihost/STATUS.txt` at
every phase (watch it from any pod). It generates the dataset onto the
volume (SHA-256 counts gate must print PASS — an abort there is the
gate working, never relax it), submits the 9 pipelines, verifies from
the event log that **executors ran on both worker hosts** (cross-host
gate), then collects with predictions and explicit `--t-load 200000`.

Expected: G/J/W pipelines OK; **F_A/F_B `SKIPPED_NOT_MATERIALISABLE` is
by design** (deviation D4), not an error.

## Phase 6 — archive (before any termination)

1. On B and C: `bash run_worker.sh --archive`.
2. On A: verify the archive on the volume is complete:
   `find /workspace/blind-multihost -type f | sort` — must include
   `results_out/spark_ground_truth_zipf_s101.json` (with a populated
   comparison section — check `verdict_agreement` totals are 9, not 0),
   `results_out/*.log`, `run_manifest_zipf_s101.json`, `STATUS.txt`,
   `eventlog/`, `logs_master/`, `logs_worker-b/`, `logs_worker-c/`,
   `data/zipf_s101/` (parquet + counts_summary.json).
3. Copy `run_master_console.log` and each pod's `setup` terminal output
   into `/workspace/blind-multihost/` as text files if not already there.

## Phase 7 — terminate ALL 3 pods + balance

Terminate (not stop) all three pods, verify the Pods page shows none
running / $0.000/hr billing, record the ending balance. Total spend =
start − end across the whole run; it must be ≤ $10 (expected ≈ $1–2).

**Return to the parent session:** starting/ending balance and spend;
each pod's spec/rate; the Phase-2 mapping table; preflight results ×3;
aliveworkers + distinct-host evidence; the counts SHA line; the
cross-host gate output; the COMPLETE `spark_ground_truth_zipf_s101.json`
verbatim; STATUS.txt verbatim; any failure/diagnosis notes; termination
confirmation for all 3 pods.

## Failure diagnosis

- **F1 — shifted NAT mapping on a single-knob port** (driver 41000,
  block-manager 42000, worker 7177): symptom = `setup_multihost.sh`
  port guard refuses, or (if the guard was bypassed — it must not be)
  executors never register / shuffle fetch hangs. Knob: none in Spark
  (bind port == advertised port). Action: redeploy the pod (≤2 tries),
  else STOP and report the platform block.
- **F2 — worker registered under its container hostname, or master
  cannot call the worker back:** master /json/ shows a hostname that
  is not the worker's public host. Knobs in `spark-env.sh` (rendered by
  `setup_multihost.sh`): `SPARK_LOCAL_HOSTNAME` and `SPARK_PUBLIC_DNS`
  must be the pod's PUBLIC host; `SPARK_LOCAL_IP` must stay the pod's
  real interface IP (binding). Re-render via setup and restart the
  worker daemon.
- **F3 — `FetchFailedException` / shuffle blocks unreachable:**
  executors registered but tasks fail fetching blocks. Check, in order:
  (a) block-manager 1:1 gate (Phase 2) for BOTH workers; (b) from pod A
  during a run, `bash -c 'exec 3<>/dev/tcp/<workerB-public>/42000'`
  (and C) succeeds; (c) rendered `spark-defaults.conf` on A contains
  `spark.blockManager.port 42000` (executors inherit the app conf).
- **F4 — executors cannot reach the driver:** tasks stall at
  registration, driver log shows no executor heartbeats. Knobs:
  `spark.driver.host` (= A's public host), `spark.driver.port` (41000),
  `spark.driver.bindAddress` (0.0.0.0) in A's rendered defaults; probe
  from B: `/dev/tcp/<A-public>/41000` while a submit is running.
- **F5 — workers cannot reach the master:** worker log loops on
  registration retries. The worker's master URL is
  `spark://MASTER_PUBLIC_HOST:EXT_MASTER_PORT` — confirm `EXT_MASTER_PORT`
  in B/C's `cluster.env` matches the console mapping for A's 7077, and
  probe `/dev/tcp/<A-public>/<EXT_MASTER_PORT>` from B.
- **F6 — web UIs unreachable over the public endpoints:** non-fatal.
  Inspect via pod A's local `curl http://127.0.0.1:8080/json/` and the
  event log instead; do not block the run on UI access.

**Hard rule.** If cross-host executor registration or shuffle fetch
cannot be established with the knobs above, STOP: preserve every log
and partial output on the volume, terminate all pods, and report the
platform block precisely. **Never** fall back to `local[*]`,
`local-cluster`, or extra worker processes on one pod and present the
result as multi-host.
