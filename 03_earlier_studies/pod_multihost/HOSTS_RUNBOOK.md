# HOSTS RUNBOOK — Blind at Scale multi-host ground truth on ordinary Linux hosts

The generic-hosts variant: run the cluster on **3 ordinary Linux
machines** (a teammate's servers, lab machines, cloud VMs — anything
with SSH/terminal access). No RunPod, no shared volume assumed.
Everything else — scripts, gates, thresholds, labels — is identical to
the RunPod path.

Topology: **Host A** = Spark master + driver · **Host B** = worker ·
**Host C** = worker. Spark 3.5.3 standalone, 9 pipelines, zipf seed-101
at 10M rows, verdict threshold **T = 200,000** (fixed — do not change).

## 1. Prerequisites (every host)

- Ubuntu 22.04-class Linux (Debian family expected by the installer;
  other distros: install OpenJDK 17 yourself and the script will skip
  its apt step), **≥ 8 vCPU / 16 GB RAM**, ≥ 40 GB free disk.
- A user with **sudo** (the setup script installs OpenJDK 17 via apt if
  Java 17 is missing, and downloads the Spark 3.5.3 tarball from
  apache.org mirrors — outbound HTTPS required for both).
- Python ≥ 3.12 available for the pinned interpreter (the scripts
  refuse anything older and refuse numpy ≠ 2.5.4; on a bare machine,
  create it once: `python3.12 -m venv ~/bas-venv` — or any ≥3.12 venv —
  and set `PY=` to it in `cluster.env`).
- The package (this directory) copied to the **same path on all 3
  hosts**, e.g. `~/bas-mh/`.

## 2. Ports that must be open BETWEEN the hosts

No NAT here: services bind and advertise the same numbers. Open these
in any host firewall / security group (they only need to be reachable
**from the other two hosts**, not from the internet):

| Port | From → To | Purpose |
|---|---|---|
| 7077/tcp | B, C → A | Spark master RPC (worker registration) |
| 41000/tcp | B, C → A | Driver RPC (executors connect back to the driver) |
| 42000/tcp | A, B, C → A, B, C (all pairs) | Block manager — **shuffle block fetch; this is the cross-host traffic that matters** |
| 7177/tcp | A → B, A → C | Worker daemon (master launches executors) |
| 8080/tcp | admin → A | Master web UI (optional, inspection only) |
| 8081/tcp | admin → B, C | Worker web UIs (optional) |

Quick check from each host before setup (example from B):
`bash -c 'exec 3<>/dev/tcp/<A-ip>/7077'` after A's master is up.

## 3. cluster.env

On each host, in the package directory: `cp cluster.env.template
cluster.env`, then fill **identically on all 3 hosts except `ROLE`**:

- `ROLE=` → `master` (A), `worker-b` (B), `worker-c` (C)
- `MASTER_PUBLIC_HOST` / `WORKER_B_PUBLIC_HOST` / `WORKER_C_PUBLIC_HOST`
  → the IP or DNS name the other hosts use to reach each machine.
- Every `EXT_*` port = its internal counterpart (7077, 41000, 42000,
  42000, 7177, 7177, 8080, 8081, 8081) — no NAT on generic hosts.
- `BAS_LABEL` → use the second (commented) line:
  `"Spark standalone cluster across 3 hosts (1 master/driver + 2 workers)"`
- `MH_ROOT` → the dataset/results root (see §4): a shared mount path,
  or the same local path on every host, e.g. `/data/blind-multihost`.
- `PY=` → your ≥3.12 venv python (same path on all hosts), if the
  auto-detection doesn't find one.

## 4. Dataset placement

The driver (Host A) generates the dataset; all hosts must read
**identical** parquet from the **same path** (`$MH_ROOT/data/zipf_s101`).

- **Option 1 — shared mount (NFS etc.):** point `MH_ROOT` at the mount
  on all hosts. Nothing else to do; `run_master.sh` generates in place.
- **Option 2 — local copies:** let `run_master.sh` generate on A (step
  §5), then copy to B and C at the identical path BEFORE the pipelines
  start — practically: run the generation phase by starting
  `run_master.sh` on A only after the copy step is inserted, or
  pre-generate standalone:
  ```bash
  # on A
  $PY gen_on_pod.py --family zipf --seed 101 --n-fact 10000000 \
      --n-fact2 6000000 --outdir $MH_ROOT/data/zipf_s101
  # then, to B and C:
  rsync -a $MH_ROOT/data/zipf_s101/ <host>:$MH_ROOT/data/zipf_s101/
  ```
  (`run_master.sh` reuses an existing dataset whose counts summary
  says PASS, so pre-generation + copy is fully supported.)

**Gate (non-negotiable):** `counts_summary.json` in the dataset must
show `"headline_expected_check": "PASS"` and
`counts_sha256 = f9f3fa8b29356e870df37e292f755a03d20fdcd016484d79dbcced148dcb528f`.
The generator enforces this on A and aborts otherwise. After copying,
confirm integrity on B and C: `sha256sum $MH_ROOT/data/zipf_s101/fact.parquet`
must be identical on all 3 hosts. If the SHA gate fails, the usual
cause is the wrong numpy (must be **2.5.4**) or wrong interpreter —
fix the environment, never the check.

## 5. Run — the three role commands

```bash
# Host A (master + driver)
cd ~/bas-mh && bash setup_multihost.sh        # installs, starts master
# Host B (worker)
cd ~/bas-mh && bash setup_multihost.sh        # probes master, starts worker
# Host C (worker)
cd ~/bas-mh && bash setup_multihost.sh
```

Gates before continuing: on B and C, `bash run_worker.sh` prints
`registered: ... state=ALIVE`; on A,
`curl -s http://127.0.0.1:8080/json/` shows `aliveworkers: 2` on
**distinct hosts**.

```bash
# Host A only — generation (if not pre-done), pipelines, collect:
cd ~/bas-mh && bash run_master.sh
# Hosts B and C, after the run:
cd ~/bas-mh && bash run_worker.sh --archive
```

`run_master.sh` submits against the cluster, verifies from the event
log that executors ran on **both** worker hosts, and collects with
`--predictions staging/predictions_main.csv --summary staging/summary.json --t-load 200000`
(explicit threshold; the comparison section of the output JSON must
show totals of 9, not 0). F_A/F_B reporting
`SKIPPED_NOT_MATERIALISABLE` is expected by design (their raw join
outputs are ~5.5×10¹¹ and ~1.8×10¹⁰ rows).

## 6. Archive — what constitutes the result

Everything under `$MH_ROOT` on Host A (results_out/, eventlog/,
STATUS.txt, run_manifest, logs_master/, data counts summary) **plus**
`logs_worker-b/` and `logs_worker-c/` (from `--archive`; with local
copies, collect those two directories from B and C) **plus** each
host's setup/run terminal output. Tar it from A:

```bash
tar czf blind_multihost_run_$(date +%Y%m%d).tgz -C "$(dirname "$MH_ROOT")" "$(basename "$MH_ROOT")"
```

If cross-host shuffle or registration fails: do **not** work around it
by running everything on one host. Send back the logs (worker/master
logs under `~/bas-spark-mh/logs/` on each host + STATUS.txt) — see
TEAMMATE_HANDOFF.md.
