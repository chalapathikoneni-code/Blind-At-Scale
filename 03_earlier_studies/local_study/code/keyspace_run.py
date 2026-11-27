"""Key-space scaling runs for the Blind at Scale program (WORKSTREAM 3).

Answers hostile-review item 7.2.6 / N4: what happens when the dimension /
key space is NOT 20,000, with the synopsis heavy-hitter count k = 1024
fixed (so k/|keys| shrinks: 5.12% @20k -> 0.512% @200k -> 0.1024% @1M).

This script REUSES the existing modules (common / gen_dataset / arms)
without changing their defaults: the module-level K constant is patched
in-process (common.K, gen_dataset.K, arms.K) before any function that
reads it is called. Existing data/results are never written by this
script; keyspace datasets go to data/keyspace_<config>_<family>_s<seed>/
and scoring output to results/keyspace/.

Stages (each stage is meant to run in a fresh process for memory hygiene;
--stage all spawns the gen and run stages as subprocesses):
  gen   : materialise one dataset at the requested K (parquet + DuckDB
          SQL cross-validation, identical checks to gen_dataset.py).
  run   : synopsis + arms -> predictions_keyspace.csv (same schema as
          run_dataset.py's predictions CSV).
  score : pool all keyspace datasets -> results/keyspace/
          keyspace_summary.json + keyspace_table.md, under TWO threshold
          views (see below).

Threshold views (both recomputed from the recorded loads, which are
threshold-independent):
  fixed200k : T_load = 200,000 rows (the preregistration's absolute
              threshold), T_distinct = 50,000.
  norm002   : T_load = 0.02 * N_fact (the preregistration's relative
              form; identical to fixed200k when N_fact = 10M, = 100,000
              for the reduced K1M fact size of 5M), T_distinct = 50,000.
The stored gt/pred verdict strings in the predictions CSV use the
program's common.verdict (norm002 form), exactly as run_dataset.py.
"""
import argparse, csv, json, os, subprocess, sys, time
import numpy as np

BASE = os.path.expanduser("~/workspace/blind-at-scale")
NULL_SENTINEL = -1
ARMS_ALL = ["A1", "A2", "A3", "A4", "A5", "A8"]
T_DISTINCT = 50_000
T_LOAD_FIXED = 200_000


def _patch(Kval):
    import common
    common.K = Kval
    import gen_dataset as gd
    gd.K = Kval
    import arms as arms_mod
    arms_mod.K = Kval
    return common, gd, arms_mod


# ---------------- generation (mirrors gen_dataset.main, K-parameterised) --
def stage_gen(a):
    common, gd, _ = _patch(a.K)
    import pandas as pd
    import duckdb
    os.makedirs(a.outdir, exist_ok=True)
    t0 = time.time()
    rng = np.random.default_rng(a.seed)
    rng2 = np.random.default_rng(a.seed + 777)
    fam, n, n2, K = a.family, a.n_fact, a.n_fact2, a.K
    pnull = gd.null_prob(fam)

    fact_counts, fact2_counts = {}, {}
    for col in ("kA", "kB", "kC"):
        fact_counts[col] = gd.draw_counts(rng, fam, col, n)
    nonnull = int(round(n * (1 - pnull)))
    fact_counts["kN"] = gd.draw_counts(rng, fam, "kN", nonnull)
    null_count = int(n - nonnull)
    for col in ("kA", "kB"):
        fact2_counts[col] = gd.draw_counts(rng2, fam, col, n2)

    kn_rows = np.concatenate([
        gd.rows_from_counts(rng, fact_counts["kN"]),
        np.full(null_count, NULL_SENTINEL, dtype=np.int32)]).astype(np.int32)
    rng.shuffle(kn_rows)
    fact = pd.DataFrame({
        "kA": gd.rows_from_counts(rng, fact_counts["kA"]),
        "kB": gd.rows_from_counts(rng, fact_counts["kB"]),
        "kC": gd.rows_from_counts(rng, fact_counts["kC"]),
        "kN": kn_rows,
        "s": rng.integers(0, 100_000, size=n, dtype=np.int32),
        "v": rng.random(n, dtype=np.float32),
    })
    fact2 = pd.DataFrame({
        "kA": gd.rows_from_counts(rng2, fact2_counts["kA"]),
        "kB": gd.rows_from_counts(rng2, fact2_counts["kB"]),
    })
    dim = pd.DataFrame({"dim_key": np.arange(K, dtype=np.int32),
                        "payload": rng.integers(0, 1000, size=K, dtype=np.int32)})
    gen_s = time.time() - t0

    con = duckdb.connect(os.path.join(a.outdir, "data.duckdb"))
    con.execute(f"SET threads={a.duckdb_threads}")
    con.execute(f"SET memory_limit='{a.duckdb_memory}'")
    fp = os.path.join(a.outdir, "fact.parquet")
    f2p = os.path.join(a.outdir, "fact2.parquet")
    dp = os.path.join(a.outdir, "dim.parquet")
    gd.write_parquet(con, fact, fp)
    gd.write_parquet(con, fact2, f2p)
    gd.write_parquet(con, dim, dp)
    del fact, fact2, dim
    con.execute(f"CREATE VIEW fact AS SELECT * FROM '{fp}'")
    con.execute(f"CREATE VIEW fact2 AS SELECT * FROM '{f2p}'")
    con.execute(f"CREATE VIEW dim AS SELECT * FROM '{dp}'")

    t1 = time.time()
    checks = {}
    gt_fact = {}
    for col in ("kA", "kB", "kC", "kN"):
        df = con.execute(
            f"SELECT {col} AS k, COUNT(*) AS c FROM fact GROUP BY {col}").df()
        arr = np.zeros(K, dtype=np.int64)
        nn = df[df["k"] >= 0]
        arr[nn["k"].to_numpy(dtype=np.int64)] = nn["c"].to_numpy(dtype=np.int64)
        gt_fact[col] = arr
        checks[f"counts_{col}"] = int(np.abs(arr - fact_counts[col]).max())
        if col == "kN":
            nrow = df[df["k"] == NULL_SENTINEL]
            checks["null_count"] = int(
                abs((int(nrow["c"].iloc[0]) if len(nrow) else 0) - null_count))
    gt_fact2 = {}
    for col in ("kA", "kB"):
        df = con.execute(
            f"SELECT {col} AS k, COUNT(*) AS c FROM fact2 GROUP BY {col}").df()
        arr = np.zeros(K, dtype=np.int64)
        arr[df["k"].to_numpy(dtype=np.int64)] = df["c"].to_numpy(dtype=np.int64)
        gt_fact2[col] = arr
        checks[f"counts_fact2_{col}"] = int(np.abs(arr - fact2_counts[col]).max())
    for col in ("kA", "kB", "kN"):
        df = con.execute(
            f"SELECT f.{col} AS k, COUNT(*) AS c FROM fact f JOIN dim d "
            f"ON f.{col} = d.dim_key GROUP BY f.{col}").df()
        arr = np.zeros(K, dtype=np.int64)
        arr[df["k"].to_numpy(dtype=np.int64)] = df["c"].to_numpy(dtype=np.int64)
        checks[f"join_counts_{col}"] = int(np.abs(arr - gt_fact[col]).max())
    df = con.execute("SELECT kA AS k, COUNT(DISTINCT s) AS d FROM fact GROUP BY kA").df()
    distinct_A = np.zeros(K, dtype=np.int64)
    distinct_A[df["k"].to_numpy(dtype=np.int64)] = df["d"].to_numpy(dtype=np.int64)
    gt_s = time.time() - t1
    con.close()

    ok = all(v == 0 for v in checks.values())
    np.savez_compressed(
        os.path.join(a.outdir, "counts.npz"),
        **{f"fact_{c}": gt_fact[c] for c in ("kA", "kB", "kC", "kN")},
        **{f"fact2_{c}": gt_fact2[c] for c in ("kA", "kB")},
        distinct_A=distinct_A, null_count=np.int64(null_count))
    with open(os.path.join(a.outdir, "validation.json"), "w") as f:
        json.dump({"checks_max_abs_diff": checks, "all_exact": ok}, f, indent=1)
    with open(os.path.join(a.outdir, "meta.json"), "w") as f:
        json.dump({"family": fam, "seed": a.seed, "n_fact": n, "n_fact2": n2,
                   "K": K, "top_k": 1024, "k_over_K": 1024 / K,
                   "p_null": pnull, "gen_seconds": round(gen_s, 2),
                   "ground_truth_sql_seconds": round(gt_s, 2),
                   "duckdb_threads": a.duckdb_threads,
                   "duckdb_memory_limit": a.duckdb_memory,
                   "validation_ok": ok}, f, indent=1)
    print(f"KEYSPACE-GEN {os.path.basename(a.outdir)} K={K} {fam} seed={a.seed} "
          f"n={n}: validation_ok={ok} gen={gen_s:.1f}s gt={gt_s:.1f}s", flush=True)
    if not ok:
        raise SystemExit(f"VALIDATION FAILED: {checks}")


# ---------------- arms ------------------------------------------------------
def stage_run(a):
    common, _, arms_mod = _patch(a.K)
    from common import PIPELINES
    d = a.outdir
    with open(os.path.join(d, "meta.json")) as f:
        meta = json.load(f)
    assert meta["K"] == a.K, (meta["K"], a.K)
    z = np.load(os.path.join(d, "counts.npz"))
    counts = {k: z[k] for k in z.files}
    counts["null_count"] = int(counts["null_count"])
    t0 = time.time()
    syn = arms_mod.build_synopsis(counts)
    syn_path = os.path.join(d, "synopsis.json")
    with open(syn_path, "w") as f:
        json.dump(syn, f)
    syn_bytes = os.path.getsize(syn_path)
    arms = a.arms.split(",")
    gt_loads, gt_verd, results = arms_mod.run_arms(
        counts, syn, meta["seed"], arms, r=a.r)
    out = os.path.join(d, "predictions_keyspace.csv")
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["dataset", "family", "seed", "n_fact", "pipeline",
                    "gt_load", "gt_verdict", "arm", "pred_load",
                    "pred_verdict", "testbed_fact_rows", "r",
                    "synopsis_bytes"])
        tag = os.path.basename(d.rstrip("/"))
        for pid, _, _ in PIPELINES:
            for arm in arms:
                res = results[arm]
                w.writerow([tag, meta["family"], meta["seed"], meta["n_fact"],
                            pid, f"{gt_loads[pid]:.1f}", gt_verd[pid], arm,
                            f"{res['pred_loads'][pid]:.1f}",
                            res["pred_verdicts"][pid],
                            res["testbed_fact_rows"], a.r, syn_bytes])
    print(f"KEYSPACE-RUN {tag} K={a.K} r={a.r} arms={arms}: "
          f"done in {time.time()-t0:.1f}s synopsis_bytes={syn_bytes} -> {out}",
          flush=True)


# ---------------- scoring ---------------------------------------------------
def _verdict(load, pid, view, n_fact):
    if pid == "D_A":
        t = T_DISTINCT
    elif view == "fixed200k":
        t = T_LOAD_FIXED
    else:  # norm002
        t = 0.02 * n_fact
    return "FAIL" if load > t else "PASS"


def _prf(pairs):
    tp = sum(1 for g, p in pairs if g == "FAIL" and p == "FAIL")
    fp = sum(1 for g, p in pairs if g == "PASS" and p == "FAIL")
    fn = sum(1 for g, p in pairs if g == "FAIL" and p == "PASS")
    tn = sum(1 for g, p in pairs if g == "PASS" and p == "PASS")
    rec = tp / (tp + fn) if tp + fn else float("nan")
    prec = tp / (tp + fp) if tp + fp else float("nan")
    f1 = (2 * rec * prec / (rec + prec)) if (rec + prec) else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn, "recall": rec,
            "precision": prec, "f1": f1, "n": len(pairs)}


def stage_score(a):
    from scipy import stats
    data_root = os.path.join(BASE, "data")
    tags = sorted(t for t in os.listdir(data_root)
                  if t.startswith("keyspace_")
                  and os.path.exists(os.path.join(
                      data_root, t, "predictions_keyspace.csv")))
    if not tags:
        raise SystemExit("no keyspace datasets with predictions found")
    # instance table: (dataset, pipeline) -> gt/pred loads per arm
    inst = []
    ds_meta = {}
    for tag in tags:
        with open(os.path.join(data_root, tag, "meta.json")) as f:
            meta = json.load(f)
        with open(os.path.join(data_root, tag, "validation.json")) as f:
            val = json.load(f)
        syn_bytes = os.path.getsize(os.path.join(data_root, tag, "synopsis.json"))
        cfg = {200_000: "K200", 1_000_000: "K1M"}.get(
            meta["K"], f"K{meta['K']}")
        ds_meta[tag] = {"K": meta["K"], "family": meta["family"],
                        "seed": meta["seed"], "n_fact": meta["n_fact"],
                        "n_fact2": meta["n_fact2"],
                        "config": cfg,
                        "k_over_K": 1024 / meta["K"],
                        "synopsis_bytes": syn_bytes,
                        "validation_all_exact": val["all_exact"]}
        per_pid = {}
        with open(os.path.join(data_root, tag, "predictions_keyspace.csv")) as f:
            for r in csv.DictReader(f):
                e = per_pid.setdefault(r["pipeline"], {
                    "dataset": tag, "config": cfg, "K": meta["K"],
                    "family": meta["family"], "seed": meta["seed"],
                    "n_fact": meta["n_fact"], "pipeline": r["pipeline"],
                    "gt_load": float(r["gt_load"])})
                e[r["arm"]] = float(r["pred_load"])
        inst.extend(per_pid.values())
    arms = sorted({k for e in inst for k in e
                   if k.startswith("A") and k[1:].isdigit()})
    JF = ("J_A", "J_B", "J_N", "F_A", "F_B")
    GDW = ("G_A", "G_B", "G_C", "G_N", "D_A", "W_A")
    _k_of = {e["config"]: e["K"] for e in inst}
    configs = sorted({e["config"] for e in inst}, key=lambda c: _k_of[c])
    S = {"datasets": ds_meta, "arms": arms, "configs": {}}
    for cfg in configs:
        sub = [e for e in inst if e["config"] == cfg]
        entry = {"K": sub[0]["K"], "n_instances": len(sub),
                 "datasets": sorted({e["dataset"] for e in sub}),
                 "synopsis_bytes": {t: ds_meta[t]["synopsis_bytes"]
                                    for t in sorted({e["dataset"] for e in sub})},
                 "k_over_K": 1024 / sub[0]["K"], "views": {}}
        for view in ("fixed200k", "norm002"):
            v = {"n_gt_fail": sum(1 for e in sub if _verdict(
                e["gt_load"], e["pipeline"], view, e["n_fact"]) == "FAIL"),
                "arms": {}, "recall_JF": {}, "recall_GDW": {},
                "recall_by_pipeline": {}}
            for arm in arms:
                pairs = [(_verdict(e["gt_load"], e["pipeline"], view, e["n_fact"]),
                          _verdict(e[arm], e["pipeline"], view, e["n_fact"]))
                         for e in sub]
                m = _prf(pairs)
                loads = np.array([e[arm] for e in sub])
                gt = np.array([e["gt_load"] for e in sub])
                m["spearman_rho"] = float(stats.spearmanr(
                    np.log10(np.maximum(loads, 1e-9)),
                    np.log10(np.maximum(gt, 1e-9))).statistic)
                q = np.maximum(loads / gt, gt / loads)
                m["median_q_error"] = float(np.median(q))
                v["arms"][arm] = m
                for name, pids in (("JF", JF), ("GDW", GDW)):
                    pp = [(_verdict(e["gt_load"], e["pipeline"], view, e["n_fact"]),
                           _verdict(e[arm], e["pipeline"], view, e["n_fact"]))
                          for e in sub if e["pipeline"] in pids]
                    v["recall_JF" if name == "JF" else "recall_GDW"][arm] = _prf(pp)
                v["recall_by_pipeline"][arm] = {
                    pid: _prf([(_verdict(e["gt_load"], pid, view, e["n_fact"]),
                                _verdict(e[arm], pid, view, e["n_fact"]))
                               for e in sub if e["pipeline"] == pid])
                    for pid in sorted({e["pipeline"] for e in sub})}
            entry["views"][view] = v
        # per-dataset detail (fixed200k view verdicts are per-dataset n_fact
        # independent; record gt loads for breakage inspection)
        entry["gt_loads"] = {f"{e['dataset']}|{e['pipeline']}": e["gt_load"]
                             for e in sub}
        entry["pred_loads"] = {arm: {f"{e['dataset']}|{e['pipeline']}": e[arm]
                                     for e in sub} for arm in arms}
        S["configs"][cfg] = entry
    outdir = os.path.join(BASE, "results", "keyspace")
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "keyspace_summary.json"), "w") as f:
        json.dump(S, f, indent=1)

    def fmt(x):
        return "nan" if x != x else f"{x:.3f}"
    lines = ["# Key-space scaling -- headline table (WORKSTREAM 3)",
             "",
             "Synopsis k = 1024 fixed. Threshold views: **fixed200k** "
             "T_load = 200,000 (preregistration absolute); **norm002** "
             "T_load = 0.02·N_fact (identical to fixed200k at N_fact = 10M; "
             "100,000 at the reduced K1M N_fact = 5M). T_distinct = 50,000 "
             "in both views. FAIL is the positive class.",
             ""]
    for cfg in configs:
        e = S["configs"][cfg]
        syn = e["synopsis_bytes"]
        lines += [f"## Config {cfg} (K = {e['K']:,}, k/K = {e['k_over_K']:.4%})",
                  "",
                  f"Datasets: {', '.join(e['datasets'])}; instances: "
                  f"{e['n_instances']}; synopsis bytes: "
                  + ", ".join(f"{t}={b:,}" for t, b in syn.items()),
                  ""]
        for view in ("fixed200k", "norm002"):
            v = e["views"][view]
            lines += [f"### View: {view} (ground-truth FAILs: {v['n_gt_fail']}"
                      f"/{e['n_instances']})",
                      "",
                      "| arm | recall | precision | F1 | Spearman ρ | median Q-err | recall J/F | recall G/D/W |",
                      "|---|---|---|---|---|---|---|---|"]
            for arm in arms:
                m = v["arms"][arm]
                lines.append(
                    f"| {arm} | {fmt(m['recall'])} | {fmt(m['precision'])} | "
                    f"{fmt(m['f1'])} | {fmt(m['spearman_rho'])} | "
                    f"{fmt(m['median_q_error'])} | "
                    f"{fmt(v['recall_JF'][arm]['recall'])} | "
                    f"{fmt(v['recall_GDW'][arm]['recall'])} |")
            lines.append("")
    with open(os.path.join(outdir, "keyspace_table.md"), "w") as f:
        f.write("\n".join(lines))
    print(f"KEYSPACE-SCORE configs={configs} arms={arms} -> {outdir}", flush=True)
    print(json.dumps({c: {vw: {arm: S["configs"][c]["views"][vw]["arms"][arm]
                               for arm in arms}
                          for vw in ("fixed200k", "norm002")}
                      for c in configs}, indent=1, default=str))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", default="all",
                    choices=["gen", "run", "score", "all"])
    ap.add_argument("--K", type=int, default=200_000)
    ap.add_argument("--family", default="zipf")
    ap.add_argument("--seed", type=int, default=101)
    ap.add_argument("--n-fact", type=int, default=10_000_000)
    ap.add_argument("--n-fact2", type=int, default=6_000_000)
    ap.add_argument("--outdir", default=None)
    ap.add_argument("--arms", default=",".join(ARMS_ALL))
    ap.add_argument("--r", type=float, default=0.01)
    ap.add_argument("--duckdb-memory", default="2GB")
    ap.add_argument("--duckdb-threads", type=int, default=2)
    a = ap.parse_args()
    if a.stage == "score":
        stage_score(a)
        return
    if a.outdir is None:
        raise SystemExit("--outdir required for gen/run/all")
    if a.stage == "gen":
        stage_gen(a)
    elif a.stage == "run":
        stage_run(a)
    else:  # all: fresh process per stage
        base = [sys.executable, os.path.abspath(__file__),
                "--K", str(a.K), "--family", a.family, "--seed", str(a.seed),
                "--n-fact", str(a.n_fact), "--n-fact2", str(a.n_fact2),
                "--outdir", a.outdir, "--arms", a.arms, "--r", str(a.r),
                "--duckdb-memory", a.duckdb_memory,
                "--duckdb-threads", str(a.duckdb_threads)]
        if not os.path.exists(os.path.join(a.outdir, "validation.json")):
            subprocess.run(base + ["--stage", "gen"], check=True)
        subprocess.run(base + ["--stage", "run"], check=True)


if __name__ == "__main__":
    main()
