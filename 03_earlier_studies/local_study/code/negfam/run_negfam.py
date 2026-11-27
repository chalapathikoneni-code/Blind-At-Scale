"""Per-dataset driver for the negative family (Amendment 4).

Ground truth + A1-NF come from real DuckDB SQL aggregations over the
materialised parquet (LENGTH(payload)); A5-NF/A8-NF use ONLY the key-count
synopsis (existing arms.py reconstruction machinery) with the declared
row-load verdict mapping; A4-NF is independent-marginal synthesis in numpy.
"""
import argparse, csv, json, os, sys, time
import numpy as np
import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from common import K, max_partition_load  # noqa: E402
from arms import _reconstruct_col  # noqa: E402

PIPES = ["V1", "V2", "V3", "V4"]
THRESH = {"V1": 4_000_000.0, "V2": 10_000_000.0,
          "V3": 50_000_000.0, "V4": 50_000_000_000.0}
T_ROW = 100_000.0  # 0.02 * N, main study Section 3 row threshold
ARMS = ["A8-NF", "A5-NF", "A1-NF", "A4-NF"]


def verdict(load, pipe):
    return "FAIL" if load > THRESH[pipe] else "PASS"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-dir", required=True)
    args = ap.parse_args()
    d = args.dataset_dir
    with open(os.path.join(d, "meta.json")) as f:
        meta = json.load(f)
    seed, n = meta["seed"], meta["n_fact"]
    z = np.load(os.path.join(d, "counts.npz"))
    true_counts = z["fact_k"].astype(np.int64)
    t0 = time.time()

    con = duckdb.connect()
    con.execute("PRAGMA threads=2")
    con.execute("SET memory_limit='2GB'")
    fp = os.path.join(d, "fact.parquet")
    dp = os.path.join(d, "dim.parquet")
    con.execute(f"CREATE VIEW fact AS SELECT * FROM '{fp}'")
    con.execute(f"CREATE VIEW dim AS SELECT * FROM '{dp}'")

    def per_key(where=""):
        df = con.execute(
            f"SELECT k, COUNT(*) AS c, SUM(LENGTH(payload)) AS b, "
            f"SUM(CAST(LENGTH(payload) AS BIGINT) * CAST(LENGTH(payload) AS BIGINT)) AS cost "
            f"FROM fact {where} GROUP BY k").df()
        c = np.zeros(K, dtype=np.float64)
        b = np.zeros(K, dtype=np.float64)
        co = np.zeros(K, dtype=np.float64)
        kk = df["k"].to_numpy(dtype=np.int64)
        c[kk] = df["c"].to_numpy(dtype=np.float64)
        b[kk] = df["b"].to_numpy(dtype=np.float64)
        co[kk] = df["cost"].to_numpy(dtype=np.float64)
        return c, b, co

    def join_per_key(where=""):
        df = con.execute(
            f"SELECT f.k AS k, SUM(LENGTH(f.payload) + LENGTH(d.dpayload)) AS jb "
            f"FROM fact f JOIN dim d ON f.k = d.dim_key {where} GROUP BY f.k").df()
        jb = np.zeros(K, dtype=np.float64)
        jb[df["k"].to_numpy(dtype=np.int64)] = df["jb"].to_numpy(dtype=np.float64)
        return jb

    def sprefix_max(where=""):
        df = con.execute(
            f"SELECT sprefix, SUM(LENGTH(payload)) AS b FROM fact {where} "
            f"GROUP BY sprefix").df()
        i = int(np.argmax(df["b"].to_numpy(dtype=np.float64)))
        return float(df["b"].iloc[i]), str(df["sprefix"].iloc[i])

    # ---------------- ground truth (SQL) ----------------
    cnt, byt, cost = per_key()
    jb = join_per_key()
    v3_load, v3_arg = sprefix_max()
    assert np.abs(cnt - true_counts).max() == 0, "GT counts != drawn counts"
    assert np.abs(jb - (byt + 48.0 * cnt)).max() == 0, "join bytes identity failed"
    gt_loads = {"V1": float(byt.max()), "V2": max_partition_load(jb),
                "V3": v3_load, "V4": max_partition_load(cost)}
    gt = {p: verdict(gt_loads[p], p) for p in PIPES}

    # ---------------- A1-NF: uniform 1-in-100 sample, payload retained ----
    w = "WHERE rid % 100 = 0"
    sc, sb, sco = per_key(w)
    sjb = join_per_key("WHERE f.rid % 100 = 0")
    sv3, _ = sprefix_max(w)
    a1_loads = {"V1": float(sb.max()) * 100.0,
                "V2": max_partition_load(sjb) * 100.0,
                "V3": sv3 * 100.0,
                "V4": max_partition_load(sco) * 100.0}
    n_sample = int(sc.sum())

    con.close()

    # ---------------- synopsis (key counts ONLY) + A5/A8-NF ----------------
    order = np.argsort(-true_counts, kind="stable")
    hh_keys, hh_counts = order[:1024], true_counts[order[:1024]]
    syn_col = {"total": int(true_counts.sum()),
               "cardinality": int((true_counts > 0).sum()),
               "hh_keys": hh_keys.tolist(), "hh_counts": hh_counts.tolist(),
               "tail_mass": int(true_counts.sum() - hh_counts.sum())}
    syn = {"dim_rows": K, "cols": {"k": syn_col}}
    with open(os.path.join(d, "negfam_synopsis.json"), "w") as f:
        json.dump(syn, f)
    syn_loads = {}
    for arm, r in (("A8-NF", None), ("A5-NF", 0.01)):
        rng = np.random.default_rng(seed * 1000 + 5)
        v = _reconstruct_col(syn_col, r=r, rng=rng)
        if r is not None:
            v = v / r
        syn_loads[arm] = {"V1": float(v.max()), "V2": max_partition_load(v),
                          "V3": float(v.max()), "V4": max_partition_load(v)}

    # ---------------- A4-NF: independent marginal synthesis ----------------
    zm = np.load(os.path.join(d, "marginals.npz"))
    plen_emp = zm["plen"]
    sg_counts = zm["sg_counts"].astype(np.float64)
    rng4 = np.random.default_rng(seed + 4242)
    key_counts_s = rng4.multinomial(n, true_counts / true_counts.sum())
    key_s = np.repeat(np.arange(K, dtype=np.int32), key_counts_s)
    rng4.shuffle(key_s)
    len_s = rng4.choice(plen_emp, size=n, replace=True).astype(np.float64)
    sg_counts_s = rng4.multinomial(n, sg_counts / sg_counts.sum())
    sg_s = np.repeat(np.arange(10_001, dtype=np.int32), sg_counts_s)
    rng4.shuffle(sg_s)
    b_key = np.bincount(key_s, weights=len_s, minlength=K)
    jb_key = np.bincount(key_s, weights=len_s + 48.0, minlength=K)
    co_key = np.bincount(key_s, weights=len_s * len_s, minlength=K)
    b_sg = np.bincount(sg_s, weights=len_s, minlength=10_001)
    a4_loads = {"V1": float(b_key.max()), "V2": max_partition_load(jb_key),
                "V3": float(b_sg.max()), "V4": max_partition_load(co_key)}
    del key_s, len_s, sg_s, plen_emp

    # ---------------- write outputs ----------------
    preds = {}
    for arm in ("A8-NF", "A5-NF"):
        preds[arm] = {p: ("FAIL" if syn_loads[arm][p] > T_ROW else "PASS")
                      for p in PIPES}
        preds[arm + "_loads"] = syn_loads[arm]
    preds["A1-NF"] = {p: verdict(a1_loads[p], p) for p in PIPES}
    preds["A1-NF_loads"] = a1_loads
    preds["A4-NF"] = {p: verdict(a4_loads[p], p) for p in PIPES}
    preds["A4-NF_loads"] = a4_loads

    out = os.path.join(d, "predictions_negfam.csv")
    with open(out, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["dataset", "seed", "n_fact", "pipeline", "gt_load",
                     "gt_verdict", "threshold", "arm", "pred_load",
                     "pred_verdict"])
        tag = os.path.basename(d.rstrip("/"))
        for p in PIPES:
            for arm in ARMS:
                wr.writerow([tag, seed, n, p, f"{gt_loads[p]:.1f}", gt[p],
                             f"{THRESH[p]:.1f}", arm,
                             f"{preds[arm + '_loads'][p]:.1f}", preds[arm][p]])
    gt_info = {"gt_loads": gt_loads, "gt_verdicts": gt, "thresholds": THRESH,
               "t_row": T_ROW, "v3_argmax_sprefix": v3_arg,
               "total_payload_bytes": float(byt.sum()),
               "total_join_bytes": float(jb.sum()),
               "total_cost": float(cost.sum()),
               "max_key_row_count": int(cnt.max()),
               "a1_sample_rows": n_sample,
               "synopsis_bytes": os.path.getsize(os.path.join(d, "negfam_synopsis.json")),
               "seconds": round(time.time() - t0, 2)}
    with open(os.path.join(d, "gt_negfam.json"), "w") as f:
        json.dump(gt_info, f, indent=1)
    print(json.dumps({"dataset": tag, "gt": gt, "gt_loads": gt_loads,
                      "preds": {a: preds[a] for a in ARMS},
                      "a1_loads": a1_loads, "a4_loads": a4_loads,
                      "syn_loads": syn_loads, "sample_rows": n_sample}, indent=1))


if __name__ == "__main__":
    main()
