"""Collect Snowflake ground truth into the collect_results.py GT JSON schema.

Assembles, LOCALLY, the ground-truth JSON that shared/collect_cloud.py
consumes for the S-XENG / S-SIZE arms. Inputs are all local files the
operator downloaded with sf_driver.py:

  --profiles      sf_driver.py profile output (04_profiles.sql results)
  --res           sf_driver.py dump-res output (RUN.RES_* tables)
  --f-products    dir with F_A_products.csv / F_B_products.csv
                  (downloaded via sf_driver.py get-counts --stage-path
                  .../f_products/)
  --counts-npz    local data/zipf_s101/counts.npz (expected data-level
                  values + the SHA anchor; NEVER downloaded counts)
  --counts-export downloaded counts_export.csv (optional; enables the
                  Snowflake-side SHA-256 check)

WHAT THIS SCRIPT COMPUTES LOCALLY (D4 mirror):
- The F analytic splitmix64 load from the Snowflake-produced per-key
  product vector (03_pipelines.sql exports the vector; the analytic
  formula runs HERE, exactly as the pod driver computed it from
  Spark-produced counts). splitmix64 in SQL is error-prone and is
  deliberately not attempted in 03_pipelines.sql.
- The counts SHA-256 from the exported Snowflake counts (when
  --counts-export is given), compared against the local anchor
  f9f3fa8b...28f (PROGRAM_DESIGN.md §6).

LOAD SEMANTICS (mixed -- stated explicitly, never silently pooled):
- G/J/W pipelines: gt_load is TOTAL_ELAPSED_TIME (ms) from
  QUERY_HISTORY -- a COST metric, NOT a row count. Verdicts come from
  the pre-registered remote-spill proxy (04_profiles.sql), NOT from
  load > T. Per-pipeline "load_semantics": "proxy-cost".
- F pipelines: gt_load is the D4 analytic splitmix64 load (a row
  count); verdicts come from analytic load > T. Per-pipeline
  "load_semantics": "row-count".
- collect_cloud.py therefore computes verdict agreement + Spearman
  rank correlation for this engine, and NO Q-error (the engine-level
  "engine_load_semantics" field says "proxy" and Q-error requires
  row-count semantics).

DATA-LEVEL CHECKS (PROGRAM_DESIGN.md §4.5): Snowflake-computed max
group counts, join output counts, and per-key product totals must
equal the local GT values (recomputed here from --counts-npz) EXACTLY.
Any mismatch is recorded (data_level_checks_pass = false) and printed
as a VOID warning -- the arm is void per the design, not estimated.
The script still writes the GT JSON for the record.

PROFILE SOURCE: prefers the ACCOUNT_USAGE rows (query 1 of
04_profiles.sql); falls back to the INFORMATION_SCHEMA rows (query 2)
when query 1 is empty (latency). The source used is recorded per
pipeline. A pipeline with NO profile row gets verdict/gt_load None
and a "profile missing -- arm void" note.

Usage:
  python3 collect_sf.py --profiles sf_profiles_*.json --res sf_res_*.json \\
      --f-products ./f_products --counts-npz \\
      ~/workspace/blind-at-scale/data/zipf_s101/counts.npz \\
      --counts-export ./counts/counts_export.csv \\
      --warehouse BAS_SF_WH_XS --out snowflake_ground_truth.json
"""
import argparse, csv, hashlib, json, os, sys

import numpy as np

K = 20_000
P_PARTITIONS = 128
EXPECTED_COUNTS_SHA256 = "f9f3fa8b29356e870df37e292f755a03d20fdcd016484d79dbcced148dcb528f"
PIPES = ["G_A", "G_B", "G_N", "J_A", "J_B", "J_N", "F_A", "F_B", "W_A"]


def splitmix64_hash(keys):
    """Copied from code/common.py -- D4 analytic F load only."""
    z = (keys.astype(np.uint64) + np.uint64(1)).copy()
    z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    z = z ^ (z >> np.uint64(31))
    return z


def max_partition_load(counts, p=P_PARTITIONS):
    keys = np.arange(len(counts), dtype=np.int64)
    parts = (splitmix64_hash(keys) % np.uint64(p)).astype(np.int64)
    loads = np.bincount(parts, weights=np.asarray(counts, dtype=np.float64),
                        minlength=p)
    return float(loads.max())


def fnum(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profiles", required=True)
    ap.add_argument("--res", required=True)
    ap.add_argument("--f-products", required=True)
    ap.add_argument("--counts-npz", required=True)
    ap.add_argument("--counts-export", default=None)
    ap.add_argument("--warehouse", default="BAS_SF_WH_XS")
    ap.add_argument("--t-load", type=float, default=200_000.0)
    ap.add_argument("--dataset", default="zipf_s101")
    ap.add_argument("--arm", default="sxeng")
    ap.add_argument("--labelling", default="Snowflake Enterprise, XS Gen2 "
                    "warehouse -- spill-proxy verdicts; see manifest")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    with open(args.profiles) as f:
        prof_doc = json.load(f)
    with open(args.res) as f:
        res_doc = json.load(f)
    z = np.load(args.counts_npz)
    counts = {k: z[k].astype(np.int64) for k in
              ("fact_kA", "fact_kB", "fact_kC", "fact_kN",
               "fact2_kA", "fact2_kB")}
    n_fact = int(counts["fact_kA"].sum())
    n_nonnull = int(counts["fact_kN"].sum())

    # ---- profile rows: prefer ACCOUNT_USAGE (query 1), else INFO_SCHEMA (2)
    stmts = prof_doc.get("statements", [])
    q1_rows, q2_rows = [], []
    for s in stmts:
        prev = s.get("statement_preview", "")
        if "SNOWFLAKE_VERDICT" in prev and "ACCOUNT_USAGE" in prev:
            q1_rows = s.get("rows", []) or []
        elif "SNOWFLAKE_VERDICT" in prev and "INFORMATION_SCHEMA" in prev:
            q2_rows = s.get("rows", []) or []
    prof_source = "ACCOUNT_USAGE"
    prof_rows = [r for r in q1_rows if r.get("WAREHOUSE_NAME") == args.warehouse]
    if not prof_rows:
        prof_rows = [r for r in q2_rows if r.get("WAREHOUSE_NAME") == args.warehouse]
        prof_source = "INFORMATION_SCHEMA (fallback; ACCOUNT_USAGE empty)"
    prof = {r.get("PIPELINE"): r for r in prof_rows}
    print(f"profile source: {prof_source}; {len(prof)} pipelines profiled "
          f"on {args.warehouse}")

    # ---- RES metrics ----
    res = {}
    for table, rows in res_doc.items():
        pid = table.replace("RES_", "")
        res[pid] = {r.get("METRIC"): {"value": fnum(r.get("VALUE")),
                                      "note": r.get("NOTE")} for r in rows}

    # ---- data-level checks (exact; from local counts.npz) ----
    expected = {
        "G_A": {"max_group_count": float(counts["fact_kA"].max())},
        "G_B": {"max_group_count": float(counts["fact_kB"].max())},
        "G_N": {"max_group_count": float(counts["fact_kN"].max()),
                "null_group_count": float(n_fact - n_nonnull)},
        "J_A": {"join_output_rows": float(n_fact),
                "max_per_key_output": float(counts["fact_kA"].max())},
        "J_B": {"join_output_rows": float(n_fact),
                "max_per_key_output": float(counts["fact_kB"].max())},
        "J_N": {"join_output_rows": float(n_nonnull),
                "max_per_key_output": float(counts["fact_kN"].max())},
        "F_A": {"exact_join_output_rows": float(
            (counts["fact_kA"] * counts["fact2_kA"]).sum())},
        "F_B": {"exact_join_output_rows": float(
            (counts["fact_kB"] * counts["fact2_kB"]).sum())},
        "W_A": {"max_window_partition_rows": float(counts["fact_kA"].max())},
    }
    checks, all_pass = {}, True
    for pid, metrics in expected.items():
        for metric, exp in metrics.items():
            got = (res.get(pid, {}).get(metric) or {}).get("value")
            ok = got is not None and abs(got - exp) == 0
            all_pass = all_pass and ok
            checks[f"{pid}.{metric}"] = {"expected": exp, "actual": got,
                                         "match": bool(ok)}
    if not all_pass:
        print("WARNING: DATA-LEVEL CHECK FAILED -- the arm is VOID per "
              "PROGRAM_DESIGN.md §4.5 (bad upload, not a scientific result).",
              file=sys.stderr)

    # ---- counts SHA check (Snowflake-side counts vs local anchor) ----
    sha_check = {"checked": False}
    if args.counts_export:
        vecs = {}
        with open(args.counts_export, newline="") as f:
            for row in csv.DictReader(f):
                vecs.setdefault(row["VEC"], {})[int(row["K"])] = int(row["CNT"])
        order = ["fact_kA", "fact_kB", "fact_kC", "fact_kN",
                 "fact2_kA", "fact2_kB"]
        h = hashlib.sha256()
        for name in order:
            arr = np.zeros(K, dtype=np.int64)
            for k, v in vecs.get(name, {}).items():
                arr[k] = v
            h.update(arr.astype("<i8").tobytes())
        sha_check = {"checked": True, "counts_sha256": h.hexdigest(),
                     "expected": EXPECTED_COUNTS_SHA256,
                     "match": h.hexdigest() == EXPECTED_COUNTS_SHA256}
        print(f"counts SHA: {h.hexdigest()[:12]}... match={sha_check['match']}")

    # ---- F analytic loads (D4 mirror, computed locally) ----
    f_loads = {}
    for pid, col in (("F_A", "kA"), ("F_B", "kB")):
        path = os.path.join(args.f_products, f"{pid}_products.csv")
        if not os.path.exists(path):
            print(f"WARNING: {path} not found; {pid} analytic load missing",
                  file=sys.stderr)
            f_loads[pid] = None
            continue
        prod = np.zeros(K, dtype=np.float64)
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                prod[int(row["K"])] = float(row["PROD"])
        f_loads[pid] = max_partition_load(prod)
        print(f"{pid} analytic load: {f_loads[pid]:,.0f}")

    # ---- assemble the GT JSON ----
    out_pipes = {}
    for pid in PIPES:
        p = prof.get(pid)
        entry = {"res_metrics": {m: v["value"] for m, v in
                                 res.get(pid, {}).items()},
                 "profile_source": prof_source if p else None}
        if p is not None:
            spill_local = fnum(p.get("BYTES_SPILLED_TO_LOCAL_STORAGE")) or 0.0
            spill_remote = fnum(p.get("BYTES_SPILLED_TO_REMOTE_STORAGE")) or 0.0
            entry["cost_metrics"] = {
                "total_elapsed_ms": fnum(p.get("TOTAL_ELAPSED_TIME")),
                "bytes_scanned": fnum(p.get("BYTES_SCANNED")),
                "bytes_spilled_local": spill_local,
                "bytes_spilled_remote": spill_remote,
                "bytes_spilled_total": spill_local + spill_remote,
                "partitions_scanned": fnum(p.get("PARTITIONS_SCANNED")),
                "partitions_total": fnum(p.get("PARTITIONS_TOTAL")),
                "rows_produced": fnum(p.get("ROWS_PRODUCED")),
                "warehouse_name": p.get("WAREHOUSE_NAME"),
                "warehouse_size": p.get("WAREHOUSE_SIZE"),
                "query_id": p.get("QUERY_ID"),
                "error_code": p.get("ERROR_CODE"),
                "error_message": p.get("ERROR_MESSAGE")}
            entry["snowflake_verdict"] = p.get("SNOWFLAKE_VERDICT")
        if pid in ("F_A", "F_B"):
            # Row-count semantics: D4 analytic load vs T.
            entry["gt_load"] = f_loads[pid]
            entry["gt_load_source"] = ("analytic_splitmix64_from_snowflake_counts "
                                       "(F pipeline; D4 mirror -- computed "
                                       "locally in collect_sf.py)")
            entry["load_semantics"] = "row-count"
            entry["verdict"] = ("FAIL" if f_loads[pid] is not None
                                and f_loads[pid] > args.t_load else
                                "PASS" if f_loads[pid] is not None else None)
            entry["exact_join_output_rows"] = (
                res.get(pid, {}).get("exact_join_output_rows") or {}).get("value")
            entry["raw_join_status"] = (
                res.get(pid, {}).get("raw_join_status") or {}).get("note")
        else:
            # Proxy-cost semantics: gt_load is elapsed time; verdict is the
            # pre-registered remote-spill proxy, NOT load > T.
            entry["gt_load"] = (entry.get("cost_metrics") or {}).get(
                "total_elapsed_ms")
            entry["gt_load_source"] = ("snowflake_account_usage_query_history "
                                       "(TOTAL_ELAPSED_TIME, ms -- a COST "
                                       "metric, not a row count)")
            entry["load_semantics"] = "proxy-cost"
            entry["verdict"] = entry.get("snowflake_verdict")
            if p is None:
                entry["note"] = ("profile missing -- arm void per "
                                 "PROGRAM_DESIGN.md §4.5")
        entry["t_load"] = args.t_load
        out_pipes[pid] = entry

    result = {
        "experiment": "Blind at Scale -- Snowflake cross-engine ground truth",
        "labelling": args.labelling,
        "dataset": f"{args.dataset} (fact 10M / fact2 6M / dim 20k)",
        "arm": args.arm,
        "engine": f"snowflake-{args.warehouse.lower().replace('bas_sf_wh_', '')}",
        "warehouse": args.warehouse,
        "n_fact": n_fact,
        "t_load": args.t_load,
        "engine_load_semantics": ("proxy: gt_load is TOTAL_ELAPSED_TIME (ms) "
                                  "for G/J/W pipelines -- a cost metric, NOT a "
                                  "row count; F pipelines carry the D4 analytic "
                                  "row-count load. Verdicts for G/J/W come from "
                                  "the pre-registered remote-spill proxy, not "
                                  "from load > T."),
        "verdict_basis": ("remote-spill proxy, pre-registered "
                          "(PROGRAM_DESIGN.md §4.5); F verdicts from the D4 "
                          "analytic splitmix64 load vs T"),
        "load_definition": ("Snowflake has no user-visible shuffle partitions "
                            "and no Spark-style shuffle-read metric. Data "
                            "level: exact result counts (RES tables). Cost "
                            "level: elapsed time, bytes scanned/spilled from "
                            "QUERY_HISTORY. See PROGRAM_DESIGN.md §4.5."),
        "deviations": ("D4 F raw join not materialisable -- analytic load from "
                       "Snowflake-produced counts, computed locally. Snowflake "
                       "verdicts are spill-proxy verdicts, never the study's "
                       "load verdicts."),
        "profile_source": prof_source,
        "counts_sha256_check": sha_check,
        "data_level_checks": {"all_exact": all_pass, "checks": checks},
        "pipelines": out_pipes,
    }
    with open(args.out, "w") as f:
        json.dump(result, f, indent=1)
    n_fail = sum(1 for e in out_pipes.values() if e.get("verdict") == "FAIL")
    print(f"wrote {args.out}: {n_fail}/{len(PIPES)} FAIL verdicts "
          f"(data-level exact: {all_pass})")


if __name__ == "__main__":
    main()
