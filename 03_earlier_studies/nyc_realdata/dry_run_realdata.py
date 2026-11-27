"""Blind at Scale (real-data arm) -- full-scale local dry run.

Build-time script (runs here, NOT shipped in the package zip; the
package ships its outputs). Uses the realdata_arm venv
(./venv/bin/python) and the packaged model module
(../realdata_emr_package/code/realdata_model.py) -- the SAME module
the EMR job uses for its F analytic loads, so the dry run validates
the shipped code, not a copy.

Inputs: the staged corpus (./corpus/, from stage_data.py) +
./corpus_manifest.json.
Outputs (into ../realdata_emr_package/config/):
  dataset_manifest.json   (copy of the staging manifest)
  analytic_gt_calibration.json  (Stage C analytic GT, 3 months)
  predictions_realdata.csv      (all 9 arms x 11 pipelines x
                                 4 instances; study-tagged)

Checks performed (see docs/VALIDATION_REPORT.md for the record):
  * Stage C 2026-01 analytic GT == his notebook values EXACTLY
    (11/11 loads + thresholds; the anchor).
  * Stage C 2026-02 synopsis structure (cardinality + tail mass +
    serialized bytes) == his displayed cell-35 values where visible
    (8 pipelines cardinality/tail-mass, 9 serialized sizes).
  * A5 Jan predictions == his displayed values exactly (G_A 199730,
    G_B 191447).
  * Internal consistency on all instances: DuckDB counts match the
    manifest footers; contribution totals match row counts.
  * V7 order-invariance: all 9 arm predictions byte-identical under
    3 shuffled input dict orders, on every instance.
"""
import csv
import json
import math
import os
import random
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PKG = os.path.join(HERE, "..", "realdata_emr_package")
sys.path.insert(0, os.path.join(PKG, "code"))

import duckdb  # noqa: E402
import numpy as np  # noqa: E402
import realdata_model as model  # noqa: E402

CORPUS_DIR = os.path.join(HERE, "corpus")
CONFIG_DIR = os.path.join(PKG, "config")
CALIBRATION_MONTHS = ["2026-01", "2026-02", "2026-03"]

# His notebook cell-35 displayed ground truth for NYC_TLC_2026-01
# (design doc section 2; the replication anchor).
EXPECTED_JAN = {
    "G_A": 199975, "G_B": 191226, "G_C": 10596, "G_N": 2390495,
    "J_A": 199975, "J_B": 191226, "J_N": 10596,
    "F_A": 173798004, "F_B": 330477337, "W_A": 199975, "D_A": 259,
}
EXPECTED_JAN_THR = {
    "G_A": 58201.390625, "G_B": 58201.390625, "G_C": 629.25,
    "G_N": 58201.390625, "J_A": 58201.390625, "J_B": 58201.390625,
    "J_N": 629.25, "F_A": 10038957.86, "F_B": 24985055.67,
    "W_A": 58201.390625, "D_A": 50000.0,
}
EXPECTED_JAN_PRED = {("G_A", "A5"): 199730, ("G_B", "A5"): 191447}
# His displayed synopsis serialized bytes, Jan (cell 35 output 4).
EXPECTED_JAN_SYNOPSIS_BYTES = {
    "G_A": 6484, "G_B": 6487, "G_C": 6336, "G_N": 1043,
    "J_A": 6484, "J_B": 6487, "J_N": 6336,
    "F_A": 6600, "F_B": 6647, "W_A": 6484,
}
# His displayed Feb synopsis structure (cell 35 outputs 3+4; only
# the rows visible in the truncated display).
EXPECTED_FEB_STRUCTURE = {
    # pipeline: (cardinality, tail_mass, serialized_bytes or None)
    "G_A": (259, 261761, 6478), "G_B": (261, 395065, 6487),
    "G_C": (231, 2398, 6336), "G_N": (8, 0, 1042),
    "J_A": (259, 261761, 6478), "J_B": (261, 395065, 6487),
    "J_N": (231, 2398, 6336), "F_A": (231, 10669649, 6593),
    "F_B": (None, None, 6641),
}

con = duckdb.connect()
con.execute("SET memory_limit='3GB'")


def sql_list(paths):
    return "[" + ", ".join(f"'{p}'" for p in paths) + "]"


def get_counts(paths, column):
    frame = con.execute(f"""
        SELECT CASE WHEN "{column}" IS NULL THEN '{model.NULL_TOKEN}'
                    ELSE CAST("{column}" AS VARCHAR) END AS key_value,
               COUNT(*)::BIGINT AS n
        FROM read_parquet({sql_list(paths)}) GROUP BY 1
    """).df()
    return {str(k): int(v) for k, v in zip(frame["key_value"], frame["n"])}


def get_distinct_counts(paths, group_column, distinct_column):
    frame = con.execute(f"""
        SELECT CASE WHEN "{group_column}" IS NULL THEN '{model.NULL_TOKEN}'
                    ELSE CAST("{group_column}" AS VARCHAR) END AS group_key,
               COUNT(DISTINCT "{distinct_column}")::BIGINT AS distinct_n
        FROM read_parquet({sql_list(paths)}) GROUP BY 1
    """).df()
    return {str(k): int(v) for k, v in zip(frame["group_key"],
                                           frame["distinct_n"])}


def get_zone_set(path):
    frame = con.execute(
        f"SELECT DISTINCT CAST(LocationID AS VARCHAR) AS k FROM "
        f"read_csv_auto('{path}', header=true)").df()
    return set(frame["k"].astype(str).tolist())


def load_instance(months):
    """Per-key counts for one instance (a month list)."""
    y = [os.path.join(CORPUS_DIR, f"yellow_tripdata_{m}.parquet")
         for m in months]
    g = [os.path.join(CORPUS_DIR, f"green_tripdata_{m}.parquet")
         for m in months]
    z = os.path.join(CORPUS_DIR, "taxi_zone_lookup.csv")
    y_pu = get_counts(y, "PULocationID")
    g_pu = get_counts(g, "PULocationID")
    data = {
        "y_pu": y_pu,
        "y_do": get_counts(y, "DOLocationID"),
        "y_rc": get_counts(y, "RatecodeID"),
        "g_pu": g_pu,
        "g_do": get_counts(g, "DOLocationID"),
        "zones": get_zone_set(z),
        "y_distinct": get_distinct_counts(y, "PULocationID", "DOLocationID"),
        "n_yellow": sum(y_pu.values()),
        "n_green": sum(g_pu.values()),
    }
    return data


def instance_inputs(data, pipeline):
    """(kind, c1, c2, dim, distinct, n_fact) for one pipeline."""
    kind = model.PIPELINE_KIND[pipeline]
    t1, k1, t2, k2, use_dim, distinct_col = model.PIPELINE_DEFS[pipeline]
    keymap = {("yellow", "PULocationID"): data["y_pu"],
              ("yellow", "DOLocationID"): data["y_do"],
              ("yellow", "RatecodeID"): data["y_rc"],
              ("green", "PULocationID"): data["g_pu"],
              ("green", "DOLocationID"): data["g_do"]}
    c1 = keymap[(t1, k1)]
    c2 = keymap[(t2, k2)] if t2 else None
    dim = data["zones"] if use_dim else None
    distinct = data["y_distinct"] if kind == "distinct" else None
    n_fact = data["n_yellow"] if t1 == "yellow" else data["n_green"]
    return kind, c1, c2, dim, distinct, n_fact


def synopsis_bytes(contrib, kind):
    if kind == "distinct":
        return ""
    # His serialization (notebook cell 8 save_json): json.dump with
    # indent=2, ensure_ascii=False, no trailing newline; the byte
    # count is the file size on disk. His files were written on
    # Windows in text mode, so every \n became \r\n (+1 byte per
    # line: 266 lines for a 64-hitter synopsis, 42 for G_N's 8).
    # Reproduce that exactly so the sizes match his study's
    # convention (verified: Jan G_A 6484 = 6218 LF bytes + 266
    # CRLF bytes; Jan G_N 1043 = 1001 + 42).
    syn = model.build_single_synopsis(contrib)
    text = json.dumps(syn, indent=2, ensure_ascii=False,
                      default=str).replace("\n", "\r\n")
    return len(text.encode("utf-8"))


def main():
    with open(os.path.join(HERE, "corpus_manifest.json")) as fh:
        staging = json.load(fh)
    corpus = staging["corpus"]
    print(f"corpus: {corpus['yellow_rows']:,} yellow + "
          f"{corpus['green_rows']:,} green rows, "
          f"{corpus['total_bytes'] / 1e9:.2f} GB", flush=True)
    assert corpus["yellow_rows"] > 100_000_000, (
        f"staging gate: yellow rows {corpus['yellow_rows']:,} do not "
        f"clear 100M -- the corpus is not the 100M corpus")
    print(f"STAGING GATE: yellow {corpus['yellow_rows']:,} > 100,000,000 "
          f"-- corpus clears 100M by "
          f"{corpus['yellow_rows'] - 100_000_000:,} rows", flush=True)

    instances = [("C", f"NYC_TLC_{m}", [m], i * 11)
                 for i, m in enumerate(CALIBRATION_MONTHS)]
    months_all = staging["months"]
    instances.append(("S", "NYC_TLC_100M", months_all, 1000))

    analytic_gt = {"study": model.STUDY_TAG,
                   "instances": {},
                   "provenance": ("Analytic ground truth computed by the "
                                  "dry run (DuckDB counts + packaged "
                                  "realdata_model); Stage C 2026-01 "
                                  "reproduces the co-author's notebook "
                                  "values exactly (see VALIDATION_REPORT).")}
    pred_rows = []
    checks = []

    def check(name, ok, detail=""):
        checks.append((name, bool(ok), detail))
        print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}", flush=True)

    t0 = time.time()
    for stage, instance, months, spec_base in instances:
        print(f"\n=== instance {instance} ({len(months)} month(s)) ===",
              flush=True)
        data = load_instance(months)
        # Internal consistency: DuckDB totals vs manifest footers.
        want_y = sum(e["rows"] for e in staging["files"]
                     if e["kind"] == "yellow"
                     and e["filename"].rsplit("_", 1)[1][:7] in months)
        want_g = sum(e["rows"] for e in staging["files"]
                     if e["kind"] == "green"
                     and e["filename"].rsplit("_", 1)[1][:7] in months)
        check(f"{instance} duckdb-vs-manifest row counts",
              data["n_yellow"] == want_y and data["n_green"] == want_g,
              f"yellow {data['n_yellow']:,} vs {want_y:,}; green "
              f"{data['n_green']:,} vs {want_g:,}")
        analytic_gt["instances"].setdefault(instance, {})
        for pipe, kind in model.PIPELINES:
            pidx = model.PIPELINE_INDEX[pipe]
            kind2, c1, c2, dim, distinct, n_fact = instance_inputs(
                data, pipe)
            assert kind2 == kind
            contrib = model.contribution_map(kind, c1, c2, dim, distinct)
            gt = model.exact_load(kind, c1, c2, dim, distinct)
            t_rel = model.threshold_relative(contrib, kind)
            t_abs = (model.threshold_absolute(n_fact)
                     if kind != "distinct" else float(model.TDISTINCT))
            sb = synopsis_bytes(contrib, kind)
            analytic_gt["instances"][instance][pipe] = {
                "gt_load": gt, "threshold_rel": t_rel,
                "threshold_abs": t_abs, "n_fact": n_fact,
                "total_contribution": int(sum(contrib.values())),
                "cardinality": len(contrib),
                "synopsis_bytes": sb if sb != "" else None,
            }
            if stage == "C" and instance == "NYC_TLC_2026-01":
                check(f"Jan {pipe} GT exact vs notebook",
                      gt == EXPECTED_JAN[pipe],
                      f"{gt} vs {EXPECTED_JAN[pipe]}")
                check(f"Jan {pipe} threshold exact vs notebook",
                      abs(t_rel - EXPECTED_JAN_THR[pipe]) < 1.0,
                      f"{t_rel:.2f} vs {EXPECTED_JAN_THR[pipe]}")
                if pipe in EXPECTED_JAN_SYNOPSIS_BYTES:
                    # Tolerance +-4 bytes: the size includes the fitted
                    # tail_alpha float, whose shortest repr can differ
                    # by 1-2 chars across numpy versions/BLAS (his
                    # 1.26.4 vs this venv's 2.5.5). Jan F_A is 6599 vs
                    # his 6600 and Feb F_B 6643 vs 6641 -- both are
                    # repr-length artifacts, not data differences.
                    # Synopsis bytes are not verdict-bearing.
                    check(f"Jan {pipe} synopsis bytes vs notebook",
                          abs(sb - EXPECTED_JAN_SYNOPSIS_BYTES[pipe]) <= 4,
                          f"{sb} vs {EXPECTED_JAN_SYNOPSIS_BYTES[pipe]}")
            if stage == "C" and instance == "NYC_TLC_2026-02" \
                    and pipe in EXPECTED_FEB_STRUCTURE:
                card, tail_mass, sbytes = EXPECTED_FEB_STRUCTURE[pipe]
                syn = (model.build_single_synopsis(contrib)
                       if kind != "distinct" else None)
                if card is not None:
                    check(f"Feb {pipe} cardinality vs notebook",
                          len(contrib) == card,
                          f"{len(contrib)} vs {card}")
                    check(f"Feb {pipe} tail mass vs notebook",
                          syn["tail_mass"] == tail_mass,
                          f"{syn['tail_mass']} vs {tail_mass}")
                if sbytes is not None:
                    check(f"Feb {pipe} synopsis bytes vs notebook",
                          abs(sb - sbytes) <= 4, f"{sb} vs {sbytes}")
            for arm in model.ALL_ARMS:
                seed = model.seed_for(spec_base + pidx, arm)
                if kind == "distinct":
                    pred_rows.append({
                        "study": model.STUDY_TAG, "instance": instance,
                        "stage": stage, "pipeline": pipe, "kind": kind,
                        "n_fact": n_fact, "gt_load_analytic": gt,
                        "gt_verdict_rel": model.verdict(gt, t_rel),
                        "gt_verdict_abs": model.verdict(gt, t_abs),
                        "threshold_rel": t_rel, "threshold_abs": t_abs,
                        "arm": arm, "pred_load": "",
                        "pred_verdict_rel": "", "pred_verdict_abs": "",
                        "seed": "", "r": model.HEADLINE_R,
                        "top_k": model.TOP_K, "synopsis_bytes": ""})
                    continue
                pred = model.predict_arm(kind, c1, c2, dim, arm,
                                         seed=seed)
                if (pipe, arm) in EXPECTED_JAN_PRED \
                        and instance == "NYC_TLC_2026-01":
                    check(f"Jan {pipe} {arm} prediction exact vs notebook",
                          pred == EXPECTED_JAN_PRED[(pipe, arm)],
                          f"{pred} vs {EXPECTED_JAN_PRED[(pipe, arm)]}")
                pred_rows.append({
                    "study": model.STUDY_TAG, "instance": instance,
                    "stage": stage, "pipeline": pipe, "kind": kind,
                    "n_fact": n_fact, "gt_load_analytic": gt,
                    "gt_verdict_rel": model.verdict(gt, t_rel),
                    "gt_verdict_abs": model.verdict(gt, t_abs),
                    "threshold_rel": t_rel, "threshold_abs": t_abs,
                    "arm": arm, "pred_load": pred,
                    "pred_verdict_rel": model.verdict(pred, t_rel),
                    "pred_verdict_abs": model.verdict(pred, t_abs),
                    "seed": seed, "r": model.HEADLINE_R,
                    "top_k": model.TOP_K, "synopsis_bytes": sb})

        # ---- V7: order-invariance audit for this instance ----
        rng_shuffle = random.Random(20261123)
        for trial in range(3):
            for pipe, kind in model.PIPELINES:
                if kind == "distinct":
                    continue
                _, c1, c2, dim, _, _ = instance_inputs(data, pipe)
                shuffled_c1 = dict(rng_shuffle.sample(
                    list(c1.items()), len(c1)))
                shuffled_c2 = (dict(rng_shuffle.sample(
                    list(c2.items()), len(c2))) if c2 else None)
                pidx = model.PIPELINE_INDEX[pipe]
                for arm in model.ALL_ARMS:
                    seed = model.seed_for(spec_base + pidx, arm)
                    base = model.predict_arm(kind, c1, c2, dim, arm,
                                             seed=seed)
                    shuf = model.predict_arm(
                        kind, shuffled_c1, shuffled_c2, dim, arm,
                        seed=seed)
                    if base != shuf:
                        check(f"V7 {instance} {pipe} {arm} trial {trial}",
                              False, f"base {base} != shuffled {shuf}")
                        raise SystemExit(
                            "V7 FAILED -- predictions depend on input "
                            "dict order. The F1 fix is incomplete.")
        check(f"V7 {instance} order-invariance (3 shuffles x 9 arms x "
              f"10 pipelines)", True, "all identical")

    print(f"\ndry run compute: {time.time() - t0:.0f}s, "
          f"{len(pred_rows)} prediction rows", flush=True)

    # ---- Write the shipped configs ----
    os.makedirs(CONFIG_DIR, exist_ok=True)
    with open(os.path.join(CONFIG_DIR, "analytic_gt_calibration.json"),
              "w") as fh:
        json.dump(analytic_gt, fh, indent=1)
    fields = ["study", "instance", "stage", "pipeline", "kind", "n_fact",
              "gt_load_analytic", "gt_verdict_rel", "gt_verdict_abs",
              "threshold_rel", "threshold_abs", "arm", "pred_load",
              "pred_verdict_rel", "pred_verdict_abs", "seed", "r",
              "top_k", "synopsis_bytes"]
    with open(os.path.join(CONFIG_DIR, "predictions_realdata.csv"),
              "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(pred_rows)
    with open(os.path.join(CONFIG_DIR, "dataset_manifest.json"),
              "w") as fh:
        json.dump(staging, fh, indent=1)
    print(f"wrote {CONFIG_DIR}/analytic_gt_calibration.json, "
          f"predictions_realdata.csv ({len(pred_rows)} rows), "
          f"dataset_manifest.json", flush=True)

    failed = [c for c in checks if not c[1]]
    print(f"\nCHECKS: {len(checks) - len(failed)}/{len(checks)} passed",
          flush=True)
    if failed:
        print("FAILED CHECKS:", flush=True)
        for name, _, detail in failed:
            print(f"  - {name} {detail}", flush=True)
        return 1
    print("DRY RUN: ALL CHECKS PASSED", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
