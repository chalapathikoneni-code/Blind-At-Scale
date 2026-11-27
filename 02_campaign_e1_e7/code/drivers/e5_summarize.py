"""E5 summariser: medians from results/e5_raw.json -> results/rows_e5.csv
and a printed median table for the report."""

import csv
import json
import os
import statistics

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
RESULTS = os.path.join(ROOT, "final_scientific_strengthening", "results")
ENV = "local VM, realdata venv python 3.12.3/numpy 2.5.5"
CLASS = "measured (local timing)"
ART = "final_scientific_strengthening/results/e5_raw.json"


def med(rs, key):
    return statistics.median(r[key] for r in rs)


def main():
    raw = json.load(open(os.path.join(RESULTS, "e5_raw.json")))
    rows = []

    def add(tag, scale, metric, value, denom, notes=""):
        rows.append({
            "experiment": "E5", "dataset": tag, "scale": scale,
            "environment": ENV, "metric": metric, "value": value,
            "denominator": denom, "classification": CLASS,
            "artifact": ART, "notes": notes,
        })

    for tag, e in raw.items():
        scale = e["scale"]
        reps = e["reps"]
        scan_s = med(reps["scan"], "seconds")
        build_s = med(reps["build"], "seconds")
        ser = reps["serialize"]
        ser_s = med(ser, "seconds")
        ser_b = int(med(ser, "bytes"))
        pa8 = med(reps["predict_A8"], "seconds")
        pa5 = med(reps["predict_A5"], "seconds")
        pa9 = med(reps["predict_A9"], "seconds")
        total_bytes = e["fact_bytes"] + e["fact2_bytes"]
        total_rows = e["fact_rows"] + e["fact2_rows"]

        add(tag, scale, "profiler_scan_seconds", round(scan_s, 4),
            f"{total_rows} rows (fact+fact2)",
            f"DuckDB GROUP BY per-key counts, fact+fact2 parquet; "
            f"counts source for downstream: {e['counts_source']}")
        add(tag, scale, "rows_scanned", total_rows,
            "rows (fact+fact2)",
            f"fact {e['fact_rows']} + fact2 {e['fact2_rows']}")
        add(tag, scale, "bytes_scanned", total_bytes,
            "parquet file bytes (fact+fact2)",
            f"fact {e['fact_bytes']} B + fact2 {e['fact2_bytes']} B")
        add(tag, scale, "synopsis_build_seconds", round(build_s, 4),
            "per dataset (median of 3 reps)", "arms.build_synopsis(counts)")
        add(tag, scale, "serialize_seconds", round(ser_s, 6),
            "per dataset (median of 3 reps)", "json.dumps(synopsis)")
        add(tag, scale, "serialize_bytes", ser_b, "bytes",
            "json.dumps(synopsis) default separators")
        add(tag, scale, "predict_A8_seconds", round(pa8, 4),
            "per dataset (median of 3 reps)",
            "arms.arm_predictions('A8', counts, syn, seed)")
        add(tag, scale, "predict_A5_seconds", round(pa5, 4),
            "per dataset (median of 3 reps)",
            "arms.arm_predictions('A5', counts, syn, seed); includes the "
            "counts-level testbed generation path (protocol S.5e)")
        add(tag, scale, "predict_A9_seconds", round(pa9, 4),
            "per dataset (median of 3 reps)",
            "classical_arms.predict_A9(counts), default target")
        for phase, key in (("scan", "scan"), ("build", "build"),
                           ("serialize", "serialize"),
                           ("predict_A8", "predict_A8"),
                           ("predict_A5", "predict_A5"),
                           ("predict_A9", "predict_A9")):
            add(tag, scale, f"peak_mem_{phase}_mb",
                round(med(reps[key], "peak_mem_mb_delta"), 2),
                "MB (median of 3 reps)",
                "ru_maxrss delta inside the phase subprocess")
        e2e = scan_s + build_s + ser_s + pa8
        add(tag, scale, "end_to_end_seconds", round(e2e, 4),
            "per dataset",
            "sum of medians: scan + build + serialize + predict A8")
        add(tag, scale, "counts_validation_exact",
            1 if e["counts_validated"] else 0, "boolean",
            "DuckDB scan counts vs counts.npz, max abs diff over all "
            "arrays (fact_*, fact2_*, distinct_A, null_count)")

        print(f"== {tag} ({scale}) validated={e['counts_validated']} "
              f"src={e['counts_source']}")
        print(f"   scan={scan_s:.3f}s build={build_s:.4f}s ser={ser_s:.5f}s "
              f"({ser_b} B) A8={pa8:.4f}s A5={pa5:.4f}s A9={pa9:.4f}s "
              f"e2e={e2e:.3f}s")
        print(f"   mem MB: scan={med(reps['scan'], 'peak_mem_mb_delta'):.1f} "
              f"build={med(reps['build'], 'peak_mem_mb_delta'):.1f} "
              f"A8={med(reps['predict_A8'], 'peak_mem_mb_delta'):.1f} "
              f"A5={med(reps['predict_A5'], 'peak_mem_mb_delta'):.1f} "
              f"A9={med(reps['predict_A9'], 'peak_mem_mb_delta'):.1f}")

    out = os.path.join(RESULTS, "rows_e5.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "experiment", "dataset", "scale", "environment", "metric",
            "value", "denominator", "classification", "artifact", "notes"])
        w.writeheader()
        w.writerows(rows)
    print("wrote", out, len(rows), "rows")


if __name__ == "__main__":
    main()
