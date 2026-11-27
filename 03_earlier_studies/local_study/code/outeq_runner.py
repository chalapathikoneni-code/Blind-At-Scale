"""Single-query runner for the output-equivalence study (Amendment 5 A5.3).

Invoked as a subprocess by code/outeq_check.py, once per
(dataset, pipeline, side) query, so the 900 s per-query wall-clock cap
is enforced by the driver's subprocess timeout (on exceed the driver
kills this process and records the instance INFEASIBLE; the query is
not retried under weaker settings).

Connects DuckDB under the pre-registered envelope (threads=2,
memory_limit='2GB'), creates views over the corpus parquet files,
executes the one SQL statement given on the command line, writes the
result relation to a parquet file, and prints a JSON line with the
wall-clock seconds to stdout.
"""
import argparse, json, time
import duckdb


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fact", required=True)
    ap.add_argument("--fact2", required=True)
    ap.add_argument("--dim", required=True)
    ap.add_argument("--sql", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    t0 = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET threads=2")
    con.execute("SET memory_limit='2GB'")
    con.execute(f"CREATE VIEW fact AS SELECT * FROM '{args.fact}'")
    con.execute(f"CREATE VIEW fact2 AS SELECT * FROM '{args.fact2}'")
    con.execute(f"CREATE VIEW dim AS SELECT * FROM '{args.dim}'")
    con.execute(
        f"COPY ({args.sql}) TO '{args.out}' (FORMAT PARQUET)")
    con.close()
    wall = time.perf_counter() - t0
    print(json.dumps({"wall_s": wall, "out": args.out}))


if __name__ == "__main__":
    main()
