"""Replicate the co-author's analytic ground truth for NYC TLC 2026-01
from the PUBLIC parquet file, using his exact model (notebook cells 28 + 32):
xxhash64(seed=42) -> splitmix64 -> % 128 partitions; top-k=64; relative
threshold 2x mean. Expected values are his cell-35 displayed outputs.
Run from ~/workspace/blind-at-scale/realdata_arm with ./venv/bin/python.
"""
import duckdb
import numpy as np
import xxhash

MASK64 = (1 << 64) - 1
NULL_TOKEN = "__NULL__"
HASH_SEED = 42
PARTITIONS = 128
TOP_K = 64
LOAD_THRESHOLD_MULTIPLIER = 2.0
RANDOM_SEED = 42
HEADLINE_R = 0.01

Y = "sample/yellow_tripdata_2026-01.parquet"
G = "sample/green_tripdata_2026-01.parquet"
Z = "sample/taxi_zone_lookup.csv"

con = duckdb.connect()


def get_counts(path, column):
    src = f"read_parquet('{path}')" if path.endswith(".parquet") \
        else f"read_csv_auto('{path}', header=true)"
    frame = con.execute(f"""
        SELECT CASE WHEN "{column}" IS NULL THEN '{NULL_TOKEN}'
                    ELSE CAST("{column}" AS VARCHAR) END AS key_value,
               COUNT(*)::BIGINT AS n
        FROM {src} GROUP BY 1
    """).df()
    return {str(k): int(v) for k, v in zip(frame["key_value"], frame["n"])}


def get_unique_values(path, column):
    frame = con.execute(f"""
        SELECT DISTINCT CAST("{column}" AS VARCHAR) AS key_value
        FROM read_csv_auto('{path}', header=true) WHERE "{column}" IS NOT NULL
    """).df()
    return set(frame["key_value"].astype(str).tolist())


def get_distinct_counts(path, group_column, distinct_column):
    frame = con.execute(f"""
        SELECT CASE WHEN "{group_column}" IS NULL THEN '{NULL_TOKEN}'
                    ELSE CAST("{group_column}" AS VARCHAR) END AS group_key,
               COUNT(DISTINCT "{distinct_column}")::BIGINT AS distinct_n
        FROM read_parquet('{path}') GROUP BY 1
    """).df()
    return {str(k): int(v) for k, v in zip(frame["group_key"], frame["distinct_n"])}


def stable_key_u64(key):
    text = str(key)
    if text == NULL_TOKEN:
        return 0
    return int(xxhash.xxh64(text.encode("utf-8"), seed=HASH_SEED).intdigest())


def splitmix64(value):
    z = (int(value) + 0x9E3779B97F4A7C15) & MASK64
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK64
    return (z ^ (z >> 31)) & MASK64


def partition_load(contributions, partitions=PARTITIONS):
    buckets = np.zeros(partitions, dtype=np.int64)
    for key, count in contributions.items():
        value = int(round(float(count)))
        if value <= 0:
            continue
        buckets[int(splitmix64(stable_key_u64(key)) % partitions)] += value
    return int(buckets.max())


def contribution_map(kind, c1, c2=None, dim=None, distinct=None):
    if kind in ("group", "window"):
        return {k: int(v) for k, v in c1.items() if int(v) > 0}
    if kind == "fact_dimension":
        valid = dim or set()
        return {k: int(v) for k, v in c1.items()
                if k != NULL_TOKEN and k in valid and int(v) > 0}
    if kind == "fact_fact":
        common = (set(c1) & set(c2)) - {NULL_TOKEN}
        return {k: int(c1[k]) * int(c2[k]) for k in common
                if int(c1[k]) > 0 and int(c2[k]) > 0}
    if kind == "distinct":
        return {k: int(v) for k, v in (distinct or {}).items()}
    raise ValueError(kind)


def threshold(contributions, kind):
    if kind == "distinct":
        return 50000.0
    return max(1.0, LOAD_THRESHOLD_MULTIPLIER * sum(contributions.values()) / PARTITIONS)


def predict_a5(contributions, seed):
    rng = np.random.default_rng(seed)
    items = sorted(((str(k), int(v)) for k, v in contributions.items() if int(v) > 0),
                   key=lambda kv: (-kv[1], kv[0]))
    heavy, tail = items[:TOP_K], items[TOP_K:]
    estimated = {k: v for k, v in heavy}
    for key, count in tail:
        observed = int(rng.binomial(int(count), HEADLINE_R))
        if observed > 0:
            estimated[key] = int(round(observed / HEADLINE_R))
    return partition_load(estimated)


def predict_a1(contributions, seed):
    rng = np.random.default_rng(seed)
    estimated = {}
    for key, count in contributions.items():
        observed = int(rng.binomial(int(count), HEADLINE_R))
        if observed > 0:
            estimated[key] = int(round(observed / HEADLINE_R))
    return partition_load(estimated)


# Load frequency maps (cell 32 load_spec_data equivalents)
y_pu = get_counts(Y, "PULocationID")
y_do = get_counts(Y, "DOLocationID")
y_rc = get_counts(Y, "RatecodeID")
g_pu = get_counts(G, "PULocationID")
g_do = get_counts(G, "DOLocationID")
zones = get_unique_values(Z, "LocationID")
y_distinct = get_distinct_counts(Y, "PULocationID", "DOLocationID")

# (pipeline, kind, contributions) -- spec order matches notebook cell 30
specs = [
    ("G_A", "group", contribution_map("group", y_pu)),
    ("G_B", "group", contribution_map("group", y_do)),
    ("G_C", "group", contribution_map("group", g_pu)),
    ("G_N", "group", contribution_map("group", y_rc)),
    ("J_A", "fact_dimension", contribution_map("fact_dimension", y_pu, dim=zones)),
    ("J_B", "fact_dimension", contribution_map("fact_dimension", y_do, dim=zones)),
    ("J_N", "fact_dimension", contribution_map("fact_dimension", g_pu, dim=zones)),
    ("F_A", "fact_fact", contribution_map("fact_fact", y_pu, g_pu)),
    ("F_B", "fact_fact", contribution_map("fact_fact", y_do, g_do)),
    ("W_A", "window", contribution_map("window", y_pu)),
    ("D_A", "distinct", contribution_map("distinct", None, distinct=y_distinct)),
]

# His cell-35 displayed ground truth for NYC_TLC_2026-01
expected = {
    "G_A": 199975, "G_B": 191226, "G_C": 10596, "G_N": 2390495,
    "J_A": 199975, "J_B": 191226, "J_N": 10596,
    "F_A": 173798004, "F_B": 330477337, "W_A": 199975, "D_A": 259,
}
expected_thr = {
    "G_A": 58201.390625, "G_B": 58201.390625, "G_C": 629.25,
    "G_N": 58201.390625, "J_A": 58201.390625, "J_B": 58201.390625,
    "J_N": 629.25, "F_A": 10038960.0, "F_B": 24985060.0,
    "W_A": 58201.390625, "D_A": 50000.0,
}
# His cell-35 A1/A5 predictions (seed = 42 + si*100 + ai; si=0 for Jan specs)
expected_pred = {("G_A", "A1"): 197100, ("G_A", "A5"): 199730,
                 ("G_B", "A1"): 191000, ("G_B", "A5"): 191447}

print(f"{'pipe':5s} {'actual':>12s} {'expected':>12s} {'match':>6s} "
      f"{'threshold':>14s} {'exp_thr':>14s} {'total':>12s}")
all_ok = True
for si, (pipe, kind, contrib) in enumerate(specs):
    if kind == "distinct":
        actual = int(max(contrib.values())) if contrib else 0
    else:
        actual = partition_load(contrib)
    thr = threshold(contrib, kind)
    ok = actual == expected[pipe]
    tok = abs(thr - expected_thr[pipe]) < 1.0
    all_ok &= ok and tok
    print(f"{pipe:5s} {actual:12d} {expected[pipe]:12d} {str(ok):>6s} "
          f"{thr:14.2f} {expected_thr[pipe]:14.2f} {sum(contrib.values()):12d}")
    if pipe in ("G_A", "G_B"):
        for arm, ai in (("A1", 0), ("A5", 4)):
            seed = RANDOM_SEED + si * 100 + ai
            pred = predict_a1(contrib, seed) if arm == "A1" else predict_a5(contrib, seed)
            exp = expected_pred[(pipe, arm)]
            print(f"  {arm} pred {pred} expected {exp} match={pred == exp}")
print("\nALL GROUND-TRUTH LOADS + THRESHOLDS MATCH:", all_ok)
