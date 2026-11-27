"""Snowflake driver for the Blind at Scale cloud program (S-XENG / S-SIZE).

Uses snowflake-connector-python (import guarded: if the package is
absent, this script prints the pip install command and exits 2 -- it
never installs anything itself).

AUTH -- key pair, Blind at Scale's OWN, generated at execution time:
  BAS_SNOWFLAKE_PRIVATE_KEY_PATH   path to the private key PEM file (0600)
  BAS_SNOWFLAKE_USER               e.g. CHALAKONENI
  BAS_SNOWFLAKE_ACCOUNT            e.g. giobwtb-urc15524 (bare URC15524 404s)
  BAS_SNOWFLAKE_ROLE               default ACCOUNTADMIN (the role choice is
                                   the author's; the scripts only need CREATE
                                   DATABASE / CREATE WAREHOUSE privileges)
  BAS_SNOWFLAKE_WAREHOUSE          default BAS_SF_WH_XS
Path-only key support: the private key is read from the file, PEM armor
stripped, base64-decoded to DER bytes, and passed to the connector.
NEVER VISTA's key (rsa_key.p8) or VISTA's VB_SNOWFLAKE_* variables.
The key must be unencrypted (generated without a passphrase for this
purpose); the key file must never be committed, uploaded, or pasted
into chat. The key value is never printed or logged by this script.

Commands:
  run-sql <file>     execute statements sequentially; record per-statement
                     id (sfqid), timing, row counts to a JSON run log.
                     --arm-tag / --run-tag substitute the ARM_TAG / RUN_TAG
                     placeholders (03_pipelines.sql). --warehouse selects
                     the warehouse; --no-warehouse connects without one
                     (for 01_setup.sql -- no warehouse exists yet).
  put <local> <stage-path>
                     PUT a local file to a stage path.
  get-counts <local-dir>
                     GET the counts export from @UTIL.BAS_STAGE/counts/
                     (--stage-path overrides the stage path, e.g. for
                     the F product-vector exports).
  profile            run 04_profiles.sql and save the SELECT results as JSON.
  dump-res           SELECT * from every RUN.RES_* table; save JSON.
  suspend-all        suspend BAS_SF_WH_XS/S/M (safety command -- run at the
                     end of every arm).

Global flags:
  --dry-run          print statements without connecting (run-sql/profile).
  --selftest         parse all four SQL files (statement splitting) without
                     connecting; report statement counts and placeholder
                     checks.

ISOLATION: this driver only ever touches BLINDATSCALE_SF / BAS_SF_WH_*.
VISTA's database VERDICTBENCH_D2 and warehouse VB_D2_WH are never
referenced. See CREDIT_AND_INTERFERENCE_REPORT.md §2.
"""
import argparse, base64, json, os, re, sys, time

SQL_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_FILES = ["01_setup.sql", "02_load.sql", "03_pipelines.sql", "04_profiles.sql"]
WAREHOUSES = ["BAS_SF_WH_XS", "BAS_SF_WH_S", "BAS_SF_WH_M"]

EXIT_NO_CONNECTOR = 2


def require_connector():
    try:
        import snowflake.connector  # noqa: F401
        return snowflake.connector
    except ImportError:
        print("snowflake-connector-python is not installed. Install it with:\n"
              "  pip install snowflake-connector-python\n"
              "(this script never installs packages itself.)",
              file=sys.stderr)
        sys.exit(EXIT_NO_CONNECTOR)


def load_private_key_der(path):
    """Read a PEM private key file; return DER bytes (armor stripped).

    The key value is never printed. Supports PKCS#8 ("PRIVATE KEY") and
    traditional ("RSA PRIVATE KEY") PEM blocks; the key must be
    unencrypted.
    """
    with open(path, "r") as f:
        text = f.read()
    body = "".join(line.strip() for line in text.splitlines()
                   if line.strip() and not line.strip().startswith("-----"))
    try:
        return base64.b64decode(body)
    except Exception as exc:
        raise SystemExit(f"could not decode private key at {path}: {exc}")


def connect(args):
    connector = require_connector()
    key_path = os.environ.get("BAS_SNOWFLAKE_PRIVATE_KEY_PATH", "")
    user = os.environ.get("BAS_SNOWFLAKE_USER", "")
    account = os.environ.get("BAS_SNOWFLAKE_ACCOUNT", "")
    role = os.environ.get("BAS_SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    missing = [n for n, v in (("BAS_SNOWFLAKE_PRIVATE_KEY_PATH", key_path),
                              ("BAS_SNOWFLAKE_USER", user),
                              ("BAS_SNOWFLAKE_ACCOUNT", account)) if not v]
    if missing:
        raise SystemExit(f"missing env vars: {', '.join(missing)} "
                         f"(see header; the key pair is generated at "
                         f"execution time and is NEVER VISTA's key)")
    if not os.path.exists(key_path):
        raise SystemExit(f"private key file not found: {key_path}")
    kwargs = {"user": user, "account": account, "role": role,
              "private_key": load_private_key_der(key_path)}
    if not args.no_warehouse:
        kwargs["warehouse"] = args.warehouse or os.environ.get(
            "BAS_SNOWFLAKE_WAREHOUSE", "BAS_SF_WH_XS")
        kwargs["database"] = "BLINDATSCALE_SF"
        kwargs["schema"] = "RUN"
    print(f"connecting as {user}@{account} role={role} "
          f"warehouse={kwargs.get('warehouse', '(none)')}")
    return connector.connect(**kwargs)


def split_statements(text):
    """Split SQL text into statements on top-level semicolons.

    Handles single-quoted strings ('' escape), double-quoted identifiers
    ("" escape), line comments (--) and block comments (/* */).
    Returns a list of non-empty statement strings (comments stripped
    only where they are whole statements).
    """
    stmts, buf = [], []
    i, n = 0, len(text)
    in_single, in_double, in_line, in_block = False, False, False, False
    while i < n:
        c = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if in_line:
            if c == "\n":
                in_line = False
                buf.append(c)
            i += 1
            continue
        if in_block:
            if c == "*" and nxt == "/":
                in_block = False
                i += 2
            else:
                i += 1
            continue
        if in_single:
            buf.append(c)
            if c == "'":
                if nxt == "'":
                    buf.append(nxt)
                    i += 2
                    continue
                in_single = False
            i += 1
            continue
        if in_double:
            buf.append(c)
            if c == '"':
                if nxt == '"':
                    buf.append(nxt)
                    i += 2
                    continue
                in_double = False
            i += 1
            continue
        # Top level.
        if c == "-" and nxt == "-":
            in_line = True
            i += 2
            continue
        if c == "/" and nxt == "*":
            in_block = True
            i += 2
            continue
        if c == "'":
            in_single = True
            buf.append(c)
            i += 1
            continue
        if c == '"':
            in_double = True
            buf.append(c)
            i += 1
            continue
        if c == ";":
            stmt = "".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(c)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    if in_single or in_double or in_block:
        raise ValueError("unterminated string or block comment in SQL text")
    return stmts


def substitute_placeholders(text, args):
    """Substitute ARM_TAG / RUN_TAG placeholders (03_pipelines.sql)."""
    if getattr(args, "arm_tag", None):
        text = text.replace("ARM_TAG", args.arm_tag)
    if getattr(args, "run_tag", None):
        text = text.replace("RUN_TAG", args.run_tag)
    if getattr(args, "run_start", None):
        new_set = f"SET BAS_RUN_START = '{args.run_start}'"
        text2, n = re.subn(r"SET BAS_RUN_START\s*=\s*[^;]+;", new_set + ";",
                           text, count=1)
        if n:
            text = text2
        else:
            print("WARNING: --run-start given but no SET BAS_RUN_START found",
                  file=sys.stderr)
    return text


def execute_statements(conn, stmts, log_path, dry_run=False):
    """Execute statements sequentially; record per-statement results."""
    log = {"statements": []}
    cur = conn.cursor() if not dry_run else None
    for idx, stmt in enumerate(stmts):
        rec = {"index": idx, "statement_preview": stmt[:500],
               "statement_chars": len(stmt)}
        if dry_run:
            print(f"--- statement {idx} ({len(stmt)} chars) ---\n{stmt}\n")
            rec["dry_run"] = True
            log["statements"].append(rec)
            continue
        t0 = time.time()
        try:
            cur.execute(stmt)
            rec["sfqid"] = cur.sfqid
            rec["rowcount"] = cur.rowcount
            rows = []
            truncated = False
            try:
                cols = [d[0] for d in (cur.description or [])]
                fetched = cur.fetchall()
                if len(fetched) > 1000:
                    fetched = fetched[:1000]
                    truncated = True
                rows = [dict(zip(cols, [str(v) for v in r])) for r in fetched]
            except Exception:  # noqa: BLE001 - not all statements return rows
                rows = []
            rec["columns"] = cols if "cols" in dir() else []
            rec["rows"] = rows
            rec["rows_truncated_to_1000"] = truncated
            rec["status"] = "OK"
        except Exception as e:  # noqa: BLE001 - record and stop (fail fast)
            rec["status"] = "ERROR"
            rec["error"] = repr(e)[:1000]
            rec["elapsed_ms"] = round((time.time() - t0) * 1000, 1)
            log["statements"].append(rec)
            with open(log_path, "w") as f:
                json.dump(log, f, indent=1)
            print(f"statement {idx} FAILED: {e!r} (log -> {log_path})",
                  file=sys.stderr)
            raise SystemExit(1)
        rec["elapsed_ms"] = round((time.time() - t0) * 1000, 1)
        log["statements"].append(rec)
        print(f"statement {idx}: OK {rec['elapsed_ms']} ms "
              f"sfqid={rec.get('sfqid')} rows={rec.get('rowcount')}")
    if cur is not None:
        cur.close()
    with open(log_path, "w") as f:
        json.dump(log, f, indent=1)
    return log


def cmd_run_sql(conn, args):
    with open(args.sql_file) as f:
        text = f.read()
    text = substitute_placeholders(text, args)
    if "ARM_TAG" in text or "RUN_TAG" in text:
        raise SystemExit("placeholders ARM_TAG/RUN_TAG still present; pass "
                         "--arm-tag and --run-tag")
    stmts = split_statements(text)
    print(f"{args.sql_file}: {len(stmts)} statements")
    log_path = args.log or f"sf_run_{time.strftime('%Y%m%dT%H%M%S')}.json"
    execute_statements(conn, stmts, log_path, dry_run=args.dry_run)
    print(f"run log -> {log_path}")


def cmd_put(conn, args):
    local = os.path.abspath(args.local_file)
    if not os.path.exists(local):
        raise SystemExit(f"local file not found: {local}")
    stage = args.stage_path
    if not stage.startswith("@"):
        raise SystemExit("stage path must start with @, e.g. "
                         "@BLINDATSCALE_SF.UTIL.BAS_STAGE/fact.parquet")
    stmt = (f"PUT file://{local} {stage} "
            f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
    print(f"PUT {local} -> {stage}")
    cur = conn.cursor()
    cur.execute(stmt)
    for row in cur.fetchall():
        print(row)
    cur.close()


def cmd_get_counts(conn, args):
    stage = args.stage_path or "@BLINDATSCALE_SF.UTIL.BAS_STAGE/counts/"
    local_dir = os.path.abspath(args.local_dir)
    os.makedirs(local_dir, exist_ok=True)
    stmt = f"GET {stage} file://{local_dir}/"
    print(f"GET {stage} -> {local_dir}/")
    cur = conn.cursor()
    cur.execute(stmt)
    for row in cur.fetchall():
        print(row)
    cur.close()


def cmd_profile(conn, args):
    path = args.sql_file or os.path.join(SQL_DIR, "04_profiles.sql")
    with open(path) as f:
        text = f.read()
    text = substitute_placeholders(text, args)
    stmts = split_statements(text)
    out_path = args.out or f"sf_profiles_{time.strftime('%Y%m%dT%H%M%S')}.json"
    log = execute_statements(conn, stmts, out_path, dry_run=args.dry_run)
    print(f"profiles -> {out_path}")
    return log


def cmd_dump_res(conn, args):
    cur = conn.cursor()
    cur.execute(
        "SELECT TABLE_NAME FROM BLINDATSCALE_SF.INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = 'RUN' AND STARTSWITH(TABLE_NAME, 'RES_') "
        "ORDER BY TABLE_NAME")
    tables = [r[0] for r in cur.fetchall()]
    print(f"RES tables: {tables}")
    out = {}
    for t in tables:
        cur.execute(f"SELECT * FROM BLINDATSCALE_SF.RUN.{t}")
        cols = [d[0] for d in cur.description]
        out[t] = [dict(zip(cols, [str(v) for v in r])) for r in cur.fetchall()]
    cur.close()
    out_path = args.out or f"sf_res_{time.strftime('%Y%m%dT%H%M%S')}.json"
    with open(out_path, "w") as f:
        json.dump(out, f, indent=1)
    print(f"RES dump -> {out_path}")


def cmd_suspend_all(conn, args):
    cur = conn.cursor()
    for wh in WAREHOUSES:
        cur.execute(f"ALTER WAREHOUSE IF EXISTS {wh} SUSPEND")
        print(f"suspended {wh}")
    cur.close()


def cmd_selftest():
    """Parse all four SQL files without connecting; check placeholders."""
    failures = []
    total = 0
    for name in SQL_FILES:
        path = os.path.join(SQL_DIR, name)
        try:
            with open(path) as f:
                text = f.read()
            stmts = split_statements(text)
            total += len(stmts)
            print(f"  {name}: {len(stmts)} statements parsed")
            if name == "03_pipelines.sql":
                ok = "ARM_TAG" in text and "RUN_TAG" in text
                print(f"    placeholders ARM_TAG/RUN_TAG present: {ok}")
                if not ok:
                    failures.append("03 placeholders")
            # Forbidden-ref check runs on the PARSED STATEMENTS (comments
            # stripped by the splitter), not the raw text: header comments
            # legitimately NAME the VISTA resources to avoid; no executable
            # statement may reference them.
            exec_text = "\n".join(stmts)
            bad = [w for w in ("VERDICTBENCH", "VB_D2", "VB_SNOWFLAKE")
                   if w in exec_text]
            if bad:
                failures.append(f"{name}: forbidden VISTA refs in statements {bad}")
                print(f"    FAIL forbidden refs in statements: {bad}")
        except Exception as e:  # noqa: BLE001
            failures.append(f"{name}: {e!r}")
            print(f"  {name}: PARSE FAILED {e!r}")
    print(f"selftest: {total} statements across {len(SQL_FILES)} files")
    if failures:
        print(f"SELFTEST FAILED: {failures}")
        return 1
    print("SELFTEST PASSED (parse-only, no connection)")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--warehouse", default=None,
                    help="default: $BAS_SNOWFLAKE_WAREHOUSE or BAS_SF_WH_XS")
    ap.add_argument("--no-warehouse", action="store_true",
                    help="connect without a warehouse/database (01_setup.sql)")
    ap.add_argument("--dry-run", action="store_true",
                    help="print statements without connecting (run-sql/profile)")
    ap.add_argument("--selftest", action="store_true",
                    help="parse the SQL files without connecting")
    ap.add_argument("--arm-tag", default=None)
    ap.add_argument("--run-tag", default=None)
    ap.add_argument("--run-start", default=None,
                    help="override SET BAS_RUN_START (e.g. '2026-11-25 00:00:00')")
    ap.add_argument("--log", default=None, help="run log JSON path (run-sql)")
    ap.add_argument("--out", default=None, help="output JSON path (profile/dump-res)")
    sub = ap.add_subparsers(dest="command")
    p = sub.add_parser("run-sql")
    p.add_argument("sql_file")
    p = sub.add_parser("put")
    p.add_argument("local_file")
    p.add_argument("stage_path")
    p = sub.add_parser("get-counts")
    p.add_argument("local_dir")
    p.add_argument("--stage-path", default=None)
    p = sub.add_parser("profile")
    p.add_argument("sql_file", nargs="?", default=None)
    sub.add_parser("dump-res")
    sub.add_parser("suspend-all")
    args = ap.parse_args()

    if args.selftest:
        return cmd_selftest()
    if not args.command:
        ap.error("a command is required (or --selftest)")

    conn = None if args.dry_run else connect(args)
    try:
        fn = {"run-sql": cmd_run_sql, "put": cmd_put,
              "get-counts": cmd_get_counts, "profile": cmd_profile,
              "dump-res": cmd_dump_res, "suspend-all": cmd_suspend_all}[args.command]
        fn(conn, args)
    finally:
        if conn is not None:
            conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
