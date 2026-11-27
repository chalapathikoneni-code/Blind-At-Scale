"""Databricks REST driver for the Blind at Scale cloud program.

Stdlib-only (urllib). Mirrors the VISTA D1 client pattern
(verdictbench-dec2026/.../results/D1/code/src/databricks_d1/client.py,
read for the pattern only -- nothing is imported from VISTA):

- The API token is supplied by the caller (--token-stdin reads one line
  from stdin, or the DATABRICKS_TOKEN env var) and held only in this
  process's memory. It is never written to a file, never printed, never
  logged, and never appears on a command line. All requests carry it as
  a Bearer header; the token is in a header, never in a URL.
- Redirects are refused (NoRedirect).
- HTTP 429 is a HARD STOP (RateLimitedError), never retried.
- Transient 5xx responses are retried up to three times with backoff
  (2s, 4s, 8s).
- Every command logs its requests/responses (method, path, status,
  truncated body -- never the token) to a local run dir.

Commands (all against the trial workspace by default):
  probe            READ-ONLY: list clusters + workspace status + warehouses.
                   Used by Gate G0 to confirm the serverless-workspace shape.
  create-job       POST /api/2.2/jobs/create from a JSON def file.
                   --run-tag substitutes the RUN_TAG placeholder.
  run-job          POST run-now, poll runs/get to a terminal state, print
                   the run URL.
  get-output       GET run output (+ per-task outputs) and save to the log dir.
  upload-file      PUT bytes to a workspace file or a Unity Catalog volume
                   file (--target workspace|volume).
  download-results GET a volume file's bytes to a local path.

Global flags:
  --workspace URL  (default the trial workspace below)
  --log-dir DIR    (default ./dbx_runs/<timestamp>_<cmd>/)
  --dry-run        print the request without sending (no token needed)
  --selftest       exercise arg parsing + 429/5xx handling + run polling
                   against an in-process fake transport (NO network)

No credential is stored anywhere in this package. Do NOT reuse any
VISTA token (none is stored in any VISTA file in any case).

Workspace: https://<WORKSPACE_HOST>
Org/workspace id: 7474659864638567 (used in run URLs as ?o=...).
"""
import argparse, io, json, os, sys, time, urllib.error, urllib.request

DEFAULT_WORKSPACE = "https://<WORKSPACE_HOST>"
ORG_ID = "7474659864638567"
TERMINAL_LIFE_CYCLE = ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")


class DatabricksError(Exception):
    """A Databricks API call failed (message is safe to record)."""


class RateLimitedError(DatabricksError):
    """HTTP 429: hard stop, never retried."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        raise RuntimeError("redirect refused, verify the endpoint")


def _is_retryable(exc):
    """True for transient 5xx responses and network errors."""
    text = str(exc)
    if text.startswith("URL error"):
        return True
    if text.startswith("HTTP "):
        try:
            code = int(text.split()[1])
        except (IndexError, ValueError):
            return False
        return 500 <= code < 600
    return False


class Client:
    """Minimal Databricks REST client (token held in memory only).

    `transport` is an injectable opener-like callable
    (request -> response context manager) used by --selftest; the
    default is the real urllib opener with NoRedirect.
    """

    def __init__(self, workspace_url, token, transport=None, logger=None):
        if not token:
            raise ValueError("empty token")
        self.workspace_url = workspace_url.rstrip("/")
        self._token = token
        self._transport = transport
        self.logger = logger  # callable(method, path, status, body_summary)

    def _open_once(self, request, method, path):
        opener = self._transport
        if opener is None:
            opener = urllib.request.build_opener(NoRedirect()).open
        try:
            with opener(request, timeout=120) as response:
                body = response.read()
                status = getattr(response, "status", 200)
                if self.logger:
                    self.logger(method, path, status, body[:2000])
                return status, body
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:2000]
            if self.logger:
                self.logger(method, path, exc.code, detail.encode())
            if exc.code == 429:
                raise RateLimitedError(f"HTTP 429 on {method} {path}: {detail}")
            raise DatabricksError(f"HTTP {exc.code} on {method} {path}: {detail}")
        except urllib.error.URLError as exc:
            raise DatabricksError(f"URL error on {method} {path}: {exc}")

    def call(self, method, path, payload=None, raw=None, content_type=None):
        """One REST call; returns the decoded JSON body (or {}).

        Transient 5xx retried up to 3x with backoff; 429 raises
        RateLimitedError immediately.
        """
        url = self.workspace_url + path
        if raw is not None:
            data = raw
        elif payload is not None:
            data = json.dumps(payload).encode()
        else:
            data = None
        request = urllib.request.Request(url, data=data, method=method)
        request.add_header("Authorization", "Bearer " + self._token)
        if content_type:
            request.add_header("Content-Type", content_type)
        elif payload is not None:
            request.add_header("Content-Type", "application/json")
        last_error = None
        for attempt in range(4):
            try:
                _status, body = self._open_once(request, method, path)
                return json.loads(body) if body else {}
            except RateLimitedError:
                raise
            except DatabricksError as exc:
                if _is_retryable(exc) and attempt < 3:
                    last_error = str(exc)
                    time.sleep(2 ** (attempt + 1))
                    continue
                raise
        raise DatabricksError(last_error or "request failed")

    def call_raw(self, method, path, raw=None, content_type=None):
        """One REST call returning raw bytes (file up/download)."""
        url = self.workspace_url + path
        request = urllib.request.Request(url, data=raw, method=method)
        request.add_header("Authorization", "Bearer " + self._token)
        if content_type:
            request.add_header("Content-Type", content_type)
        last_error = None
        for attempt in range(4):
            try:
                _status, body = self._open_once(request, method, path)
                return body
            except RateLimitedError:
                raise
            except DatabricksError as exc:
                if _is_retryable(exc) and attempt < 3:
                    last_error = str(exc)
                    time.sleep(2 ** (attempt + 1))
                    continue
                raise
        raise DatabricksError(last_error or "request failed")


def make_logger(log_dir):
    """Returns (logger, path). The logger records method/path/status/body
    only -- the token is never in any logged field."""
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, "requests.log.jsonl")
    fh = open(path, "a")

    def logger(method, req_path, status, body):
        rec = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "method": method, "path": req_path, "status": status,
               "body": body.decode(errors="replace")[:2000]
               if isinstance(body, bytes) else str(body)[:2000]}
        fh.write(json.dumps(rec) + "\n")
        fh.flush()

    return logger, path


def get_token(args):
    """Token from stdin (--token-stdin) or DATABRICKS_TOKEN env var."""
    if args.token_stdin:
        token = sys.stdin.readline().strip()
        if token:
            return token
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if token:
        return token
    return None


# ---------------------------------------------------------------- commands

def cmd_probe(client, log_dir, args):
    """READ-ONLY Gate G0 probe: clusters, workspace, warehouses."""
    out = {}
    out["clusters"] = client.call("GET", "/api/2.1/clusters/list")
    out["workspace"] = client.call(
        "GET", "/api/2.0/workspace/get-status?path=%2FWorkspace")
    out["warehouses"] = client.call("GET", "/api/2.0/sql/warehouses")
    with open(os.path.join(log_dir, "probe.json"), "w") as f:
        json.dump(out, f, indent=1)
    n_clusters = len(out["clusters"].get("clusters", []))
    print(f"probe: {n_clusters} cluster(s) listed; "
          f"{len(out['warehouses'].get('warehouses', []))} warehouse(s)")
    print("probe.json written. G0 reading: if clusters/list is empty and no "
          "classic clusters are creatable, the workspace is serverless-only "
          "(G0-serverless, expected); any classic cluster is G0-classic -- "
          "see README_JOB_DEFS.md.")
    return out


def cmd_create_job(client, log_dir, args):
    with open(args.job_def) as f:
        text = f.read()
    if args.run_tag:
        if "RUN_TAG" not in text:
            print("WARNING: --run-tag given but no RUN_TAG placeholder in the def",
                  file=sys.stderr)
        text = text.replace("RUN_TAG", args.run_tag)
    elif "RUN_TAG" in text:
        raise SystemExit("job def still contains the RUN_TAG placeholder; "
                         "pass --run-tag (e.g. --run-tag 20261125)")
    payload = json.loads(text)
    resp = client.call("POST", "/api/2.2/jobs/create", payload=payload)
    with open(os.path.join(log_dir, "create_job_response.json"), "w") as f:
        json.dump(resp, f, indent=1)
    print(f"created job_id={resp.get('job_id')}")
    return resp


def poll_run(client, run_id, poll_seconds, timeout_seconds):
    """Poll runs/get to a terminal life-cycle state; return the last state."""
    deadline = time.monotonic() + timeout_seconds
    state = None
    while True:
        state = client.call("GET", f"/api/2.2/jobs/runs/get?run_id={run_id}")
        lc = state.get("state", {}).get("life_cycle_state")
        if lc in TERMINAL_LIFE_CYCLE:
            return state
        if time.monotonic() > deadline:
            raise DatabricksError(
                f"run {run_id} did not reach a terminal state within "
                f"{timeout_seconds}s (last: {lc})")
        time.sleep(poll_seconds)


def cmd_run_job(client, log_dir, args):
    resp = client.call("POST", "/api/2.2/jobs/run-now",
                       payload={"job_id": int(args.job_id)})
    run_id = resp["run_id"]
    url = (f"{client.workspace_url}/jobs/{args.job_id}/runs/{run_id}"
           f"?o={ORG_ID}")
    print(f"run_id={run_id}\nrun URL: {url}")
    state = poll_run(client, run_id, args.poll_seconds, args.timeout)
    with open(os.path.join(log_dir, "run_state.json"), "w") as f:
        json.dump(state, f, indent=1)
    result = state.get("state", {}).get("result_state")
    print(f"run {run_id} terminal: {state.get('state', {}).get('life_cycle_state')} "
          f"result={result}\nrun URL: {url}")
    return state


def cmd_get_output(client, log_dir, args):
    out = client.call("GET",
                      f"/api/2.2/jobs/runs/get-output?run_id={args.run_id}")
    with open(os.path.join(log_dir, "run_output.json"), "w") as f:
        json.dump(out, f, indent=1)
    # Per-task outputs (task run ids from runs/get).
    state = client.call("GET", f"/api/2.2/jobs/runs/get?run_id={args.run_id}")
    n_tasks = 0
    for task in state.get("tasks", []):
        tid = task.get("run_id")
        if not tid:
            continue
        tout = client.call(
            "GET",
            f"/api/2.2/jobs/runs/get-output?run_id={args.run_id}&task_run_id={tid}")
        with open(os.path.join(log_dir, f"task_output_{tid}.json"), "w") as f:
            json.dump(tout, f, indent=1)
        n_tasks += 1
    print(f"run output + {n_tasks} task output(s) saved to {log_dir}")
    return out


def cmd_upload_file(client, log_dir, args):
    with open(args.local_file, "rb") as f:
        data = f.read()
    if args.target == "workspace":
        # Workspace Files API: PUT .../import-file/<workspace path>.
        path = ("/api/2.0/workspace-files/import-file" + args.remote_path
                + "?overwrite=true")
    else:
        # Unity Catalog volume file: PUT /api/2.0/fs/files/<volume path>.
        path = "/api/2.0/fs/files" + args.remote_path + "?overwrite=true"
    resp = client.call_raw("PUT", path, raw=data,
                           content_type="application/octet-stream")
    print(f"uploaded {len(data)} bytes -> {args.target}:{args.remote_path} "
          f"(response {len(resp)} bytes)")
    return resp


def cmd_download_results(client, log_dir, args):
    path = "/api/2.0/fs/files" + args.remote_path
    body = client.call_raw("GET", path)
    os.makedirs(os.path.dirname(os.path.abspath(args.local_file)),
                exist_ok=True)
    with open(args.local_file, "wb") as f:
        f.write(body)
    print(f"downloaded {len(body)} bytes -> {args.local_file}")
    return len(body)


# ---------------------------------------------------------------- dry-run / selftest

class DryRunTransport:
    """Records the request, returns a canned JSON body; sends nothing."""

    def __init__(self):
        self.requests = []

    def __call__(self, request, timeout=None):
        body = request.data or b""
        self.requests.append({"method": request.get_method(),
                              "url": request.full_url,
                              "body_len": len(body),
                              "body": body[:2000].decode(errors="replace")})
        print(f"[dry-run] {request.get_method()} {request.full_url} "
              f"({len(body)} bytes body)")
        return _FakeResponse(200, b'{"dry_run": true}')


class _FakeResponse:
    def __init__(self, status, body):
        self.status = status
        self._body = body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class ScriptedTransport:
    """Fake transport with a scripted response queue per (method, path).

    Each script entry is ("json", status, dict) or ("error", code, text).
    Used by --selftest only; no network.
    """

    def __init__(self):
        self.script = {}
        self.calls = []

    def add(self, method, path_substr, entries):
        self.script[(method, path_substr)] = list(entries)

    def __call__(self, request, timeout=None):
        method = request.get_method()
        url = request.full_url
        self.calls.append((method, url))
        for (m, sub), entries in self.script.items():
            if m == method and sub in url:
                if not entries:
                    raise AssertionError(f"script exhausted for {method} {sub}")
                kind, code, payload = entries.pop(0)
                if kind == "error":
                    raise urllib.error.HTTPError(
                        url, code, "scripted", {}, io.BytesIO(payload.encode()))
                return _FakeResponse(code, json.dumps(payload).encode())
        raise AssertionError(f"no script for {method} {url}")


def cmd_selftest():
    """In-process checks: 429 hard stop, 5xx retry, run polling, dry-run."""
    failures = []

    def check(name, ok, detail=""):
        print(f"  {'PASS' if ok else 'FAIL'} {name} {detail}")
        if not ok:
            failures.append(name)

    print("selftest: 429 is a hard stop (no retry)")
    tr = ScriptedTransport()
    tr.add("GET", "/api/2.1/clusters/list",
           [("error", 429, "rate limited"), ("json", 200, {"clusters": []})])
    c = Client(DEFAULT_WORKSPACE, "fake-token", transport=tr)
    try:
        c.call("GET", "/api/2.1/clusters/list")
        check("429 raises RateLimitedError", False, "(no exception)")
    except RateLimitedError:
        check("429 raises RateLimitedError", True)
    except Exception as e:  # noqa: BLE001
        check("429 raises RateLimitedError", False, f"(wrong exception: {e!r})")
    check("429 attempted exactly once", len(tr.calls) == 1,
          f"(calls={len(tr.calls)})")

    print("selftest: transient 5xx retried up to 3x, then succeeds")
    tr = ScriptedTransport()
    tr.add("GET", "/api/2.1/clusters/list",
           [("error", 500, "boom"), ("error", 503, "boom"),
            ("json", 200, {"clusters": []})])
    c = Client(DEFAULT_WORKSPACE, "fake-token", transport=tr)
    try:
        out = c.call("GET", "/api/2.1/clusters/list")
        check("5xx retried then succeeds", out == {"clusters": []})
    except Exception as e:  # noqa: BLE001
        check("5xx retried then succeeds", False, f"({e!r})")
    check("5xx took 3 attempts", len(tr.calls) == 3, f"(calls={len(tr.calls)})")

    print("selftest: persistent 5xx fails after 4 attempts")
    tr = ScriptedTransport()
    tr.add("GET", "/api/2.1/clusters/list",
           [("error", 500, "boom")] * 5)
    c = Client(DEFAULT_WORKSPACE, "fake-token", transport=tr)
    try:
        c.call("GET", "/api/2.1/clusters/list")
        check("persistent 5xx raises", False, "(no exception)")
    except DatabricksError:
        check("persistent 5xx raises", True)
    check("persistent 5xx took 4 attempts", len(tr.calls) == 4,
          f"(calls={len(tr.calls)})")

    print("selftest: run-job polling reaches terminal state")
    tr = ScriptedTransport()
    tr.add("POST", "/api/2.2/jobs/run-now",
           [("json", 200, {"run_id": 123})])
    tr.add("GET", "/api/2.2/jobs/runs/get",
           [("json", 200, {"state": {"life_cycle_state": "PENDING"}}),
            ("json", 200, {"state": {"life_cycle_state": "RUNNING"}}),
            ("json", 200, {"state": {"life_cycle_state": "TERMINATED",
                                     "result_state": "SUCCEEDED"}})])
    c = Client(DEFAULT_WORKSPACE, "fake-token", transport=tr)
    resp = c.call("POST", "/api/2.2/jobs/run-now", payload={"job_id": 1})
    state = poll_run(c, resp["run_id"], poll_seconds=0, timeout_seconds=60)
    check("poll reaches TERMINATED/SUCCEEDED",
          state["state"]["result_state"] == "SUCCEEDED")

    print("selftest: dry-run transport sends nothing, records the request")
    dry = DryRunTransport()
    c = Client(DEFAULT_WORKSPACE, "fake-token", transport=dry)
    c.call("POST", "/api/2.2/jobs/create", payload={"name": "x"})
    check("dry-run recorded 1 request", len(dry.requests) == 1)
    check("dry-run request has no token in URL",
          "fake-token" not in dry.requests[0]["url"])

    print("selftest: token never appears in the request log")
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        logger, log_path = make_logger(td)
        logger("GET", "/api/2.1/clusters/list", 200, b'{"clusters": []}')
        with open(log_path) as f:
            content = f.read()
        check("log contains no token", "fake-token" not in content)

    if failures:
        print(f"SELFTEST FAILED: {failures}")
        return 1
    print("SELFTEST PASSED (all checks, no network)")
    return 0


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    ap.add_argument("--token-stdin", action="store_true",
                    help="read the PAT from stdin (one line)")
    ap.add_argument("--log-dir", default=None,
                    help="default: ./dbx_runs/<timestamp>_<cmd>/")
    ap.add_argument("--dry-run", action="store_true",
                    help="print requests without sending (no token needed)")
    ap.add_argument("--selftest", action="store_true",
                    help="run the in-process self-test (no network, no token)")
    sub = ap.add_subparsers(dest="command")

    sub.add_parser("probe", help="READ-ONLY Gate G0 probe")
    p = sub.add_parser("create-job", help="create a job from a JSON def")
    p.add_argument("job_def")
    p.add_argument("--run-tag", default=None,
                   help="substitute the RUN_TAG placeholder (e.g. 20261125)")
    p = sub.add_parser("run-job", help="run a job and poll to completion")
    p.add_argument("job_id")
    p.add_argument("--poll-seconds", type=float, default=30.0)
    p.add_argument("--timeout", type=float, default=14400.0)
    p = sub.add_parser("get-output", help="fetch run + task outputs")
    p.add_argument("run_id")
    p = sub.add_parser("upload-file", help="upload a file (workspace or volume)")
    p.add_argument("local_file")
    p.add_argument("remote_path", help="e.g. /Workspace/blind-at-scale/... "
                   "or /Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/...")
    p.add_argument("--target", choices=["workspace", "volume"],
                   default="workspace")
    p = sub.add_parser("download-results", help="download a volume file")
    p.add_argument("remote_path", help="volume path, e.g. "
                   "/Volumes/blindatscale_dbx/bas_dbx_run/bas_vol/zipf_s101/counts.npz")
    p.add_argument("local_file")
    args = ap.parse_args()

    if args.selftest:
        return cmd_selftest()
    if not args.command:
        ap.error("a command is required (or --selftest)")

    log_dir = args.log_dir or os.path.join(
        "dbx_runs", time.strftime("%Y%m%dT%H%M%S") + "_" + args.command)
    logger, log_path = make_logger(log_dir)

    if args.dry_run:
        transport = DryRunTransport()
        client = Client(args.workspace, "dry-run-token", transport=transport,
                        logger=logger)
        print(f"[dry-run] no requests will be sent; log -> {log_path}")
    else:
        token = get_token(args)
        if not token:
            raise SystemExit("no token: pass --token-stdin (PAT on stdin) or "
                             "set DATABRICKS_TOKEN")
        client = Client(args.workspace, token, logger=logger)

    fn = {"probe": cmd_probe, "create-job": cmd_create_job,
          "run-job": cmd_run_job, "get-output": cmd_get_output,
          "upload-file": cmd_upload_file,
          "download-results": cmd_download_results}[args.command]
    fn(client, log_dir, args)
    print(f"log -> {log_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
