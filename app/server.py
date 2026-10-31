"""Cosmos Observatory: ingest endpoint + query API + static UI (stdlib only)."""
import json
import os
import sys
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(__file__))
from store import Item, Store  # noqa: E402
from validate import Invalid, validate_objects, validate_types  # noqa: E402

DATA_DIR = os.environ.get("DATA_DIR", "./data")
PORT = int(os.environ.get("PORT", "8000"))
STATIC = os.path.join(os.path.dirname(__file__), "static")
MAX_BODY = 32 * 1024 * 1024
os.makedirs(DATA_DIR, exist_ok=True)
STORE = Store(os.path.join(DATA_DIR, "observatory.db"))
STARTED = time.time()


def subtree_ids(tree, root):
    children = {}
    for t, (p, _) in list(tree.items()):
        children.setdefault(p, []).append(t)
    out, stack = [], [root]
    while stack:
        n = stack.pop()
        out.append(n)
        stack.extend(children.get(n, []))
    return out


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "CosmosObservatory/1.0"

    def log_message(self, *a):
        pass

    def _send(self, code, body=b"", ctype="application/json"):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype + ("; charset=utf-8" if ctype.startswith(("application/json", "text")) else ""))
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    # ---------------- ingest ----------------
    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if path not in ("/ingest", "/api/ingest"):
            return self._send(404, {"error": "not found"})
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            return self._send(411, {"error": "length"})
        if n <= 0 or n > MAX_BODY:
            if n > 0:
                self.rfile.read(n)
            return self._send(413 if n > MAX_BODY else 400, {"error": "body size"})
        raw = self.rfile.read(n)
        tid, kind, bid = None, None, None
        try:
            m = json.loads(raw)
            if not isinstance(m, dict):
                raise Invalid("message not an object")
            kind, tid = m.get("kind"), m.get("test_id")
            tid = tid if isinstance(tid, str) else None
            if kind == "types":
                validate_types(m)
                item = Item("types", m)
            elif kind == "objects":
                bid = m.get("batch_id") if isinstance(m.get("batch_id"), int) else None
                if isinstance(tid, str) and isinstance(m.get("tree_revision"), int) \
                        and m["tree_revision"] > STORE.tree_rev.get(tid, 0):
                    # tree revision this batch depends on not stored yet: ask to retry
                    return self._send(409, {"error": "tree revision not yet received"})
                rows = validate_objects(m, STORE.known_types(tid))
                item = Item("objects", m, rows)
            else:
                raise Invalid("unknown kind")
        except (ValueError, Invalid) as e:
            STORE.record_rejection(tid, kind, bid, str(e)[:200])
            return self._send(400, {"error": "invalid", "reason": str(e)[:200]})
        status, info = STORE.submit(item)
        if status in ("ok", "dup"):
            return self._send(200, {"status": status, "stored": info})
        if status == "gap":
            return self._send(409, {"error": "tree revision gap", "have": info})
        if status == "busy":
            return self._send(503, {"error": "busy, retry"})
        return self._send(500, {"error": status, "detail": info})

    # ---------------- query ----------------
    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        qs = {k: v[-1] for k, v in urllib.parse.parse_qs(u.query).items()}
        path = u.path
        try:
            if path == "/" or path.startswith("/static/"):
                return self._static("index.html" if path == "/" else path[len("/static/"):])
            fn = ROUTES.get(path)
            if not fn:
                return self._send(404, {"error": "not found"})
            return self._send(200, fn(qs))
        except BadRequest as e:
            return self._send(400, {"error": str(e)})
        except Exception as e:
            return self._send(500, {"error": repr(e)})

    def _static(self, name):
        p = os.path.normpath(os.path.join(STATIC, name))
        if not p.startswith(STATIC) or not os.path.isfile(p):
            return self._send(404, {"error": "not found"})
        ctype = {"html": "text/html", "js": "text/javascript", "css": "text/css"}.get(p.rsplit(".", 1)[-1], "application/octet-stream")
        with open(p, "rb") as f:
            return self._send(200, f.read(), ctype)


class BadRequest(Exception):
    pass


def _tid(qs):
    t = qs.get("test_id")
    if t:
        return t
    r = STORE.reader().execute("SELECT test_id FROM runs ORDER BY last_seen DESC LIMIT 1").fetchone()
    return r[0] if r else ""


def _int(qs, k, d, lo, hi):
    try:
        v = int(qs.get(k, d))
    except ValueError:
        raise BadRequest(k)
    return max(lo, min(hi, v))


def api_health(qs):
    return {"ok": True, "uptime_s": round(time.time() - STARTED, 1), "queue": STORE.q.qsize()}


def api_runs(qs):
    cols = ["test_id", "first_seen", "last_seen", "objects", "batches", "dup_batches", "rejected", "tree_revision"]
    rows = STORE.reader().execute(f"SELECT {','.join(cols)} FROM runs ORDER BY last_seen DESC").fetchall()
    return [dict(zip(cols, r)) for r in rows]


def api_overview(qs):
    tid = _tid(qs)
    db = STORE.reader()
    run = next((r for r in api_runs({}) if r["test_id"] == tid), None)
    frames = dict(db.execute("SELECT frame_kind, n FROM frame_counts WHERE test_id=?", (tid,)).fetchall())
    now = int(time.time())
    last = db.execute("SELECT coalesce(sum(objects),0) FROM ingest_seconds WHERE test_id=? AND sec>=?",
                      (tid, now - 10)).fetchone()[0]
    cm = list(STORE.commit_ms)
    return {"test_id": tid, "run": run, "frames": frames, "rate_10s": last / 10.0,
            "server": {"queue": STORE.q.qsize(), "metrics": dict(STORE.metrics),
                       "commit_ms_avg": round(sum(cm) / len(cm), 2) if cm else None,
                       "types_known": len(STORE.known_types(tid))}}


def api_rate(qs):
    tid = _tid(qs)
    secs = _int(qs, "seconds", 300, 10, 86400)
    rows = STORE.reader().execute(
        "SELECT sec, objects, batches FROM ingest_seconds WHERE test_id=? AND sec>=? ORDER BY sec",
        (tid, int(time.time()) - secs)).fetchall()
    return {"test_id": tid, "points": rows}


def api_tree(qs):
    tid = _tid(qs)
    tree = STORE.known_types(tid)
    counts = dict(STORE.reader().execute("SELECT type_id, n FROM type_counts WHERE test_id=?", (tid,)).fetchall())
    nodes = {t: {"id": t, "parent": p, "name": n, "own": counts.get(t, 0), "total": 0, "children": []}
             for t, (p, n) in list(tree.items())}
    roots = []
    for t, nd in nodes.items():
        par = nodes.get(nd["parent"])
        (par["children"] if par else roots).append(nd)

    def tot(nd, depth=0):
        if depth > 64:
            return 0
        nd["total"] = nd["own"] + sum(tot(c, depth + 1) for c in nd["children"])
        nd["children"].sort(key=lambda c: -c["total"])
        return nd["total"]
    for r in roots:
        tot(r)
    hist = STORE.reader().execute(
        "SELECT revision, op, type_id, parent_id, name FROM type_history WHERE test_id=? AND op!='create' "
        "ORDER BY revision DESC LIMIT 50", (tid,)).fetchall()
    return {"test_id": tid, "revision": STORE.tree_rev.get(tid, 0), "roots": roots, "changes": hist}


def api_objects(qs):
    tid = _tid(qs)
    limit = _int(qs, "limit", 50, 1, 500)
    where, args = ["test_id=?"], [tid]
    if qs.get("type_id"):
        ids = subtree_ids(STORE.known_types(tid), _int(qs, "type_id", 0, 0, 1 << 40))
        where.append(f"type_id IN ({','.join('?' * len(ids))})")
        args += ids
    if qs.get("frame_kind"):
        where.append("frame_kind=?")
        args.append(qs["frame_kind"])
    if qs.get("before_seq"):
        where.append("seq<?")
        args.append(_int(qs, "before_seq", 0, 0, 1 << 62))
    q = (qs.get("q") or "").strip()
    if q:
        where.append("(id=? OR name LIKE ?)")
        args += [q, f"%{q}%"]
    sql = (f"SELECT id, seq, type_id, name, ts, frame_kind, frame_id, x, y, z FROM objects "
           f"WHERE {' AND '.join(where)} ORDER BY seq DESC LIMIT ?")
    rows = STORE.reader().execute(sql, args + [limit]).fetchall()
    tree = STORE.known_types(tid)
    cols = ["id", "seq", "type_id", "name", "ts", "frame_kind", "frame_id", "x", "y", "z"]
    out = []
    for r in rows:
        d = dict(zip(cols, r))
        d["type_name"] = tree.get(d["type_id"], [None, "?"])[1]
        out.append(d)
    return {"test_id": tid, "items": out, "next_before_seq": out[-1]["seq"] if len(out) == limit else None}


def type_path(tree, t):
    path, seen = [], set()
    while t in tree and t not in seen:
        seen.add(t)
        path.append({"id": t, "name": tree[t][1]})
        t = tree[t][0]
    return list(reversed(path))


def api_object(qs):
    tid = _tid(qs)
    r = STORE.reader().execute("SELECT body, batch_id FROM objects WHERE test_id=? AND id=?",
                               (tid, qs.get("id", ""))).fetchone()
    if not r:
        raise BadRequest("object not found")
    o = json.loads(r[0])
    return {"object": o, "batch_id": r[1], "type_path": type_path(STORE.known_types(tid), o["type_id"])}


def api_attrs(qs):
    """Schema discovery: attribute keys, value kinds and numeric ranges for a type subtree (sampled)."""
    tid = _tid(qs)
    args, where = [tid], "test_id=?"
    if qs.get("type_id"):
        ids = subtree_ids(STORE.known_types(tid), _int(qs, "type_id", 0, 0, 1 << 40))
        where += f" AND type_id IN ({','.join('?' * len(ids))})"
        args += ids
    rows = STORE.reader().execute(f"SELECT body FROM objects WHERE {where} ORDER BY seq DESC LIMIT 500",
                                  args).fetchall()
    stats = {}
    for (b,) in rows:
        for k, v in json.loads(b).get("attributes", {}).items():
            s = stats.setdefault(k, {"key": k, "count": 0, "kinds": {}, "min": None, "max": None, "top": {}})
            s["count"] += 1
            kind = type(v).__name__ if v is not None else "null"
            s["kinds"][kind] = s["kinds"].get(kind, 0) + 1
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                s["min"] = v if s["min"] is None else min(s["min"], v)
                s["max"] = v if s["max"] is None else max(s["max"], v)
            elif isinstance(v, (str, bool)) and len(str(v)) <= 40:
                s["top"][str(v)] = s["top"].get(str(v), 0) + 1
    out = sorted(stats.values(), key=lambda s: -s["count"])
    for s in out:
        s["top"] = sorted(s["top"].items(), key=lambda kv: -kv[1])[:5]
    return {"test_id": tid, "sampled": len(rows), "attributes": out}


def api_map(qs):
    tid = _tid(qs)
    limit = _int(qs, "limit", 4000, 100, 20000)
    fk = qs.get("frame_kind", "galactic")
    rows = STORE.reader().execute(
        "SELECT x, y, type_id, id FROM objects WHERE test_id=? AND frame_kind=? ORDER BY seq DESC LIMIT ?",
        (tid, fk, limit)).fetchall()
    tree = STORE.known_types(tid)
    # colour by top-level category (child of root)
    def top(t):
        seen = set()
        while t in tree and tree[t][0] in tree and tree[tree[t][0]][0] in tree and t not in seen:
            seen.add(t)
            t = tree[t][0]
        return t
    return {"test_id": tid, "frame_kind": fk,
            "points": [[r[0], r[1], top(r[2]), r[3]] for r in rows],
            "categories": {t: tree[t][1] for t in {top(r[2]) for r in rows} if t in tree}}


def api_rejections(qs):
    tid = _tid(qs)
    rows = STORE.reader().execute("SELECT at, kind, batch_id, reason FROM rejections WHERE test_id=? "
                                  "ORDER BY at DESC LIMIT 50", (tid,)).fetchall()
    return {"test_id": tid, "items": rows}


ROUTES = {"/api/health": api_health, "/api/runs": api_runs, "/api/overview": api_overview,
          "/api/rate": api_rate, "/api/tree": api_tree, "/api/objects": api_objects,
          "/api/object": api_object, "/api/attrs": api_attrs, "/api/map": api_map,
          "/api/rejections": api_rejections}


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256


if __name__ == "__main__":
    print(f"Cosmos Observatory listening on :{PORT}, data in {DATA_DIR}", flush=True)
    Server(("0.0.0.0", PORT), H).serve_forever()
