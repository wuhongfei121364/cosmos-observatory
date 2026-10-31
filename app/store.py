"""SQLite storage with a single writer thread doing group commit.

HTTP handler threads validate a message, enqueue it, and wait. The writer
drains everything queued, applies it in ONE transaction, commits (fsync), and
only then wakes the waiters, who answer 2xx. So a 2xx always means durable.
"""
import collections
import json
import queue
import sqlite3
import threading
import time

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS runs(
  test_id TEXT PRIMARY KEY, first_seen REAL, last_seen REAL,
  objects INTEGER DEFAULT 0, batches INTEGER DEFAULT 0,
  dup_batches INTEGER DEFAULT 0, rejected INTEGER DEFAULT 0, tree_revision INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS batches(
  test_id TEXT, batch_id INTEGER, tree_revision INTEGER, n INTEGER, received_at REAL,
  PRIMARY KEY(test_id, batch_id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS type_revisions(
  test_id TEXT, revision INTEGER, body TEXT, received_at REAL,
  PRIMARY KEY(test_id, revision)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS types(
  test_id TEXT, type_id INTEGER, parent_id INTEGER, name TEXT,
  created_rev INTEGER, updated_rev INTEGER, PRIMARY KEY(test_id, type_id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS type_history(
  test_id TEXT, revision INTEGER, op TEXT, type_id INTEGER, parent_id INTEGER, name TEXT);
CREATE TABLE IF NOT EXISTS objects(
  test_id TEXT, id TEXT, seq INTEGER, type_id INTEGER, name TEXT, ts INTEGER,
  frame_kind TEXT, frame_id TEXT, unit TEXT, x REAL, y REAL, z REAL,
  batch_id INTEGER, body TEXT, PRIMARY KEY(test_id, id));
CREATE INDEX IF NOT EXISTS obj_seq ON objects(test_id, seq);
CREATE INDEX IF NOT EXISTS obj_type ON objects(test_id, type_id, seq);
CREATE INDEX IF NOT EXISTS obj_frame ON objects(test_id, frame_kind, seq);
CREATE TABLE IF NOT EXISTS type_counts(
  test_id TEXT, type_id INTEGER, n INTEGER, PRIMARY KEY(test_id, type_id)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS frame_counts(
  test_id TEXT, frame_kind TEXT, n INTEGER, PRIMARY KEY(test_id, frame_kind)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS ingest_seconds(
  test_id TEXT, sec INTEGER, objects INTEGER, batches INTEGER, PRIMARY KEY(test_id, sec)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS rejections(
  test_id TEXT, at REAL, kind TEXT, batch_id INTEGER, reason TEXT);
"""


class Item:
    __slots__ = ("kind", "msg", "rows", "done", "result")

    def __init__(self, kind, msg, rows=None):
        self.kind, self.msg, self.rows = kind, msg, rows
        self.done = threading.Event()
        self.result = None


class Store:
    def __init__(self, path, max_queue=2000, max_group=400):
        self.path = path
        self.q = queue.Queue(maxsize=max_queue)
        self.max_group = max_group
        self.lock = threading.Lock()
        self.trees = {}          # test_id -> {type_id: [parent_id, name]}
        self.tree_rev = {}       # test_id -> applied revision
        self.metrics = collections.Counter()
        self.commit_ms = collections.deque(maxlen=200)
        w = self._connect()
        w.executescript(SCHEMA)
        for tid, typ, par, name in w.execute("SELECT test_id,type_id,parent_id,name FROM types"):
            self.trees.setdefault(tid, {})[typ] = [par, name]
        for tid, rev in w.execute("SELECT test_id, tree_revision FROM runs"):
            self.tree_rev[tid] = rev
        w.close()
        self.writer = threading.Thread(target=self._writer_loop, daemon=True, name="writer")
        self.writer.start()
        self._local = threading.local()

    def _connect(self):
        c = sqlite3.connect(self.path, timeout=30, check_same_thread=False, isolation_level=None)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=FULL")   # 2xx must mean durable
        c.execute("PRAGMA cache_size=-65536")
        c.execute("PRAGMA busy_timeout=30000")
        return c

    def reader(self):
        c = getattr(self._local, "c", None)
        if c is None:
            c = self._connect()
            c.execute("PRAGMA query_only=1")
            self._local.c = c
        return c

    # ---------- write path ----------
    def known_types(self, test_id):
        return self.trees.get(test_id, {})

    def submit(self, item, timeout=25):
        try:
            self.q.put(item, timeout=2)
        except queue.Full:
            self.metrics["backpressure_503"] += 1
            return ("busy", None)
        if not item.done.wait(timeout):
            return ("timeout", None)
        return item.result

    def record_rejection(self, test_id, kind, batch_id, reason):
        self.metrics["rejected"] += 1
        try:  # bounded wait: rejection stats should not be silently lost under load
            self.q.put(Item("reject", (test_id, kind, batch_id, reason)), timeout=5)
        except queue.Full:
            self.metrics["rejections_unrecorded"] += 1

    def _writer_loop(self):
        db = self._connect()
        while True:
            first = self.q.get()
            group = [first]
            while len(group) < self.max_group:
                try:
                    group.append(self.q.get_nowait())
                except queue.Empty:
                    break
            t0 = time.time()
            try:
                db.execute("BEGIN IMMEDIATE")
                agg = _Agg()
                results = [self._apply(db, it, agg, t0) for it in group]
                agg.flush(db)
                db.execute("COMMIT")
                ok = True
            except Exception as e:  # never leave waiters hanging
                try:
                    db.execute("ROLLBACK")
                except Exception:
                    pass
                ok = False
                err = repr(e)
                self.metrics["writer_errors"] += 1
                # tree cache may be ahead of DB; reload it
                self._reload_trees(db)
            self.commit_ms.append((time.time() - t0) * 1000)
            self.metrics["commits"] += 1
            for i, it in enumerate(group):
                it.result = results[i] if ok else ("error", err)
                it.done.set()

    def _reload_trees(self, db):
        with self.lock:
            self.trees.clear()
            for tid, typ, par, name in db.execute("SELECT test_id,type_id,parent_id,name FROM types"):
                self.trees.setdefault(tid, {})[typ] = [par, name]
            self.tree_rev = dict(db.execute("SELECT test_id, tree_revision FROM runs").fetchall())

    def _ensure_run(self, db, tid, now):
        db.execute("INSERT INTO runs(test_id, first_seen, last_seen) VALUES(?,?,?) "
                   "ON CONFLICT(test_id) DO UPDATE SET last_seen=excluded.last_seen", (tid, now, now))

    def _apply(self, db, it, agg, now):
        if it.kind == "reject":
            tid, kind, bid, reason = it.msg
            if tid:
                self._ensure_run(db, tid, now)
                db.execute("UPDATE runs SET rejected=rejected+1 WHERE test_id=?", (tid,))
            db.execute("INSERT INTO rejections VALUES(?,?,?,?,?)", (tid, now, kind, bid, reason))
            return ("ok", None)
        m = it.msg
        tid = m["test_id"]
        self._ensure_run(db, tid, now)
        if it.kind == "types":
            rev = m["revision"]
            cur = self.tree_rev.get(tid, 0)
            if rev <= cur:
                self.metrics["dup_types"] += 1
                return ("dup", None)
            if rev != cur + 1:
                return ("gap", cur)
            tree = self.trees.setdefault(tid, {})
            db.execute("INSERT INTO type_revisions VALUES(?,?,?,?)",
                       (tid, rev, json.dumps(m, ensure_ascii=False), now))
            for c in m["changes"]:
                n = c["node"]
                db.execute("INSERT INTO type_history VALUES(?,?,?,?,?,?)",
                           (tid, rev, c["op"], n["type_id"], n["parent_id"], n["name"]))
                db.execute("INSERT INTO types VALUES(?,?,?,?,?,?) ON CONFLICT(test_id,type_id) DO UPDATE SET "
                           "parent_id=excluded.parent_id, name=excluded.name, updated_rev=excluded.updated_rev",
                           (tid, n["type_id"], n["parent_id"], n["name"], rev, rev))
                tree[n["type_id"]] = [n["parent_id"], n["name"]]
            self.tree_rev[tid] = rev
            db.execute("UPDATE runs SET tree_revision=? WHERE test_id=?", (rev, tid))
            return ("ok", None)
        # objects
        bid = m["batch_id"]
        cur = db.execute("INSERT OR IGNORE INTO batches VALUES(?,?,?,?,?)",
                         (tid, bid, m["tree_revision"], len(it.rows), now))
        if cur.rowcount == 0:
            db.execute("UPDATE runs SET dup_batches=dup_batches+1 WHERE test_id=?", (tid,))
            self.metrics["dup_batches"] += 1
            return ("dup", None)
        params = []
        for o, raw in it.rows:
            p = o.get("position") or {}
            fk = p.get("frame_kind") or ("none" if not p else "unknown")
            params.append((tid, o["id"], o["sequence"], o["type_id"], o["name"], o["timestamp_ms"],
                           fk, p.get("frame_id"), p.get("unit"), p.get("x"), p.get("y"), p.get("z"), bid, raw))
        before = db.total_changes
        db.executemany("INSERT OR IGNORE INTO objects VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", params)
        inserted = db.total_changes - before
        if inserted == len(params):
            for o, _ in it.rows:
                agg.type[(tid, o["type_id"])] += 1
            for row in params:
                agg.frame[(tid, row[6])] += 1
        else:  # rare: object id seen in an earlier batch; recount precisely
            self.metrics["dup_object_ids"] += len(params) - inserted
            agg.recount.add(tid)
        sec = int(now)
        agg.sec[(tid, sec)][0] += inserted
        agg.sec[(tid, sec)][1] += 1
        agg.run[tid][0] += inserted
        agg.run[tid][1] += 1
        self.metrics["objects"] += inserted
        self.metrics["batches"] += 1
        return ("ok", inserted)


class _Agg:
    def __init__(self):
        self.type = collections.Counter()
        self.frame = collections.Counter()
        self.sec = collections.defaultdict(lambda: [0, 0])
        self.run = collections.defaultdict(lambda: [0, 0])
        self.recount = set()

    def flush(self, db):
        db.executemany("INSERT INTO type_counts VALUES(?,?,?) ON CONFLICT DO UPDATE SET n=n+excluded.n",
                       [(t, ty, n) for (t, ty), n in self.type.items()])
        db.executemany("INSERT INTO frame_counts VALUES(?,?,?) ON CONFLICT DO UPDATE SET n=n+excluded.n",
                       [(t, f, n) for (t, f), n in self.frame.items()])
        db.executemany("INSERT INTO ingest_seconds VALUES(?,?,?,?) ON CONFLICT DO UPDATE SET "
                       "objects=objects+excluded.objects, batches=batches+excluded.batches",
                       [(t, s, v[0], v[1]) for (t, s), v in self.sec.items()])
        db.executemany("UPDATE runs SET objects=objects+?, batches=batches+? WHERE test_id=?",
                       [(v[0], v[1], t) for t, v in self.run.items()])
        for t in self.recount:
            db.execute("DELETE FROM type_counts WHERE test_id=?", (t,))
            db.execute("INSERT INTO type_counts SELECT test_id,type_id,count(*) FROM objects WHERE test_id=? "
                       "GROUP BY type_id", (t,))
            db.execute("DELETE FROM frame_counts WHERE test_id=?", (t,))
            db.execute("INSERT INTO frame_counts SELECT test_id,frame_kind,count(*) FROM objects WHERE test_id=? "
                       "GROUP BY frame_kind", (t,))
