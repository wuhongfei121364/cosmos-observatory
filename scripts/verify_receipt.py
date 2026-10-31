#!/usr/bin/env python3
"""Check stored data against the generator's receipt (the generator's ground truth).

usage: verify_receipt.py RECEIPT.json [DB_PATH]
Checks: every ACKed batch is stored exactly once (sent-but-unACKed batches,
e.g. requests cut off when the run's duration ends, may legitimately be stored); nothing stored that was never
sent; object totals match; every injected fault probe was rejected; tree revision.
"""
import base64
import json
import sqlite3
import sys


def bits(b64):
    raw = base64.b64decode(b64 or "")
    return {i * 8 + j for i, byte in enumerate(raw) for j in range(8) if byte >> j & 1}  # lsb0


def main():
    rec = json.load(open(sys.argv[1]))
    db = sqlite3.connect(sys.argv[2] if len(sys.argv) > 2 else "data/observatory.db")
    cfg, st = rec["config"], rec["stats"]
    tid, maxb = cfg["test_id"], cfg["max_batches"]
    sent, acked = bits(rec["sent_bitmap"]), bits(rec["ack_bitmap"])
    stored = {r[0]: r[1] for r in db.execute("SELECT batch_id, n FROM batches WHERE test_id=?", (tid,))}
    normal_stored = {b for b in stored if b < maxb}
    probes_stored = {b for b in stored if b >= maxb}
    inj = rec.get("injections", {})
    fault_sent = bits(inj.get("fault_sent_bitmap"))
    n_in_batches = sum(stored.values())
    n_obj = db.execute("SELECT count(*) FROM objects WHERE test_id=?", (tid,)).fetchone()[0]
    rev = (db.execute("SELECT tree_revision FROM runs WHERE test_id=?", (tid,)).fetchone() or [0])[0]
    checks = [
        ("acked batches all stored", acked <= normal_stored, len(acked - normal_stored)),
        ("no stored batch that was never sent", normal_stored <= sent, len(normal_stored - sent)),
        ("fault probes never stored", not probes_stored, len(probes_stored)),
        ("generator saw every fault probe rejected", bits(inj.get("fault_rejected_bitmap")) == fault_sent,
         f"{len(bits(inj.get('fault_rejected_bitmap')))} of {len(fault_sent)}"),
        ("objects table == sum of stored batch sizes (no partial batch)", n_obj == n_in_batches,
         f"{n_obj} vs {n_in_batches}"),
        ("acknowledged_objects <= stored objects <= attempted_objects",
         st["acknowledged_objects"] <= n_obj <= st["attempted_objects"],
         f"{st['acknowledged_objects']} <= {n_obj} <= {st['attempted_objects']}"),
        ("tree revision == acknowledged", rev == rec["tree"]["acknowledged_revision"],
         f"{rev} vs {rec['tree']['acknowledged_revision']}"),
    ]
    print(f"test_id={tid} sent={len(sent)} acked={len(acked)} stored={len(normal_stored)} "
          f"objects={n_obj} fault_probes_sent={len(fault_sent)} duplicates_sent={len(bits(inj.get('duplicate_sent_bitmap')))}")
    ok = True
    for name, passed, detail in checks:
        ok &= passed
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}  ({detail})")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
