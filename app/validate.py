"""Message validation. A batch is accepted or rejected as a whole (atomic)."""
import json

MAX_OBJECT_BYTES = 8192


class Invalid(Exception):
    pass


def _is_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def _is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def validate_types(m):
    if not isinstance(m.get("test_id"), str) or not m["test_id"]:
        raise Invalid("types: test_id")
    if not _is_int(m.get("revision")) or m["revision"] < 1:
        raise Invalid("types: revision")
    ch = m.get("changes")
    if not isinstance(ch, list):
        raise Invalid("types: changes")
    for c in ch:
        if not isinstance(c, dict) or c.get("op") not in ("create", "rename", "move"):
            raise Invalid("types: op")
        n = c.get("node")
        if not isinstance(n, dict) or not _is_int(n.get("type_id")) or not _is_int(n.get("parent_id")) \
                or not isinstance(n.get("name"), str):
            raise Invalid("types: node")


def validate_object(o, known_types):
    if not isinstance(o, dict):
        raise Invalid("object not a dict")
    if not isinstance(o.get("id"), str) or not o["id"]:
        raise Invalid("object id")
    if not _is_int(o.get("sequence")) or o["sequence"] < 0:
        raise Invalid("object sequence")
    if not _is_int(o.get("type_id")) or o["type_id"] not in known_types:
        raise Invalid("object type_id")
    if not isinstance(o.get("name"), str):
        raise Invalid("object name")
    if not _is_int(o.get("timestamp_ms")):
        raise Invalid("object timestamp_ms")
    p = o.get("position", "__missing__")
    if p == "__missing__":
        raise Invalid("object position missing")
    if p is not None:
        if not isinstance(p, dict) or not isinstance(p.get("frame_id"), str) \
                or not all(_is_num(p.get(k)) for k in ("x", "y", "z")):
            raise Invalid("object position")
    if not isinstance(o.get("attributes"), dict):
        raise Invalid("object attributes")
    raw = json.dumps(o, ensure_ascii=False, separators=(",", ":"))
    if len(raw.encode("utf-8")) > MAX_OBJECT_BYTES:
        raise Invalid("object too large")
    return raw


def validate_objects(m, known_types):
    if not isinstance(m.get("test_id"), str) or not m["test_id"]:
        raise Invalid("objects: test_id")
    if not _is_int(m.get("batch_id")) or m["batch_id"] < 0:
        raise Invalid("objects: batch_id")
    if not _is_int(m.get("tree_revision")):
        raise Invalid("objects: tree_revision")
    objs = m.get("objects")
    if not isinstance(objs, list) or not objs:
        raise Invalid("objects: objects")
    seen = set()
    rows = []
    for o in objs:
        raw = validate_object(o, known_types)
        if o["id"] in seen:
            raise Invalid("duplicate id inside batch")
        seen.add(o["id"])
        rows.append((o, raw))
    return rows
