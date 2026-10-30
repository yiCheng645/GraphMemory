from __future__ import annotations

from memory_service import chunks, digest, encode

def source_units(db, user_id, msg, start, end):
    """Return exact indexed chunks covering a quote, including boundary-crossing quotes."""
    found, covered = [], start
    for a, b, content in chunks(msg["content"]):
        if b <= start or a >= end:
            continue
        unit_id = digest([msg["id"], a, b])
        unit = db.execute(
            "SELECT * FROM units WHERE id=? AND user_id=? AND message_id=?",
            (unit_id, user_id, msg["id"])).fetchone()
        if unit is None or unit["content"] != content:
            raise ValueError("missing or changed source chunk")
        if a > covered:
            raise ValueError("quote coverage has a gap")
        covered = max(covered, b)
        header = {"session_id": msg["session_id"], "role": msg["role"]}
        if msg["timestamp"] is not None:
            header["timestamp_ms"] = msg["timestamp"]
        found.append({
            "id": unit_id, "content": "[source " + encode(header) + "]\n" + content,
            "message_id": msg["id"], "span": [a, b],
        })
    if not found or covered < end:
        raise ValueError("quote is not fully covered by indexed chunks")
    return found
