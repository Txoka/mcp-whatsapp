#!/usr/bin/env python3
"""Read-only local state diagnostics. No message bodies, tokens, or session keys."""
import json
import sqlite3
from pathlib import Path
root = Path(__file__).resolve().parents[1]
path = root / "deployment/store/messages.db"
connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
result = {}
for table in ("projection_inbox", "outgoing_intents", "history_requests", "operations"):
    if table not in tables:
        result[table] = {"available": False}
        continue
    if table == "operations":
        rows = connection.execute("SELECT kind,state,COUNT(*) FROM operations GROUP BY kind,state ORDER BY kind,state")
        result[table] = [{"kind": kind, "state": state, "records": count} for kind, state, count in rows]
    else:
        result[table] = dict(connection.execute(f"SELECT state,COUNT(*) FROM {table} GROUP BY state"))
if "message_conflicts" in tables:
    result["unresolved_content_records"] = connection.execute("SELECT COUNT(*) FROM message_conflicts").fetchone()[0]
if "projection_clock" in tables:
    result["projection_version"] = connection.execute("SELECT version FROM projection_clock WHERE id=1").fetchone()[0]
print(json.dumps(result, indent=2))
