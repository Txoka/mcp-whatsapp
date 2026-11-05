#!/usr/bin/env python3
"""Read-only health check; never prints credentials or message contents."""

import json
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def rpc(method: str, params=None):
    authorization = (ROOT / "deployment/secrets/whatsapp.authorization").read_text().strip()
    request = urllib.request.Request(
        "http://127.0.0.1:8765/mcp",
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}).encode(),
        headers={
            "Authorization": authorization,
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
    )
    with urllib.request.urlopen(request, timeout=135) as response:
        body = response.read().decode()
    if not body.lstrip().startswith("{"):
        body = next(line[6:] for line in body.splitlines() if line.startswith("data: "))
    return json.loads(body)


def check() -> None:
    initialization = rpc(
        "initialize",
        {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "local-check", "version": "1"},
        },
    )
    print("Server:", initialization["result"]["serverInfo"])
    print("Tools:", len(rpc("tools/list")["result"]["tools"]))
    status = json.loads(rpc("tools/call", {"name": "get_status", "arguments": {}})["result"]["content"][0]["text"])
    print("Connection:", {key: status.get(key) for key in ("connected", "paired")})
    if not status.get("connected") or not status.get("paired"):
        raise SystemExit("WhatsApp is not connected and paired.")

    for name, arguments in (
        ("list_chats", {"limit": 1, "include_last_message": False}),
        ("list_messages", {"limit": 1}),
    ):
        result = rpc("tools/call", {"name": name, "arguments": arguments})
        if result.get("error") or result.get("result", {}).get("isError"):
            raise SystemExit(f"{name} failed.")
        print(f"{name}: OK")

    try:
        with urllib.request.urlopen("http://127.0.0.1:8765/mcp", timeout=5):
            pass
    except urllib.error.HTTPError as error:
        if error.code != 401:
            raise
        print("Unauthenticated access: rejected")
    else:
        raise SystemExit("Unauthenticated request was accepted.")


if __name__ == "__main__":
    check()
