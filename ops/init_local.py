#!/usr/bin/env python3
"""Initialize private deployment settings without replacing existing credentials."""

import os
import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def write_private_file(path: Path, text: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(text)


def initialize() -> None:
    private = ROOT / "deployment/secrets"
    private.mkdir(parents=True, exist_ok=True)
    private.chmod(0o700)
    store = ROOT / "deployment/store"
    store.mkdir(parents=True, exist_ok=True)
    store.chmod(0o700)
    uploads = store / "uploads"
    uploads.mkdir(exist_ok=True)

    environment = private / "whatsapp.env"
    if environment.exists():
        values = dict(
            line.split("=", 1)
            for line in environment.read_text().splitlines()
            if line and not line.startswith("#") and "=" in line
        )
        token = values.get("WHATSAPP_MCP_TOKEN", "")
        if not token:
            raise SystemExit("Existing credential file lacks WHATSAPP_MCP_TOKEN; inspect it locally.")
    else:
        token = secrets.token_urlsafe(32)
        write_private_file(environment, f"WHATSAPP_MCP_TOKEN={token}\n")
    environment.chmod(0o600)

    authorization = private / "whatsapp.authorization"
    if authorization.exists():
        if authorization.read_text().strip() != f"Bearer {token}":
            raise SystemExit("Existing authorization file does not match the token; inspect it locally.")
    else:
        write_private_file(authorization, f"Bearer {token}\n")
    authorization.chmod(0o600)

    settings = ROOT / ".env"
    if not settings.exists():
        if any(character in str(uploads) for character in ('\n', '\r', '$', '#', '"')):
            raise SystemExit("Project path cannot be safely written to Compose dotenv automatically.")
        write_private_file(
            settings,
            "COMPOSE_PROJECT_NAME=mcp-whatsapp\n"
            "WHATSAPP_IMAGE=mcp-whatsapp:local\n"
            f'WHATSAPP_UPLOADS_PATH="{uploads}"\n'
            f"LOCAL_UID={os.getuid()}\n"
            f"LOCAL_GID={os.getgid()}\n"
            "MCP_VERSION=dev\n",
        )
    settings.chmod(0o600)
    print("Local settings and private credentials are ready; no secret values printed.")


if __name__ == "__main__":
    initialize()
