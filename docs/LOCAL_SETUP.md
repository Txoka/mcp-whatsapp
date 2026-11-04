# Local deployment

This fork provides a compact reading interface over the upstream WhatsApp bridge. See [FORK_CHANGES.md](FORK_CHANGES.md) for behavior changes and limits.

## Setup

```sh
python3 ops/init_local.py
docker compose up -d --build
python3 ops/probe.py
```

The initializer preserves existing credentials. It creates ignored `.env` settings and private files under `deployment/secrets`. If the device is not paired, complete the authenticated pairing flow described in the upstream README before expecting the probe to pass.

| Path relative to the repository | Purpose |
| --- | --- |
| `deployment/store/messages.db` | Cached messages and locally synchronized metadata |
| `deployment/store/whatsapp.db` | Paired-device session and WhatsApp state |
| `deployment/store/uploads` | Shared attachment directory |
| `deployment/secrets/whatsapp.env` | Server token environment file |
| `deployment/secrets/whatsapp.authorization` | Authorization-header file for the client |
| `local-backups` and `archive` | Optional private recovery material; never committed |

The database directory mounts at `/home/app/store`. The upload directory also mounts at the same absolute path as on the host, using `WHATSAPP_UPLOADS_PATH` from `.env`. Media tools can use that path directly; arbitrary files such as `/tmp/example.png` still require copying into it. No network file-upload adapter is installed.

Runtime data, credentials, local settings and backups are excluded from Git and the Docker build context. Do not override those exclusions when publishing.

## Operation

```sh
docker compose restart
docker compose logs --tail 50
docker compose ps
python3 ops/probe.py
```

`ops/manage` wraps Compose from any working directory. The server exposes only `127.0.0.1:8765`. Keep this local binding unless you deliberately configure a different authenticated deployment.

For OpenCode, adapt `ops/opencode-whatsapp.example.json` with your absolute credential-file path. Configure approval for outgoing messages, read receipts and other mutations in the client. MCP annotations do not themselves enforce human approval; the local configuration used to develop this fork permits read-only tools and asks for writes.

Before replacing a running deployment, stop it and take a consistent copy of its store. Do not run two clients against the same session database. Local recovery snapshots and historical scripts are intentionally not published.
