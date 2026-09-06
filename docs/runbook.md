# Runbook: Second Brain Indexer

## Install and configure

Create a system user with no login shell, then install the binary, [service unit](../deploy/second-brain-indexer.service), [nginx fragment](../deploy/nginx-second-brain-indexer.conf), and [configuration template](../deploy/config.toml.example). The service binds only `127.0.0.1:9184`; nginx is the TLS and rate-limit boundary, while the indexer enforces bearer authentication.

Create `/etc/second-brain-indexer/secrets.env` as root, owned by `second-brain-indexer`, mode `0600`:

```text
MCP_MEMORY_TOKEN=replace-with-rotated-token
OPENAI_API_KEY=replace-with-openai-key
```

Never put either value in TOML, systemd unit text, nginx, Git, logs, or a support ticket.

Create `/etc/second-brain-indexer/indexer-api-token`, owned by `second-brain-indexer`, mode `0600`. It holds one raw bearer token, trimmed and read once at startup:

```text
openssl rand -base64 48 | sudo tee /etc/second-brain-indexer/indexer-api-token > /dev/null
sudo chown second-brain-indexer:second-brain-indexer /etc/second-brain-indexer/indexer-api-token
sudo chmod 0600 /etc/second-brain-indexer/indexer-api-token
```

The config template points `api.auth_token_file` to it. An absent field disables authentication only for local development; an empty or unreadable configured file prevents startup. Rotate by atomically replacing the file and restarting the service.

## Nginx rate-limit zone

The supplied nginx fragment uses `limit_req zone=indexer_api`. Define that zone once in nginx's `http` scope; on Debian/Ubuntu, create `/etc/nginx/conf.d/second-brain-indexer-rate-limit.conf` with:

```nginx
limit_req_zone $binary_remote_addr zone=indexer_api:10m rate=10r/m;
```

The `:10m` shared-memory size is required. Do not place `limit_req_zone` inside the TLS `server` block or an `/indexer/` `location` block. Validate with `sudo nginx -t` before reloading nginx.

## Preflight

Run these checks before installation or an upgrade. They do not alter MCP graph or vector data.

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

```fish
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

The commands are identical in bash and fish. Before deployment and after every MCP upgrade, run the approved read-only MCP contract probe against the intended endpoint; accept it only when its observed vector dimension is `384` and its complete-read/delete contract remains supported. This checkout does not currently include the `probe-mcp` binary described in the LLD, so that target validation remains an explicit release blocker rather than a command to improvise.

## Start and validate

After copying files, run `systemctl daemon-reload` and `systemctl enable --now second-brain-indexer`. Verify local readiness through nginx's intended path, then submit a full scan:

```bash
curl --fail-with-body https://example.invalid/indexer/status
curl --fail-with-body -X POST https://example.invalid/indexer/fullscan
```

```fish
curl --fail-with-body https://example.invalid/indexer/status
curl --fail-with-body -X POST https://example.invalid/indexer/fullscan
```

Replace `example.invalid` with the approved TLS hostname and add `-H "Authorization: Bearer $INDEXER_API_TOKEN"` to every request. Poll run status with the `runId` returned by the `202` response. Do not call the loopback service from outside the host.

## Shutdown and recovery

`systemctl stop second-brain-indexer` sends SIGTERM. The process first refuses new `POST` requests with `503` and error code `shutting_down`; read-only status and metrics remain available while the server drains. The next exclusive start recovers leased/indexing state before accepting work.

If readiness is false, inspect `journalctl -u second-brain-indexer` for the bounded error class, then verify: secret-file permissions, MCP reachability, and the configured 384 dimension. Do not edit `indexer-state.db` or the `mcp-memory` database directly.

## Alerts

Alert when no successful poll completes for two polling intervals, `delete_pending` is non-zero, authorization errors repeat, or the queue backlog grows. Resolve MCP/model dimension changes as a coordinated external maintenance operation: prepare the target vector store first, update configuration, then run a full scan.
