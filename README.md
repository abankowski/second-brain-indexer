# Second Brain Indexer

Second Brain Indexer keeps one embedding per `mcp-memory` entity up to date. It is a small Rust service: it reads the graph through MCP, obtains embeddings from OpenAI, and writes vectors back through MCP. It never edits the `mcp-memory` database directly.

For V1, an entity's exact name is its identity and vector address. The target vector store currently requires 384-dimensional vectors. The service refuses to write if its configuration does not match the store.

## What you need

- A Debian/Ubuntu VM with `sudo` access.
- An HTTPS hostname pointing at that VM (for example `indexer.example.com`).
- A running `mcp-memory` endpoint and a **rotated** bearer token.
- An OpenAI API key and an embedding model that produces the target store's configured dimension.
- Nginx and a real authentication mechanism in front of the public endpoint.

This repository deliberately does not include a production token, TLS certificate, or deployment receiver.

## Fast path: install on a Debian/Ubuntu VM

Run the following on the VM as a regular sudo-capable user. Unless explicitly labelled otherwise, commands are identical in bash and fish.

### 1. Install system packages

```text
sudo apt update
sudo apt install -y build-essential ca-certificates curl git nginx pkg-config libssl-dev
```

### 2. Clone and build the pinned Rust project

```bash
git clone https://github.com/abankowski/second-brain-indexer.git
cd second-brain-indexer
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
. "$HOME/.cargo/env"
cargo build --release
```

```fish
git clone https://github.com/abankowski/second-brain-indexer.git
cd second-brain-indexer
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
source "$HOME/.cargo/env.fish"
cargo build --release
```

The project pins Rust in `rust-toolchain.toml`; you do not need to choose a Rust version yourself.

### 3. Run the local preflight

The commands are identical in bash and fish:

```text
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

All tests should pass before installation. The test suite does not contact your MCP endpoint or OpenAI.

### 4. Create the service account and directories

```text
sudo useradd --system --home /var/lib/second-brain-indexer --shell /usr/sbin/nologin second-brain-indexer
sudo install -d -o second-brain-indexer -g second-brain-indexer -m 0700 /var/lib/second-brain-indexer
sudo install -d -o root -g root -m 0755 /etc/second-brain-indexer
sudo install -m 0755 target/release/second-brain-indexer /usr/local/bin/second-brain-indexer
```

### 5. Configure the indexer

```text
sudo cp deploy/config.toml.example /etc/second-brain-indexer/config.toml
sudoedit /etc/second-brain-indexer/config.toml
```

Set the MCP endpoint, model, and dimension. Keep `dimensions = 384` unless the `mcp-memory` vector store has been deliberately reconfigured first. Never switch model/dimension by editing the indexer alone.

Create the secret file:

```text
sudoedit /etc/second-brain-indexer/secrets.env
sudo chown second-brain-indexer:second-brain-indexer /etc/second-brain-indexer/secrets.env
sudo chmod 0600 /etc/second-brain-indexer/secrets.env
```

Its contents are only:

```text
MCP_MEMORY_TOKEN=replace-with-a-rotated-token
OPENAI_API_KEY=replace-with-your-openai-key
```

### 6. Install and start systemd

```text
sudo cp deploy/second-brain-indexer.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now second-brain-indexer
sudo systemctl status second-brain-indexer --no-pager
```

If it fails, inspect the safe, redacted service log:

```text
sudo journalctl -u second-brain-indexer -n 100 --no-pager
```

### 7. Put nginx in front of it

Copy [`deploy/nginx-second-brain-indexer.conf`](deploy/nginx-second-brain-indexer.conf), replace the hostname and add TLS plus authentication. The service listens only on `127.0.0.1:9184`; do not expose that port directly.

Check and reload nginx (same in bash and fish):

```text
sudo nginx -t
sudo systemctl reload nginx
```

### 8. Verify and start the first scan

After nginx authentication is configured:

```text
curl --fail-with-body https://indexer.example.com/indexer/status
curl --fail-with-body -X POST https://indexer.example.com/indexer/fullscan
```

Replace `indexer.example.com` with your hostname. Save the `runId` from the `202` response, then query `/indexer/runs/<runId>` until it reaches a final state.

## Before production use

The deployed `mcp-memory` contract must be revalidated after every MCP upgrade. This checkout does not yet contain the read-only probe binary named by an older design note, so do not improvise a destructive check. In particular, verify that the target vector dimension is 384 and that the indexer has no deletion proof unless the MCP server explicitly provides one. Without that proof the service still indexes new/changed entities safely, but it deliberately does not delete vectors for missing entities.

## GitHub CI/CD

GitHub Actions runs CI on pushes to `master` and on pull requests. It checks formatting, strict Clippy, and all tests.

Deployment is manual: open **Actions → Deploy**, choose the protected environment, and provide an existing release tag. Configure `DEPLOY_URL` and `DEPLOY_TOKEN` as environment secrets. The required receiver contract is in [docs/github-cicd.md](docs/github-cicd.md).

## Common problems

| Symptom | First check |
|---|---|
| Service is not ready | `journalctl`; token file ownership/mode; MCP endpoint reachability; 384-dimension configuration. |
| `502 Bad Gateway` from nginx | `systemctl status second-brain-indexer`; nginx prefix config; local listener on `127.0.0.1:9184`. |
| `401` from the indexer | nginx/auth-proxy configuration, not the Rust service. |
| No vectors are deleted | Expected unless MCP provides an explicit complete-read proof. |
| Model/dimension mismatch | Prepare the MCP vector store externally first, then update indexer config and run a full scan. |

For operational detail, see [docs/runbook.md](docs/runbook.md).
