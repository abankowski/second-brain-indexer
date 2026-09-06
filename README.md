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

Most VM users run Bash, so its instructions are shown first. The Fish alternative is collapsed; the only difference is the command that loads Cargo into the current shell.

```bash
git clone https://github.com/abankowski/second-brain-indexer.git
cd second-brain-indexer
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
. "$HOME/.cargo/env"
cargo build --release
```

<details>
<summary>Using Fish instead of Bash?</summary>

```fish
git clone https://github.com/abankowski/second-brain-indexer.git
cd second-brain-indexer
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
source "$HOME/.cargo/env.fish"
cargo build --release
```

</details>

The project pins Rust in `rust-toolchain.toml`; you do not need to choose a Rust version yourself.

### 3. Run the local preflight

The commands are identical in bash and fish:

```text
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

All tests should pass before installation. The test suite does not contact your MCP endpoint or OpenAI.

## Quick local smoke test (no systemd or nginx)

Use this when you want to start the executable directly from your shell before setting up the VM service. It starts on the `server.bind` address from the config (the template uses `127.0.0.1:9184`). It contacts MCP at startup to verify the vector dimension, but does not create embeddings or write vectors until you submit an indexing request.

Build the release binary, then make a local copy of the template:

```text
cargo build --release
mkdir -p .local
cp deploy/config.toml.example .local/config.toml
vi .local/config.toml
vi .local/secrets.env
openssl rand -base64 48 > .local/indexer-api-token
chmod 0600 .local/secrets.env
chmod 0600 .local/indexer-api-token
```

Set the real MCP endpoint and compatible embedding model in `.local/config.toml`; set `api.auth_token_file = ".local/indexer-api-token"`; and leave `dimensions = 384` unless the MCP vector store was deliberately changed. In `.local/secrets.env`, put the two secrets as unquoted `NAME=value` lines:

```text
MCP_MEMORY_TOKEN=replace-with-your-MCP-token
OPENAI_API_KEY=replace-with-your-OpenAI-key
```

`.local/` is ignored by Git. Keep the values free of whitespace; do not commit, paste, or pass secrets on the command line.

In **Bash**, load that file and give the executable its config path:

```bash
set -a
. .local/secrets.env
set +a
target/release/second-brain-indexer --config .local/config.toml
```

Direct shell runs print readable INFO messages to standard error: configuration accepted, MCP session/dimension verification, then `indexer ready` with its listener and polling state. This startup probe does **not** list MCP tools, read the graph, create embeddings, or write vectors. Leave that terminal running. In a second terminal, set the local API token and check readiness without nginx:

```bash
read -r -s -p "Indexer API token: " INDEXER_API_TOKEN; echo
curl --fail-with-body \
  -H "Authorization: Bearer $INDEXER_API_TOKEN" \
  http://127.0.0.1:9184/indexer/status
```

<details>
<summary>Using Fish instead of Bash?</summary>

```fish
for line in (string match -rv '^\s*(#|$)' < .local/secrets.env)
  set -l fields (string split -m1 = $line)
  set -gx $fields[1] $fields[2]
end
target/release/second-brain-indexer --config .local/config.toml
```

</details>

The VM service uses `/etc/second-brain-indexer/secrets.env` through systemd's `EnvironmentFile`; it is intentionally owned by the service account and should not be loaded into your login shell. Use the separate `.local/secrets.env` only for this direct local test. Do **not** call `POST /indexer/fullscan` until you are ready to use the configured OpenAI account and update vectors in your MCP instance. Stop the smoke test with `Ctrl+C`; the service safely drains and exits.

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

Create the separate API bearer-token file. It contains one raw token, following the `mcp-memory` HTTP-auth pattern; it is read once when the service starts.

```text
openssl rand -base64 48 | sudo tee /etc/second-brain-indexer/indexer-api-token > /dev/null
sudo chown second-brain-indexer:second-brain-indexer /etc/second-brain-indexer/indexer-api-token
sudo chmod 0600 /etc/second-brain-indexer/indexer-api-token
```

Do not put this token in `secrets.env`, TOML, Nginx, Git, logs, or shell history. Replacing the file and restarting the service rotates it; the old token stops working after restart.

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

Copy [`deploy/nginx-second-brain-indexer.conf`](deploy/nginx-second-brain-indexer.conf), replace the hostname and add TLS. The indexer itself enforces bearer authentication; Nginx passes the `Authorization` header through by default. The service listens only on `127.0.0.1:9184`; do not expose that port directly.

Before enabling the `/indexer/` location, define its rate-limit zone **once** in nginx's `http` scope. On Debian/Ubuntu, a file in `/etc/nginx/conf.d/` is included from that scope:

```text
sudoedit /etc/nginx/conf.d/second-brain-indexer-rate-limit.conf
```

Put exactly this line in that file (the `:10m` is required):

```nginx
limit_req_zone $binary_remote_addr zone=indexer_api:10m rate=10r/m;
```

Do not put `limit_req_zone` inside the `server` or `location` block. The supplied `/indexer/` fragment uses that allocation with `limit_req zone=indexer_api burst=20 nodelay;`.

Check and reload nginx (same in bash and fish):

```text
sudo nginx -t
sudo systemctl reload nginx
```

### 8. Verify and start the first scan

In **Bash**, enter the API token without putting it in shell history, then call the protected API:

```bash
read -r -s -p "Indexer API token: " INDEXER_API_TOKEN; echo
curl --fail-with-body \
  -H "Authorization: Bearer $INDEXER_API_TOKEN" \
  https://indexer.example.com/indexer/status
curl --fail-with-body \
  -H "Authorization: Bearer $INDEXER_API_TOKEN" \
  -X POST https://indexer.example.com/indexer/fullscan
```

<details>
<summary>Using Fish instead of Bash?</summary>

```fish
read -s -P "Indexer API token: " INDEXER_API_TOKEN; echo
curl --fail-with-body \
  -H "Authorization: Bearer $INDEXER_API_TOKEN" \
  https://indexer.example.com/indexer/status
curl --fail-with-body \
  -H "Authorization: Bearer $INDEXER_API_TOKEN" \
  -X POST https://indexer.example.com/indexer/fullscan
```

</details>

Replace `indexer.example.com` with your hostname. Save the `runId` from the `202` response, then query `/indexer/runs/<runId>` with the same `Authorization: Bearer` header until it reaches a final state.

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
| `zero size shared memory zone "indexer_api"` from nginx | Create `/etc/nginx/conf.d/second-brain-indexer-rate-limit.conf` in the `http` scope with `limit_req_zone $binary_remote_addr zone=indexer_api:10m rate=10r/m;`, then rerun `sudo nginx -t`. |
| `401` from the indexer | nginx/auth-proxy configuration, not the Rust service. |
| No vectors are deleted | Expected unless MCP provides an explicit complete-read proof. |
| Model/dimension mismatch | Prepare the MCP vector store externally first, then update indexer config and run a full scan. |

For operational detail, see [docs/runbook.md](docs/runbook.md).
