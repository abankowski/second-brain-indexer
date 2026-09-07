# Second Brain Indexer

Second Brain Indexer keeps one embedding per `mcp-memory` entity up to date. It is a small Rust service: it reads the graph through MCP, obtains embeddings from an OpenAI-compatible provider or native Ollama, and writes vectors back through MCP. It never edits the `mcp-memory` database directly.

For V1, an entity's exact name is its identity and vector address. The target vector store currently requires 384-dimensional vectors. The service refuses to write if its configuration does not match the store.

## What you need

- A Debian/Ubuntu VM with `sudo` access.
- An HTTPS hostname pointing at that VM (for example `indexer.example.com`).
- A running `mcp-memory` endpoint and a **rotated** bearer token.
- Either an OpenAI-compatible embedding API key, or a reachable native Ollama server with the selected model installed.
- Nginx and a real authentication mechanism in front of the public endpoint.

This repository deliberately does not include a production token, TLS certificate, or deployment receiver.

## Fast path: install on a Debian/Ubuntu VM

Run the following on the VM as a regular sudo-capable user. Unless explicitly labelled otherwise, commands are identical in bash and fish.

### 1. Install system packages

```text
sudo apt update
sudo apt install -y build-essential ca-certificates curl git nginx openssl pkg-config libssl-dev
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

Set the real MCP endpoint and a provider configuration in `.local/config.toml`; set `api.auth_token_file = ".local/indexer-api-token"`. The copied template starts with the OpenAI-compatible 384-dimensional example. In `.local/secrets.env`, put the MCP token and, only for the OpenAI-compatible provider, its API key as unquoted `NAME=value` lines:

```text
MCP_MEMORY_TOKEN=replace-with-your-MCP-token
OPENAI_API_KEY=replace-with-your-openai-compatible-key
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

The VM service uses `/etc/second-brain-indexer/secrets.env` through systemd's `EnvironmentFile`; it is intentionally owned by the service account and should not be loaded into your login shell. Use the separate `.local/secrets.env` only for this direct local test. Native Ollama does not use `OPENAI_API_KEY`; remove that line rather than leaving an unused key in the file. Do **not** call `POST /indexer/fullscan` until you are ready to use the configured provider and update vectors in your MCP instance. Stop the smoke test with `Ctrl+C`; the service safely drains and exits.

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

Set the MCP endpoint, model, and dimension. The template's active example is OpenAI-compatible: `provider = "openai-compatible"`, `base_url = "https://api.openai.com/v1"`, and `api_key_env = "OPENAI_API_KEY"`. Its `dimensions = 384` must equal the MCP vector-store dimension. For native Ollama, replace the entire `[embedding]` block with the commented `provider = "ollama"`, `model = "bge-m3"`, `dimensions = 1024`, and `base_url = "http://127.0.0.1:11434"` alternative. Ollama rejects `api_key_env`; do not add it.

Create the secret file:

```text
sudoedit /etc/second-brain-indexer/secrets.env
sudo chown second-brain-indexer:second-brain-indexer /etc/second-brain-indexer/secrets.env
sudo chmod 0600 /etc/second-brain-indexer/secrets.env
```

Its contents are only:

```text
MCP_MEMORY_TOKEN=replace-with-a-rotated-token
OPENAI_API_KEY=replace-with-your-openai-compatible-key
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

The service writes startup and completed-run events to its standard output (systemd stores them in the journal). A failed run includes safe aggregate fields such as `failed=253` and `failure_classes={"embedding_unauthorized": 253}`; it never logs tokens, entity text, or provider response bodies. Inspect the service log with:

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

### Diagnose a completed failed run

`/indexer/runs/<runId>` intentionally returns counters only. On an installed VM, the protected SQLite state holds the safe error class and message for each failed work item. This command is identical in Bash, Zsh, and Fish; replace the run ID with yours:

```text
sudo sqlite3 -header -column /var/lib/second-brain-indexer/indexer-state.db \
  "SELECT last_error_code AS code, last_error_message AS message, COUNT(*) AS affected_entities FROM run_work WHERE run_id = 'YOUR-RUN-ID' AND status = 'failed' GROUP BY last_error_code, last_error_message ORDER BY affected_entities DESC;"
```

Typical results identify the boundary that failed: `embedding_unauthorized` means the configured OpenAI-compatible embedding service rejected its key; `mcp_transport`, `mcp_unauthorized`, or `mcp_server` identify the mcp-memory side. The database path is the `state.database_path` value in your config if you changed the deployment default.

For the standard OpenAI endpoint, set `embedding.provider = "openai-compatible"`, `embedding.model = "text-embedding-3-small"`, and choose a dimension supported by both OpenAI and the rebuilt MCP store (the template uses 384). The indexer sends that dimension explicitly in the embeddings request. For native Ollama, use `embedding.provider = "ollama"`; it sends batches to Ollama's stable `/api/embed` endpoint and does not use Ollama's experimental OpenAI-compatible API.

## BGE-M3 1024-dimensional migration

Changing a provider, model, or dimension changes the embedding space. Old and new vectors must never coexist. This is a coordinated maintenance operation owned by the `mcp-memory` operator; the indexer neither deletes nor rebuilds that database.

1. Stop the indexer and pause callers that could enqueue scans.
2. Rebuild the mcp-memory vector store for 1024 dimensions, using that deployment's documented rebuild procedure. This removes the old vector index; do not attempt to preserve 384-dimensional vectors.
3. Run the mcp-memory read-only vector-store check and proceed only when it reports `dims: 1024` and an empty/rebuilt vector index. This is the migration gate: the indexer refuses to bind when its configured, provider-probed, and MCP dimensions differ.
4. Replace `[embedding]` with the native Ollama `bge-m3` example in `config.toml`, remove `OPENAI_API_KEY` from the service secret file, then restart the service. Confirm the journal records matching configured and MCP dimensions before continuing.
5. Trigger exactly one full reindex and poll its run to a final state. The full scan is required because every entity needs a new BGE-M3 vector.

The commands below are identical in Bash and Fish after `INDEXER_API_TOKEN` is set with the shell-specific secure prompt shown above:

```text
curl --fail-with-body -H "Authorization: Bearer $INDEXER_API_TOKEN" \
  -X POST https://indexer.example.com/indexer/fullscan
curl --fail-with-body -H "Authorization: Bearer $INDEXER_API_TOKEN" \
  https://indexer.example.com/indexer/runs/YOUR-RUN-ID
```

Do not use an incremental entity reindex as a substitute for this migration. If the provider probe or MCP dimension check fails, leave the service stopped and correct the configuration or vector-store rebuild before retrying.

## Indexer MCP for semantic search and orchestration

`https://indexer.example.com/indexer/mcp` is an authenticated Streamable HTTP MCP endpoint. It uses the same `Authorization: Bearer` token as the REST indexer API, embeds natural-language queries on the server, and never returns raw vectors or provider credentials. Keep `mcp-memory` configured separately for graph reads and writes.

For natural-language retrieval, use `indexer_semantic_search` or `indexer_hybrid_search` rather than mcp-memory raw-vector tools. The indexer provides exactly these tools:

| Tool | Use |
| --- | --- |
| `indexer_semantic_search` | Natural-language vector search; accepts `query`, optional `limit`, optional `entityType`. |
| `indexer_hybrid_search` | Natural-language text-plus-vector search; accepts the same inputs. |
| `indexer_reindex_entity` | Queue one exact `entityName`; returns a durable `runId`. |
| `indexer_reindex_all` | Queue a durable full graph reindex; returns a `runId`. |
| `indexer_run_status` | Read a run by `runId`. |

Before production use, test this endpoint against a controlled/local target: call MCP `initialize`, `tools/list`, one semantic search, one hybrid search, one entity reindex, one full reindex, and `indexer_run_status`. Confirm bearer authentication is enforced and inspect only the normalized tool results; never send raw embeddings from a client.

## Before production use

The deployed `mcp-memory` contract must be revalidated after every MCP upgrade. This checkout does not yet contain the read-only probe binary named by an older design note, so do not improvise a destructive check. In particular, verify that the target vector dimension equals the configured provider dimension and that the indexer has no deletion proof unless the MCP server explicitly provides one. Without that proof the service still indexes new/changed entities safely, but it deliberately does not delete vectors for missing entities.

## GitHub CI/CD

GitHub Actions runs CI on pushes to `master` and on pull requests. It checks formatting, strict Clippy, and all tests.

Deployment is manual: open **Actions → Deploy**, choose the protected environment, and provide an existing release tag. Configure `DEPLOY_URL` and `DEPLOY_TOKEN` as environment secrets. The required receiver contract is in [docs/github-cicd.md](docs/github-cicd.md).

## Common problems

| Symptom | First check |
|---|---|
| Service is not ready | `journalctl`; token file ownership/mode; MCP endpoint reachability; matching provider/MCP dimensions. |
| `502 Bad Gateway` from nginx | `systemctl status second-brain-indexer`; nginx prefix config; local listener on `127.0.0.1:9184`. |
| `zero size shared memory zone "indexer_api"` from nginx | Create `/etc/nginx/conf.d/second-brain-indexer-rate-limit.conf` in the `http` scope with `limit_req_zone $binary_remote_addr zone=indexer_api:10m rate=10r/m;`, then rerun `sudo nginx -t`. |
| `401` from the indexer | Send the configured `Authorization: Bearer` token; check the token file's owner/mode and restart the service after rotation. |
| No vectors are deleted | Expected unless MCP provides an explicit complete-read proof. |
| Model/dimension mismatch | Stop the indexer; rebuild the MCP vector store for the new dimension; verify it; update configuration; then run one full scan. |

For operational detail, see [docs/runbook.md](docs/runbook.md).
