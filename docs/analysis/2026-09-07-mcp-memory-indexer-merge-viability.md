# Viability: mcp-memory and Second Brain Indexer

## Decision

**Viable, but do not merge the current indexer process into mcp-memory's MCP request dispatcher.**

Fork `mcp-memory` into one Cargo workspace with a small shared domain core and optional worker crates. A small VM may run one composed binary, but indexing and webhook delivery must remain independently bounded, disableable roles. The lean graph-only MCP server must keep working without an embedding provider, network dependency, or worker workload.

This is not a cosmetic refactor. Reliable hooks require a transaction-safe graph mutation boundary first; the current mutation paths are not uniformly transactional.

## Source snapshot and evidence

Analysis used upstream `corporatepiyush/mcp-memory` commit `d6fe34be505b52afb88a347141ee63378b25663e` (package 5.2.1).

- One runtime creates an `MCPServer`, then chooses stdio or HTTP (`src/main.rs:7-10`, `39-50`). The server already holds optional `GraphHandle` and `VectorStore` state (`src/server.rs:215-244`), which is a useful feature boundary.
- HTTP routes support Streamable MCP `POST` and `GET /mcp` (`src/http.rs:69-87`), but GET is a keep-alive SSE stream, not server event delivery (`src/http.rs:201-214`). Existing MCP is not an event-subscription API.
- Graph reads use WAL reader connections while writes use a single writer mutex (`src/kg.rs:402-437`, `492-502`). This is the performance property to preserve.
- Vector storage opens another SQLite connection to the same file and rebuilds ANN state from durable vector rows on start (`src/vector_store.rs:345-369`, `446-487`). ANN maintenance and some vector writes contend on their own locks; synchronous reindex is currently an MCP tool path (`src/vector_actions.rs:740-758`).
- Some graph mutations use an immediate transaction (`create_entities`, `src/kg.rs:771-878`; relations, `1026-1100`), while deletes and observation changes span multiple statements without that common transaction boundary (`894-1009`, `1277-1324`).
- The existing watcher is only a feature-gated filesystem/code watcher (`src/watcher.rs:1-102`), so it cannot become an entity hook system.

## Recommended shape

```text
MCP / HTTP / stdio
        |
   memory-mcp              Query path: graph/FTS/vector search only
        |
    memory-core
  Graph mutation service -- one SQLite transaction -- change_event + outbox
        |                                      |
  graph/vector reads                      bounded worker roles
                                          |- indexer-worker
                                          `- webhook-worker
```

Suggested crates:

| Crate | Owns | Must not own |
|---|---|---|
| `memory-core` | graph mutation service, typed change sets, outbox/job repositories | HTTP, MCP JSON, provider clients |
| `memory-mcp` | MCP tools, authentication, tool registry | embedding/webhook execution |
| `indexer-worker` | canonicalization, embedding provider, vector upsert/delete, reconciliation full scan | request-path search |
| `webhook-worker` | subscription matching, signed HTTP delivery, retry/dead-letter | graph writes |
| composition binary | config and lifecycle of selected roles | domain policy duplication |

### Build features and runtime roles

Use two layers of selection; they solve different problems and must not be conflated.

| Layer | Mechanism | Purpose |
|---|---|---|
| Build time | Cargo features `indexer`, `webhooks`, `bedrock` | removes optional code and dependencies from a lean graph-only binary |
| Runtime | repeatable `--role mcp`, `--role indexer`, `--role webhooks` | decides which compiled roles are started in this process |

`--role mcp,indexer,webhooks` is the convenient one-VM mode. The same compiled binary may instead be started three times with one role each. Reject a selected role whose feature was not compiled, and reject an empty role set. Configuration must expose per-role budgets rather than process-wide defaults:

- `mcp.max_in_flight_requests`, reader-pool size, and vector-search capacity;
- `indexer.max_jobs`, `embedding.max_in_flight`, batch size, provider timeout, and CPU/ANN rebuild limit;
- `webhooks.max_deliveries`, per-subscription ordering limit, timeout, retry/backoff, and dead-letter threshold.

The composed mode is operational convenience, not hard isolation. A single process still shares CPU, memory, and Tokio's global blocking pool. Therefore `memory-mcp` must use its own admission limits, and CPU-heavy ANN/rebuild work needs dedicated named threads or a dedicated worker runtime. Separate role processes under systemd/cgroups remain the production option when resource isolation is required.

### Replaceable indexer/provider contract

`memory-core` stores a provider-neutral job: "entity revision R requires index profile P". It must not know OpenAI, Ollama, Bedrock, or HTTP. `indexer-worker` selects an `EmbeddingProvider` through a registry:

```text
EmbeddingProvider::embed(profile, texts) -> Result<Vec<Embedding>, EmbeddingError>
IndexProfile { provider, model, dimensions, representation_version }
```

OpenAI-compatible, Ollama, Bedrock, and future providers implement that contract behind optional Cargo features. The `bedrock` feature alone pulls AWS SDK dependencies; a graph-only build has none. `IndexProfileRegistry` keeps exactly one active profile per vector store. A profile change enters a rebuilding state, blocks incompatible jobs/searches, clears or rebuilds vectors through an explicit operation, and activates only after validation. Vectors from different profiles must never be mixed.

Keep the existing graph/vector tools and add server-side semantic query tools in `memory-mcp`. Do not run the existing indexer as an HTTP client of its own fork: that preserves duplicate local state, polling, and a non-atomic graph-to-index gap.

## Hooks and external automation

Add a **transactional outbox**, not callbacks in MCP handlers and not reuse of `code_watch`.

Every effective committed entity change creates immutable rows:

- `change_event`: UUID, transaction/change ID, `create|update|delete`, entity name/type, before/after snapshots or tombstone, changed fields, actor, origin, correlation ID, causation ID, hop count, timestamp. Webhook payloads use an allowlisted projection, never arbitrary observation bodies.
- `webhook_subscription`: endpoint, encrypted/secret reference, event mask, entity-type filter, name/prefix filter, enabled state, producer/origin policy.
- `event_outbox`: `(event_id, subscription_id)` unique pair, lease, attempts, next attempt, error/dead-letter state.
- `index_job`: entity identity plus latest graph revision, coalesced by entity; delete is an idempotent vector-delete job.

Commit graph state and enqueue events/jobs in the same SQLite transaction. Workers claim leased rows, perform side effects, retry with backoff/jitter, and eventually dead-letter. Delivery is **at least once**; consumers deduplicate by `eventId`. Exactly-once HTTP delivery is neither realistic nor needed.

A relation mutation emits an `update` change for both endpoint entities with a relation delta. Both canonical entity documents can therefore be reindexed, and entity-filtered subscriptions can react without a later graph read.

Any external automation client — n8n, Node-RED, a CI job, or a custom service — uses the same authenticated write contract: machine identity, caller-selected `origin`, correlation ID, causation ID, hop count, and idempotency key. Outbound subscriptions default to excluding their own origin; a causal-hop budget prevents direct loops and bounds faulty multi-system loops. The webhook payload is a generic signed HTTP event envelope, not an n8n schema.

Subscription registration belongs in a protected REST/admin API, not MCP tools exposed to an LLM: callback URLs and secret references are infrastructure configuration, not model input.

Webhook registration applies HTTPS-by-default, redirects-disabled egress policy and either an explicit allowlist or resolver-backed rejection of loopback, link-local, private, and DNS-rebinding destinations. The delivery connector must dial the validated `SocketAddr` while retaining the original Host/SNI, or use an egress proxy that enforces the policy; validating DNS and then allowing a second resolver lookup is insufficient. Only administrators authorized for the secret scope can create/view a subscription using that secret reference.

## Performance rule

The MCP read/search path must never await embedding, webhook HTTP, ANN rebuild, or a worker queue. Workers need separate bounded concurrency and rate limits; CPU-heavy ANN rebuild/training belongs in a dedicated blocking pool or separate worker process, not a casual `tokio::spawn` on the request runtime. ANN rebuild builds a replacement serving index and swaps it only when ready; query tasks retain a reader snapshot. A durable `ann_generation` marker and reconciliation path prevent a post-vector-commit crash from hiding a durable row from ANN search. Reserve capacity for search and collect separate queue-depth, latency, retry, and dead-letter metrics.

Only SQLite mutation, claim, and completion operations use `BEGIN IMMEDIATE`, a finite busy timeout/retry policy, and database-owned leases with monotonically increasing fencing epochs. Search uses the existing deferred read-only WAL path. Before a vector write, the indexer rechecks lease epoch, entity revision, and active profile in the write transaction; stale workers may waste a provider call but cannot commit or publish a vector effect.

For a one-VM deployment, one binary may start all enabled roles. For production isolation, run the same binary as `--role mcp`, `--role indexer`, and `--role webhooks` in separate processes against the same database with explicit role configuration. This keeps the lightweight original use case intact.

## First implementation slice

1. Refactor every graph mutation behind one transactional `MutationService`; no hooks yet.
2. Persist one typed change event and one coalesced index job atomically with a graph mutation.
3. Add a single-worker indexer execution path and crash/retry tests.
4. Measure search p95 while deliberately stalling embeddings; set a budget before enabling concurrency.
5. Add subscriptions/outbox delivery, HMAC signing, generic origin-loop tests, retries, and dead-letter administration.
6. Add a second provider adapter only after the provider-neutral index job and profile migration tests pass; Bedrock is an optional feature, not an architectural exception.

Success criteria: no lost event across crash/restart; duplicate delivery has no duplicate external effect; a slow provider/webhook does not move graph/FTS/vector-search latency beyond the agreed budget; graph-only build has no provider/network dependency.

## Rejected approaches

- **One giant MCP dispatcher:** couples every tool call to indexer and delivery lifetime.
- **Polling as the primary index signal:** slower and retains a correctness gap; keep only reconciliation.
- **Post-commit webhook call from tool handlers:** crashes can lose or phantom-publish events.
- **Database triggers as the whole solution:** they cannot reliably produce typed snapshots, authenticated actor/origin, or subscription policy.
- **Reuse `code_watch`:** it watches files, has no durable delivery/retry contract, and is unrelated to graph mutation.

## Open product decision

Choose the operational default after the first slice:

- **Single composed binary:** simplest VM install, roles remain configurable and internally isolated.
- **Three supervised binaries:** strongest fault and CPU isolation, slightly more operational setup.

The code structure should support both; this choice must not change the transactional event contract.
