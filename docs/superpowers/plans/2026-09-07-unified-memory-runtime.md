# Unified Memory Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the `mcp-memory` fork into a feature-gated, role-composable service that keeps graph/vector search responsive while durable index and webhook workers react to committed knowledge changes.

**Architecture:** The fork gains a `memory-core` mutation and job boundary. A graph change, its event record, matching webhook delivery records, and a coalesced index job commit in one SQLite transaction. Build features decide which adapters exist; runtime roles decide which independently budgeted services run in one process or as separate processes.

**Tech Stack:** Rust 2024, Tokio, Axum, rusqlite SQLite WAL, MCP Streamable HTTP, optional reqwest embedding clients; optional AWS SDK only behind `bedrock`.

**Spec:** `docs/analysis/2026-09-07-mcp-memory-indexer-merge-viability.md`

## Global Constraints

- Target repository is the `corporatepiyush/mcp-memory` fork, pinned initially from upstream commit `d6fe34be505b52afb88a347141ee63378b25663e`.
- Keep graph-only builds free of embedding provider, webhook HTTP, and AWS dependencies.
- Search/MCP request paths must never await embedding, webhook delivery, or ANN rebuild.
- A graph mutation plus its effective `change_event`, matching outbox rows when subscriptions exist, and index job must commit atomically.
- Delivery is at-least-once; consumers deduplicate `eventId`. Never claim exactly-once webhook delivery.
- Preserve current mcp-memory graph tool behavior and exact case-sensitive entity identity before adding new roles.
- Every runtime role has an explicit bounded concurrency limit and independently exported queue/latency metrics.
- Cross-process mutation, claim, and completion writes use `BEGIN IMMEDIATE`, finite busy retry, and database-owned monotonically increasing lease fencing epochs; search remains deferred read-only WAL work.
- Run formatting, strict Clippy, the complete test suite, release build, and a migration/compatibility test before each publish.

---

### Task 1: Establish workspace and role composition without behavior change

**Files:**
- Modify: `Cargo.toml`, `src/main.rs`, `src/lib.rs`, `src/config.rs`, `src/server.rs`
- Create: `crates/memory-core/Cargo.toml`, `crates/memory-core/src/lib.rs`, `crates/memory-runtime/Cargo.toml`, `crates/memory-runtime/src/lib.rs`, `tests/role_composition.rs`

**Interfaces:**
- Produces `RuntimeRole::{Mcp, Indexer, Webhooks}` and `RoleSet::parse_csv(&str) -> Result<RoleSet, ConfigError>`.
- Produces `RuntimeComposition::start(RoleSet, Arc<AppServices>) -> Result<RunningRoles, RuntimeError>`.
- `indexer` and `webhooks` are disabled unless their Cargo feature is compiled.

- [ ] **Step 1: Write failing role parsing tests** for `mcp`, `mcp,indexer,webhooks`, duplicate roles, an empty list, and a role absent from the compiled feature set.
- [ ] **Step 2: Run `cargo test --test role_composition`** and observe that `RuntimeRole` and feature validation do not exist.
- [ ] **Step 3: Convert the root package to a workspace** while retaining the existing `mcp-memory` binary and all present tool behavior; move only reusable types into `memory-core`.
- [ ] **Step 4: Implement `RuntimeRole`, `RoleSet`, and `RuntimeComposition`**. `Mcp` starts the existing transport; `Indexer` and `Webhooks` initially start no-op supervised services that expose lifecycle state only.
- [ ] **Step 5: Add Cargo features**: `indexer`, `webhooks`, and `bedrock`; make `bedrock` depend on `indexer`. Verify `cargo tree --no-default-features` contains neither reqwest nor AWS crates.
- [ ] **Step 6: Run role tests and existing e2e tests**, then `cargo test --workspace`.
- [ ] **Step 7: Commit** `refactor: add feature-gated runtime roles`.

### Task 2: Make graph mutations transactional and report effective changes

**Files:**
- Modify: `crates/memory-core/src/graph.rs` (extracted from `src/kg.rs`), `src/actions/memory.rs`, `src/server.rs`
- Create: `crates/memory-core/src/mutation.rs`, `tests/mutation_service.rs`

**Interfaces:**
- Produces `MutationService::apply(MutationRequest, MutationContext) -> Result<CommittedChangeSet, MutationError>`.
- `CommittedChangeSet { transaction_id: Uuid, changes: Vec<EntityChange> }` contains only effective changes.
- `EntityChange { operation: Create|Update|Delete, before: Option<EntitySnapshot>, after: Option<EntitySnapshot>, relation_delta: Option<RelationDelta> }` retains a delete tombstone. A relation mutation produces an update for both endpoint entities.
- `MutationContext { actor, origin, correlation_id, causation_id, hop_count, idempotency_key }` is validated at the ingress boundary.

- [ ] **Step 1: Write failing tests** for create, observation update, delete, relation endpoint updates, no-op upsert, and a forced later-statement failure; assert no partial graph state or change set persists.
- [ ] **Step 2: Run `cargo test --test mutation_service`** and observe the existing public `GraphHandle` methods cannot return a committed change set.
- [ ] **Step 3: Implement one SQLite transaction abstraction** used by every graph write path: entity, relation, observation, merge, compact, and delete operations.
- [ ] **Step 4: Implement `MutationService`** to calculate effective entity-level before/after snapshots inside that transaction and return only after commit.
- [ ] **Step 5: Adapt MCP action handlers** to translate JSON into `MutationRequest` and keep their existing response shapes.
- [ ] **Step 6: Run mutation and current graph e2e tests**; inspect a forced rollback directly from SQLite.
- [ ] **Step 7: Commit** `refactor: centralize transactional graph mutations`.

### Task 3: Persist events and coalesced index jobs atomically

**Files:**
- Modify: `crates/memory-core/src/mutation.rs`, `crates/memory-core/src/graph.rs`
- Create: `crates/memory-core/src/events.rs`, `crates/memory-core/src/jobs.rs`, `migrations/0001_change_events.sql`, `tests/event_outbox.rs`

**Interfaces:**
- Produces `ChangeEvent`, `IndexJob`, `IndexProfile`, and `IndexProfileRegistry`.
- Produces `EventRepository::claim_due(...)` and `IndexJobRepository::claim_due(...)` using lease tokens, plus `AnnGenerationRepository` for durable ANN reconciliation state.
- `IndexProfile { provider: String, model: String, dimensions: u32, representation_version: String }` identifies one compatible vector space. `IndexProfileRegistry` has exactly one `Active` profile or an explicit `Rebuilding` state per vector store.

- [ ] **Step 1: Write failing SQLite tests** asserting a successful mutation writes event, empty `event_outbox`, and one latest-revision job; a rollback writes neither; repeated updates coalesce to the latest revision; delete produces a tombstone job; concurrent process connections serialize writers without lost events.
- [ ] **Step 2: Run `cargo test --test event_outbox`** and observe missing tables/repositories.
- [ ] **Step 3: Add schema migration** for immutable `change_event`, `event_outbox`, `index_job`, `idempotency_record`, `index_profile`, and `ann_generation`; add unique keys for event identity, event/subscription delivery, entity/profile job coalescing, and `(principal_id, idempotency_key)` ingress idempotency.
- [ ] **Step 4: Write event/job rows inside Task 2's mutation transaction**, never in MCP dispatch after the commit.
- [ ] **Step 5: Add `BEGIN IMMEDIATE` repository operations** with finite busy retry, lease epoch fencing, recovery, and idempotent completion. Persist a request fingerprint with each `(principal_id, idempotency_key)` and reject reuse with a different request; stale leases become runnable but cannot complete a newer job.
- [ ] **Step 6: Implement profile activation/rebuild state**. Reject or hold jobs/searches whose profile differs from the active profile; test that a profile transition cannot mix vector writes.
- [ ] **Step 7: Run event tests, restart/reopen tests, cross-process writer tests, and full graph suite.** Verify read-only WAL searches continue while a writer is busy.
- [ ] **Step 8: Commit** `feat: enqueue durable entity-change index jobs`.

### Task 4: Add a provider-neutral indexer worker and OpenAI/Ollama adapters

**Files:**
- Create: `crates/indexer-worker/src/lib.rs`, `crates/indexer-worker/src/provider.rs`, `crates/indexer-worker/src/openai.rs`, `crates/indexer-worker/src/ollama.rs`, `tests/indexer_worker.rs`
- Modify: `Cargo.toml`, `crates/memory-runtime/src/lib.rs`, `src/vector_store.rs`

**Interfaces:**
- Produces `EmbeddingProvider::embed(&IndexProfile, &[CanonicalDocument]) -> Result<Vec<Embedding>, EmbeddingError>`.
- Produces `IndexerWorker::run_once() -> Result<RunReport, WorkerError>`.
- A completed job writes only vectors compatible with its exact active `IndexProfile`; a profile mismatch is held until the explicit rebuild completes, never dimension coercion.

- [ ] **Step 1: Write failing worker tests** for slow/failing provider retry, latest-revision coalescing, entity deletion, provider timeout, profile mismatch hold, and two workers where an expired first lease cannot commit/upsert after a newer claimant.
- [ ] **Step 2: Run `cargo test --test indexer_worker`** and observe missing worker/provider interfaces.
- [ ] **Step 3: Port canonicalization and job semantics from the existing Second Brain Indexer** as pure `memory-core` functions; preserve exact name identity and no-delete-without-verified-snapshot behavior where full reconciliation is used.
- [ ] **Step 4: Implement bounded OpenAI-compatible and Ollama adapters** behind `indexer`; reuse native Ollama `/api/embed`, reject credentials in Ollama URLs, and apply finite request deadlines.
- [ ] **Step 5: Implement worker lease renewal and fenced commit**: after embedding, use one SQLite write transaction to recheck lease epoch, entity revision, and active profile before durable vector mutation and job completion; mark the relevant `ann_generation` dirty in that transaction. Update ANN state only after that transaction succeeds, on a dedicated bounded worker runtime.
- [ ] **Step 6: Implement startup and background ANN reconciliation**. Build a replacement reader snapshot from a durable generation, atomically swap it when ready, and retain the prior snapshot while rebuilding.
- [ ] **Step 7: Run worker tests plus an opt-in local Ollama probe** that writes only a temporary test database. Force a crash between durable vector commit and ANN swap, then prove the reconciler restores searchability without request-path rebuild.
- [ ] **Step 8: Commit** `feat: add durable provider-neutral indexer worker`.

### Task 5: Add optional Bedrock provider without changing the worker contract

**Files:**
- Create: `crates/indexer-worker/src/bedrock.rs`, `tests/bedrock_provider.rs`
- Modify: `Cargo.toml`, `crates/indexer-worker/src/provider.rs`, configuration docs/tests

**Interfaces:**
- `BedrockEmbeddingProvider` implements Task 4's `EmbeddingProvider` unchanged.
- Provider registration maps `provider = "bedrock"` only when the `bedrock` feature is compiled.

- [ ] **Step 1: Write failing configuration/provider-registry tests** for disabled feature rejection, valid Bedrock profile construction, request timeout, and malformed response/dimension mismatch.
- [ ] **Step 2: Run `cargo test --test bedrock_provider`** under `--features bedrock` and observe missing adapter/registry entry.
- [ ] **Step 3: Add AWS dependencies only to the `bedrock` feature** and implement the adapter with credential-chain configuration, request deadlines, and safe error redaction.
- [ ] **Step 4: Verify a non-Bedrock build** with `cargo build --no-default-features` and dependency-tree check; run Bedrock fixture tests without live credentials.
- [ ] **Step 5: Commit** `feat: add optional bedrock embeddings`.

### Task 6: Add webhook subscriptions and asynchronous delivery

**Files:**
- Create: `crates/webhook-worker/src/lib.rs`, `crates/webhook-worker/src/delivery.rs`, `crates/memory-core/src/subscriptions.rs`, `tests/webhook_outbox.rs`
- Modify: `Cargo.toml`, `crates/memory-core/src/mutation.rs`, `crates/memory-runtime/src/lib.rs`

**Interfaces:**
- Produces `WebhookSubscription { id, endpoint, event_mask, entity_filter, ignore_origins, secret_ref, enabled }`, `AdminPrincipal { id, secret_scopes }`, and protected `WebhookAdminApi` registration/unregistration operations.
- Produces signed `WebhookEnvelope { event_id, operation, entity, origin, correlation_id, causation_id, hop_count, occurred_at }`.
- Produces `WebhookWorker::run_once() -> Result<DeliveryReport, WorkerError>`.

- [ ] **Step 1: Write failing tests** for create/update/delete filters, no-op exclusion, relation endpoint events, same-origin suppression, hop-limit rejection, duplicate event delivery, HTTP retry/backoff, ordering per subscription, dead-letter transition, endpoint SSRF rejection, DNS rebinding, redirect refusal, payload redaction of observation bodies, and secret-scope authorization for create/read/update/replace.
- [ ] **Step 2: Run `cargo test --test webhook_outbox`** and observe missing subscription/outbox tables.
- [ ] **Step 3: Add subscription and delivery schema use** for Task 3's `event_outbox`, with unique `(event_id, subscription_id)`, lease epoch, attempt counter, next-attempt timestamp, and bounded last-error field.
- [ ] **Step 4: Match subscriptions within the mutation transaction and enqueue delivery rows with the event.** Store only a secret reference; read webhook signing material from the configured secret source at dispatch time.
- [ ] **Step 5: Implement bounded HTTP delivery** with timestamp/event-ID HMAC headers, retries with jitter, dead-lettering, stable idempotency key equal to `event_id`, HTTPS/egress allowlist validation, redirects disabled, and a connector that dials the validated `SocketAddr` while retaining original Host/SNI. A second unconstrained DNS resolution is forbidden.
- [ ] **Step 6: Implement authenticated write ingress**. Require a machine principal, idempotency key, origin/correlation/causation metadata, and monotonic hop increment; reject unauthorized origin spoofing and over-budget hops before `MutationService`.
- [ ] **Step 7: Run webhook tests using an Axum fake receiver**, including crash/reclaim and a generic automation A-to-outbound-to-B loop fixture. The fixture must use only the public HTTP event envelope and write-ingress metadata, so it applies equally to n8n, Node-RED, and custom consumers.
- [ ] **Step 8: Commit** `feat: add durable filtered webhook subscriptions`.

### Task 7: Expose protected operations, observability, and resource isolation

**Files:**
- Modify: `src/http.rs`, `src/server.rs`, `crates/memory-runtime/src/lib.rs`, `README.md`, `tests/e2e.rs`
- Create: `crates/memory-mcp/src/indexer_tools.rs`, `tests/resource_isolation.rs`, deployment assets for role-specific systemd services

**Interfaces:**
- Adds server-side semantic search/reindex MCP tools only when `indexer` is compiled and the `Mcp` role is active.
- Adds protected REST administration for webhook subscription lifecycle and worker/job status; these are not LLM-facing MCP tools.
- Produces metrics for request latency, job age/depth, provider duration/errors, webhook outcomes, and dead-letter counts.

- [ ] **Step 1: Write failing contract tests** for feature-gated MCP tool visibility, protected webhook registration/unregistration, role-disabled responses, and metrics with no secret/observation leakage.
- [ ] **Step 2: Write load regression tests** that stall embedding and webhook receivers, and separately hold an ANN rebuild, while concurrent graph/FTS/vector searches run; record and enforce an agreed p95 latency budget.
- [ ] **Step 3: Implement independent admission controls and dedicated worker execution resources.** Do not use the MCP request semaphore or request runtime for worker CPU/network operations.
- [ ] **Step 4: Add protected admin endpoints and feature-gated MCP tool registration** using per-app state rather than current process-global category state.
- [ ] **Step 5: Document one-process and separate-role systemd deployments**, including provider profile migration, generic consumer deduplication by `eventId`, webhook signing verification, retry/dead-letter operations, and Bash/Fish commands where syntax differs. Show n8n and Node-RED only as non-normative examples.
- [ ] **Step 6: Run all workspace checks, migration/upgrade tests, resource-isolation tests, and an end-to-end one-process smoke test.**
- [ ] **Step 7: Commit** `feat: compose isolated memory runtime roles`.

## Final verification

- [ ] Run `cargo fmt --all -- --check`.
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- [ ] Run `cargo test --workspace --all-targets --all-features`.
- [ ] Run `cargo build --workspace --release --all-features`, `cargo build -p mcp-memory --no-default-features`, and `cargo tree -p mcp-memory -e normal --no-default-features`.
- [ ] Run migration tests against a copied pre-fork `memory.mcpmem` fixture; verify existing graph and vector searches retain results.
- [ ] Run crash/restart tests at each outbox/job phase and verify no lost graph event, duplicate vector effect, or duplicate consumer effect after `eventId` deduplication.
- [ ] Run the stalled-provider/stalled-webhook latency test and record p50/p95/p99 against the agreed budget.
