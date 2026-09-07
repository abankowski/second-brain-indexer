# Ollama and Query MCP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add native Ollama embeddings and an authenticated indexer MCP façade for natural-language search and durable reindex requests.

**Architecture:** `EmbeddingProvider` stays the single application port. Configuration selects an OpenAI-compatible or native Ollama adapter and startup probes it before binding. A new `/indexer/mcp` Streamable HTTP delivery module embeds server-side then delegates vector queries to mcp-memory; all reindex operations reuse the durable queue.

**Tech Stack:** Rust 1.85, Axum 0.8, reqwest 0.12, serde, Tokio, MCP 2025-03-26.

**Spec:** `docs/superpowers/specs/2026-09-06-native-ollama-embeddings-design.md`

## Global Constraints

- Do not expose provider credentials, raw vectors, entity observations, or provider response bodies in logs or MCP responses.
- `embedding.dimensions`, provider probe dimensions, and MCP vector dimensions must be exactly equal before HTTP binds.
- Reindex tools enqueue existing selectors; the indexer never edits mcp-memory SQLite directly.
- Provider/model/dimension migration documentation must include MCP vector rebuild and the authenticated local-state reset that queues a forced full reindex; a plain fullscan skips unchanged local hashes.
- Run `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace --all-targets`, `cargo build --release`, and `git diff --check` before publish.

---

### Task 1: Tagged embedding configuration and provider-neutral composition

**Files:**
- Modify: `src/config.rs`, `src/main.rs`, `tests/runtime.rs`
- Test: `src/config.rs`, `tests/runtime.rs`

**Interfaces:**
- Produces `EmbeddingEngine::{OpenAiCompatible, Ollama}` and a `build_embedding_provider(&EmbeddingConfig) -> Result<Arc<dyn EmbeddingProvider>, EmbeddingError>` factory.
- `EmbeddingConfig` exposes `engine`, `model`, `dimensions`, input limits, and provider-specific URL/auth data without a secret for Ollama.

- [ ] **Step 1: Write failing configuration tests** for an Ollama TOML block without `api_key_env`, an OpenAI-compatible block requiring it, and a legacy `openai_base_url` block mapping to OpenAI-compatible.
- [ ] **Step 2: Run the focused config tests** and observe parsing/validation failures for `provider` and Ollama config.
- [ ] **Step 3: Add tagged raw/config types**; preserve legacy OpenAI config as the default and reject `api_key_env` when `provider = "ollama"`.
- [ ] **Step 4: Refactor `ProductionProcessor` to hold `Arc<dyn EmbeddingProvider>`** rather than `OpenAiEmbeddingAdapter`; add the factory but retain OpenAI behavior.
- [ ] **Step 5: Run focused tests**, then `cargo test --test runtime`.
- [ ] **Step 6: Commit** `feat: add selectable embedding engines`.

### Task 2: Native Ollama adapter and startup probe

**Files:**
- Create: `src/adapters/ollama.rs`
- Modify: `src/adapters/mod.rs`, `src/main.rs`, `tests/adapters.rs`
- Test: `tests/adapters.rs`, `tests/runtime.rs`

**Interfaces:**
- Produces `OllamaEmbeddingAdapter::new(&EmbeddingConfig) -> Result<Self, EmbeddingError>` implementing `EmbeddingProvider`.
- Produces `probe_embedding(provider: &dyn EmbeddingProvider, expected: Dimension) -> Result<(), EmbeddingError>` used before listener bind.

- [ ] **Step 1: Write failing Axum-backed adapter tests** asserting `POST /api/embed`, JSON `{model,input:[...],truncate:false}`, ordered multi-input embeddings, and no authorization header.
- [ ] **Step 2: Run the adapter test** and observe the missing adapter failure.
- [ ] **Step 3: Implement the adapter** with bounded status classification, cardinality/order validation, and exact `Dimension` construction.
- [ ] **Step 4: Write a failing startup test** where the probe returns a non-configured dimension and assert no listener-ready path is reached.
- [ ] **Step 5: Add the fixed non-user probe before `TcpListener::bind`**; compare provider result to configuration and MCP stats.
- [ ] **Step 6: Run adapter/runtime tests and an opt-in local Ollama BGE-M3 probe** (`POST /api/embed`, no vector write); record its measured dimension.
- [ ] **Step 7: Commit** `feat: add native ollama embeddings`.

### Task 3: mcp-memory vector query client contract

**Files:**
- Modify: `src/ports.rs`, `src/adapters/mcp.rs`, `tests/adapters.rs`
- Test: `tests/adapters.rs`

**Interfaces:**
- Extends `McpMemoryPort` with typed semantic and hybrid query methods accepting an `Embedding`, optional limit/entity type, and returning normalized result rows.
- Produces a checked mcp-memory tool contract fixture based on a fresh read-only `tools/list` and query probe.

- [ ] **Step 1: Probe the deployed mcp-memory tools read-only** to pin exact `vector_search_entities` and `hybrid_search` names, arguments, and result fields; store only redacted/static fixtures.
- [ ] **Step 2: Write failing adapter tests** with those fixtures, including malformed response and tool `isError` cases.
- [ ] **Step 3: Implement typed request/response DTOs and `McpMemoryPort` methods**; validate result cardinality/types and map all malformed/tool-error responses safely.
- [ ] **Step 4: Run `cargo test --test adapters`** and confirm the tool-contract fixture passes.
- [ ] **Step 5: Commit** `feat: add mcp vector query client`.

### Task 4: Authenticated indexer MCP façade

**Files:**
- Create: `src/indexer_mcp.rs`
- Modify: `src/http/mod.rs`, `src/main.rs`, `tests/http_contract.rs`
- Test: `tests/http_contract.rs`

**Interfaces:**
- Produces `indexer_mcp_router(...) -> Router` mounted at `/indexer/mcp` behind `BearerAuth`.
- Implements MCP `initialize`, `notifications/initialized`, `tools/list`, and `tools/call` for `indexer_semantic_search`, `indexer_hybrid_search`, `indexer_reindex_entity`, `indexer_reindex_all`, and `indexer_run_status`.

- [ ] **Step 1: Write failing MCP contract tests** for initialization, tool listing, missing/wrong bearer token, semantic query delegation without vectors in response, hybrid delegation, and tool errors.
- [ ] **Step 2: Run them** and observe `/indexer/mcp` is absent.
- [ ] **Step 3: Implement protocol envelope/session-free Streamable HTTP handling** with typed per-tool arguments and fixed tool definitions.
- [ ] **Step 4: Reuse the existing enqueue/query seams** for both reindex tools and status; do not duplicate queue or SQLite SQL.
- [ ] **Step 5: Ensure search tools embed server-side then call Task 3 methods**; return normalized rows only.
- [ ] **Step 6: Run focused MCP contract tests**, including an active-queue coalescing case and shutdown refusal.
- [ ] **Step 7: Commit** `feat: expose indexer query mcp tools`.

### Task 5: Operational documentation and Second Brain skill guidance

**Files:**
- Modify: `README.md`, `deploy/config.toml.example`, `deploy/nginx-second-brain-indexer.conf`, `tests/deployment_assets.rs`
- Modify: `/Users/abankowski/.codex/skills/second-brain-memory/SKILL.md` only after reading its instructions and validating its package rules.
- Test: `tests/deployment_assets.rs`

**Interfaces:**
- Documents `/indexer/mcp`, bearer auth, provider examples, and migration commands/verification.
- Skill routes natural-language semantic retrieval to indexer MCP tools and forbids raw-vector tool invocation from client contexts.

- [ ] **Step 1: Write failing deployment-asset tests** asserting the config has both provider examples, nginx preserves `/indexer/mcp`, and README contains the migration gate plus verification commands.
- [ ] **Step 2: Run the deployment test** and observe missing provider/MCP documentation.
- [ ] **Step 3: Document OpenAI-compatible and native Ollama setup**, including Bash/Fish variants where syntax differs and an explicit BGE-M3 1024 migration sequence.
- [ ] **Step 4: Add nginx MCP proxy/SSE timeout configuration** without weakening bearer auth or rate limiting.
- [ ] **Step 5: Read and update the Second Brain skill** with the indexer MCP endpoint/tool preference and raw-vector prohibition; validate the skill by its own prescribed check.
- [ ] **Step 6: Run deployment/skill checks and full project gate.**
- [ ] **Step 7: Commit** `docs: document indexer mcp and embedding migration`.

### Post-implementation correction: migration reset after external vector rebuild

**Why this supersedes the earlier migration path:** Task 5 described a plain full scan after rebuilding mcp-memory's vector store. That is incorrect: a Full selector still skips entities whose local `entity_index_state` content hashes are unchanged, so no replacement vectors are written.

**Interfaces:**
- `POST /indexer/reset-local-index-state` accepts exactly `{}`, requires configured bearer authentication and an `Idempotency-Key`, and returns `202` plus the durable Full `runId`.
- It clears only local `entity_index_state`; it does not call, rebuild, delete, or directly write mcp-memory graph/vector data.
- It is unavailable when bearer authentication is disabled. It rejects an active queued or leased run and idempotency conflicts without clearing state.

**Required tests:** local-state clear plus Full queueing; queue/lease rejection preserving state; replay preserving newly indexed state; cross-operation idempotency conflict; rollback/persistence; disabled/missing/wrong authentication; strict body; and no remote MCP call in the reset handler.

## Final verification

- [ ] Run the complete global gate from Global Constraints.
- [ ] Run the local Ollama BGE-M3 acceptance probe if reachable; record endpoint reachability and measured dimension without revealing queries or credentials.
- [ ] Call indexer MCP `initialize`, `tools/list`, one semantic search, one hybrid search, single reindex, full reindex, and run-status against a controlled/local target before production use.
- [ ] Review README migration instructions against the actual Nginx and systemd assets.
