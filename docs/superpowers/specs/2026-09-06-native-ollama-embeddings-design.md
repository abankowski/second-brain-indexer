# Native Ollama embeddings

## Decision

Add a native Ollama embedding engine beside the existing OpenAI-compatible engine. It uses Ollama's stable `POST /api/embed` endpoint, not its experimental OpenAI compatibility layer. The active production configuration remains OpenAI-compatible; no migration runs as part of this work.

## Configuration contract

`[embedding]` becomes a tagged configuration:

```toml
[embedding]
provider = "ollama" # "openai-compatible" is the default for existing configs
model = "bge-m3"
dimensions = 1024
base_url = "http://127.0.0.1:11434"
max_input_chars = 24000
max_input_tokens = 8192
```

For `openai-compatible`, `api_key_env` is required and `base_url` names the `/v1` base URL. For `ollama`, `api_key_env` is rejected rather than ignored and `base_url` names the Ollama server root. Existing `openai_base_url` is accepted only for backward compatibility and maps to `base_url`; new examples use `base_url`.

## Runtime contract

`OllamaEmbeddingAdapter` implements the existing `EmbeddingProvider` port. It sends one batch to `/api/embed`:

```json
{"model":"bge-m3","input":["..."],"truncate":false}
```

It requires `embeddings.length == input.length`, preserves order, and validates every vector against `embedding.dimensions`. Authentication is omitted unless a future explicit Ollama authentication field is added.

Startup creates the selected adapter, embeds a fixed non-user probe string, and refuses to bind HTTP unless the returned dimension equals both configured `embedding.dimensions` and MCP `vector_store_stats.dims`. The probe is never stored in mcp-memory.

## Provider selection and errors

`main` owns the provider factory and passes `Arc<dyn EmbeddingProvider>` to `ProductionProcessor`; application code remains provider-neutral. Native Ollama errors use the existing bounded classes: transport, unauthorized, rate-limited, server, and invalid response. Logs retain provider-qualified aggregate error classes and never include input text, response bodies, or secrets.

## Migration safety

Changing provider, model, or dimensions changes embedding space. README must state that vectors from old and new engines must never coexist. For any switch:

1. Stop the indexer.
2. Rebuild/reconfigure mcp-memory's vector store for the target dimension.
3. Update the indexer embedding configuration and restart; its probe must verify the new dimension.
4. Trigger `POST /indexer/fullscan`.
5. Verify a succeeded run and matching indexed count.

The indexer does not delete or rewrite mcp-memory's database directly. The operator owns the mcp-memory rebuild step.

## Tests and gates

Unit/integration tests cover tagged configuration validation, Ollama request shape, ordered batched response, all response/error classes, probe dimension mismatch before HTTP bind, and unchanged OpenAI-compatible behavior. A local acceptance probe against the user's Ollama BGE-M3 instance runs only when it is reachable and checks non-empty 1024-dimensional output; it writes no vectors. Completion requires `cargo fmt --all -- --check`, strict Clippy, all tests, release build, `git diff --check`, and the local probe when available.

## Rejected alternatives

- Ollama's OpenAI-compatible API: fewer files, but experimental and masks engine-specific behavior.
- Output truncation to 384 dimensions: it avoids a rebuild but changes BGE-M3 representation without evidence that retrieval quality remains acceptable.
- Automatic mcp-memory migration: unsafe because the indexer does not own that database and mcp-memory owns vector lifecycle.
