use std::{fs, path::Path};

#[test]
fn deployment_assets_keep_the_indexer_private_and_preserve_the_indexer_prefix() {
    let service = read("deploy/second-brain-indexer.service");
    assert!(service.contains("User=second-brain-indexer"));
    assert!(service.contains("EnvironmentFile=/etc/second-brain-indexer/secrets.env"));
    assert!(service.contains("ProtectSystem=strict"));
    assert!(service.contains("ReadWritePaths=/var/lib/second-brain-indexer"));

    let config = read("deploy/config.toml.example");
    assert!(config.contains("auth_token_file = \"/etc/second-brain-indexer/indexer-api-token\""));

    let nginx = read("deploy/nginx-second-brain-indexer.conf");
    assert!(nginx.contains("location /indexer/"));
    assert!(nginx.contains("proxy_pass http://127.0.0.1:9184;"));
    assert!(!nginx.contains("proxy_pass http://127.0.0.1:9184/;"));
    assert!(nginx.contains("limit_req zone=indexer_api"));

    assert!(nginx.contains("location = /indexer/mcp"));
    assert!(nginx.contains("proxy_pass http://127.0.0.1:9184/indexer/mcp;"));
    assert!(nginx.contains("proxy_buffering off;"));
    assert!(nginx.contains("proxy_read_timeout 3600s;"));

    let readme = read("README.md");
    assert!(readme.contains("limit_req_zone $binary_remote_addr zone=indexer_api:10m rate=10r/m;"));
    assert!(readme.contains("/etc/nginx/conf.d/second-brain-indexer-rate-limit.conf"));
    assert!(readme.contains("Authorization: Bearer"));
}

#[test]
fn deployment_assets_document_both_embedding_providers_and_the_1024_migration_gate() {
    let config = read("deploy/config.toml.example");
    assert!(config.contains("provider = \"openai-compatible\""));
    assert!(config.contains("provider = \"ollama\""));
    assert!(config.contains("model = \"bge-m3\""));
    assert!(config.contains("dimensions = 1024"));

    let readme = read("README.md");
    assert!(readme.contains("BGE-M3 1024-dimensional migration"));
    assert!(readme.contains("Rebuild the mcp-memory vector store for 1024 dimensions"));
    assert!(readme.contains("indexer_semantic_search"));
    assert!(readme.contains("indexer_hybrid_search"));
    assert!(readme.contains("indexer_reindex_entity"));
    assert!(readme.contains("indexer_reindex_all"));
    assert!(readme.contains("indexer_run_status"));
}

#[test]
fn runbook_contains_a_non_destructive_preflight_before_deployment() {
    let runbook = read("docs/runbook.md");
    assert!(runbook.contains("cargo test --workspace --all-targets"));
    assert!(runbook.contains("MCP contract probe"));
    assert!(runbook.contains("MCP_MEMORY_TOKEN"));
    assert!(runbook.contains("OPENAI_API_KEY"));
    assert!(runbook.contains("503"));
}

fn read(path: &str) -> String {
    fs::read_to_string(Path::new(path))
        .unwrap_or_else(|error| panic!("required deployment asset {path} is missing: {error}"))
}
