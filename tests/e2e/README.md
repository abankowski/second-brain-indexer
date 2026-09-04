# Local acceptance coverage

The acceptance suite is intentionally split by its real boundary rather than
using a single networked test process. Every listed test uses local fakes,
temporary SQLite, or Axum loopback listeners; it never contacts production MCP
or OpenAI services.

| Spec criterion | Local acceptance evidence |
| --- | --- |
| 1-4 | `planner.rs` reconciliation cases and `executor.rs` successful write path |
| 5-6 | `sqlite_state.rs::delete_work_requires_a_complete_full_snapshot_and_is_audited`, `planner.rs::non_full_selectors_never_emit_deletes`, `http_contract.rs` |
| 7 | `sqlite_state.rs::idempotency_replays_before_ttl_and_reuses_after_expiry` |
| 8-9 | `executor.rs::rate_limit_retries_inside_jitter_bound_and_auth_does_not` and `dimension_mismatch_writes_no_vectors` |
| 10-11 | `runtime.rs` lock/recovery/scheduler tests and `http_contract.rs::shutdown_refuses_new_posts_but_keeps_read_routes_available` |
| 12 | `http_contract.rs::read_routes_and_body_limit_follow_the_contract` and `deployment_assets.rs` |

Run all acceptance evidence with `cargo test --workspace --all-targets`.
