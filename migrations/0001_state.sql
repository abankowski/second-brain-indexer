CREATE TABLE index_configuration (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  taxonomy_version TEXT NOT NULL,
  representation_version TEXT NOT NULL,
  embedding_model TEXT NOT NULL,
  dimensions INTEGER NOT NULL CHECK (dimensions > 0),
  verified_at TEXT NOT NULL
);

CREATE TABLE entity_index_state (
  entity_name TEXT PRIMARY KEY,
  entity_type TEXT NOT NULL,
  vector_address TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('indexed','pending','indexing','failed','delete_pending','deleted')),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  last_indexed_at TEXT,
  last_seen_at TEXT NOT NULL,
  last_error_code TEXT,
  last_error_message TEXT
);

CREATE TABLE run (
  id TEXT PRIMARY KEY,
  trigger TEXT NOT NULL CHECK (trigger IN ('poll','api','fullscan','startup')),
  selector_kind TEXT NOT NULL CHECK (selector_kind IN ('full','entity','entity_type')),
  selector_value TEXT,
  selector_json TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','partial','failed','cancelled')),
  queue_state TEXT NOT NULL CHECK (queue_state IN ('queued','leased','finished','coalesced')),
  requested_at TEXT NOT NULL,
  not_before TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  lease_owner TEXT,
  lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
  lease_expires_at TEXT,
  coalesced_into_run_id TEXT REFERENCES run(id),
  claim_count INTEGER NOT NULL DEFAULT 0 CHECK (claim_count >= 0),
  entities_seen INTEGER NOT NULL DEFAULT 0,
  entities_indexed INTEGER NOT NULL DEFAULT 0,
  entities_skipped INTEGER NOT NULL DEFAULT 0,
  entities_deleted INTEGER NOT NULL DEFAULT 0,
  entities_failed INTEGER NOT NULL DEFAULT 0,
  error_summary TEXT,
  CHECK ((selector_kind = 'full' AND selector_value IS NULL) OR (selector_kind <> 'full' AND selector_value IS NOT NULL)),
  CHECK ((queue_state = 'leased') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)),
  CHECK ((queue_state = 'coalesced') = (coalesced_into_run_id IS NOT NULL))
);

CREATE INDEX run_claimable ON run(queue_state, not_before, requested_at)
  WHERE queue_state = 'queued';
CREATE UNIQUE INDEX one_queued_selector ON run(selector_kind, selector_value)
  WHERE queue_state = 'queued';

CREATE TABLE run_snapshot (
  run_id TEXT PRIMARY KEY REFERENCES run(id) ON DELETE CASCADE,
  is_complete INTEGER NOT NULL CHECK (is_complete IN (0, 1)),
  entity_count INTEGER NOT NULL CHECK (entity_count >= 0),
  relation_count INTEGER NOT NULL CHECK (relation_count >= 0),
  verified_at TEXT NOT NULL
);

CREATE TABLE run_work (
  run_id TEXT NOT NULL REFERENCES run(id) ON DELETE CASCADE,
  entity_name TEXT NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('upsert','delete')),
  content_hash TEXT,
  vector_address TEXT,
  status TEXT NOT NULL CHECK (status IN ('pending','indexing','succeeded','failed')),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  last_error_code TEXT,
  last_error_message TEXT,
  PRIMARY KEY (run_id, entity_name, action),
  CHECK ((action = 'upsert' AND content_hash IS NOT NULL) OR (action = 'delete' AND vector_address IS NOT NULL))
);

CREATE TABLE deletion_audit (
  id INTEGER PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES run(id) ON DELETE CASCADE,
  entity_name TEXT NOT NULL,
  vector_address TEXT NOT NULL,
  requested_at TEXT NOT NULL,
  completed_at TEXT,
  result TEXT NOT NULL CHECK (result IN ('pending','deleted','failed')),
  error_code TEXT,
  error_message TEXT,
  UNIQUE (run_id, entity_name)
);

CREATE TRIGGER deletion_work_requires_complete_full_snapshot
BEFORE INSERT ON run_work
WHEN NEW.action = 'delete'
BEGIN
  SELECT CASE WHEN NOT EXISTS (
    SELECT 1
    FROM run
    JOIN run_snapshot ON run_snapshot.run_id = run.id
    WHERE run.id = NEW.run_id
      AND run.selector_kind = 'full'
      AND run_snapshot.is_complete = 1
  ) THEN RAISE(ABORT, 'delete work requires a complete full snapshot') END;
END;

CREATE TABLE idempotency_key (
  key TEXT PRIMARY KEY,
  request_hash TEXT NOT NULL,
  run_id TEXT NOT NULL REFERENCES run(id),
  response_status INTEGER NOT NULL CHECK (response_status BETWEEN 100 AND 599),
  response_body_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);

CREATE INDEX idempotency_expiry ON idempotency_key(expires_at);
