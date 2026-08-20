import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Fork releases previously occupied migration ids 36-41. Existing fork
  // databases therefore skip upstream's migrations at those ids, so this
  // first post-merge migration idempotently establishes both schemas.
  const threadColumns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_threads)
  `;
  if (!threadColumns.some((column) => column.name === "pinned_at")) {
    yield* sql`
      ALTER TABLE projection_threads
      ADD COLUMN pinned_at TEXT
    `;
  }
  if (!threadColumns.some((column) => column.name === "pin_order_key")) {
    yield* sql`
      ALTER TABLE projection_threads
      ADD COLUMN pin_order_key TEXT
    `;
  }

  const projectColumns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_projects)
  `;
  if (!projectColumns.some((column) => column.name === "default_thread_env_mode")) {
    yield* sql`
      ALTER TABLE projection_projects
      ADD COLUMN default_thread_env_mode TEXT
    `;
  }
  if (!projectColumns.some((column) => column.name === "favicon_path")) {
    yield* sql`
      ALTER TABLE projection_projects
      ADD COLUMN favicon_path TEXT
    `;
  }

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_projection_turns_thread_keyset
    ON projection_turns(thread_id, requested_at, turn_id)
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS pi_external_lifecycle_overrides (
      source_key TEXT PRIMARY KEY,
      command_id TEXT NOT NULL UNIQUE,
      lifecycle_override TEXT NOT NULL
        CHECK (lifecycle_override IN ('settled', 'active')),
      observed_file_size INTEGER NOT NULL,
      observed_file_mtime_ms REAL NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS pi_external_lifecycle_command_receipts (
      command_id TEXT PRIMARY KEY,
      source_key TEXT NOT NULL,
      lifecycle_override TEXT NOT NULL
        CHECK (lifecycle_override IN ('settled', 'active')),
      observed_file_size INTEGER NOT NULL,
      observed_file_mtime_ms REAL NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
});
