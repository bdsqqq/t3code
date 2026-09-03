import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import Migration0041 from "./041_AuthSessionClientConnection.ts";
import Migration0042 from "./042_ProjectionThreadLinkedPullRequest.ts";
import Migration0043 from "./043_ProjectionThreadsUnsettledAt.ts";
import Migration0044 from "./044_ClearAutomaticProjectModelDefaults.ts";
import Migration0045 from "./045_ProjectionProjectsAutoPull.ts";
import Migration0046 from "./046_RepairAutomaticSettlementTimestamps.ts";
import Migration0047 from "./047_ProjectionProjectIcon.ts";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Fork releases occupied ids that upstream later assigned through 46.
  // Migrator advances only from the largest recorded id, so replay upstream's
  // idempotent migrations behind this new fence before adding Pi state.
  yield* Migration0041;
  yield* Migration0042;
  yield* Migration0043;
  yield* Migration0044;
  yield* Migration0045;
  yield* Migration0046;
  yield* Migration0047;

  // Older fork releases also occupied migration ids 36-41. Establish the
  // upstream schemas from that range for databases crossing either fence.
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
