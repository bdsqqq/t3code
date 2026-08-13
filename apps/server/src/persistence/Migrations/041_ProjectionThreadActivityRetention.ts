import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    ALTER TABLE projection_thread_activities
    ADD COLUMN retention_identity TEXT
  `;
  yield* sql`
    ALTER TABLE projection_thread_activities
    ADD COLUMN payload_utf8_bytes INTEGER
  `;

  yield* sql`
    CREATE INDEX idx_projection_thread_activities_retention_scope
    ON projection_thread_activities(
      thread_id,
      turn_id,
      retention_identity,
      sequence,
      created_at,
      activity_id
    )
    WHERE kind = 'tool.updated' AND retention_identity IS NOT NULL
  `;

  yield* sql`
    CREATE TABLE projection_thread_activity_history (
      thread_id TEXT PRIMARY KEY,
      retention_floor_applied_sequence INTEGER NOT NULL DEFAULT 0
        CHECK (retention_floor_applied_sequence >= 0),
      history_revision INTEGER NOT NULL DEFAULT 0 CHECK (history_revision >= 0),
      updated_at TEXT NOT NULL
    )
  `;
});
