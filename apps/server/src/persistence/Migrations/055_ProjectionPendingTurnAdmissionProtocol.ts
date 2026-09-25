import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import PendingTurnIntent from "./053_ProjectionPendingTurnIntent.ts";
import PendingTurnOperationId from "./054_ProjectionPendingTurnOperationId.ts";

// Follows the fork/upstream migration-id compatibility bridge in migration 52.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_turns)
  `;

  // Upstream databases at ids 53–54 have different schemas. Repair the skipped
  // fork prerequisites before migration 56 reads them; applied fork ids stay intact.
  if (!columns.some((column) => column.name === "pending_model_selection_json")) {
    yield* PendingTurnIntent;
  }
  if (!columns.some((column) => column.name === "pending_operation_id")) {
    yield* PendingTurnOperationId;
  }

  if (!columns.some((column) => column.name === "pending_admission_protocol")) {
    yield* sql`
      ALTER TABLE projection_turns
      ADD COLUMN pending_admission_protocol TEXT
    `;
  }

  yield* sql`
    UPDATE projection_turns AS pending_turn
    SET pending_admission_protocol = (
      SELECT CASE json_extract(event.payload_json, '$.admissionProtocol')
        WHEN 'managed-admission-v1' THEN 'managed-admission-v1'
        ELSE NULL
      END
      FROM orchestration_events AS event
      WHERE event.aggregate_kind = 'thread'
        AND event.event_type = 'thread.turn-start-requested'
        AND event.stream_id = pending_turn.thread_id
        AND json_extract(event.payload_json, '$.messageId') = pending_turn.pending_message_id
      ORDER BY event.sequence DESC
      LIMIT 1
    )
    WHERE pending_turn.turn_id IS NULL
      AND pending_turn.state = 'pending'
      AND pending_turn.pending_message_id IS NOT NULL
  `;
});
