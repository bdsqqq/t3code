import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_turns)
  `;

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
