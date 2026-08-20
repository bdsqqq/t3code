import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

// Follows the fork/upstream migration-id compatibility bridge in migration 42.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(projection_turns)
  `;

  if (!columns.some((column) => column.name === "pending_model_selection_json")) {
    yield* sql`
      ALTER TABLE projection_turns
      ADD COLUMN pending_model_selection_json TEXT
    `;
  }

  if (!columns.some((column) => column.name === "pending_title_seed")) {
    yield* sql`
      ALTER TABLE projection_turns
      ADD COLUMN pending_title_seed TEXT
    `;
  }

  if (!columns.some((column) => column.name === "pending_interaction_mode")) {
    yield* sql`
      ALTER TABLE projection_turns
      ADD COLUMN pending_interaction_mode TEXT
    `;
  }

  yield* sql`
    UPDATE projection_turns AS pending_turn
    SET (
      pending_model_selection_json,
      pending_title_seed,
      pending_interaction_mode
    ) = (
      SELECT
        json_extract(event.payload_json, '$.modelSelection'),
        json_extract(event.payload_json, '$.titleSeed'),
        COALESCE(json_extract(event.payload_json, '$.interactionMode'), 'default')
      FROM orchestration_events AS event
      WHERE event.event_type = 'thread.turn-start-requested'
        AND event.stream_id = pending_turn.thread_id
        AND json_extract(event.payload_json, '$.messageId') = pending_turn.pending_message_id
      ORDER BY event.sequence DESC
      LIMIT 1
    )
    WHERE pending_turn.turn_id IS NULL
      AND pending_turn.state = 'pending'
      AND pending_turn.pending_message_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM orchestration_events AS event
        WHERE event.event_type = 'thread.turn-start-requested'
          AND event.stream_id = pending_turn.thread_id
          AND json_extract(event.payload_json, '$.messageId') = pending_turn.pending_message_id
      )
  `;
});
