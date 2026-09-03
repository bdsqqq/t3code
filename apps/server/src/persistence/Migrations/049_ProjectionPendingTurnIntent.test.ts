import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("049_ProjectionPendingTurnIntent", (it) => {
  it.effect("adds accepted turn intent columns without replacing pending turns", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 48 });
      yield* sql`
        INSERT INTO projection_turns (
          thread_id,
          turn_id,
          pending_message_id,
          state,
          requested_at,
          checkpoint_files_json
        ) VALUES (
          'thread-1',
          NULL,
          'message-1',
          'pending',
          '2026-01-01T00:00:00.000Z',
          '[]'
        )
      `;
      yield* sql`
        INSERT INTO orchestration_events (
          event_id,
          aggregate_kind,
          stream_id,
          stream_version,
          event_type,
          occurred_at,
          command_id,
          causation_event_id,
          correlation_id,
          actor_kind,
          payload_json,
          metadata_json
        ) VALUES (
          'event-1',
          'thread',
          'thread-1',
          1,
          'thread.turn-start-requested',
          '2026-01-01T00:00:00.000Z',
          'command-1',
          NULL,
          NULL,
          'user',
          '{"threadId":"thread-1","messageId":"message-1","modelSelection":{"instanceId":"pi","model":"gpt-5"},"titleSeed":"accepted title","interactionMode":"plan","createdAt":"2026-01-01T00:00:00.000Z"}',
          '{}'
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 49 });

      const columns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_turns)
      `;
      const names = new Set(columns.map((column) => column.name));
      assert.ok(names.has("pending_model_selection_json"));
      assert.ok(names.has("pending_title_seed"));
      assert.ok(names.has("pending_interaction_mode"));

      const pendingTurns = yield* sql<{
        readonly pending_message_id: string;
        readonly pending_model_selection_json: string | null;
        readonly pending_title_seed: string | null;
        readonly pending_interaction_mode: string | null;
      }>`
        SELECT
          pending_message_id,
          pending_model_selection_json,
          pending_title_seed,
          pending_interaction_mode
        FROM projection_turns
        WHERE thread_id = 'thread-1'
      `;
      assert.deepStrictEqual(pendingTurns, [
        {
          pending_message_id: "message-1",
          pending_model_selection_json: '{"instanceId":"pi","model":"gpt-5"}',
          pending_title_seed: "accepted title",
          pending_interaction_mode: "plan",
        },
      ]);
    }),
  );
});
