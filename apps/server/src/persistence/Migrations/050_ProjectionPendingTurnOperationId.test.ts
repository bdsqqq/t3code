import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("050_ProjectionPendingTurnOperationId", (it) => {
  it.effect("backfills the accepted command id without inventing one for unmatched turns", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 49 });
      yield* sql`
        INSERT INTO projection_turns (
          thread_id,
          turn_id,
          pending_message_id,
          state,
          requested_at,
          checkpoint_files_json
        ) VALUES
          ('thread-matched', NULL, 'message-matched', 'pending', '2026-01-01T00:00:00.000Z', '[]'),
          ('thread-unmatched', NULL, 'message-unmatched', 'pending', '2026-01-01T00:00:00.000Z', '[]')
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
        ) VALUES
          (
            'event-project-wrong-kind',
            'project',
            'thread-matched',
            1,
            'thread.turn-start-requested',
            '2026-01-01T00:00:00.000Z',
            'command-project-wrong-kind',
            NULL,
            NULL,
            'user',
            '{"messageId":"message-matched"}',
            '{}'
          ),
          (
            'event-thread-old',
            'thread',
            'thread-matched',
            1,
            'thread.turn-start-requested',
            '2026-01-01T00:00:01.000Z',
            'command-thread-old',
            NULL,
            NULL,
            'user',
            '{"messageId":"message-matched"}',
            '{}'
          ),
          (
            'event-thread-latest',
            'thread',
            'thread-matched',
            2,
            'thread.turn-start-requested',
            '2026-01-01T00:00:02.000Z',
            'command-thread-latest',
            NULL,
            NULL,
            'user',
            '{"messageId":"message-matched"}',
            '{}'
          ),
          (
            'event-thread-null-command',
            'thread',
            'thread-unmatched',
            1,
            'thread.turn-start-requested',
            '2026-01-01T00:00:03.000Z',
            NULL,
            NULL,
            NULL,
            'user',
            '{"messageId":"message-unmatched"}',
            '{}'
          )
      `;

      yield* runMigrations({ toMigrationInclusive: 50 });

      const columns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_turns)
      `;
      assert.ok(columns.some((column) => column.name === "pending_operation_id"));

      const pendingTurns = yield* sql<{
        readonly thread_id: string;
        readonly pending_operation_id: string | null;
      }>`
        SELECT thread_id, pending_operation_id
        FROM projection_turns
        ORDER BY thread_id ASC
      `;
      assert.deepStrictEqual(pendingTurns, [
        {
          thread_id: "thread-matched",
          pending_operation_id: "command-thread-latest",
        },
        {
          thread_id: "thread-unmatched",
          pending_operation_id: null,
        },
      ]);
    }),
  );
});
