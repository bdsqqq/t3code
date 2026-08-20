import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("045_ProjectionPendingTurnAdmissionProtocol", (it) => {
  it.effect("marks only pending turns accepted by the managed admission build", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 44 });
      yield* sql`
        INSERT INTO projection_turns (
          thread_id,
          turn_id,
          pending_message_id,
          state,
          requested_at,
          checkpoint_files_json
        ) VALUES
          ('thread-managed', NULL, 'message-managed', 'pending', '2026-01-01T00:00:00.000Z', '[]'),
          ('thread-legacy', NULL, 'message-legacy', 'pending', '2026-01-01T00:00:00.000Z', '[]')
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
            'event-managed',
            'thread',
            'thread-managed',
            1,
            'thread.turn-start-requested',
            '2026-01-01T00:00:01.000Z',
            'command-managed',
            NULL,
            NULL,
            'user',
            '{"messageId":"message-managed","admissionProtocol":"managed-admission-v1"}',
            '{}'
          ),
          (
            'event-legacy',
            'thread',
            'thread-legacy',
            1,
            'thread.turn-start-requested',
            '2026-01-01T00:00:01.000Z',
            'command-legacy',
            NULL,
            NULL,
            'user',
            '{"messageId":"message-legacy"}',
            '{}'
          )
      `;

      yield* runMigrations({ toMigrationInclusive: 45 });

      const pendingTurns = yield* sql<{
        readonly thread_id: string;
        readonly pending_admission_protocol: string | null;
      }>`
        SELECT thread_id, pending_admission_protocol
        FROM projection_turns
        ORDER BY thread_id ASC
      `;
      assert.deepStrictEqual(pendingTurns, [
        { thread_id: "thread-legacy", pending_admission_protocol: null },
        {
          thread_id: "thread-managed",
          pending_admission_protocol: "managed-admission-v1",
        },
      ]);
    }),
  );
});
