import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("041_ProjectionThreadActivityRetention", (it) => {
  it.effect("adds conservative activity metadata and per-thread cursor invalidation state", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 40 });
      yield* sql`
        INSERT INTO projection_thread_activities (
          activity_id, thread_id, turn_id, tone, kind, summary,
          payload_json, sequence, applied_sequence, created_at
        ) VALUES (
          'activity-existing', 'thread-existing', NULL, 'tool', 'tool.updated',
          'existing', '{"data":{"toolCallId":"call-existing"}}', 1, 3,
          '2026-08-11T00:00:00.000Z'
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 41 });

      const activities = yield* sql<{
        readonly retentionIdentity: string | null;
        readonly payloadUtf8Bytes: number | null;
      }>`
        SELECT
          retention_identity AS "retentionIdentity",
          payload_utf8_bytes AS "payloadUtf8Bytes"
        FROM projection_thread_activities
        WHERE activity_id = 'activity-existing'
      `;
      assert.deepStrictEqual(activities, [{ retentionIdentity: null, payloadUtf8Bytes: null }]);

      const historyColumns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_thread_activity_history)
      `;
      assert.deepEqual(
        historyColumns.map((column) => column.name),
        ["thread_id", "retention_floor_applied_sequence", "history_revision", "updated_at"],
      );
    }),
  );
});
