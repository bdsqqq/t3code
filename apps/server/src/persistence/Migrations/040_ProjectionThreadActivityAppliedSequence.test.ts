import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("040_ProjectionThreadActivityAppliedSequence", (it) => {
  it.effect("backfills existing activities before tracking new projector positions", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 39 });
      yield* sql`
        INSERT INTO projection_thread_activities (
          activity_id, thread_id, turn_id, tone, kind, summary, payload_json,
          sequence, created_at
        ) VALUES (
          'activity-existing', 'thread-1', NULL, 'tool', 'tool.updated',
          'Existing', '{}', 41, '2026-08-11T00:00:00.000Z'
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 40 });

      const columns = yield* sql<{
        readonly name: string;
        readonly notnull: number;
        readonly dflt_value: string | null;
      }>`
        PRAGMA table_info(projection_thread_activities)
      `;
      const appliedSequence = columns.find((column) => column.name === "applied_sequence");
      assert.equal(appliedSequence?.notnull, 1);
      assert.equal(appliedSequence?.dflt_value, "0");

      const rows = yield* sql<{ readonly applied_sequence: number }>`
        SELECT applied_sequence
        FROM projection_thread_activities
        WHERE activity_id = 'activity-existing'
      `;
      assert.deepStrictEqual(rows, [{ applied_sequence: 0 }]);
    }),
  );
});
