import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("036_PiExternalLifecycleOverrides", (it) => {
  it.effect("adds the external Pi lifecycle override table", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 35 });
      yield* runMigrations({ toMigrationInclusive: 36 });

      const tables = yield* sql<{ readonly name: string }>`
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name IN (
            'pi_external_lifecycle_overrides',
            'pi_external_lifecycle_command_receipts'
          )
      `;
      assert.equal(tables.length, 2);
    }),
  );
});
