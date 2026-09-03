import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("048_PiExternalLifecycleOverrides", (it) => {
  it.effect("bridges fork migration ids while adding external Pi lifecycle tables", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 35 });
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name)
        VALUES
          (36, 'PiExternalLifecycleOverrides'),
          (37, 'ProjectionPendingTurnIntent'),
          (38, 'ProjectionPendingTurnOperationId'),
          (39, 'ProjectionPendingTurnAdmissionProtocol'),
          (40, 'ProjectionThreadActivityAppliedSequence'),
          (41, 'ProjectionThreadActivityRetention'),
          (42, 'PiExternalLifecycleOverrides'),
          (43, 'ProjectionPendingTurnIntent'),
          (44, 'ProjectionPendingTurnOperationId'),
          (45, 'ProjectionPendingTurnAdmissionProtocol'),
          (46, 'AuthSessionClientConnection')
      `;

      yield* runMigrations({ toMigrationInclusive: 48 });

      const threadColumns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_threads)
      `;
      const projectColumns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_projects)
      `;
      const authColumns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(auth_sessions)
      `;
      const indexes = yield* sql<{ readonly name: string }>`
        SELECT name
        FROM sqlite_master
        WHERE type = 'index'
          AND name = 'idx_projection_turns_thread_keyset'
      `;
      const tables = yield* sql<{ readonly name: string }>`
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name IN (
            'pi_external_lifecycle_overrides',
            'pi_external_lifecycle_command_receipts'
          )
      `;

      const threadColumnNames = new Set(threadColumns.map((column) => column.name));
      const projectColumnNames = new Set(projectColumns.map((column) => column.name));
      const authColumnNames = new Set(authColumns.map((column) => column.name));
      assert.ok(threadColumnNames.has("pinned_at"));
      assert.ok(threadColumnNames.has("pin_order_key"));
      assert.ok(threadColumnNames.has("linked_pull_request_json"));
      assert.ok(threadColumnNames.has("unsettled_at"));
      assert.ok(projectColumnNames.has("default_thread_env_mode"));
      assert.ok(projectColumnNames.has("favicon_path"));
      assert.ok(projectColumnNames.has("auto_pull"));
      assert.ok(projectColumnNames.has("project_icon_json"));
      assert.ok(authColumnNames.has("client_surface"));
      assert.ok(authColumnNames.has("client_app_version"));
      assert.equal(indexes.length, 1);
      assert.equal(tables.length, 2);
    }),
  );
});
