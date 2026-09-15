import { assert, describe, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "./Migrations.ts";
import ContextMigration from "./Migrations/051_ProjectionThreadMessageContext.ts";
import TitleMigration from "./Migrations/052_ProjectionThreadTitleState.ts";

describe("fork/upstream migration compatibility", () => {
  for (const history of ["fresh", "fork-56", "upstream-52"] as const) {
    it.effect(`reconciles ${history} without losing either schema`, () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        if (history === "fork-56") {
          yield* runMigrations({ toMigrationInclusive: 56 });
          yield* sql`ALTER TABLE projection_thread_messages DROP COLUMN context_json`;
        } else if (history === "upstream-52") {
          yield* runMigrations({ toMigrationInclusive: 50 });
          yield* ContextMigration;
          yield* TitleMigration;
          yield* sql`INSERT INTO effect_sql_migrations (migration_id, name, created_at)
            VALUES (52, ${"ProjectionThreadTitleState"}, CURRENT_TIMESTAMP)`;
        }
        yield* runMigrations();
        const messages = yield* sql<{
          name: string;
        }>`PRAGMA table_info(projection_thread_messages)`;
        const threads = yield* sql<{ name: string }>`PRAGMA table_info(projection_threads)`;
        const turns = yield* sql<{ name: string }>`PRAGMA table_info(projection_turns)`;
        const lifecycle = yield* sql<{
          name: string;
        }>`PRAGMA table_info(pi_external_lifecycle_overrides)`;
        assert.include(
          messages.map(({ name }) => name),
          "context_json",
        );
        assert.include(
          threads.map(({ name }) => name),
          "title_state_json",
        );
        assert.include(
          turns.map(({ name }) => name),
          "pending_model_selection_json",
        );
        assert.include(
          lifecycle.map(({ name }) => name),
          "source_key",
        );
        assert.deepStrictEqual(yield* runMigrations(), []);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
    );
  }
});
