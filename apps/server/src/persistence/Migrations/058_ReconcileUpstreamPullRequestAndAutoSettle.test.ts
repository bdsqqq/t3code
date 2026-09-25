import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { runMigrations } from "../Migrations.ts";
import TitleState from "./052_ProjectionThreadTitleState.ts";
import FilesViewed from "./053_PullRequestFilesViewed.ts";
import AutoSettle from "./054_ProjectionThreadsAutoSettleDisabledAt.ts";

for (const history of ["fresh", "fork57", "upstream53", "upstream54"] as const) {
  it.effect(`reconciles ${history} without rewriting recorded migration ids`, () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      if (history === "fork57") {
        yield* runMigrations({ toMigrationInclusive: 57 });
      } else if (history !== "fresh") {
        yield* runMigrations({ toMigrationInclusive: 51 });
        yield* TitleState;
        yield* FilesViewed;
        yield* sql`INSERT INTO effect_sql_migrations (migration_id, name) VALUES
          (52, 'ProjectionThreadTitleState'), (53, 'PullRequestFilesViewed')`;
        if (history === "upstream54") {
          yield* AutoSettle;
          yield* sql`INSERT INTO effect_sql_migrations (migration_id, name)
            VALUES (54, 'ProjectionThreadsAutoSettleDisabledAt')`;
        }
        yield* sql`INSERT INTO pull_request_files_viewed
          (provider, host, repository, number, viewer, path, revision, viewed_at)
          VALUES ('github', 'github.com', 'owner/repo', 1, 'reader', 'a.ts', 'abc', '2026-09-25')`;
      }
      const before =
        history === "fresh"
          ? []
          : yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`;
      yield* runMigrations();
      const after =
        yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`;
      assert.deepEqual(after.slice(0, before.length), before);
      const columns = yield* sql<{ readonly name: string }>`PRAGMA table_info(projection_turns)`;
      for (const name of [
        "pending_model_selection_json",
        "pending_title_seed",
        "pending_interaction_mode",
        "pending_operation_id",
        "pending_admission_protocol",
      ]) {
        assert.isTrue(columns.some((column) => column.name === name));
      }
      const threadColumns = yield* sql<{
        readonly name: string;
      }>`PRAGMA table_info(projection_threads)`;
      for (const name of ["title_state_json", "auto_settle_disabled_at"]) {
        assert.isTrue(threadColumns.some((column) => column.name === name));
      }
      yield* sql`SELECT * FROM pi_external_lifecycle_overrides`;
      const viewed = yield* sql`SELECT revision FROM pull_request_files_viewed`;
      assert.deepEqual(viewed, history.startsWith("upstream") ? [{ revision: "abc" }] : []);
      assert.deepEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
}
