import { DatabaseSync } from "node:sqlite";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import orchestrationEventsMigration from "../src/persistence/Migrations/001_OrchestrationEvents.ts";
import projectionsMigration from "../src/persistence/Migrations/005_Projections.ts";
import activitySequenceMigration from "../src/persistence/Migrations/008_ProjectionThreadActivitySequence.ts";
import activityAppliedSequenceMigration from "../src/persistence/Migrations/040_ProjectionThreadActivityAppliedSequence.ts";
import retentionMigration from "../src/persistence/Migrations/041_ProjectionThreadActivityRetention.ts";
import * as NodeSqliteClient from "../src/persistence/NodeSqliteClient.ts";
import { runActivityCompact } from "./t3-sqlite-activity-compact.ts";

const confirmation = "semantic-tool-updates-v1";
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.UnknownFromJsonString);
const updatePayload = (sequence: number) =>
  encodeUnknownJson({ toolCallId: "same-tool", text: sequence === 1 ? "é" : "x" });
const invalidPayload = "not json é";
const unknownIdentityPayload = '{"detail":"unknown identity"}';
const semanticPayload = "{}";
const byteLength = (value: string) => Buffer.byteLength(value, "utf8");

const createFixture = Effect.fn("createActivityCompactFixture")(function* (directory: string) {
  const path = yield* Path.Path;
  const database = path.join(directory, "state.sqlite");
  yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* orchestrationEventsMigration;
    yield* projectionsMigration;
    yield* activitySequenceMigration;
    yield* activityAppliedSequenceMigration;
    yield* retentionMigration;
    yield* sql`INSERT INTO projection_state VALUES (
      'projection.thread-activities', 77, '2026-01-01T00:00:00Z'
    )`;
    yield* sql`INSERT INTO orchestration_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind,
      payload_json, metadata_json
    ) VALUES ('event-1', 'thread', 'thread-1', 1, 'fixture', '2026-01-01', 'system',
      ${'{"canonical":"é"}'}, '{}')`;
    for (let sequence = 1; sequence <= 102; sequence += 1) {
      yield* sql`INSERT INTO projection_thread_activities (
        activity_id, thread_id, turn_id, sequence, tone, kind, summary, payload_json, created_at
      ) VALUES (${`a-${sequence.toString().padStart(3, "0")}`}, 'thread-1', 'turn-1',
        ${sequence}, 'neutral', 'tool.updated', '', ${updatePayload(sequence)}, ${`2026-01-01T00:00:00.${sequence.toString().padStart(3, "0")}Z`})`;
    }
    yield* sql`INSERT INTO projection_thread_activities (
      activity_id, thread_id, turn_id, sequence, tone, kind, summary, payload_json, created_at
    ) VALUES ('invalid', 'thread-1', 'turn-1', 200, 'neutral', 'tool.updated', '',
      ${invalidPayload}, '2026-01-02')`;
    yield* sql`INSERT INTO projection_thread_activities (
      activity_id, thread_id, turn_id, sequence, tone, kind, summary, payload_json, created_at
    ) VALUES ('unknown', 'thread-1', 'turn-1', 199, 'neutral', 'tool.updated', '',
      ${unknownIdentityPayload}, '2026-01-01T23:00:00.000Z')`;
    yield* sql`INSERT INTO projection_thread_activities (
      activity_id, thread_id, turn_id, sequence, tone, kind, summary, payload_json, created_at
    ) VALUES ('semantic', 'thread-1', 'turn-1', 201, 'neutral', 'tool.completed', '',
      ${semanticPayload}, '2026-01-03')`;
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: database })));
  return database;
});

function identity(dbPath: string): { readonly rows: number; readonly bytes: number } {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db
      .prepare(`SELECT COUNT(*) AS rows,
      COALESCE(SUM(length(CAST(payload_json AS BLOB))), 0) AS bytes FROM orchestration_events`)
      .get() as {
      rows: number;
      bytes: number;
    };
  } finally {
    db.close();
  }
}

it.layer(NodeServices.layer)("t3-sqlite-activity-compact", (it) => {
  it.effect(
    "is conservative, resumable, idempotent, bounded, and never changes canonical events",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "activity-compact-" });
        const database = yield* createFixture(directory);
        const beforeFile = yield* fs.readFile(database);
        const canonicalBefore = identity(database);

        const dryRun = runActivityCompact({ database });
        const expectedCandidateBytes = byteLength(updatePayload(1)) + byteLength(updatePayload(2));
        const expectedTotalBytes =
          Array.from({ length: 102 }, (_, index) => byteLength(updatePayload(index + 1))).reduce(
            (total, bytes) => total + bytes,
            0,
          ) +
          byteLength(invalidPayload) +
          byteLength(unknownIdentityPayload) +
          byteLength(semanticPayload);
        assert.equal(dryRun.mode, "strict-readonly-dry-run");
        assert.deepStrictEqual(dryRun.candidate, {
          rows: 2,
          payloadUtf8Bytes: expectedCandidateBytes,
        });
        assert.deepStrictEqual(dryRun.preserved, {
          rows: 103,
          payloadUtf8Bytes: expectedTotalBytes - expectedCandidateBytes,
        });
        assert.deepStrictEqual(Array.from(yield* fs.readFile(database)), Array.from(beforeFile));
        assert.include(
          dryRun.byThreadKindCategory.map(({ category }) => category),
          "invalid-json",
        );
        assert.include(
          dryRun.byThreadKindCategory.map(({ category }) => category),
          "unknown-tool-identity",
        );
        assert.include(
          dryRun.byThreadKindCategory.map(({ category }) => category),
          "lifecycle-boundary",
        );
        assert.throws(() => runActivityCompact({ database, apply: true }), confirmation);
        assert.throws(
          () => runActivityCompact({ database, apply: true, confirm: "wrong" }),
          confirmation,
        );

        const interrupted = runActivityCompact({
          database,
          apply: true,
          confirm: confirmation,
          maxTransactions: 1,
        });
        assert.equal(interrupted.state, "interrupted");
        assert.isAtMost(interrupted.transactions.maxRows, 100);
        assert.isAtMost(interrupted.transactions.maxPayloadUtf8Bytes, 4 * 1024 * 1024);
        const applied = runActivityCompact({ database, apply: true, confirm: confirmation });
        assert.equal(applied.state, "completed");
        assert.deepStrictEqual(identity(database), canonicalBefore);
        const rerun = runActivityCompact({ database, apply: true, confirm: confirmation });
        assert.equal(rerun.candidate.rows, 0);

        const db = new DatabaseSync(database, { readOnly: true });
        try {
          const invalid = db
            .prepare(
              "SELECT retention_identity AS identity FROM projection_thread_activities WHERE activity_id='invalid'",
            )
            .get() as { identity: string | null };
          assert.isNull(invalid.identity);
          const history = db
            .prepare(
              "SELECT retention_floor_applied_sequence AS floor, history_revision AS revision FROM projection_thread_activity_history WHERE thread_id='thread-1'",
            )
            .get() as { floor: number; revision: number };
          assert.equal(history.floor, 77);
          assert.isAtLeast(history.revision, 1);
        } finally {
          db.close();
        }
      }),
  );

  it.effect("skips a row changed after classification and keeps the captured activity floor", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "activity-compact-race-" });
      const database = yield* createFixture(directory);
      let changed = false;

      runActivityCompact({
        database,
        apply: true,
        confirm: confirmation,
        beforeMutationBatch: ({ activityIds }) => {
          if (changed || !activityIds.includes("a-001")) {
            return;
          }
          changed = true;
          const sqlite = new DatabaseSync(database);
          try {
            sqlite
              .prepare(`
                UPDATE projection_thread_activities
                SET tone = 'error',
                  payload_json = '{"toolCallId":"same-tool","status":"failed"}',
                  applied_sequence = 78
                WHERE activity_id = 'a-001'
              `)
              .run();
          } finally {
            sqlite.close();
          }
        },
      });

      assert.isTrue(changed);
      const sqlite = new DatabaseSync(database, { readOnly: true });
      try {
        const changedRow = sqlite
          .prepare(`
            SELECT tone, applied_sequence AS appliedSequence
            FROM projection_thread_activities
            WHERE activity_id = 'a-001'
          `)
          .get() as { readonly tone: string; readonly appliedSequence: number };
        assert.deepStrictEqual(changedRow, { tone: "error", appliedSequence: 78 });
        const history = sqlite
          .prepare(`
            SELECT retention_floor_applied_sequence AS floor
            FROM projection_thread_activity_history
            WHERE thread_id = 'thread-1'
          `)
          .get() as { readonly floor: number };
        assert.equal(history.floor, 77);
      } finally {
        sqlite.close();
      }
    }),
  );

  it.effect("isolates one oversized row in a write transaction", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "activity-compact-large-" });
      const database = yield* createFixture(directory);
      const oversizedPayload = encodeUnknownJson({
        note: "x".repeat(4 * 1024 * 1024),
      });
      const sqlite = new DatabaseSync(database);
      try {
        sqlite.exec("DELETE FROM projection_thread_activities");
        sqlite
          .prepare(`
            INSERT INTO projection_thread_activities (
              activity_id, thread_id, turn_id, sequence, tone, kind, summary,
              payload_json, applied_sequence, created_at
            ) VALUES (
              $activityId, $threadId, NULL, 1, 'info', 'runtime.note', 'oversized',
              $payloadJson, 77, '2026-01-01T00:00:00.000Z'
            )
          `)
          .run({
            $activityId: "activity-oversized",
            $threadId: "thread-oversized",
            $payloadJson: oversizedPayload,
          });
      } finally {
        sqlite.close();
      }

      const applied = runActivityCompact({ database, apply: true, confirm: confirmation });
      assert.equal(applied.transactions.count, 1);
      assert.equal(applied.transactions.maxRows, 1);
      assert.isAbove(applied.transactions.maxPayloadUtf8Bytes, 4 * 1024 * 1024);
      assert.equal(applied.transactions.oversizedSingleRowCount, 1);
    }),
  );
});
