import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import orchestrationEventsMigration from "../src/persistence/Migrations/001_OrchestrationEvents.ts";
import projectionsMigration from "../src/persistence/Migrations/005_Projections.ts";
import * as NodeSqliteClient from "../src/persistence/NodeSqliteClient.ts";
import {
  formatSqliteGrowthReport,
  formatSqliteGrowthReportJson,
  runSqliteGrowthAnalysis,
} from "./t3-sqlite-growth.ts";

const analyzedAt = "2026-08-11T00:00:00.000Z";
const asciiPayload = '{"message":"plain"}';
const unicodePayload = '{"message":"café 😀"}';
const oldPayload = '{"old":true}';
const invalidJsonPayload = "é is deliberately not JSON";

const byteLength = (value: string) => Buffer.byteLength(value, "utf8");

const createFixtureDatabase = Effect.fn("createSqliteGrowthFixtureDatabase")(function* (
  directory: string,
) {
  const path = yield* Path.Path;
  const databasePath = path.join(directory, "state.sqlite");

  yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* orchestrationEventsMigration;
    yield* projectionsMigration;

    yield* sql`
      INSERT INTO projection_thread_activities (
        activity_id, thread_id, turn_id, tone, kind, summary, payload_json, created_at
      ) VALUES (
        'activity-1', 'thread-a', 'turn-1', 'neutral', 'tool.completed', 'ASCII',
        ${asciiPayload}, '2026-08-10T00:00:00.000Z'
      )
    `;
    yield* sql`
      INSERT INTO projection_thread_activities (
        activity_id, thread_id, turn_id, tone, kind, summary, payload_json, created_at
      ) VALUES (
        'activity-2', 'thread-a', 'turn-1', 'neutral', 'tool.completed', 'Unicode',
        ${unicodePayload}, '2026-07-20T00:00:00.000Z'
      )
    `;
    yield* sql`
      INSERT INTO projection_thread_activities (
        activity_id, thread_id, turn_id, tone, kind, summary, payload_json, created_at
      ) VALUES (
        'activity-3', 'thread-b', 'turn-2', 'neutral', 'context-compaction', 'Old',
        ${oldPayload}, '2025-01-01T00:00:00.000Z'
      )
    `;
    yield* sql`
      INSERT INTO projection_thread_activities (
        activity_id, thread_id, turn_id, tone, kind, summary, payload_json, created_at
      ) VALUES (
        'activity-4', 'thread-b', NULL, 'warning', 'runtime.warning', 'Invalid JSON',
        ${invalidJsonPayload}, 'not-a-date'
      )
    `;

    yield* sql`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
        command_id, causation_event_id, correlation_id, actor_kind, payload_json, metadata_json
      ) VALUES (
        'event-1', 'thread', 'thread-a', 1, 'thread.activity-appended',
        '2026-08-10T00:00:00.000Z', NULL, NULL, NULL, 'system', ${unicodePayload}, '{}'
      )
    `;
    yield* sql`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
        command_id, causation_event_id, correlation_id, actor_kind, payload_json, metadata_json
      ) VALUES (
        'event-2', 'thread', 'thread-b', 1, 'thread.created',
        '2026-04-01T00:00:00.000Z', NULL, NULL, NULL, 'user', ${asciiPayload}, '{}'
      )
    `;
    yield* sql`
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
        command_id, causation_event_id, correlation_id, actor_kind, payload_json, metadata_json
      ) VALUES (
        'event-3', 'project', 'project-a', 1, 'project.created',
        '2025-01-01T00:00:00.000Z', NULL, NULL, NULL, 'user', ${oldPayload}, '{}'
      )
    `;
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: databasePath })));

  return databasePath;
});

function findByKey<A extends Record<K, string>, K extends keyof A>(
  values: ReadonlyArray<A>,
  key: K,
  expected: string,
): A {
  const value = values.find((candidate) => candidate[key] === expected);
  assert.isDefined(value);
  return value;
}

it.layer(NodeServices.layer)("t3-sqlite-growth", (it) => {
  it.effect("accounts for UTF-8 bytes without parsing JSON and groups growth dimensions", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-growth-" });
      const databasePath = yield* createFixtureDatabase(directory);

      assert.isBelow(unicodePayload.length, byteLength(unicodePayload));

      const report = yield* runSqliteGrowthAnalysis({ database: databasePath, analyzedAt });
      const expectedProjectionBytes =
        byteLength(asciiPayload) +
        byteLength(unicodePayload) +
        byteLength(oldPayload) +
        byteLength(invalidJsonPayload);
      const expectedCanonicalBytes =
        byteLength(unicodePayload) + byteLength(asciiPayload) + byteLength(oldPayload);

      assert.equal(report.measurement.parsesPayloadJson, false);
      assert.deepStrictEqual(report.derivedProjection.totals, {
        rows: 4,
        payloadUtf8Bytes: expectedProjectionBytes,
        oldestAt: "2025-01-01T00:00:00.000Z",
        newestAt: "not-a-date",
      });
      assert.equal(report.canonicalEvents.totals.rows, 3);
      assert.equal(report.canonicalEvents.totals.payloadUtf8Bytes, expectedCanonicalBytes);

      const projectionThread = findByKey(report.derivedProjection.byThread, "threadId", "thread-a");
      assert.equal(projectionThread.rows, 2);
      assert.equal(
        projectionThread.payloadUtf8Bytes,
        byteLength(asciiPayload) + byteLength(unicodePayload),
      );

      const activityKind = findByKey(
        report.derivedProjection.byActivityKind,
        "kind",
        "tool.completed",
      );
      assert.equal(activityKind.rows, 2);
      assert.equal(
        activityKind.payloadUtf8Bytes,
        byteLength(asciiPayload) + byteLength(unicodePayload),
      );

      assert.equal(findByKey(report.derivedProjection.byAge, "bucket", "under_7_days").rows, 1);
      assert.equal(findByKey(report.derivedProjection.byAge, "bucket", "seven_to_30_days").rows, 1);
      assert.equal(findByKey(report.derivedProjection.byAge, "bucket", "over_365_days").rows, 1);
      assert.equal(findByKey(report.derivedProjection.byAge, "bucket", "unknown").rows, 1);

      assert.equal(report.canonicalEvents.byThread.length, 2);
      assert.equal(
        findByKey(report.canonicalEvents.byAggregateKind, "aggregateKind", "project").rows,
        1,
      );
      assert.equal(
        findByKey(report.canonicalEvents.byEventKind, "kind", "thread.activity-appended")
          .payloadUtf8Bytes,
        byteLength(unicodePayload),
      );
      assert.equal(findByKey(report.canonicalEvents.byAge, "bucket", "ninety_to_180_days").rows, 1);

      const human = formatSqliteGrowthReport(report);
      assert.include(human, "DERIVED PROJECTION DATA");
      assert.include(human, "CANONICAL ORCHESTRATION EVENTS");
      assert.include(human, "payload JSON was not parsed");
      assert.include(formatSqliteGrowthReportJson(report), `"analyzedAt": "${analyzedAt}"`);
    }),
  );

  it.effect("uses a strict read-only connection without changing the database or sidecars", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-growth-readonly-" });
      const databasePath = yield* createFixtureDatabase(directory);
      const before = yield* fs.readFile(databasePath);
      yield* fs.chmod(databasePath, 0o444);

      const report = yield* runSqliteGrowthAnalysis({ database: databasePath, analyzedAt });

      const after = yield* fs.readFile(databasePath);
      assert.equal(report.database.openMode, "read-only");
      assert.deepStrictEqual(Array.from(after), Array.from(before));
      assert.isFalse(yield* fs.exists(`${databasePath}-journal`));
      assert.isFalse(yield* fs.exists(`${databasePath}-wal`));
      assert.isFalse(yield* fs.exists(`${databasePath}-shm`));
    }),
  );

  it.effect("requires an explicitly supplied existing database", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-sqlite-growth-missing-" });
      const databasePath = path.join(directory, "missing.sqlite");

      const error = yield* runSqliteGrowthAnalysis({ database: databasePath }).pipe(Effect.flip);
      assert.equal(error._tag, "SqliteGrowthDatabaseMissingError");
      if (error._tag === "SqliteGrowthDatabaseMissingError") {
        assert.equal(error.databasePath, databasePath);
      }
    }),
  );
});
