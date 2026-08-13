import { EventId, ThreadId, TurnId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "./Sqlite.ts";
import { ProjectionThreadActivityRepositoryLive } from "./ProjectionThreadActivities.ts";
import { ProjectionThreadActivityRepository } from "../Services/ProjectionThreadActivities.ts";

const TestLayer = ProjectionThreadActivityRepositoryLive.pipe(
  Layer.provideMerge(SqlitePersistenceMemory),
);

it.layer(TestLayer)("ProjectionThreadActivityRepository retention", (it) => {
  it.effect("converges to the same retained projection after a clean replay", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionThreadActivityRepository;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread-retention-replay");
      const turnId = TurnId.make("turn-retention-replay");
      const rows = Array.from({ length: 102 }, (_, index) => {
        const sequence = index + 1;
        return {
          activityId: EventId.make(`activity-replay-${sequence}`),
          threadId,
          turnId,
          tone: "tool" as const,
          kind: "tool.updated",
          summary: `Replay ${sequence}`,
          payload: { itemId: "tool-replay", detail: `snapshot ${sequence}` },
          sequence,
          appliedSequence: sequence,
          createdAt: `2026-08-11T00:00:00.${sequence.toString().padStart(3, "0")}Z`,
        };
      });
      const readProjection = () =>
        sql<{
          readonly activityId: string;
          readonly retentionIdentity: string | null;
          readonly payloadUtf8Bytes: number | null;
        }>`
          SELECT
            activity_id AS "activityId",
            retention_identity AS "retentionIdentity",
            payload_utf8_bytes AS "payloadUtf8Bytes"
          FROM projection_thread_activities
          WHERE thread_id = ${threadId}
          ORDER BY sequence ASC, created_at ASC, activity_id ASC
        `;

      yield* Effect.forEach(rows, repository.upsert, { concurrency: 1, discard: true });
      const onlineProjection = yield* readProjection();

      yield* sql`DELETE FROM projection_thread_activities WHERE thread_id = ${threadId}`;
      yield* sql`DELETE FROM projection_thread_activity_history WHERE thread_id = ${threadId}`;
      yield* Effect.forEach(rows, repository.upsert, { concurrency: 1, discard: true });
      const replayProjection = yield* readProjection();

      assert.equal(onlineProjection.length, 100);
      assert.deepStrictEqual(replayProjection, onlineProjection);
    }),
  );

  it.effect("bounds identified updates at write time and preserves conservative exceptions", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionThreadActivityRepository;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread-retention-bounded");
      const turnId = TurnId.make("turn-retention-bounded");

      for (let sequence = 1; sequence <= 102; sequence += 1) {
        yield* repository.upsert({
          activityId: EventId.make(`activity-known-${sequence}`),
          threadId,
          turnId,
          tone: "tool",
          kind: "tool.updated",
          summary: `Update ${sequence}`,
          payload: { itemId: "tool-1", detail: sequence === 1 ? "café 😀" : "update" },
          sequence,
          appliedSequence: sequence,
          createdAt: `2026-08-11T00:00:00.${sequence.toString().padStart(3, "0")}Z`,
        });
      }
      for (let sequence = 1; sequence <= 102; sequence += 1) {
        yield* repository.upsert({
          activityId: EventId.make(`activity-unknown-${sequence}`),
          threadId,
          turnId,
          tone: "tool",
          kind: "tool.updated",
          summary: `Unknown ${sequence}`,
          payload: { detail: "No stable identity" },
          sequence: 1_000 + sequence,
          appliedSequence: 1_000 + sequence,
          createdAt: `2026-08-12T00:00:00.${sequence.toString().padStart(3, "0")}Z`,
        });
      }
      yield* repository.upsert({
        activityId: EventId.make("activity-error"),
        threadId,
        turnId,
        tone: "error",
        kind: "tool.updated",
        summary: "Tool failed",
        payload: { itemId: "tool-1", status: "failed" },
        sequence: 2_000,
        appliedSequence: 2_000,
        createdAt: "2026-08-13T00:00:00.000Z",
      });

      const totals = yield* sql<{
        readonly rows: number;
        readonly identifiedRows: number;
        readonly measuredPayloadRows: number;
      }>`
        SELECT
          COUNT(*) AS rows,
          COUNT(retention_identity) AS identifiedRows,
          COUNT(payload_utf8_bytes) AS measuredPayloadRows
        FROM projection_thread_activities
        WHERE thread_id = ${threadId}
      `;
      assert.deepStrictEqual(totals, [
        { rows: 203, identifiedRows: 100, measuredPayloadRows: 203 },
      ]);

      const history = yield* sql<{
        readonly floor: number;
        readonly revision: number;
      }>`
        SELECT
          retention_floor_applied_sequence AS floor,
          history_revision AS revision
        FROM projection_thread_activity_history
        WHERE thread_id = ${threadId}
      `;
      assert.deepStrictEqual(history, [{ floor: 102, revision: 2 }]);
    }),
  );

  it.effect("retains one oversized newest row as the entire recent tail", () =>
    Effect.gen(function* () {
      const repository = yield* ProjectionThreadActivityRepository;
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread-retention-oversized");
      const turnId = TurnId.make("turn-retention-oversized");

      yield* repository.upsert({
        activityId: EventId.make("activity-oversized-older"),
        threadId,
        turnId,
        tone: "tool",
        kind: "tool.updated",
        summary: "Older",
        payload: { itemId: "tool-oversized", detail: "old" },
        sequence: 1,
        appliedSequence: 1,
        createdAt: "2026-08-11T00:00:01.000Z",
      });
      yield* repository.upsert({
        activityId: EventId.make("activity-oversized-newest"),
        threadId,
        turnId,
        tone: "tool",
        kind: "tool.updated",
        summary: "Oversized",
        payload: { itemId: "tool-oversized", detail: "x".repeat(4 * 1024 * 1024) },
        sequence: 2,
        appliedSequence: 2,
        createdAt: "2026-08-11T00:00:02.000Z",
      });

      const rows = yield* sql<{ readonly activityId: string }>`
        SELECT activity_id AS "activityId"
        FROM projection_thread_activities
        WHERE thread_id = ${threadId}
      `;
      assert.deepStrictEqual(rows, [{ activityId: "activity-oversized-newest" }]);
    }),
  );
});
