import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "../Migrations.ts";
import repair from "./056_RecoverCompactionQueuedTurnStarts.ts";

const encodePayload = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

it.layer(Layer.mergeAll(NodeSqliteClient.layer({ filename: ":memory:" })))(
  "legacy compaction queue repair",
  (it) => {
    it.effect("repairs advanced cursors without replay, redelivery, or resurrection", () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 55 });
        let sequence = 0;
        const append = Effect.fn(function* (
          threadId: string,
          type: string,
          payload: object,
          commandId = `accepted-${threadId}-${sequence}`,
        ) {
          sequence += 1;
          const payloadJson = yield* encodePayload(payload);
          yield* sql`
          INSERT INTO orchestration_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, actor_kind, payload_json, metadata_json
          ) VALUES (
            ${`event-${sequence}`}, 'thread', ${threadId}, ${sequence}, ${type},
            '2026-01-01T00:00:00.000Z', ${commandId}, 'user', ${payloadJson}, '{}'
          )
        `;
        });
        for (const threadId of [
          "queued",
          "delivered",
          "canceled",
          "drained",
          "failed",
          "interrupted",
          "reverted",
          "terminal",
          "pipelined",
        ]) {
          yield* append(threadId, "thread.message-sent", {
            messageId: "compact",
            role: "user",
            text: "/compact",
            attachments: [],
          });
          yield* append(threadId, "thread.turn-start-requested", { messageId: "compact" });
          for (const messageId of ["first", "second"]) {
            yield* append(
              threadId,
              "thread.turn-start-requested",
              {
                messageId,
                interactionMode: "plan",
                titleSeed: messageId,
                modelSelection: { instanceId: "codex", model: "gpt-5" },
                admissionProtocol: "managed-admission-v1",
              },
              `${threadId}-${messageId}`,
            );
          }
          if (threadId === "queued") {
            yield* sql`
            INSERT INTO projection_turns (
              thread_id, pending_message_id, state, requested_at, checkpoint_files_json
            )
            VALUES (${threadId}, 'compact', 'pending', '2026-01-01T00:00:00.000Z', '[]')
          `;
            continue;
          }
          if (threadId === "failed") {
            yield* append(threadId, "thread.activity-appended", {
              activity: { kind: "provider.turn.start.failed", payload: { requestId: "compact" } },
            });
            continue;
          }
          if (threadId === "interrupted" || threadId === "reverted" || threadId === "terminal") {
            yield* append(
              threadId,
              threadId === "interrupted"
                ? "thread.turn-interrupt-requested"
                : threadId === "reverted"
                  ? "thread.reverted"
                  : "thread.session-set",
              { session: { status: "stopped" } },
            );
            continue;
          }
          yield* append(threadId, "thread.activity-appended", {
            activity: { kind: "context-compaction", payload: { requestId: "compact" } },
          });
          if (threadId === "canceled") {
            yield* append(threadId, "thread.session-stop-requested", {});
            continue;
          }
          yield* append(
            threadId,
            "thread.turn-start-requested",
            { messageId: "first" },
            `server:after-compaction:${threadId}-first`,
          );
          if (threadId === "pipelined") {
            yield* append(
              threadId,
              "thread.turn-start-requested",
              { messageId: "second" },
              `server:after-compaction:${threadId}-second`,
            );
          }
          for (const duplicate of [false, true]) {
            yield* append(threadId, "thread.session-set", {
              session: { status: "running", activeTurnId: "turn-first" },
              duplicate,
            });
          }
          yield* sql`
          INSERT INTO projection_turns (
            thread_id, turn_id, pending_message_id, state, checkpoint_turn_count,
            checkpoint_ref, requested_at, checkpoint_files_json
          ) VALUES (${threadId}, 'turn-first', 'first', 'interrupted', 1, 'checkpoint',
            '2026-01-01T00:00:00.000Z', '[]')
        `;
          yield* append(
            threadId,
            "thread.turn-start-requested",
            { messageId: "second" },
            `server:after-compaction:${threadId}-second`,
          );
          if (threadId === "delivered" || threadId === "pipelined") {
            yield* sql`
            INSERT INTO projection_turns (
              thread_id, pending_message_id, pending_operation_id, state, requested_at,
              checkpoint_files_json
            ) VALUES (${threadId}, 'second', 'server:after-compaction:synthetic', 'pending',
              '2026-01-01T00:00:00.000Z', '[]')
          `;
          } else {
            yield* append(threadId, "thread.session-set", {
              session: { status: "running", activeTurnId: "turn-second" },
            });
            yield* append(threadId, "thread.turn-start-requested", { messageId: "ordinary" });
          }
        }
        yield* sql`
        INSERT INTO projection_state (projector, last_applied_sequence, updated_at)
        VALUES ('projection.thread-turns', ${sequence}, '2026-01-01T00:00:00.000Z')
      `;
        const cursorsBefore = yield* sql`SELECT * FROM projection_state`;
        const concreteBefore = yield* sql`SELECT * FROM projection_turns WHERE turn_id IS NOT NULL`;

        yield* runMigrations({ toMigrationInclusive: 56 });
        yield* repair;

        assert.deepStrictEqual(
          yield* sql`
          SELECT thread_id AS thread, pending_message_id AS message,
            pending_operation_id AS operation, pending_title_seed AS title,
            pending_admission_protocol AS admission
          FROM projection_turns WHERE state = 'queued' ORDER BY row_id
        `,
          [
            {
              thread: "queued",
              message: "first",
              operation: "queued-first",
              title: "first",
              admission: "managed-admission-v1",
            },
            {
              thread: "queued",
              message: "second",
              operation: "queued-second",
              title: "second",
              admission: "managed-admission-v1",
            },
            {
              thread: "delivered",
              message: "second",
              operation: "delivered-second",
              title: "second",
              admission: "managed-admission-v1",
            },
            {
              thread: "pipelined",
              message: "second",
              operation: "pipelined-second",
              title: "second",
              admission: "managed-admission-v1",
            },
          ],
        );
        assert.deepStrictEqual(yield* sql`SELECT * FROM projection_state`, cursorsBefore);
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM projection_turns WHERE turn_id IS NOT NULL`,
          concreteBefore,
        );
      }),
    );
  },
);
