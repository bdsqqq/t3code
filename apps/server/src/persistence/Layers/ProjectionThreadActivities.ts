import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";
import { NonNegativeInt } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Struct from "effect/Struct";

import { toPersistenceDecodeError, toPersistenceSqlError } from "../Errors.ts";
import {
  classifyActivityForRetention,
  planCoalescibleToolUpdateRetention,
} from "../../orchestration/ActivityRetentionPolicy.ts";

import {
  AdvanceProjectionThreadActivityHistoryInput,
  DeleteProjectionThreadActivitiesInput,
  ListProjectionThreadActivitiesInput,
  ProjectionThreadActivity,
  ProjectionThreadActivityRepository,
  type ProjectionThreadActivityRepositoryShape,
} from "../Services/ProjectionThreadActivities.ts";

const ProjectionThreadActivityDbRowSchema = ProjectionThreadActivity.mapFields(
  Struct.assign({
    payload: Schema.fromJsonString(Schema.Unknown),
    sequence: Schema.NullOr(NonNegativeInt),
  }),
);

const CoalescibleToolUpdateDbRowSchema = Schema.Struct({
  activityId: ProjectionThreadActivity.fields.activityId,
  logicalIdentity: Schema.String,
  payloadUtf8Bytes: NonNegativeInt,
  sequence: Schema.NullOr(NonNegativeInt),
  createdAt: ProjectionThreadActivity.fields.createdAt,
});
const encodeActivityPayload = Schema.encodeUnknownEffect(Schema.UnknownFromJsonString);

function toPersistenceSqlOrDecodeError(sqlOperation: string, decodeOperation: string) {
  return (cause: unknown) =>
    Schema.isSchemaError(cause)
      ? toPersistenceDecodeError(decodeOperation)(cause)
      : toPersistenceSqlError(sqlOperation)(cause);
}

const makeProjectionThreadActivityRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const upsertProjectionThreadActivityRow = (
    row: ProjectionThreadActivity,
    retentionIdentity: string | null,
    payloadJson: string,
  ) =>
    sql`
            INSERT INTO projection_thread_activities (
              activity_id,
              thread_id,
              turn_id,
              tone,
              kind,
              summary,
              payload_json,
              sequence,
              applied_sequence,
              retention_identity,
              payload_utf8_bytes,
              created_at
            )
            VALUES (
              ${row.activityId},
              ${row.threadId},
              ${row.turnId},
              ${row.tone},
              ${row.kind},
              ${row.summary},
              ${payloadJson},
              ${row.sequence ?? null},
              ${row.appliedSequence},
              ${retentionIdentity},
              ${Buffer.byteLength(payloadJson, "utf8")},
              ${row.createdAt}
            )
            ON CONFLICT (activity_id)
            DO UPDATE SET
              thread_id = excluded.thread_id,
              turn_id = excluded.turn_id,
              tone = excluded.tone,
              kind = excluded.kind,
              summary = excluded.summary,
              payload_json = excluded.payload_json,
              sequence = excluded.sequence,
              applied_sequence = excluded.applied_sequence,
              retention_identity = excluded.retention_identity,
              payload_utf8_bytes = excluded.payload_utf8_bytes,
              created_at = excluded.created_at
          `;

  const listProjectionThreadActivityRows = SqlSchema.findAll({
    Request: ListProjectionThreadActivitiesInput,
    Result: ProjectionThreadActivityDbRowSchema,
    execute: ({ threadId }) =>
      sql`
        SELECT
          activity_id AS "activityId",
          thread_id AS "threadId",
          turn_id AS "turnId",
          tone,
          kind,
          summary,
          payload_json AS "payload",
          sequence,
          applied_sequence AS "appliedSequence",
          created_at AS "createdAt"
        FROM projection_thread_activities
        WHERE thread_id = ${threadId}
        ORDER BY
          CASE WHEN sequence IS NULL THEN 0 ELSE 1 END ASC,
          sequence ASC,
          created_at ASC,
          activity_id ASC
      `,
  });

  const deleteProjectionThreadActivityRows = SqlSchema.void({
    Request: DeleteProjectionThreadActivitiesInput,
    execute: ({ threadId }) =>
      sql`
        DELETE FROM projection_thread_activities
        WHERE thread_id = ${threadId}
      `,
  });

  const listCoalescibleToolUpdateRows = SqlSchema.findAll({
    Request: Schema.Struct({
      threadId: ProjectionThreadActivity.fields.threadId,
      turnId: ProjectionThreadActivity.fields.turnId,
    }),
    Result: CoalescibleToolUpdateDbRowSchema,
    execute: ({ threadId, turnId }) =>
      sql`
        SELECT
          activity_id AS "activityId",
          retention_identity AS "logicalIdentity",
          payload_utf8_bytes AS "payloadUtf8Bytes",
          sequence,
          created_at AS "createdAt"
        FROM projection_thread_activities
        WHERE thread_id = ${threadId}
          AND turn_id IS ${turnId}
          AND kind = 'tool.updated'
          AND retention_identity IS NOT NULL
          AND payload_utf8_bytes IS NOT NULL
      `,
  });

  const advanceProjectionThreadActivityHistory = SqlSchema.void({
    Request: AdvanceProjectionThreadActivityHistoryInput,
    execute: ({ threadId, appliedSequence, updatedAt }) =>
      sql`
        INSERT INTO projection_thread_activity_history (
          thread_id,
          retention_floor_applied_sequence,
          history_revision,
          updated_at
        ) VALUES (${threadId}, ${appliedSequence}, 1, ${updatedAt})
        ON CONFLICT (thread_id) DO UPDATE SET
          retention_floor_applied_sequence = MAX(
            retention_floor_applied_sequence,
            excluded.retention_floor_applied_sequence
          ),
          history_revision = history_revision + 1,
          updated_at = excluded.updated_at
      `,
  });

  const advanceHistory: ProjectionThreadActivityRepositoryShape["advanceHistory"] = (input) =>
    advanceProjectionThreadActivityHistory(input).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "ProjectionThreadActivityRepository.advanceHistory:query",
          "ProjectionThreadActivityRepository.advanceHistory:encodeRequest",
        ),
      ),
    );

  const upsert: ProjectionThreadActivityRepositoryShape["upsert"] = Effect.fn(
    "ProjectionThreadActivityRepository.upsert",
  )(
    function* (row) {
      const payloadJson = yield* encodeActivityPayload(row.payload ?? null);
      const classification = classifyActivityForRetention(row);
      const retentionIdentity =
        classification.kind === "coalescible-tool-update" ? classification.logicalIdentity : null;
      yield* upsertProjectionThreadActivityRow(row, retentionIdentity, payloadJson);
      if (retentionIdentity === null) {
        return;
      }

      const scopeRows = yield* listCoalescibleToolUpdateRows({
        threadId: row.threadId,
        turnId: row.turnId,
      });
      const deletedRows = planCoalescibleToolUpdateRetention(scopeRows).filter(
        ({ decision }) => decision.kind === "delete",
      );
      if (deletedRows.length === 0) {
        return;
      }
      yield* Effect.forEach(
        deletedRows,
        ({ row: deletedRow }) => sql`
        DELETE FROM projection_thread_activities
        WHERE activity_id = ${deletedRow.activityId}
      `,
        { concurrency: 1, discard: true },
      );
      yield* advanceProjectionThreadActivityHistory({
        threadId: row.threadId,
        appliedSequence: row.appliedSequence,
        updatedAt: row.createdAt,
      });
    },
    Effect.mapError(
      toPersistenceSqlOrDecodeError(
        "ProjectionThreadActivityRepository.upsert:query",
        "ProjectionThreadActivityRepository.upsert:retention",
      ),
    ),
  );

  const listByThreadId: ProjectionThreadActivityRepositoryShape["listByThreadId"] = (input) =>
    listProjectionThreadActivityRows(input).pipe(
      Effect.mapError(
        toPersistenceSqlOrDecodeError(
          "ProjectionThreadActivityRepository.listByThreadId:query",
          "ProjectionThreadActivityRepository.listByThreadId:decodeRows",
        ),
      ),
      Effect.map((rows) =>
        rows.map((row) => ({
          activityId: row.activityId,
          threadId: row.threadId,
          turnId: row.turnId,
          tone: row.tone,
          kind: row.kind,
          summary: row.summary,
          payload: row.payload,
          ...(row.sequence !== null ? { sequence: row.sequence } : {}),
          appliedSequence: row.appliedSequence,
          createdAt: row.createdAt,
        })),
      ),
    );

  const deleteByThreadId: ProjectionThreadActivityRepositoryShape["deleteByThreadId"] = (input) =>
    deleteProjectionThreadActivityRows(input).pipe(
      Effect.flatMap(() => advanceProjectionThreadActivityHistory(input)),
      Effect.mapError(
        toPersistenceSqlError("ProjectionThreadActivityRepository.deleteByThreadId:query"),
      ),
    );

  return {
    upsert,
    listByThreadId,
    deleteByThreadId,
    advanceHistory,
  } satisfies ProjectionThreadActivityRepositoryShape;
});

export const ProjectionThreadActivityRepositoryLive = Layer.effect(
  ProjectionThreadActivityRepository,
  makeProjectionThreadActivityRepository,
);
