#!/usr/bin/env node

import { DatabaseSync } from "node:sqlite";
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { Command, Flag } from "effect/unstable/cli";

import {
  ACTIVITY_RETENTION_POLICY,
  classifyActivityForRetention,
  makeActivityRetentionPlanningState,
  planNextCoalescibleToolUpdate,
} from "../src/orchestration/ActivityRetentionPolicy.ts";
import { ORCHESTRATION_PROJECTOR_NAMES } from "../src/orchestration/Layers/ProjectionPipeline.ts";

const SCAN_MAX_ROWS = 100;
const SCAN_MAX_PAYLOAD_UTF8_BYTES = 4 * 1024 * 1024;
const WRITE_MAX_ROWS = 100;
const WRITE_MAX_PAYLOAD_UTF8_BYTES = 4 * 1024 * 1024;

interface ActivityScope {
  readonly threadId: string;
  readonly turnId: string | null;
}

interface ActivityPosition {
  readonly activityId: string;
  readonly sequence: number | null;
  readonly createdAt: string;
}

interface ActivityRow extends ActivityPosition, ActivityScope {
  readonly kind: string;
  readonly tone: string;
  readonly payloadJson: string;
  readonly payloadUtf8Bytes: number;
  readonly appliedSequence: number;
  readonly storedRetentionIdentity: string | null;
  readonly storedPayloadUtf8Bytes: number | null;
}

interface CategoryTotal {
  readonly threadId: string;
  readonly activityKind: string;
  readonly category: string;
  rows: number;
  payloadUtf8Bytes: number;
}

type PlannedMutation =
  | { readonly kind: "delete"; readonly row: ActivityRow }
  | {
      readonly kind: "update-metadata";
      readonly row: ActivityRow;
      readonly retentionIdentity: string | null;
    };

interface MutableReportState {
  candidateRows: number;
  candidatePayloadUtf8Bytes: number;
  preservedRows: number;
  preservedPayloadUtf8Bytes: number;
  transactionCount: number;
  maxTransactionRows: number;
  maxTransactionPayloadUtf8Bytes: number;
  oversizedTransactionCount: number;
  interrupted: boolean;
}

export interface ActivityCompactReport {
  readonly policy: typeof ACTIVITY_RETENTION_POLICY;
  readonly activityProjectionBoundary: number;
  readonly mode: "strict-readonly-dry-run" | "apply";
  readonly state: "completed" | "interrupted";
  readonly candidate: { readonly rows: number; readonly payloadUtf8Bytes: number };
  readonly preserved: { readonly rows: number; readonly payloadUtf8Bytes: number };
  readonly byThreadKindCategory: ReadonlyArray<CategoryTotal>;
  readonly transactions: {
    readonly count: number;
    readonly rowLimit: number;
    readonly payloadUtf8ByteLimit: number;
    readonly maxRows: number;
    readonly maxPayloadUtf8Bytes: number;
    readonly oversizedSingleRowCount: number;
  };
}

export interface RunActivityCompactInput {
  readonly database: string;
  readonly apply?: boolean;
  readonly confirm?: string | undefined;
  /** Test-only interruption seam; deliberately not exposed as a CLI flag. */
  readonly maxTransactions?: number | undefined;
  /** Test-only concurrency seam; deliberately not exposed as a CLI flag. */
  readonly beforeMutationBatch?:
    | ((input: { readonly activityIds: ReadonlyArray<string> }) => void)
    | undefined;
}

function requireSchema(database: DatabaseSync): void {
  const columns = database
    .prepare("PRAGMA table_info(projection_thread_activities)")
    .all() as Array<{ readonly name: string }>;
  const history = database
    .prepare("SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = $tableName")
    .get({ $tableName: "projection_thread_activity_history" });
  if (
    history === undefined ||
    !columns.some(({ name }) => name === "retention_identity") ||
    !columns.some(({ name }) => name === "payload_utf8_bytes")
  ) {
    throw new Error(
      "Migration 041 is required: activity retention columns/history table are absent. Run the normal server migrations before this tool.",
    );
  }
}

function parsePayload(
  payloadJson: string,
): { readonly kind: "valid"; readonly payload: unknown } | { readonly kind: "invalid" } {
  try {
    return { kind: "valid", payload: JSON.parse(payloadJson) as unknown };
  } catch {
    return { kind: "invalid" };
  }
}

function addTotal(totals: Map<string, CategoryTotal>, row: ActivityRow, category: string): void {
  const key = JSON.stringify({ threadId: row.threadId, kind: row.kind, category });
  const total = totals.get(key) ?? {
    threadId: row.threadId,
    activityKind: row.kind,
    category,
    rows: 0,
    payloadUtf8Bytes: 0,
  };
  total.rows += 1;
  total.payloadUtf8Bytes += row.payloadUtf8Bytes;
  totals.set(key, total);
}

const readActivityPageSql = `
  WITH candidate_ids AS (
    SELECT
      activity_id,
      sequence,
      created_at,
      length(CAST(payload_json AS BLOB)) AS payload_utf8_bytes,
      ROW_NUMBER() OVER (
        ORDER BY (sequence IS NOT NULL) DESC, sequence DESC, created_at DESC, activity_id DESC
      ) AS row_number
    FROM projection_thread_activities
    WHERE thread_id = $threadId
      AND turn_id IS $turnId
      AND applied_sequence <= $activityProjectionBoundary
      AND (
        $hasCursor = 0
        OR (sequence IS NOT NULL) < $cursorSequenceRank
        OR (
          (sequence IS NOT NULL) = $cursorSequenceRank
          AND COALESCE(sequence, 0) < $cursorSequence
        )
        OR (
          (sequence IS NOT NULL) = $cursorSequenceRank
          AND COALESCE(sequence, 0) = $cursorSequence
          AND created_at < $cursorCreatedAt
        )
        OR (
          (sequence IS NOT NULL) = $cursorSequenceRank
          AND COALESCE(sequence, 0) = $cursorSequence
          AND created_at = $cursorCreatedAt
          AND activity_id < $cursorActivityId
        )
      )
    ORDER BY (sequence IS NOT NULL) DESC, sequence DESC, created_at DESC, activity_id DESC
    LIMIT $rowLimit
  ), sized_ids AS (
    SELECT
      candidate_ids.*,
      SUM(payload_utf8_bytes) OVER (ORDER BY row_number ASC) AS cumulative_payload_utf8_bytes
    FROM candidate_ids
  )
  SELECT
    activity.activity_id AS activityId,
    activity.thread_id AS threadId,
    activity.turn_id AS turnId,
    activity.kind,
    activity.tone,
    activity.payload_json AS payloadJson,
    sized_ids.payload_utf8_bytes AS payloadUtf8Bytes,
    activity.applied_sequence AS appliedSequence,
    activity.retention_identity AS storedRetentionIdentity,
    activity.payload_utf8_bytes AS storedPayloadUtf8Bytes,
    activity.sequence,
    activity.created_at AS createdAt
  FROM sized_ids
  INNER JOIN projection_thread_activities AS activity USING (activity_id)
  WHERE sized_ids.cumulative_payload_utf8_bytes <= $payloadByteLimit
    OR sized_ids.row_number = 1
  ORDER BY sized_ids.row_number ASC
`;

function* readScopeRows(
  database: DatabaseSync,
  scope: ActivityScope,
  activityProjectionBoundary: number,
): Generator<ActivityRow, void> {
  const readPage = database.prepare(readActivityPageSql);
  let cursor: ActivityPosition | null = null;
  for (;;) {
    const rows = readPage.all({
      $threadId: scope.threadId,
      $turnId: scope.turnId,
      $activityProjectionBoundary: activityProjectionBoundary,
      $hasCursor: cursor === null ? 0 : 1,
      $cursorSequenceRank: cursor?.sequence === null ? 0 : 1,
      $cursorSequence: cursor?.sequence ?? 0,
      $cursorCreatedAt: cursor?.createdAt ?? "",
      $cursorActivityId: cursor?.activityId ?? "",
      $rowLimit: SCAN_MAX_ROWS,
      $payloadByteLimit: SCAN_MAX_PAYLOAD_UTF8_BYTES,
    }) as unknown as Array<ActivityRow>;
    if (rows.length === 0) {
      return;
    }
    yield* rows;
    const lastRow = rows.at(-1);
    if (lastRow === undefined) {
      throw new Error("Activity scanner made no keyset progress");
    }
    cursor = lastRow;
  }
}

function readActivityProjectionBoundary(database: DatabaseSync): number {
  const row = database
    .prepare(`
      SELECT last_applied_sequence AS value
      FROM projection_state
      WHERE projector = $projector
    `)
    .get({ $projector: ORCHESTRATION_PROJECTOR_NAMES.threadActivities }) as
    | { readonly value: number }
    | undefined;
  return row?.value ?? 0;
}

function shouldFlushBeforeAdding(
  batch: ReadonlyArray<PlannedMutation>,
  batchPayloadUtf8Bytes: number,
  mutation: PlannedMutation,
): boolean {
  return (
    batch.length >= WRITE_MAX_ROWS ||
    (batch.length > 0 &&
      batchPayloadUtf8Bytes + mutation.row.payloadUtf8Bytes > WRITE_MAX_PAYLOAD_UTF8_BYTES)
  );
}

function applyMutationBatch(
  database: DatabaseSync,
  batch: ReadonlyArray<PlannedMutation>,
  retentionFloorAppliedSequence: number,
  report: MutableReportState,
): void {
  if (batch.length === 0) {
    return;
  }
  const payloadUtf8Bytes = batch.reduce(
    (total, mutation) => total + mutation.row.payloadUtf8Bytes,
    0,
  );
  database.exec("BEGIN IMMEDIATE");
  try {
    const deletedThreadIds = new Set<string>();
    for (const mutation of batch) {
      if (mutation.kind === "delete") {
        const result = database
          .prepare(`
            DELETE FROM projection_thread_activities
            WHERE activity_id = $activityId
              AND thread_id = $threadId
              AND turn_id IS $turnId
              AND kind = $activityKind
              AND tone = $tone
              AND payload_json = $payloadJson
              AND sequence IS $sequence
              AND applied_sequence = $appliedSequence
              AND created_at = $createdAt
              AND retention_identity IS $storedRetentionIdentity
              AND payload_utf8_bytes IS $storedPayloadUtf8Bytes
          `)
          .run({
            $activityId: mutation.row.activityId,
            $threadId: mutation.row.threadId,
            $turnId: mutation.row.turnId,
            $activityKind: mutation.row.kind,
            $tone: mutation.row.tone,
            $payloadJson: mutation.row.payloadJson,
            $sequence: mutation.row.sequence,
            $appliedSequence: mutation.row.appliedSequence,
            $createdAt: mutation.row.createdAt,
            $storedRetentionIdentity: mutation.row.storedRetentionIdentity,
            $storedPayloadUtf8Bytes: mutation.row.storedPayloadUtf8Bytes,
          });
        if (result.changes === 1) {
          deletedThreadIds.add(mutation.row.threadId);
        }
        continue;
      }
      database
        .prepare(`
          UPDATE projection_thread_activities
          SET retention_identity = $retentionIdentity,
            payload_utf8_bytes = $payloadUtf8Bytes
          WHERE activity_id = $activityId
            AND thread_id = $threadId
            AND turn_id IS $turnId
            AND kind = $activityKind
            AND tone = $tone
            AND payload_json = $payloadJson
            AND sequence IS $sequence
            AND applied_sequence = $appliedSequence
            AND created_at = $createdAt
            AND retention_identity IS $storedRetentionIdentity
            AND payload_utf8_bytes IS $storedPayloadUtf8Bytes
        `)
        .run({
          $retentionIdentity: mutation.retentionIdentity,
          $payloadUtf8Bytes: mutation.row.payloadUtf8Bytes,
          $activityId: mutation.row.activityId,
          $threadId: mutation.row.threadId,
          $turnId: mutation.row.turnId,
          $activityKind: mutation.row.kind,
          $tone: mutation.row.tone,
          $payloadJson: mutation.row.payloadJson,
          $sequence: mutation.row.sequence,
          $appliedSequence: mutation.row.appliedSequence,
          $createdAt: mutation.row.createdAt,
          $storedRetentionIdentity: mutation.row.storedRetentionIdentity,
          $storedPayloadUtf8Bytes: mutation.row.storedPayloadUtf8Bytes,
        });
    }
    const updatedAt = DateTime.formatIso(DateTime.nowUnsafe());
    const advanceHistory = database.prepare(`
      INSERT INTO projection_thread_activity_history (
        thread_id,
        retention_floor_applied_sequence,
        history_revision,
        updated_at
      ) VALUES ($threadId, $retentionFloorAppliedSequence, 1, $updatedAt)
      ON CONFLICT (thread_id) DO UPDATE SET
        retention_floor_applied_sequence = MAX(
          retention_floor_applied_sequence,
          excluded.retention_floor_applied_sequence
        ),
        history_revision = history_revision + 1,
        updated_at = excluded.updated_at
    `);
    for (const threadId of deletedThreadIds) {
      advanceHistory.run({
        $threadId: threadId,
        $retentionFloorAppliedSequence: retentionFloorAppliedSequence,
        $updatedAt: updatedAt,
      });
    }
    database.exec("COMMIT");
  } catch (cause) {
    database.exec("ROLLBACK");
    throw cause;
  }

  report.transactionCount += 1;
  report.maxTransactionRows = Math.max(report.maxTransactionRows, batch.length);
  report.maxTransactionPayloadUtf8Bytes = Math.max(
    report.maxTransactionPayloadUtf8Bytes,
    payloadUtf8Bytes,
  );
  if (payloadUtf8Bytes > WRITE_MAX_PAYLOAD_UTF8_BYTES) {
    report.oversizedTransactionCount += 1;
  }
}

function metadataMutation(
  row: ActivityRow,
  retentionIdentity: string | null,
): PlannedMutation | null {
  return row.storedRetentionIdentity === retentionIdentity &&
    row.storedPayloadUtf8Bytes === row.payloadUtf8Bytes
    ? null
    : { kind: "update-metadata", row, retentionIdentity };
}

export function runActivityCompact(input: RunActivityCompactInput): ActivityCompactReport {
  const apply = input.apply === true;
  if (apply && input.confirm !== ACTIVITY_RETENTION_POLICY.name) {
    throw new Error(`--apply requires --confirm ${ACTIVITY_RETENTION_POLICY.name}`);
  }

  const database = new DatabaseSync(input.database, {
    readOnly: !apply,
  });
  try {
    requireSchema(database);
    const retentionFloorAppliedSequence = readActivityProjectionBoundary(database);
    const scopes = database
      .prepare(`
        SELECT thread_id AS threadId, turn_id AS turnId
        FROM projection_thread_activities
        WHERE applied_sequence <= $activityProjectionBoundary
        GROUP BY thread_id, turn_id
        ORDER BY thread_id ASC, turn_id ASC
      `)
      .all({
        $activityProjectionBoundary: retentionFloorAppliedSequence,
      }) as unknown as Array<ActivityScope>;
    const totals = new Map<string, CategoryTotal>();
    const report: MutableReportState = {
      candidateRows: 0,
      candidatePayloadUtf8Bytes: 0,
      preservedRows: 0,
      preservedPayloadUtf8Bytes: 0,
      transactionCount: 0,
      maxTransactionRows: 0,
      maxTransactionPayloadUtf8Bytes: 0,
      oversizedTransactionCount: 0,
      interrupted: false,
    };
    let batch: Array<PlannedMutation> = [];
    let batchPayloadUtf8Bytes = 0;

    const flush = (): boolean => {
      if (!apply || batch.length === 0) {
        batch = [];
        batchPayloadUtf8Bytes = 0;
        return true;
      }
      if (input.maxTransactions !== undefined && report.transactionCount >= input.maxTransactions) {
        report.interrupted = true;
        return false;
      }
      input.beforeMutationBatch?.({
        activityIds: batch.map((mutation) => mutation.row.activityId),
      });
      applyMutationBatch(database, batch, retentionFloorAppliedSequence, report);
      batch = [];
      batchPayloadUtf8Bytes = 0;
      return true;
    };

    scopeLoop: for (const scope of scopes) {
      const planningState = makeActivityRetentionPlanningState();
      for (const row of readScopeRows(database, scope, retentionFloorAppliedSequence)) {
        const parsed = parsePayload(row.payloadJson);
        const classification =
          parsed.kind === "valid"
            ? classifyActivityForRetention({
                kind: row.kind,
                tone: row.tone,
                payload: parsed.payload,
              })
            : null;
        let category: string;
        let mutation: PlannedMutation | null;
        if (classification === null) {
          category = "invalid-json";
          mutation = metadataMutation(row, null);
        } else if (classification.kind === "coalescible-tool-update") {
          const planned = planNextCoalescibleToolUpdate(planningState, {
            activityId: row.activityId,
            logicalIdentity: classification.logicalIdentity,
            payloadUtf8Bytes: row.payloadUtf8Bytes,
            sequence: row.sequence,
            createdAt: row.createdAt,
          });
          category = planned.decision.category;
          mutation =
            planned.decision.kind === "delete"
              ? { kind: "delete", row }
              : metadataMutation(row, classification.logicalIdentity);
        } else {
          category = classification.category;
          mutation = metadataMutation(row, null);
        }

        addTotal(totals, row, category);
        if (category === "superseded-tool-update") {
          report.candidateRows += 1;
          report.candidatePayloadUtf8Bytes += row.payloadUtf8Bytes;
        } else {
          report.preservedRows += 1;
          report.preservedPayloadUtf8Bytes += row.payloadUtf8Bytes;
        }

        if (!apply || mutation === null) {
          continue;
        }
        if (shouldFlushBeforeAdding(batch, batchPayloadUtf8Bytes, mutation) && !flush()) {
          break scopeLoop;
        }
        batch.push(mutation);
        batchPayloadUtf8Bytes += mutation.row.payloadUtf8Bytes;
      }
    }
    if (!report.interrupted) {
      flush();
    }

    return {
      policy: ACTIVITY_RETENTION_POLICY,
      activityProjectionBoundary: retentionFloorAppliedSequence,
      mode: apply ? "apply" : "strict-readonly-dry-run",
      state: report.interrupted ? "interrupted" : "completed",
      candidate: {
        rows: report.candidateRows,
        payloadUtf8Bytes: report.candidatePayloadUtf8Bytes,
      },
      preserved: {
        rows: report.preservedRows,
        payloadUtf8Bytes: report.preservedPayloadUtf8Bytes,
      },
      byThreadKindCategory: [...totals.values()].sort(
        (left, right) =>
          left.threadId.localeCompare(right.threadId) ||
          left.activityKind.localeCompare(right.activityKind) ||
          left.category.localeCompare(right.category),
      ),
      transactions: {
        count: report.transactionCount,
        rowLimit: WRITE_MAX_ROWS,
        payloadUtf8ByteLimit: WRITE_MAX_PAYLOAD_UTF8_BYTES,
        maxRows: report.maxTransactionRows,
        maxPayloadUtf8Bytes: report.maxTransactionPayloadUtf8Bytes,
        oversizedSingleRowCount: report.oversizedTransactionCount,
      },
    };
  } finally {
    database.close();
  }
}

export function formatActivityCompactReport(report: ActivityCompactReport): string {
  return [
    `T3 derived activity compactor (${report.mode}; ${report.state})`,
    `Policy: ${report.policy.name}; tail ${report.policy.recentTailMaxRowsPerTurn} rows / ${report.policy.recentTailMaxPayloadUtf8BytesPerTurn} UTF-8 bytes per turn`,
    `Activity projection boundary: ${report.activityProjectionBoundary}`,
    `Delete candidates: ${report.candidate.rows} rows / ${report.candidate.payloadUtf8Bytes} bytes`,
    `Preserved: ${report.preserved.rows} rows / ${report.preserved.payloadUtf8Bytes} bytes`,
    `Transactions: ${report.transactions.count}; maximum ${report.transactions.maxRows} rows / ${report.transactions.maxPayloadUtf8Bytes} payload bytes; oversized single rows: ${report.transactions.oversizedSingleRowCount}`,
    ...report.byThreadKindCategory.map(
      (total) =>
        `  ${total.threadId} | ${total.activityKind} | ${total.category}: ${total.rows} rows / ${total.payloadUtf8Bytes} bytes`,
    ),
  ].join("\n");
}

export const t3SqliteActivityCompactCommand = Command.make(
  "t3-sqlite-activity-compact",
  {
    database: Flag.string("database").pipe(
      Flag.withDescription("Explicit path to a migrated T3 state.sqlite database."),
    ),
    apply: Flag.boolean("apply").pipe(
      Flag.withDescription("Apply derived projection cleanup instead of a read-only dry run."),
      Flag.withDefault(false),
    ),
    confirm: Flag.string("confirm").pipe(
      Flag.optional,
      Flag.withDescription(`Required with --apply; must equal ${ACTIVITY_RETENTION_POLICY.name}.`),
    ),
    json: Flag.boolean("json").pipe(
      Flag.withDescription("Emit the complete machine-readable report as JSON."),
      Flag.withDefault(false),
    ),
  },
  ({ database, apply, confirm, json }) =>
    Effect.sync(() =>
      runActivityCompact({
        database,
        apply,
        confirm: Option.getOrUndefined(confirm),
      }),
    ).pipe(
      Effect.flatMap((report) =>
        Console.log(json ? JSON.stringify(report, null, 2) : formatActivityCompactReport(report)),
      ),
    ),
).pipe(
  Command.withDescription(
    "Dry-run or apply deterministic compaction of derived thread activity projection rows.",
  ),
);

if (import.meta.main) {
  Command.run(t3SqliteActivityCompactCommand, { version: "0.0.0" }).pipe(
    Effect.provide(NodeServices.layer),
    NodeRuntime.runMain,
  );
}
