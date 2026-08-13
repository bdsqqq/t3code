import { ORCHESTRATION_ACTIVITY_PAGE_MAX_PAYLOAD_BYTES } from "@t3tools/contracts";
import * as Predicate from "effect/Predicate";

/**
 * Derived activities are not display-only: revert reconstruction, shell
 * blocked-on-user counts, task-title recovery, and checkpoint/turn history all
 * read them. Retention is therefore an allowlist for one known cumulative
 * shape, not a generic age or count cap. Unknown and semantic activity kinds
 * pass through conservatively.
 */
export const ACTIVITY_RETENTION_POLICY = {
  name: "semantic-tool-updates-v1",
  recentTailMaxRowsPerTurn: 100,
  recentTailMaxPayloadUtf8BytesPerTurn: ORCHESTRATION_ACTIVITY_PAGE_MAX_PAYLOAD_BYTES,
} as const;

export interface ActivityRetentionClassificationInput {
  readonly kind: string;
  readonly tone: string;
  readonly payload: unknown;
}

export type ActivityRetentionClassification =
  | {
      readonly kind: "protected";
      readonly category:
        | "approval-or-user-input"
        | "error"
        | "lifecycle-boundary"
        | "other-semantic"
        | "task-title-recovery"
        | "turn-or-checkpoint-boundary";
    }
  | {
      readonly kind: "conservative-tool-update";
      readonly category: "unknown-tool-identity";
    }
  | {
      readonly kind: "coalescible-tool-update";
      readonly logicalIdentity: string;
    };

export interface CoalescibleToolUpdateRow {
  readonly activityId: string;
  readonly logicalIdentity: string;
  readonly payloadUtf8Bytes: number;
  readonly sequence: number | null;
  readonly createdAt: string;
}

export type ActivityRetentionDecision =
  | { readonly kind: "delete"; readonly category: "superseded-tool-update" }
  | {
      readonly kind: "preserve";
      readonly category: "latest-tool-identity" | "recent-tool-tail";
    };

export interface PlannedActivityRetentionRow {
  readonly row: CoalescibleToolUpdateRow;
  readonly decision: ActivityRetentionDecision;
}

export interface ActivityRetentionPlanningState {
  readonly seenLogicalIdentities: Set<string>;
  recentTailOpen: boolean;
  recentTailRows: number;
  recentTailPayloadUtf8Bytes: number;
}

function readNonEmptyString(
  value: { readonly [key: PropertyKey]: unknown },
  key: PropertyKey,
): string | null {
  const candidate = value[key];
  if (!Predicate.isString(candidate)) {
    return null;
  }
  const trimmed = candidate.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function extractLogicalToolIdentity(payload: unknown): string | null {
  if (!Predicate.isObject(payload)) {
    return null;
  }
  const itemId = readNonEmptyString(payload, "itemId");
  if (itemId !== null) {
    return `itemId:${itemId}`;
  }
  const toolCallId = readNonEmptyString(payload, "toolCallId");
  if (toolCallId !== null) {
    return `toolCallId:${toolCallId}`;
  }
  const data = payload.data;
  if (!Predicate.isObject(data)) {
    return null;
  }
  const nestedToolCallId = readNonEmptyString(data, "toolCallId");
  return nestedToolCallId === null ? null : `toolCallId:${nestedToolCallId}`;
}

function isErrorToolUpdate(input: ActivityRetentionClassificationInput): boolean {
  if (input.tone === "error") {
    return true;
  }
  if (!Predicate.isObject(input.payload)) {
    return false;
  }
  const status = input.payload.status;
  return status === "failed" || status === "error";
}

function protectedCategory(
  input: ActivityRetentionClassificationInput,
): Extract<ActivityRetentionClassification, { readonly kind: "protected" }>["category"] {
  if (input.tone === "error" || input.kind.includes(".failed") || input.kind.endsWith(".error")) {
    return "error";
  }
  if (input.kind.includes("approval") || input.kind.includes("user-input")) {
    return "approval-or-user-input";
  }
  if (
    input.kind === "tool.started" ||
    input.kind === "tool.completed" ||
    input.kind === "tool.denied" ||
    input.kind === "item.started" ||
    input.kind === "item.completed"
  ) {
    return "lifecycle-boundary";
  }
  if (
    input.kind === "task.started" ||
    input.kind === "task.progress" ||
    input.kind === "task.completed"
  ) {
    return "task-title-recovery";
  }
  if (
    input.kind.includes("checkpoint") ||
    input.kind.startsWith("turn.") ||
    input.kind.startsWith("thread.turn") ||
    input.kind === "context-compaction"
  ) {
    return "turn-or-checkpoint-boundary";
  }
  return "other-semantic";
}

export function classifyActivityForRetention(
  input: ActivityRetentionClassificationInput,
): ActivityRetentionClassification {
  if (input.kind !== "tool.updated" || isErrorToolUpdate(input)) {
    return {
      kind: "protected",
      category: isErrorToolUpdate(input) ? "error" : protectedCategory(input),
    };
  }
  const logicalIdentity = extractLogicalToolIdentity(input.payload);
  return logicalIdentity === null
    ? { kind: "conservative-tool-update", category: "unknown-tool-identity" }
    : { kind: "coalescible-tool-update", logicalIdentity };
}

export function compareActivityRetentionRows(
  left: CoalescibleToolUpdateRow,
  right: CoalescibleToolUpdateRow,
): number {
  const leftSequenceRank = left.sequence === null ? 0 : 1;
  const rightSequenceRank = right.sequence === null ? 0 : 1;
  return (
    leftSequenceRank - rightSequenceRank ||
    (left.sequence ?? 0) - (right.sequence ?? 0) ||
    left.createdAt.localeCompare(right.createdAt) ||
    left.activityId.localeCompare(right.activityId)
  );
}

export function makeActivityRetentionPlanningState(): ActivityRetentionPlanningState {
  return {
    seenLogicalIdentities: new Set<string>(),
    recentTailOpen: true,
    recentTailRows: 0,
    recentTailPayloadUtf8Bytes: 0,
  };
}

/** Plans one row while scanning a retention scope in newest-first order. */
export function planNextCoalescibleToolUpdate(
  state: ActivityRetentionPlanningState,
  row: CoalescibleToolUpdateRow,
): PlannedActivityRetentionRow {
  const latestForIdentity = !state.seenLogicalIdentities.has(row.logicalIdentity);
  state.seenLogicalIdentities.add(row.logicalIdentity);

  let inRecentTail = false;
  if (state.recentTailOpen) {
    const firstRowIsOversized =
      state.recentTailRows === 0 &&
      row.payloadUtf8Bytes > ACTIVITY_RETENTION_POLICY.recentTailMaxPayloadUtf8BytesPerTurn;
    const fits =
      state.recentTailRows < ACTIVITY_RETENTION_POLICY.recentTailMaxRowsPerTurn &&
      state.recentTailPayloadUtf8Bytes + row.payloadUtf8Bytes <=
        ACTIVITY_RETENTION_POLICY.recentTailMaxPayloadUtf8BytesPerTurn;
    if (firstRowIsOversized || fits) {
      inRecentTail = true;
      state.recentTailRows += 1;
      state.recentTailPayloadUtf8Bytes += row.payloadUtf8Bytes;
      if (firstRowIsOversized) {
        state.recentTailOpen = false;
      }
    } else {
      state.recentTailOpen = false;
    }
  }

  return {
    row,
    decision: latestForIdentity
      ? { kind: "preserve", category: "latest-tool-identity" }
      : inRecentTail
        ? { kind: "preserve", category: "recent-tool-tail" }
        : { kind: "delete", category: "superseded-tool-update" },
  };
}

/**
 * Plans retention for identified non-error tool updates in one
 * `(thread_id, turn_id)` scope. Rows outside this classification never enter
 * the planner and are therefore preserved conservatively.
 */
export function planCoalescibleToolUpdateRetention(
  rows: ReadonlyArray<CoalescibleToolUpdateRow>,
): ReadonlyArray<PlannedActivityRetentionRow> {
  const descendingRows = [...rows].toSorted((left, right) =>
    compareActivityRetentionRows(right, left),
  );
  const state = makeActivityRetentionPlanningState();
  return descendingRows.map((row) => planNextCoalescibleToolUpdate(state, row));
}
