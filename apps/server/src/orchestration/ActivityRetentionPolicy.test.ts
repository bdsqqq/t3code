import { assert, it } from "@effect/vitest";

import {
  ACTIVITY_RETENTION_POLICY,
  classifyActivityForRetention,
  planCoalescibleToolUpdateRetention,
  type CoalescibleToolUpdateRow,
} from "./ActivityRetentionPolicy.ts";

function updateRow(input: {
  readonly activityId: string;
  readonly logicalIdentity?: string;
  readonly payloadUtf8Bytes?: number;
  readonly sequence: number;
}): CoalescibleToolUpdateRow {
  return {
    activityId: input.activityId,
    logicalIdentity: input.logicalIdentity ?? "tool-1",
    payloadUtf8Bytes: input.payloadUtf8Bytes ?? 1,
    sequence: input.sequence,
    createdAt: `2026-08-11T00:00:${input.sequence.toString().padStart(2, "0")}.000Z`,
  };
}

it("classifies semantic rows and unknown tool identities conservatively", () => {
  assert.deepStrictEqual(
    classifyActivityForRetention({ kind: "approval.requested", tone: "approval", payload: {} }),
    { kind: "protected", category: "approval-or-user-input" },
  );
  assert.deepStrictEqual(
    classifyActivityForRetention({ kind: "task.progress", tone: "info", payload: {} }),
    { kind: "protected", category: "task-title-recovery" },
  );
  assert.deepStrictEqual(
    classifyActivityForRetention({ kind: "tool.updated", tone: "error", payload: {} }),
    { kind: "protected", category: "error" },
  );
  assert.deepStrictEqual(
    classifyActivityForRetention({ kind: "tool.updated", tone: "tool", payload: {} }),
    { kind: "conservative-tool-update", category: "unknown-tool-identity" },
  );
  assert.deepStrictEqual(
    classifyActivityForRetention({
      kind: "tool.updated",
      tone: "tool",
      payload: { data: { toolCallId: " call-1 " } },
    }),
    { kind: "coalescible-tool-update", logicalIdentity: "toolCallId:call-1" },
  );
  assert.deepStrictEqual(
    classifyActivityForRetention({
      kind: "tool.updated",
      tone: "tool",
      payload: { itemId: "same-value" },
    }),
    { kind: "coalescible-tool-update", logicalIdentity: "itemId:same-value" },
  );
  assert.deepStrictEqual(
    classifyActivityForRetention({
      kind: "tool.updated",
      tone: "tool",
      payload: { toolCallId: "same-value" },
    }),
    { kind: "coalescible-tool-update", logicalIdentity: "toolCallId:same-value" },
  );
});

it("retains the latest identity and the exact bounded recent tail", () => {
  const rows = Array.from(
    { length: ACTIVITY_RETENTION_POLICY.recentTailMaxRowsPerTurn + 2 },
    (_, index) => updateRow({ activityId: `activity-${index}`, sequence: index }),
  );
  const plan = planCoalescibleToolUpdateRetention(rows);
  const preserved = plan.filter(({ decision }) => decision.kind === "preserve");
  const deleted = plan.filter(({ decision }) => decision.kind === "delete");

  assert.equal(preserved.length, ACTIVITY_RETENTION_POLICY.recentTailMaxRowsPerTurn);
  assert.deepEqual(
    deleted.map(({ row }) => row.sequence),
    [1, 0],
  );
  assert.equal(preserved[0]?.decision.category, "latest-tool-identity");
});

it("retains one newest oversized row and closes the recent tail", () => {
  const plan = planCoalescibleToolUpdateRetention([
    updateRow({ activityId: "older", sequence: 1 }),
    updateRow({
      activityId: "oversized",
      sequence: 2,
      payloadUtf8Bytes: ACTIVITY_RETENTION_POLICY.recentTailMaxPayloadUtf8BytesPerTurn + 1,
    }),
  ]);

  assert.deepEqual(
    plan.map(({ row, decision }) => ({ activityId: row.activityId, decision })),
    [
      {
        activityId: "oversized",
        decision: { kind: "preserve", category: "latest-tool-identity" },
      },
      {
        activityId: "older",
        decision: { kind: "delete", category: "superseded-tool-update" },
      },
    ],
  );
});

it("retains the latest row for every identity outside the recent tail", () => {
  const budget = ACTIVITY_RETENTION_POLICY.recentTailMaxPayloadUtf8BytesPerTurn;
  const plan = planCoalescibleToolUpdateRetention([
    updateRow({ activityId: "old-a", logicalIdentity: "a", sequence: 1 }),
    updateRow({ activityId: "latest-a", logicalIdentity: "a", sequence: 2 }),
    updateRow({
      activityId: "latest-b",
      logicalIdentity: "b",
      sequence: 3,
      payloadUtf8Bytes: budget,
    }),
  ]);

  assert.deepEqual(
    plan.filter(({ decision }) => decision.kind === "preserve").map(({ row }) => row.activityId),
    ["latest-b", "latest-a"],
  );
});

it("keeps retained turns stable when a later turn is reverted", () => {
  const firstTurn = Array.from({ length: 102 }, (_, index) =>
    updateRow({ activityId: `first-${index}`, sequence: index }),
  );
  const laterTurn = Array.from({ length: 102 }, (_, index) =>
    updateRow({ activityId: `later-${index}`, sequence: index }),
  );
  const firstTurnBeforeRevert = planCoalescibleToolUpdateRetention(firstTurn)
    .filter(({ decision }) => decision.kind === "preserve")
    .map(({ row }) => row.activityId);

  // A revert removes the later scope wholesale. Because policy decisions are
  // scoped per turn, replaying the retained turn cannot make its two pruned
  // rows eligible again.
  assert.equal(
    planCoalescibleToolUpdateRetention(laterTurn).filter(
      ({ decision }) => decision.kind === "preserve",
    ).length,
    100,
  );
  const firstTurnAfterRevert = planCoalescibleToolUpdateRetention(firstTurn)
    .filter(({ decision }) => decision.kind === "preserve")
    .map(({ row }) => row.activityId);
  assert.deepStrictEqual(firstTurnAfterRevert, firstTurnBeforeRevert);
});
