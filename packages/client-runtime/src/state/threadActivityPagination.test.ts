import {
  EventId,
  type OrchestrationActivityPageInfo,
  type OrchestrationActivityPageResult,
  type OrchestrationThreadActivity,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import {
  applyThreadActivityPageResult,
  initialThreadActivityHistory,
  mergeThreadActivitiesById,
} from "./threadActivityPagination.ts";

const activity = (
  id: string,
  sequence: number,
  payload: unknown = { id },
): OrchestrationThreadActivity => ({
  id: EventId.make(id),
  tone: "tool",
  kind: "tool.updated",
  summary: id,
  payload,
  turnId: null,
  sequence,
  createdAt: `2026-04-02T00:00:${sequence.toString().padStart(2, "0")}.000Z`,
});

const pageInfo = (
  nextSequence: number | null,
  overrides: Partial<OrchestrationActivityPageInfo> = {},
): OrchestrationActivityPageInfo => ({
  asOfSequence: 10,
  nextCursor:
    nextSequence === null
      ? null
      : {
          kind: "before",
          asOfSequence: 10,
          position: {
            sequence: nextSequence,
            createdAt: `2026-04-02T00:00:${nextSequence.toString().padStart(2, "0")}.000Z`,
            activityId: EventId.make(`activity-${nextSequence}`),
          },
          retentionFloor: {
            kind: "oldest-available",
            position: {
              sequence: 1,
              createdAt: "2026-04-02T00:00:01.000Z",
              activityId: EventId.make("activity-1"),
            },
          },
          historyRevision: null,
        },
  retentionFloor: {
    kind: "oldest-available",
    position: {
      sequence: 1,
      createdAt: "2026-04-02T00:00:01.000Z",
      activityId: EventId.make("activity-1"),
    },
  },
  limits: { pageSize: 50, payloadBytes: 4 * 1024 * 1024 },
  payloadBytes: 100,
  omittedPayloads: [],
  ...overrides,
});

describe("thread activity pagination", () => {
  it("initializes from the bounded first page metadata", () => {
    const state = initialThreadActivityHistory(pageInfo(8));
    expect(state).toMatchObject({ asOfSequence: 10, status: "idle", activities: [] });
  });

  it("merges the next page in deterministic order", () => {
    const initial = initialThreadActivityHistory(pageInfo(8));
    const next: OrchestrationActivityPageResult = {
      kind: "page",
      activities: [activity("activity-6", 6), activity("activity-7", 7)],
      pageInfo: pageInfo(6),
    };
    const loaded = applyThreadActivityPageResult(initial, next);

    expect(loaded.activities.map((entry) => entry.id)).toEqual(["activity-6", "activity-7"]);
    expect(loaded.status).toBe("idle");
  });

  it("keeps a concurrent live activity while merging older history", () => {
    const loaded = applyThreadActivityPageResult(initialThreadActivityHistory(pageInfo(8)), {
      kind: "page",
      activities: [activity("activity-6", 6), activity("activity-7", 7)],
      pageInfo: pageInfo(6),
    });
    const withLive = mergeThreadActivitiesById(loaded.activities, [activity("activity-11", 11)]);

    expect(withLive.map((entry) => entry.id)).toEqual(["activity-6", "activity-7", "activity-11"]);
  });

  it("deduplicates a repeated activity ID and keeps the latest value", () => {
    const current = [activity("activity-6", 6), activity("activity-7", 7)];
    const duplicate = mergeThreadActivitiesById(current, [
      activity("activity-7", 7, { replacement: true }),
    ]);

    expect(duplicate.map((entry) => entry.id)).toEqual(["activity-6", "activity-7"]);
    expect(duplicate[1]?.payload).toEqual({ replacement: true });
  });

  it("uses ordinal activity ID ordering for exact position ties", () => {
    const ordered = mergeThreadActivitiesById(
      [],
      [activity("activity-ä", 7), activity("activity-Z", 7)],
    );
    expect(ordered.map((entry) => entry.id)).toEqual(["activity-Z", "activity-ä"]);
  });

  it("marks an empty terminal page complete", () => {
    const result = applyThreadActivityPageResult(initialThreadActivityHistory(pageInfo(2)), {
      kind: "page",
      activities: [],
      pageInfo: pageInfo(null, {
        retentionFloor: { kind: "empty" },
        payloadBytes: 0,
      }),
    });
    expect(result).toMatchObject({ status: "complete", activities: [] });
  });

  it("keeps explicit byte-cap omission metadata", () => {
    const omittedPayload = {
      kind: "omitted" as const,
      reason: "page-payload-byte-limit" as const,
      originalPayloadBytes: 5_000_000,
      limitBytes: 4 * 1024 * 1024,
    };
    const omitted = activity("activity-5", 5, omittedPayload);
    const result = applyThreadActivityPageResult(initialThreadActivityHistory(pageInfo(5)), {
      kind: "page",
      activities: [omitted],
      pageInfo: pageInfo(4, {
        payloadBytes: 0,
        omittedPayloads: [
          {
            activityId: omitted.id,
            originalPayloadBytes: omittedPayload.originalPayloadBytes,
            limitBytes: omittedPayload.limitBytes,
            reason: omittedPayload.reason,
          },
        ],
      }),
    });
    expect(result.activities[0]?.payload).toEqual(omittedPayload);
    expect(result.pageInfo?.omittedPayloads).toHaveLength(1);
  });

  it("stops retrying an expired cursor and exposes the current floor", () => {
    const result = applyThreadActivityPageResult(initialThreadActivityHistory(pageInfo(8)), {
      kind: "cursor-expired",
      asOfSequence: 10,
      retentionFloor: {
        kind: "oldest-available",
        position: {
          sequence: 4,
          createdAt: "2026-04-02T00:00:04.000Z",
          activityId: EventId.make("activity-4"),
        },
      },
    });
    expect(result.status).toBe("cursor-expired");
    expect(result.pageInfo?.nextCursor).toBeNull();
    expect(result.pageInfo?.retentionFloor).toMatchObject({ kind: "oldest-available" });
  });
});
