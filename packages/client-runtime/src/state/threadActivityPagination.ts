import type {
  OrchestrationActivityPageInfo,
  OrchestrationActivityPageResult,
  OrchestrationThreadActivity,
} from "@t3tools/contracts";

export type ThreadActivityHistoryStatus =
  | "unavailable"
  | "idle"
  | "loading"
  | "complete"
  | "cursor-expired"
  | "error";

export interface ThreadActivityHistoryState {
  readonly sourceVersion: number | null;
  readonly asOfSequence: number | null;
  readonly activities: ReadonlyArray<OrchestrationThreadActivity>;
  readonly pageInfo: OrchestrationActivityPageInfo | null;
  readonly status: ThreadActivityHistoryStatus;
  readonly error: string | null;
}

export const EMPTY_THREAD_ACTIVITY_HISTORY: ThreadActivityHistoryState = {
  sourceVersion: null,
  asOfSequence: null,
  activities: [],
  pageInfo: null,
  status: "unavailable",
  error: null,
};

export function compareOrdinalStrings(left: string, right: string): number {
  const leftCodePoints = left[Symbol.iterator]();
  const rightCodePoints = right[Symbol.iterator]();
  while (true) {
    const leftPoint = leftCodePoints.next();
    const rightPoint = rightCodePoints.next();
    if (leftPoint.done || rightPoint.done) {
      return leftPoint.done === rightPoint.done ? 0 : leftPoint.done ? -1 : 1;
    }
    const difference = leftPoint.value.codePointAt(0)! - rightPoint.value.codePointAt(0)!;
    if (difference !== 0) return difference;
  }
}

export function compareThreadActivitiesByPosition(
  left: OrchestrationThreadActivity,
  right: OrchestrationThreadActivity,
): number {
  return (
    (left.sequence === undefined ? 0 : 1) - (right.sequence === undefined ? 0 : 1) ||
    (left.sequence ?? 0) - (right.sequence ?? 0) ||
    compareOrdinalStrings(left.createdAt, right.createdAt) ||
    compareOrdinalStrings(left.id, right.id)
  );
}

export function mergeThreadActivitiesById(
  current: ReadonlyArray<OrchestrationThreadActivity>,
  incoming: ReadonlyArray<OrchestrationThreadActivity>,
): ReadonlyArray<OrchestrationThreadActivity> {
  const byId = new Map(current.map((activity) => [activity.id, activity] as const));
  for (const activity of incoming) {
    byId.set(activity.id, activity);
  }
  return [...byId.values()].toSorted(compareThreadActivitiesByPosition);
}

export function initialThreadActivityHistory(
  pageInfo: OrchestrationActivityPageInfo | null,
  sourceVersion = 0,
): ThreadActivityHistoryState {
  if (pageInfo === null) return EMPTY_THREAD_ACTIVITY_HISTORY;
  return {
    sourceVersion,
    asOfSequence: pageInfo.asOfSequence,
    activities: [],
    pageInfo,
    status: pageInfo.nextCursor === null ? "complete" : "idle",
    error: null,
  };
}

export function applyThreadActivityPageResult(
  current: ThreadActivityHistoryState,
  result: OrchestrationActivityPageResult,
): ThreadActivityHistoryState {
  if (result.kind === "cursor-expired") {
    return {
      ...current,
      pageInfo:
        current.pageInfo === null
          ? null
          : {
              ...current.pageInfo,
              nextCursor: null,
              retentionFloor: result.retentionFloor,
            },
      status: "cursor-expired",
      error: null,
    };
  }
  return {
    sourceVersion: current.sourceVersion,
    asOfSequence: result.pageInfo.asOfSequence,
    activities: mergeThreadActivitiesById(current.activities, result.activities),
    pageInfo: result.pageInfo,
    status: result.pageInfo.nextCursor === null ? "complete" : "idle",
    error: null,
  };
}
