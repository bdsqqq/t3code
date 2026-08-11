import {
  CommandId,
  EventId,
  ProjectId,
  ThreadId,
  type ClientOrchestrationCommand,
  type OrchestrationThreadDetailSnapshot,
} from "@t3tools/contracts";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import type { PiExternalThreadSource } from "../../piNative/PiExternalThreadSource.ts";
import type { ProjectionSnapshotQuery } from "./ProjectionSnapshotQuery.ts";
import {
  getClientThreadActivityPage,
  getClientThreadDetailSnapshot,
  getExternalThreadDispatch,
  getExternalThreadSubscription,
} from "./ClientThreadRouter.ts";

const externalThreadId = ThreadId.make("external:pi:session-1");
const snapshot = {
  snapshotSequence: 4,
  thread: {
    id: externalThreadId,
    projectId: ProjectId.make("project-1"),
    activities: [],
  },
} as unknown as OrchestrationThreadDetailSnapshot;

const externalSource = {
  threadSnapshot: () => Effect.succeed(snapshot),
  subscribeThread: () => Stream.make({ kind: "synchronized" as const }),
  dispatch: () => Effect.succeed({ sequence: 7 }),
} as unknown as PiExternalThreadSource["Service"];

describe("ClientThreadRouter", () => {
  it.effect("routes external detail and subscription without calling internal projection", () =>
    Effect.gen(function* () {
      const internal = {
        getThreadDetailSnapshot: () =>
          Effect.die("internal projection must not load an external thread"),
      } as unknown as ProjectionSnapshotQuery["Service"];

      const detail = yield* getClientThreadDetailSnapshot(
        externalThreadId,
        Option.some(externalSource),
        internal,
      );
      expect(Option.getOrThrow(detail)).toMatchObject({
        snapshotSequence: snapshot.snapshotSequence,
        thread: { id: externalThreadId, activities: [] },
        pageInfo: { asOfSequence: snapshot.snapshotSequence, nextCursor: null },
      });

      const subscription = getExternalThreadSubscription(
        { threadId: externalThreadId, requestCompletionMarker: true },
        Option.some(externalSource),
      );
      expect(subscription).not.toBeNull();
      expect(yield* Stream.runCollect(subscription!)).toHaveLength(1);
    }),
  );

  it.effect("passes external command identity through unchanged", () =>
    Effect.gen(function* () {
      const command = {
        type: "thread.session.stop",
        commandId: CommandId.make("stable-command"),
        threadId: externalThreadId,
        createdAt: "2026-07-30T00:00:00.000Z",
      } satisfies ClientOrchestrationCommand;
      let received: ClientOrchestrationCommand | undefined;
      const source = {
        ...externalSource,
        dispatch: (input: ClientOrchestrationCommand) => {
          received = input;
          return Effect.succeed({ sequence: 8 });
        },
      } as PiExternalThreadSource["Service"];

      const routed = getExternalThreadDispatch(command, Option.some(source));
      expect(routed).not.toBeNull();
      yield* routed!;
      expect(received).toBe(command);
      expect(received?.commandId).toBe(command.commandId);
    }),
  );

  it.effect("pages external activities with the provider-neutral cursor contract", () =>
    Effect.gen(function* () {
      const pagedSnapshot = {
        ...snapshot,
        thread: {
          ...snapshot.thread,
          activities: [1, 2, 3, 4].map((sequence) => ({
            id: EventId.make(`activity-${sequence}`),
            tone: "tool",
            kind: "tool.updated",
            summary: `activity ${sequence}`,
            payload: { sequence },
            turnId: null,
            createdAt: `2026-07-30T00:00:0${sequence}.000Z`,
          })),
        },
      } as OrchestrationThreadDetailSnapshot;
      const source = {
        ...externalSource,
        threadSnapshot: () => Effect.succeed(pagedSnapshot),
      } as PiExternalThreadSource["Service"];
      const internal = {} as ProjectionSnapshotQuery["Service"];

      const initial = Option.getOrThrow(
        yield* getClientThreadActivityPage(
          externalThreadId,
          { cursor: { kind: "initial" }, pageSize: 2 },
          Option.some(source),
          internal,
        ),
      );
      expect(initial.kind).toBe("page");
      if (initial.kind !== "page" || initial.pageInfo.nextCursor === null) return;
      expect(initial.activities.map((activity) => activity.id)).toEqual([
        "activity-3",
        "activity-4",
      ]);

      const appendedSource = {
        ...source,
        threadSnapshot: () =>
          Effect.succeed({
            ...pagedSnapshot,
            thread: {
              ...pagedSnapshot.thread,
              activities: [
                ...pagedSnapshot.thread.activities,
                {
                  id: EventId.make("activity-5"),
                  tone: "tool",
                  kind: "tool.updated",
                  summary: "newer activity",
                  payload: {},
                  turnId: null,
                  createdAt: "2026-07-30T00:00:05.000Z",
                },
              ],
            },
          }),
      } as PiExternalThreadSource["Service"];
      const appended = Option.getOrThrow(
        yield* getClientThreadActivityPage(
          externalThreadId,
          { cursor: initial.pageInfo.nextCursor, pageSize: 2 },
          Option.some(appendedSource),
          internal,
        ),
      );
      expect(appended.kind).toBe("page");
      if (appended.kind === "page") {
        expect(appended.activities.map((activity) => activity.id)).toEqual([
          "activity-1",
          "activity-2",
        ]);
      }

      const changedSource = {
        ...source,
        threadSnapshot: () =>
          Effect.succeed({
            ...pagedSnapshot,
            thread: {
              ...pagedSnapshot.thread,
              activities: [
                ...pagedSnapshot.thread.activities,
                {
                  id: EventId.make("activity-2-late"),
                  tone: "tool",
                  kind: "tool.updated",
                  summary: "late older activity",
                  payload: {},
                  turnId: null,
                  createdAt: "2026-07-30T00:00:02.500Z",
                },
              ],
            },
          }),
      } as PiExternalThreadSource["Service"];
      const expired = Option.getOrThrow(
        yield* getClientThreadActivityPage(
          externalThreadId,
          { cursor: initial.pageInfo.nextCursor, pageSize: 2 },
          Option.some(changedSource),
          internal,
        ),
      );
      expect(expired.kind).toBe("cursor-expired");

      const next = Option.getOrThrow(
        yield* getClientThreadActivityPage(
          externalThreadId,
          { cursor: initial.pageInfo.nextCursor, pageSize: 2 },
          Option.some(source),
          internal,
        ),
      );
      expect(next.kind).toBe("page");
      if (next.kind === "page") {
        expect(next.activities.map((activity) => activity.id)).toEqual([
          "activity-1",
          "activity-2",
        ]);
        expect(next.pageInfo.nextCursor).toBeNull();
      }
    }),
  );
});
