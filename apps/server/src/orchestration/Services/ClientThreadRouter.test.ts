import {
  CommandId,
  OrchestrationGetSnapshotError,
  ProjectId,
  ThreadId,
  type ClientOrchestrationCommand,
  type OrchestrationEvent,
  type OrchestrationThreadDetailSnapshot,
  type OrchestrationThreadStreamItem,
} from "@t3tools/contracts";
import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import type { PiExternalThreadSource } from "../../piNative/PiExternalThreadSource.ts";
import type { ProjectionSnapshotQuery } from "./ProjectionSnapshotQuery.ts";
import {
  getClientThreadDetailSnapshot,
  getExternalThreadDispatch,
  getExternalThreadSubscription,
  makeBoundedExternalThreadSubscription,
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
      });

      const subscription = getExternalThreadSubscription(
        { threadId: externalThreadId, requestCompletionMarker: true },
        Option.some(externalSource),
      );
      expect(subscription).not.toBeNull();
      expect(yield* Stream.runCollect(subscription!)).toHaveLength(1);
    }),
  );

  it.effect("projects external snapshot and event activity payloads", () =>
    Effect.gen(function* () {
      const fullOutput = "x".repeat(200);
      const activity = {
        id: "activity-1",
        tone: "tool",
        kind: "item.completed",
        summary: "Ran command",
        payload: {
          itemType: "command_execution",
          title: "Ran command",
          status: "completed",
          data: { rawOutput: fullOutput },
        },
        turnId: null,
        sequence: 4,
        createdAt: "2026-09-11T00:00:00.000Z",
      };
      const snapshotWithOutput = {
        ...snapshot,
        thread: { ...snapshot.thread, activities: [activity] },
      } as unknown as OrchestrationThreadDetailSnapshot;
      const eventWithOutput = {
        sequence: 5,
        eventId: "event-1",
        aggregateKind: "thread",
        aggregateId: externalThreadId,
        type: "thread.activity-appended",
        payload: { threadId: externalThreadId, activity },
        occurredAt: "2026-09-11T00:00:00.000Z",
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
      } as unknown as OrchestrationEvent;
      const source = {
        ...externalSource,
        subscribeThread: () =>
          Stream.make(
            { kind: "snapshot" as const, snapshot: snapshotWithOutput },
            { kind: "event" as const, event: eventWithOutput },
            { kind: "synchronized" as const },
          ),
      } as unknown as PiExternalThreadSource["Service"];

      const subscription = getExternalThreadSubscription(
        { threadId: externalThreadId, requestCompletionMarker: true },
        Option.some(source),
      );
      const items = Array.from(yield* Stream.runCollect(subscription!));
      const summary = `${"x".repeat(83)}…`;

      expect(items).toMatchObject([
        {
          kind: "snapshot",
          snapshot: {
            thread: { activities: [{ payload: { data: { rawOutput: { content: summary } } } }] },
          },
        },
        {
          kind: "event",
          event: {
            payload: { activity: { payload: { data: { rawOutput: { content: summary } } } } },
          },
        },
        { kind: "synchronized" },
      ]);
    }),
  );

  it.effect("starts draining an external source before the client pulls", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sourceStarted = yield* Deferred.make<void>();
        const source = Stream.fromEffect(
          Deferred.succeed(sourceStarted, undefined).pipe(
            Effect.as({ kind: "synchronized" as const }),
          ),
        ).pipe(Stream.concat(Stream.never));

        yield* makeBoundedExternalThreadSubscription(source).pipe(Effect.asVoid);
        yield* Deferred.await(sourceStarted);
      }),
    ),
  );

  it.effect("drains a finite external stream before completing", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const marker = { kind: "synchronized" as const };
        const stream = yield* makeBoundedExternalThreadSubscription(Stream.make(marker));

        expect(Array.from(yield* Stream.runCollect(stream))).toEqual([marker]);
      }),
    ),
  );

  it.effect("fails when the external stream exhausts its live budget", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const marker = { kind: "synchronized" as const };
        const stream = yield* makeBoundedExternalThreadSubscription(Stream.make(marker, marker), {
          maxItems: 1,
        });
        const result = yield* Stream.runCollect(stream).pipe(Effect.result);

        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") {
          expect(result.failure.message).toBe(
            "The live event buffer is full. Resume from the last received sequence.",
          );
        }
      }),
    ),
  );

  it.effect("preserves external source failures", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const expected = new OrchestrationGetSnapshotError({ message: "external stream failed" });
        const source = Stream.concat(
          Stream.make({ kind: "synchronized" as const }),
          Stream.fail(expected),
        ) as Stream.Stream<OrchestrationThreadStreamItem, OrchestrationGetSnapshotError>;
        const stream = yield* makeBoundedExternalThreadSubscription(source);
        const result = yield* Stream.runCollect(stream).pipe(Effect.result);

        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") {
          expect(result.failure).toBe(expected);
        }
      }),
    ),
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
});
