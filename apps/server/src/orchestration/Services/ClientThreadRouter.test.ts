import {
  CommandId,
  ProjectId,
  ThreadId,
  type ClientOrchestrationCommand,
} from "@t3tools/contracts";
import { it } from "@effect/vitest";
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
} from "./ClientThreadRouter.ts";

const externalThreadId = ThreadId.make("external:pi:session-1");
const snapshot = {
  snapshotSequence: 4,
  thread: {
    id: externalThreadId,
    projectId: ProjectId.make("project-1"),
  },
} as never;

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
      expect(Option.getOrThrow(detail)).toBe(snapshot);

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
});
