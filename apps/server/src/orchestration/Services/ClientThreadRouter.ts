import type {
  ClientOrchestrationCommand,
  DispatchResult,
  OrchestrationSubscribeThreadInput,
  OrchestrationThreadDetailSnapshot,
  OrchestrationThreadStreamItem,
  ThreadId,
} from "@t3tools/contracts";
import {
  OrchestrationDispatchCommandError,
  OrchestrationGetSnapshotError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import {
  type PiExternalThreadSource,
  isPiExternalThreadId,
} from "../../piNative/PiExternalThreadSource.ts";
import { projectThreadDetailSnapshot } from "../ActivityPayloadProjection.ts";
import type { ProjectionSnapshotQuery } from "./ProjectionSnapshotQuery.ts";

type ExternalSource = Option.Option<PiExternalThreadSource["Service"]>;

const missingExternalSource = () =>
  new OrchestrationGetSnapshotError({
    message: "External pi threads are unavailable",
  });

export function getClientThreadDetailSnapshot(
  threadId: ThreadId,
  external: ExternalSource,
  internal: ProjectionSnapshotQuery["Service"],
): Effect.Effect<Option.Option<OrchestrationThreadDetailSnapshot>, OrchestrationGetSnapshotError> {
  if (isPiExternalThreadId(threadId)) {
    return Option.match(external, {
      onNone: () => Effect.fail(missingExternalSource()),
      onSome: (source) =>
        source.threadSnapshot(threadId).pipe(
          Effect.map(Option.some),
          Effect.mapError(
            (cause) =>
              new OrchestrationGetSnapshotError({
                message: `Failed to load external thread ${threadId}`,
                code: cause.code,
                cause,
              }),
          ),
        ),
    });
  }
  return internal.getThreadDetailSnapshot(threadId).pipe(
    Effect.map(Option.map(projectThreadDetailSnapshot)),
    Effect.mapError(
      (cause) =>
        new OrchestrationGetSnapshotError({
          message: `Failed to load thread ${threadId}`,
          cause,
        }),
    ),
  );
}

export function getExternalThreadSubscription(
  input: OrchestrationSubscribeThreadInput,
  external: ExternalSource,
): Stream.Stream<OrchestrationThreadStreamItem, OrchestrationGetSnapshotError> | null {
  if (!isPiExternalThreadId(input.threadId)) return null;
  return Option.match(external, {
    onNone: () => Stream.fail(missingExternalSource()),
    onSome: (source) =>
      source.subscribeThread(input).pipe(
        Stream.mapError(
          (cause) =>
            new OrchestrationGetSnapshotError({
              message: `Failed to subscribe to external thread ${input.threadId}`,
              cause,
            }),
        ),
      ),
  });
}

export function getExternalThreadDispatch(
  command: ClientOrchestrationCommand,
  external: ExternalSource,
): Effect.Effect<DispatchResult, OrchestrationDispatchCommandError> | null {
  if (!("threadId" in command) || !isPiExternalThreadId(command.threadId)) return null;
  return Option.match(external, {
    onNone: () =>
      Effect.fail(
        new OrchestrationDispatchCommandError({
          message: "External pi threads are unavailable",
        }),
      ),
    onSome: (source) =>
      source.dispatch(command).pipe(
        Effect.mapError(
          (cause) =>
            new OrchestrationDispatchCommandError({
              message: "Failed to dispatch external pi command",
              code: cause.code,
              cause,
            }),
        ),
      ),
  });
}
