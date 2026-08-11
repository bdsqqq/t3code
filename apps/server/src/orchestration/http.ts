import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";

import { normalizeDispatchCommand } from "./Normalizer.ts";
import {
  annotateEnvironmentRequest,
  failEnvironmentInternal,
  failEnvironmentInvalidRequest,
  failEnvironmentNotFound,
  requireEnvironmentScope,
} from "../auth/http.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import {
  getClientThreadDetailSnapshot,
  getClientThreadActivityPage,
  getExternalThreadDispatch,
  getLegacyClientThreadDetailSnapshot,
} from "./Services/ClientThreadRouter.ts";
import { PiExternalThreadSource } from "../piNative/PiExternalThreadSource.ts";

const externalInvalidRequestCodes = new Set([
  "attachments_unsupported",
  "command_rejected",
  "interrupt_unsupported",
  "invalid_attachment",
  "read_only",
  "runtime_starting",
  "stop_unsupported",
  "streaming_behavior_required",
  "unsupported_external_mutation",
]);

export const orchestrationHttpApiLayer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "orchestration",
  Effect.fnUntraced(function* (handlers) {
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const orchestrationEngine = yield* OrchestrationEngineService;
    const piExternalSource = yield* Effect.serviceOption(PiExternalThreadSource);

    return handlers
      .handle(
        "shellSnapshot",
        Effect.fn("environment.orchestration.shellSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* projectionSnapshotQuery
            .getShellSnapshot()
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_snapshot_failed", cause),
              ),
            );
        }),
      )
      .handle(
        "threadSnapshot",
        Effect.fn("environment.orchestration.threadSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const snapshot = yield* getClientThreadDetailSnapshot(
            args.params.threadId,
            piExternalSource,
            projectionSnapshotQuery,
          ).pipe(
            Effect.catch((cause) =>
              Effect.gen(function* () {
                if (cause.code === "thread_not_found") {
                  return yield* failEnvironmentNotFound("thread_not_found");
                }
                return yield* failEnvironmentInternal(
                  "orchestration_thread_snapshot_failed",
                  cause,
                );
              }),
            ),
          );
          if (Option.isNone(snapshot)) {
            return yield* failEnvironmentNotFound("thread_not_found");
          }
          return snapshot.value;
        }),
      )
      .handle(
        "legacyThreadSnapshot",
        Effect.fn("environment.orchestration.legacyThreadSnapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const snapshot = yield* getLegacyClientThreadDetailSnapshot(
            args.params.threadId,
            piExternalSource,
            projectionSnapshotQuery,
          ).pipe(
            Effect.catch((cause) =>
              Effect.gen(function* () {
                if (cause.code === "thread_not_found") {
                  return yield* failEnvironmentNotFound("thread_not_found");
                }
                return yield* failEnvironmentInternal(
                  "orchestration_thread_snapshot_failed",
                  cause,
                );
              }),
            ),
          );
          if (Option.isNone(snapshot)) {
            return yield* failEnvironmentNotFound("thread_not_found");
          }
          return snapshot.value;
        }),
      )
      .handle(
        "threadActivitiesPage",
        Effect.fn("environment.orchestration.threadActivitiesPage")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          const page = yield* getClientThreadActivityPage(
            args.params.threadId,
            args.payload,
            piExternalSource,
            projectionSnapshotQuery,
          ).pipe(
            Effect.catch((cause) =>
              Effect.gen(function* () {
                if (cause.code === "thread_not_found") {
                  return yield* failEnvironmentNotFound("thread_not_found");
                }
                return yield* failEnvironmentInternal(
                  "orchestration_thread_snapshot_failed",
                  cause,
                );
              }),
            ),
          );
          if (Option.isNone(page)) {
            return yield* failEnvironmentNotFound("thread_not_found");
          }
          return page.value;
        }),
      )
      .handle(
        "dispatch",
        Effect.fn("environment.orchestration.dispatch")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          const externalDispatch = getExternalThreadDispatch(args.payload, piExternalSource);
          if (externalDispatch !== null) {
            return yield* externalDispatch.pipe(
              Effect.catch((cause) =>
                Effect.gen(function* () {
                  if (cause.code === "thread_not_found") {
                    return yield* failEnvironmentNotFound("thread_not_found");
                  }
                  if (externalInvalidRequestCodes.has(cause.code ?? "")) {
                    return yield* failEnvironmentInvalidRequest("invalid_command");
                  }
                  return yield* failEnvironmentInternal("orchestration_dispatch_failed", cause);
                }),
              ),
            );
          }
          const normalizedCommand = yield* normalizeDispatchCommand(args.payload).pipe(
            Effect.catch(() => failEnvironmentInvalidRequest("invalid_command")),
          );
          return yield* orchestrationEngine
            .dispatch(normalizedCommand)
            .pipe(
              Effect.catch((cause) =>
                failEnvironmentInternal("orchestration_dispatch_failed", cause),
              ),
            );
        }),
      );
  }),
);
