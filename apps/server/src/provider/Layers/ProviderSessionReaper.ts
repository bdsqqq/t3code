import { CommandId, type ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";

import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import {
  ProviderSessionReaper,
  type ProviderSessionReaperShape,
} from "../Services/ProviderSessionReaper.ts";
import { ProviderService } from "../Services/ProviderService.ts";

const DEFAULT_INACTIVITY_THRESHOLD_MS = 30 * 60 * 1000;
const DEFAULT_SWEEP_INTERVAL_MS = 5 * 60 * 1000;

export interface ProviderSessionReaperLiveOptions {
  readonly inactivityThresholdMs?: number;
  readonly sweepIntervalMs?: number;
}

const makeProviderSessionReaper = (options?: ProviderSessionReaperLiveOptions) =>
  Effect.gen(function* () {
    const orchestrationEngine = yield* OrchestrationEngineService;
    const directory = yield* ProviderSessionDirectory;
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
    const providerService = yield* ProviderService;

    const inactivityThresholdMs = Math.max(
      1,
      options?.inactivityThresholdMs ?? DEFAULT_INACTIVITY_THRESHOLD_MS,
    );
    const sweepIntervalMs = Math.max(1, options?.sweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS);
    const startupOrphanThreadIds = new Set<ThreadId>();

    const sweep = (reconcileOrphanedActiveTurns: boolean) =>
      Effect.gen(function* () {
        const bindings = yield* directory.listBindings();
        const liveThreadIds = new Set(
          (yield* providerService.listSessions()).map((session) => session.threadId),
        );
        const now = yield* Clock.currentTimeMillis;
        let reapedCount = 0;

        for (const binding of bindings) {
          if (binding.status === "stopped") {
            startupOrphanThreadIds.delete(binding.threadId);
            continue;
          }

          const lastSeenMs = Date.parse(binding.lastSeenAt);
          if (Number.isNaN(lastSeenMs)) {
            yield* Effect.logWarning("provider.session.reaper.invalid-last-seen", {
              threadId: binding.threadId,
              provider: binding.provider,
              lastSeenAt: binding.lastSeenAt,
            });
            continue;
          }

          const idleDurationMs = now - lastSeenMs;
          const thread = yield* projectionSnapshotQuery
            .getThreadShellById(binding.threadId)
            .pipe(Effect.map(Option.getOrUndefined));
          const hasActiveTurn = thread?.session?.activeTurnId != null;
          if (!hasActiveTurn) {
            startupOrphanThreadIds.delete(binding.threadId);
          } else if (reconcileOrphanedActiveTurns && !liveThreadIds.has(binding.threadId)) {
            startupOrphanThreadIds.add(binding.threadId);
          }
          const isOrphanedActiveTurn =
            (reconcileOrphanedActiveTurns || startupOrphanThreadIds.has(binding.threadId)) &&
            hasActiveTurn &&
            !liveThreadIds.has(binding.threadId);
          if (idleDurationMs < inactivityThresholdMs && !isOrphanedActiveTurn) {
            continue;
          }
          if (hasActiveTurn && !isOrphanedActiveTurn) {
            yield* Effect.logDebug("provider.session.reaper.skipped-active-turn", {
              threadId: binding.threadId,
              activeTurnId: thread.session.activeTurnId,
              idleDurationMs,
            });
            continue;
          }
          if (!thread) {
            yield* Effect.logWarning("provider.session.reaper.missing-thread", {
              threadId: binding.threadId,
              provider: binding.provider,
            });
            continue;
          }

          const createdAt = DateTime.formatIso(yield* DateTime.now);
          const reaped = yield* orchestrationEngine
            .dispatch({
              type: "thread.session.stop",
              commandId: CommandId.make(
                `server:provider-session-reaper:${binding.threadId}:${now}`,
              ),
              threadId: binding.threadId,
              createdAt,
            })
            .pipe(
              Effect.tap(() =>
                Effect.logInfo("provider.session.reaped", {
                  threadId: binding.threadId,
                  provider: binding.provider,
                  idleDurationMs,
                  reason: "inactivity_threshold",
                }),
              ),
              Effect.as(true),
              Effect.catchCause((cause) =>
                Effect.logWarning("provider.session.reaper.stop-failed", {
                  threadId: binding.threadId,
                  provider: binding.provider,
                  idleDurationMs,
                  cause,
                }).pipe(Effect.as(false)),
              ),
            );

          if (reaped) {
            reapedCount += 1;
          }
        }

        if (reapedCount > 0) {
          yield* Effect.logInfo("provider.session.reaper.sweep-complete", {
            reapedCount,
            totalBindings: bindings.length,
          });
        }
      });

    const safeSweep = (reconcileOrphanedActiveTurns: boolean) =>
      sweep(reconcileOrphanedActiveTurns).pipe(
        Effect.catch((error: unknown) =>
          Effect.logWarning("provider.session.reaper.sweep-failed", {
            error,
          }),
        ),
        Effect.catchDefect((defect: unknown) =>
          Effect.logWarning("provider.session.reaper.sweep-defect", {
            defect,
          }),
        ),
      );

    const start: ProviderSessionReaperShape["start"] = () =>
      Effect.gen(function* () {
        yield* safeSweep(true);
        yield* Effect.forkScoped(
          safeSweep(false).pipe(
            Effect.repeat(Schedule.spaced(Duration.millis(sweepIntervalMs))),
            Effect.delay(Duration.millis(sweepIntervalMs)),
          ),
        );

        yield* Effect.logInfo("provider.session.reaper.started", {
          inactivityThresholdMs,
          sweepIntervalMs,
        });
      });

    return {
      start,
    } satisfies ProviderSessionReaperShape;
  });

export const makeProviderSessionReaperLive = (options?: ProviderSessionReaperLiveOptions) =>
  Layer.effect(ProviderSessionReaper, makeProviderSessionReaper(options));

export const ProviderSessionReaperLive = makeProviderSessionReaperLive();
