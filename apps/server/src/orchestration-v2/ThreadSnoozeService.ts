import { CommandId, type ThreadId } from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import { normalizeThreadPullRequestKey } from "@t3tools/shared/threadPullRequests";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import { forkParked } from "../serverActivation.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import { isSnoozed } from "./ThreadSettlementService.ts";

export class ThreadSnoozeService extends Context.Service<
  ThreadSnoozeService,
  {
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    readonly drain: Effect.Effect<void>;
  }
>()("t3/orchestration-v2/ThreadSnoozeService") {}

export const make = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const pullRequests = yield* PullRequestService.PullRequestService;
  const crypto = yield* Crypto.Crypto;

  type Sweep = {
    readonly threadId?: ThreadId;
    readonly checkPr?: boolean;
  };
  const sweep = Effect.fn("ThreadSnoozeService.sweep")(function* (request: Sweep) {
    const candidates = yield* projections.getConditionalSnoozeCandidates(request.threadId);
    const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
    const wake = Effect.fn("ThreadSnoozeService.wake")(function* (
      thread: (typeof candidates)[number],
    ) {
      if (thread.snoozeCommandId == null) return;
      yield* orchestrator.dispatch({
        type: "thread.snooze.wake",
        commandId: CommandId.make(`server:snooze-wake:${yield* crypto.randomUUIDv4()}`),
        threadId: thread.id,
        expectedSnoozeCommandId: thread.snoozeCommandId,
      });
    });
    const prCandidates: Array<(typeof candidates)[number]> = [];
    yield* Effect.forEach(
      candidates,
      (thread) =>
        Effect.gen(function* () {
          const condition = thread.snoozeCondition;
          if (condition == null) return;
          const shell = yield* projections.getThreadShell(thread.id);
          if (shell === null) return;
          if (!isSnoozed(shell, nowMs)) {
            yield* wake(thread);
            return;
          }
          if (condition.type === "thread-settled") {
            const target = yield* projections.getThreadShell(condition.threadId);
            // A deleted or archived dependency can no longer settle: fail open.
            if (
              target === null ||
              target.archivedAt !== null ||
              target.settledOverride === "settled"
            ) {
              yield* wake(thread);
            }
          } else if (request.checkPr) {
            prCandidates.push(thread);
          }
        }).pipe(
          Effect.catch((cause) =>
            Effect.logWarning("conditional thread snooze check failed", {
              threadId: thread.id,
              cause,
            }),
          ),
        ),
      { concurrency: 8, discard: true },
    );

    // A project's checkout and credentials route the lookup; one broken route must not
    // block another project's snoozes. Same-route PRs still share one read.
    const groups = Map.groupBy(prCandidates, (thread) => {
      const condition = thread.snoozeCondition;
      return condition?.type === "pull-request-merged"
        ? JSON.stringify([thread.projectId, normalizeThreadPullRequestKey(condition)])
        : thread.id;
    });
    yield* Effect.forEach(
      groups.values(),
      (group) =>
        Effect.gen(function* () {
          const thread = group[0]!;
          const condition = thread.snoozeCondition;
          if (condition?.type !== "pull-request-merged") return;
          const summary = yield* pullRequests.summary(
            {
              projectId: thread.projectId,
              host: condition.host,
              repository: condition.repository,
              number: condition.number,
              allowStale: false,
            },
            { recoverTransientFailure: false },
          );
          // Closed without merge, not found, auth and transient errors never spend the snooze.
          if (summary.state === "merged") {
            yield* Effect.forEach(group, wake, { concurrency: 8, discard: true });
          }
        }).pipe(
          Effect.timeout("30 seconds"),
          Effect.catch((cause) =>
            Effect.logWarning("conditional snooze pull request lookup failed", { cause }),
          ),
        ),
      { concurrency: 4, discard: true },
    );
  });

  const pending = yield* Ref.make(new Set<string>());
  // Remove before processing so events that arrive during a read get a fresh pass.
  const queue = yield* makeDrainableWorker((request: Sweep) =>
    Ref.update(pending, (keys) => {
      const next = new Set(keys);
      next.delete(JSON.stringify(request));
      return next;
    }).pipe(Effect.andThen(sweep(request))),
  );
  const requestSweep = (request: Sweep) => {
    const key = JSON.stringify(request);
    return Ref.modify(pending, (keys) =>
      keys.has(key) ? [false, keys] : [true, new Set([...keys, key])],
    ).pipe(Effect.flatMap((added) => (added ? queue.enqueue(request) : Effect.void)));
  };

  const start = Effect.fn("ThreadSnoozeService.start")(function* () {
    const merges = yield* pullRequests.subscribeMerges;
    yield* forkParked(
      Stream.runForEach(orchestrator.streamDomainEvents, (event) => {
        switch (event.type) {
          case "thread.snoozed":
            return requestSweep({ threadId: event.threadId, checkPr: true });
          case "thread.settled":
          case "thread.archived":
          case "thread.deleted":
          case "runtime-request.updated":
            return requestSweep({ threadId: event.threadId });
          case "run.updated":
            return event.payload.status === "failed"
              ? requestSweep({ threadId: event.threadId })
              : Effect.void;
          default:
            return Effect.void;
        }
      }),
    );
    yield* forkParked(Stream.runForEach(merges, () => requestSweep({ checkPr: true })));
    yield* forkParked(
      requestSweep({ checkPr: true }).pipe(
        Effect.andThen(queue.drain),
        Effect.repeat(Schedule.spaced("1 minute")),
        Effect.asVoid,
      ),
    );
  });
  return { start, drain: queue.drain } satisfies ThreadSnoozeService["Service"];
});

export const layer = Layer.effect(ThreadSnoozeService, make);
