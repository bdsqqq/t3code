import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  SourceControlProviderKind,
  ThreadId,
  PullRequestUnavailableError,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ServerCommand,
  type PullRequestSummary,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";
import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ThreadSnoozeService from "./ThreadSnoozeService.ts";
import { isSnoozed, isAutoSettlementCandidate } from "./ThreadSettlementService.ts";

const now = DateTime.makeUnsafe("2026-10-01T00:00:00.000Z");
const prCondition = {
  type: "pull-request-merged" as const,
  host: "github.com",
  repository: "arbitrary/repo",
  number: 42,
};
const makeThread = (
  id: string,
  patch: Partial<OrchestrationV2AppThread> = {},
): OrchestrationV2AppThread => ({
  id: ThreadId.make(id),
  projectId: ProjectId.make("project:1"),
  title: id,
  createdBy: "user",
  creationSource: "web",
  providerInstanceId: ProviderInstanceId.make("codex"),
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: ThreadId.make(id) },
  forkedFrom: null,
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  deletedAt: null,
  settledAt: null,
  settledOverride: null,
  lastVisitedAt: null,
  snoozedUntil: null,
  snoozedAt: now,
  snoozeCondition: prCondition,
  snoozeCommandId: CommandId.make(`snooze:${id}`),
  ...patch,
});
const shell = (thread: OrchestrationV2AppThread) =>
  ProjectionStore.threadShellFromProjection(
    ProjectionStore.emptyProjection({
      id: EventId.make(`create:${thread.id}`),
      type: "thread.created",
      threadId: thread.id,
      occurredAt: now,
      payload: thread,
    }),
  );

const harness = Effect.fn("ThreadSnoozeTest.harness")(function* (
  initial: ReadonlyArray<OrchestrationV2AppThread>,
  summary: PullRequestService.PullRequestService["Service"]["summary"],
  mergeEvents?: Stream.Stream<PullRequestService.PullRequestMergeEvent>,
) {
  const threads = yield* Ref.make(initial);
  const events = yield* PubSub.unbounded<OrchestrationV2DomainEvent>();
  const merges = yield* PubSub.unbounded<PullRequestService.PullRequestMergeEvent>();
  const reads = yield* Queue.unbounded<void>();
  const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2ServerCommand>>([]);
  const dependencies = Layer.mergeAll(
    Layer.mock(Orchestrator.OrchestratorV2)({
      streamDomainEvents: Stream.fromPubSub(events),
      dispatch: (command) =>
        Ref.update(commands, (all) => [...all, command]).pipe(
          Effect.as({ sequence: 1, storedEvents: [] }),
        ),
    }),
    Layer.mock(ProjectionStore.ProjectionStoreV2)({
      getConditionalSnoozeCandidates: (id) =>
        Queue.offer(reads, undefined).pipe(
          Effect.andThen(Ref.get(threads)),
          Effect.map((all) =>
            all.filter(
              (t) =>
                t.snoozeCondition != null &&
                t.archivedAt === null &&
                (id === undefined ||
                  t.id === id ||
                  (t.snoozeCondition.type === "thread-settled" &&
                    t.snoozeCondition.threadId === id)),
            ),
          ),
        ),
      getThreadShell: (id) =>
        Ref.get(threads).pipe(
          Effect.map((all) => {
            const thread = all.find((t) => t.id === id);
            return thread === undefined ? null : shell(thread);
          }),
        ),
    }),
    Layer.mock(PullRequestService.PullRequestService)({
      summary,
      subscribeMerges: mergeEvents
        ? Effect.succeed(mergeEvents)
        : PubSub.subscribe(merges).pipe(Effect.map(Stream.fromSubscription)),
    }),
    Layer.succeed(
      Crypto.Crypto,
      Crypto.make({
        randomBytes: (size) => new Uint8Array(size).fill(1),
        digest: (_algorithm, data) => Effect.succeed(data),
      }),
    ),
  );
  const service = yield* ThreadSnoozeService.make.pipe(Effect.provide(dependencies));
  yield* service.start();
  const drainSweep = Queue.take(reads).pipe(Effect.andThen(service.drain));
  yield* drainSweep;
  return { threads, events, merges, commands, drainSweep };
});

const summaryFor =
  (
    state: PullRequestSummary["state"],
  ): PullRequestService.PullRequestService["Service"]["summary"] =>
  (input) =>
    Effect.succeed({
      provider: SourceControlProviderKind.make("github"),
      projectId: input.projectId,
      repository: input.repository,
      number: input.number,
      state,
      title: "PR",
      url: "https://github.com/arbitrary/repo/pull/42",
      headBranch: "feature",
      baseBranch: "main",
      updatedAt: DateTime.formatIso(now),
    });

it.effect.each(["open", "closed", "merged"] as const)(
  "startup recovers arbitrary PR snoozes only when %s is merged",
  (state) =>
    Effect.gen(function* () {
      let calls = 0;
      const summary = summaryFor(state);
      const h = yield* harness(
        [makeThread("a", { pinnedAt: now }), makeThread("b", { autoSettleDisabledAt: now })],
        (input, options) => {
          calls++;
          assert.equal(input.host, "github.com");
          assert.equal(input.allowStale, false);
          assert.equal(options?.recoverTransientFailure, false);
          return summary(input, options);
        },
      );
      assert.equal(calls, 1);
      assert.equal((yield* Ref.get(h.commands)).length, state === "merged" ? 2 : 0);
    }).pipe(Effect.scoped),
);

it.effect("retains snoozes on PR failures and retries on the next periodic sweep", () =>
  Effect.gen(function* () {
    let fail = true;
    const h = yield* harness([makeThread("a")], (input) =>
      fail
        ? Effect.fail(new PullRequestUnavailableError({ reason: "provider-unsupported" }))
        : summaryFor("merged")(input),
    );
    assert.equal((yield* Ref.get(h.commands)).length, 0);
    fail = false;
    yield* TestClock.adjust("1 minute");
    yield* h.drainSweep;
    assert.equal((yield* Ref.get(h.commands)).length, 1);
  }).pipe(Effect.scoped),
);

it.effect("coalesces distinct merge notifications while a PR check is in flight", () =>
  Effect.gen(function* () {
    const publishMerges = yield* Deferred.make<void>();
    const mergesDelivered = yield* Deferred.make<void>();
    const lookupStarted = yield* Deferred.make<void>();
    const releaseLookup = yield* Deferred.make<void>();
    const notification = (number: number) => ({
      projectId: ProjectId.make("project:1"),
      repository: "other/repo",
      number,
      mergedAt: DateTime.formatIso(now),
    });
    const mergeEvents = Stream.fromEffect(Deferred.await(publishMerges)).pipe(
      Stream.map(() => notification(1)),
      // Deliver the rest only after the first notification's lookup is blocked.
      Stream.concat(
        Stream.fromEffect(Deferred.await(lookupStarted)).pipe(
          Stream.flatMap(() => Stream.fromIterable([notification(2), notification(3)])),
        ),
      ),
      Stream.concat(
        Stream.fromEffect(Deferred.succeed(mergesDelivered, undefined)).pipe(Stream.drain),
      ),
    );
    let calls = 0;
    let merged = false;
    const h = yield* harness(
      [makeThread("a")],
      (input) =>
        Effect.gen(function* () {
          calls++;
          if (merged) {
            yield* Deferred.succeed(lookupStarted, undefined);
            yield* Deferred.await(releaseLookup);
          }
          return yield* summaryFor(merged ? "merged" : "open")(input);
        }),
      mergeEvents,
    );
    assert.equal(calls, 1);
    merged = true;
    yield* Deferred.succeed(publishMerges, undefined);
    yield* Deferred.await(mergesDelivered);
    yield* Deferred.succeed(releaseLookup, undefined);
    yield* h.drainSweep;
    assert.equal(calls, 3); // Startup, the in-flight check, and one coalesced follow-up.
    const commands = yield* Ref.get(h.commands);
    assert.equal(commands.length, 2);
    assert.isTrue(commands.every((command) => command.type === "thread.snooze.wake"));
  }).pipe(Effect.scoped),
);

it.effect("a broken project route does not block another project's matching PR snooze", () =>
  Effect.gen(function* () {
    const h = yield* harness(
      [makeThread("broken"), makeThread("valid", { projectId: ProjectId.make("project:2") })],
      (input) =>
        input.projectId === ProjectId.make("project:1")
          ? Effect.fail(new PullRequestUnavailableError({ reason: "provider-unsupported" }))
          : summaryFor("merged")(input),
    );
    const commands = yield* Ref.get(h.commands);
    assert.equal(commands.length, 1);
    assert.deepEqual(
      commands.map((command) => ("threadId" in command ? command.threadId : null)),
      [ThreadId.make("valid")],
    );
  }).pipe(Effect.scoped),
);

it.effect.each(["settled", "archived", "missing"] as const)(
  "wakes on startup when a dependency is %s",
  (targetState) =>
    Effect.gen(function* () {
      const a = makeThread("a", {
        snoozeCondition: { type: "thread-settled", threadId: ThreadId.make("target") },
      });
      const target = makeThread("target", {
        snoozeCondition: null,
        snoozeCommandId: null,
        settledOverride: targetState === "settled" ? "settled" : null,
        archivedAt: targetState === "archived" ? now : null,
      });
      const h = yield* harness(targetState === "missing" ? [a] : [a, target], () =>
        Effect.die("thread dependency needs no PR lookup"),
      );
      assert.equal((yield* Ref.get(h.commands)).length, 1);
    }).pipe(Effect.scoped),
);

it.effect("settlement events wake dependents without starting agent turns", () =>
  Effect.gen(function* () {
    const target = makeThread("target", { snoozeCondition: null });
    const a = makeThread("a", { snoozeCondition: { type: "thread-settled", threadId: target.id } });
    const h = yield* harness([a, target], () => Effect.die("no PR"));
    assert.equal((yield* Ref.get(h.commands)).length, 0);
    const settled = { ...target, settledOverride: "settled" as const, settledAt: now };
    yield* Ref.set(h.threads, [a, settled]);
    yield* PubSub.publish(h.events, {
      id: EventId.make("settled:event"),
      type: "thread.settled",
      threadId: target.id,
      occurredAt: now,
      payload: settled,
    });
    yield* h.drainSweep;
    assert.deepEqual(
      (yield* Ref.get(h.commands)).map((command) => command.type),
      ["thread.snooze.wake"],
    );
  }).pipe(Effect.scoped),
);

it("conditional snoozes park timerless threads and prevent inactivity settlement", () => {
  const parked = shell(makeThread("a"));
  assert.isTrue(isSnoozed(parked, DateTime.toEpochMillis(now)));
  assert.isFalse(isAutoSettlementCandidate(parked, DateTime.toEpochMillis(now)));
  assert.isFalse(
    isSnoozed(
      { ...parked, status: "completed", latestRunCompletedAt: DateTime.add(now, { minutes: 1 }) },
      DateTime.toEpochMillis(now),
    ),
  );
});
