import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  TurnId,
  ProviderInstanceId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";

import { OrchestrationCommandInvariantError } from "../../orchestration/Errors.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../../orchestration/Services/OrchestrationEngine.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import { ProviderSessionReaper } from "../Services/ProviderSessionReaper.ts";
import { ProviderService, type ProviderServiceShape } from "../Services/ProviderService.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";
import { makeProviderSessionReaperLive } from "./ProviderSessionReaper.ts";

const defaultModelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5-codex",
} as const;

const unsupported = () => Effect.die(new Error("Unsupported provider call in test")) as never;

function makeReadModel(
  threads: ReadonlyArray<{
    readonly id: ThreadId;
    readonly session: {
      readonly threadId: ThreadId;
      readonly status: "starting" | "running" | "ready" | "interrupted" | "stopped" | "error";
      readonly providerName: "codex" | "claudeAgent" | "pi";
      readonly runtimeMode: "approval-required" | "full-access" | "auto-accept-edits";
      readonly activeTurnId: TurnId | null;
      readonly lastError: string | null;
      readonly updatedAt: string;
    } | null;
  }>,
) {
  const now = "2026-01-01T00:00:00.000Z";
  const projectId = ProjectId.make("project-provider-session-reaper");

  return {
    snapshotSequence: 0,
    updatedAt: now,
    projects: [
      {
        id: projectId,
        title: "Provider Reaper Project",
        workspaceRoot: "/tmp/provider-reaper-project",
        defaultModelSelection,
        scripts: [],
        createdAt: now,
        updatedAt: now,
        deletedAt: null,
      },
    ],
    threads: threads.map((thread) => ({
      id: thread.id,
      projectId,
      title: `Thread ${thread.id}`,
      modelSelection: defaultModelSelection,
      interactionMode: "default" as const,
      runtimeMode: "full-access" as const,
      branch: null,
      worktreePath: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      latestUserMessageAt: null,
      hasPendingApprovals: false,
      hasPendingUserInput: false,
      hasActionableProposedPlan: false,
      latestTurn: null,
      messages: [],
      session: thread.session,
      activities: [],
      proposedPlans: [],
      checkpoints: [],
      deletedAt: null,
    })),
  };
}

describe("ProviderSessionReaper", () => {
  function createHarness(input: {
    readonly readModel: ReturnType<typeof makeReadModel>;
    readonly dispatchImplementation?: OrchestrationEngineShape["dispatch"];
    readonly listSessionsImplementation?: ProviderServiceShape["listSessions"];
  }) {
    const stoppedThreadIds = new Set<ThreadId>();
    const dispatch = vi.fn<OrchestrationEngineShape["dispatch"]>(
      input.dispatchImplementation ??
        ((command) =>
          Effect.sync(() => {
            if (command.type === "thread.session.stop") {
              stoppedThreadIds.add(command.threadId);
            }
            return { sequence: 1 };
          })),
    );

    const orchestrationEngine: OrchestrationEngineShape = {
      dispatch,
      readEvents: () => Stream.empty,
      streamDomainEvents: Stream.empty,
      latestSequence: Effect.succeed(0),
    };
    const providerService: ProviderServiceShape = {
      startSession: () => unsupported(),
      sendTurn: () => unsupported(),
      interruptTurn: () => unsupported(),
      respondToRequest: () => unsupported(),
      respondToUserInput: () => unsupported(),
      stopSession: () => unsupported(),
      listSessions: input.listSessionsImplementation ?? (() => Effect.succeed([])),
      getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
      getInstanceInfo: (instanceId) => {
        const driverKind = ProviderDriverKind.make(String(instanceId));
        return Effect.succeed({
          instanceId,
          driverKind,
          displayName: undefined,
          enabled: true,
          continuationIdentity: {
            driverKind,
            continuationKey: `${driverKind}:instance:${instanceId}`,
          },
        });
      },
      rollbackConversation: () => unsupported(),
      streamEvents: Stream.empty,
    };

    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const providerSessionDirectoryLayer = ProviderSessionDirectoryLive.pipe(
      Layer.provide(runtimeRepositoryLayer),
    );
    const layer = makeProviderSessionReaperLive({
      inactivityThresholdMs: 1_000,
      sweepIntervalMs: 60_000,
    }).pipe(
      Layer.provideMerge(providerSessionDirectoryLayer),
      Layer.provideMerge(runtimeRepositoryLayer),
      Layer.provideMerge(Layer.succeed(OrchestrationEngineService, orchestrationEngine)),
      Layer.provideMerge(Layer.succeed(ProviderService, providerService)),
      Layer.provideMerge(
        Layer.succeed(ProjectionSnapshotQuery, {
          getCommandReadModel: () => Effect.die("unused"),
          getSnapshot: () => Effect.die("unused"),
          getShellSnapshot: () => Effect.die("unused"),
          getArchivedShellSnapshot: () => Effect.die("unused"),
          getSnapshotSequence: () =>
            Effect.succeed({ snapshotSequence: input.readModel.snapshotSequence }),
          getCounts: () => Effect.die("unused"),
          getActiveProjectByWorkspaceRoot: () => Effect.die("unused"),
          getProjectShellById: () => Effect.die("unused"),
          getFirstActiveThreadIdByProjectId: () => Effect.die("unused"),
          getThreadCheckpointContext: () => Effect.die("unused"),
          getFullThreadDiffContext: () => Effect.die("unused"),
          getThreadShellById: (threadId) =>
            Effect.succeed(
              input.readModel.threads.find((thread) => thread.id === threadId)
                ? Option.some(input.readModel.threads.find((thread) => thread.id === threadId)!)
                : Option.none(),
            ),
          getThreadDetailById: () => Effect.die("unused"),
          getThreadDetailSnapshot: () => Effect.die("unused"),
          getThreadDetailPageSnapshot: () => Effect.die("unused"),
          getThreadActivityPage: () => Effect.die("unused"),
          searchThreads: () => Effect.succeed({ matches: [] }),
        }),
      ),
      Layer.provideMerge(NodeServices.layer),
    );

    return { dispatch, stoppedThreadIds, layer };
  }

  it.effect("reaps stale persisted sessions without active turns", () => {
    const threadId = ThreadId.make("thread-reaper-stale");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    return Effect.gen(function* () {
      const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      yield* repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "1900-01-01T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-stale",
        },
        runtimePayload: null,
      });
      const reaper = yield* ProviderSessionReaper;
      yield* reaper.start();

      assert.strictEqual(harness.dispatch.mock.calls.length, 1);
      const command = harness.dispatch.mock.calls[0]?.[0];
      assert.strictEqual(command?.type, "thread.session.stop");
      assert.strictEqual(command && "threadId" in command ? command.threadId : null, threadId);
      assert.isTrue(harness.stoppedThreadIds.has(threadId));
    }).pipe(Effect.provide(harness.layer), Effect.scoped);
  });

  it.effect("reaps startup active turns that have no live process", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-reaper-active-turn");
      const turnId = TurnId.make("turn-reaper-active");
      const now = DateTime.formatIso(yield* DateTime.now);
      const harness = createHarness({
        readModel: makeReadModel([
          {
            id: threadId,
            session: {
              threadId,
              status: "running",
              providerName: "claudeAgent",
              runtimeMode: "full-access",
              activeTurnId: turnId,
              lastError: null,
              updatedAt: now,
            },
          },
        ]),
      });
      yield* Effect.gen(function* () {
        const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
        yield* repository.upsert({
          threadId,
          providerName: "claudeAgent",
          providerInstanceId: null,
          adapterKey: "claudeAgent",
          runtimeMode: "full-access",
          status: "running",
          lastSeenAt: now,
          resumeCursor: {
            opaque: "resume-active-turn",
          },
          runtimePayload: null,
        });
        const reaper = yield* ProviderSessionReaper;
        yield* reaper.start();

        assert.strictEqual(harness.dispatch.mock.calls.length, 1);
        const command = harness.dispatch.mock.calls[0]?.[0];
        assert.strictEqual(command?.type, "thread.session.stop");
        assert.strictEqual(command && "threadId" in command ? command.threadId : null, threadId);
        assert.isTrue(Option.isSome(yield* repository.getByThreadId({ threadId })));

        yield* TestClock.adjust(60_000);
        assert.strictEqual(harness.dispatch.mock.calls.length, 2);
        yield* TestClock.adjust(60_000);
        assert.strictEqual(harness.dispatch.mock.calls.length, 3);
      }).pipe(Effect.provide(harness.layer), Effect.scoped);
    }),
  );

  it.effect("keeps stale sessions when their active turn still has a live process", () => {
    const threadId = ThreadId.make("thread-reaper-live-active-turn");
    const turnId = TurnId.make("turn-reaper-live-active");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "running",
            providerName: "pi",
            runtimeMode: "full-access",
            activeTurnId: turnId,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
      listSessionsImplementation: () =>
        Effect.succeed([
          {
            provider: ProviderDriverKind.make("pi"),
            providerInstanceId: ProviderInstanceId.make("pi"),
            status: "running",
            runtimeMode: "full-access",
            threadId,
            activeTurnId: turnId,
            createdAt: now,
            updatedAt: now,
          },
        ]),
    });
    return Effect.gen(function* () {
      const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      yield* repository.upsert({
        threadId,
        providerName: "pi",
        providerInstanceId: ProviderInstanceId.make("pi"),
        adapterKey: "pi",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "1900-01-01T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-live-active-turn",
        },
        runtimePayload: null,
      });
      const reaper = yield* ProviderSessionReaper;
      yield* reaper.start();

      assert.strictEqual(harness.dispatch.mock.calls.length, 0);
      assert.isTrue(Option.isSome(yield* repository.getByThreadId({ threadId })));
    }).pipe(Effect.provide(harness.layer), Effect.scoped);
  });

  it.effect("does not reap sessions that are still within the inactivity threshold", () =>
    Effect.gen(function* () {
      const threadId = ThreadId.make("thread-reaper-fresh");
      const now = DateTime.formatIso(yield* DateTime.now);
      const harness = createHarness({
        readModel: makeReadModel([
          {
            id: threadId,
            session: {
              threadId,
              status: "ready",
              providerName: "claudeAgent",
              runtimeMode: "full-access",
              activeTurnId: null,
              lastError: null,
              updatedAt: now,
            },
          },
        ]),
      });
      yield* Effect.gen(function* () {
        const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
        yield* repository.upsert({
          threadId,
          providerName: "claudeAgent",
          providerInstanceId: null,
          adapterKey: "claudeAgent",
          runtimeMode: "full-access",
          status: "running",
          lastSeenAt: now,
          resumeCursor: {
            opaque: "resume-fresh",
          },
          runtimePayload: null,
        });
        const reaper = yield* ProviderSessionReaper;
        yield* reaper.start();

        assert.strictEqual(harness.dispatch.mock.calls.length, 0);
        assert.isTrue(Option.isSome(yield* repository.getByThreadId({ threadId })));
      }).pipe(Effect.provide(harness.layer), Effect.scoped);
    }),
  );

  it.effect("skips persisted sessions that are already marked stopped", () => {
    const threadId = ThreadId.make("thread-reaper-stopped");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = createHarness({
      readModel: makeReadModel([
        {
          id: threadId,
          session: {
            threadId,
            status: "stopped",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
    });
    return Effect.gen(function* () {
      const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      yield* repository.upsert({
        threadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "stopped",
        lastSeenAt: "1900-01-01T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-stopped",
        },
        runtimePayload: null,
      });
      const reaper = yield* ProviderSessionReaper;
      yield* reaper.start();

      assert.strictEqual(harness.dispatch.mock.calls.length, 0);
      assert.isTrue(Option.isSome(yield* repository.getByThreadId({ threadId })));
    }).pipe(Effect.provide(harness.layer), Effect.scoped);
  });

  it.effect("continues reaping other sessions when one stop attempt fails", () => {
    const failedThreadId = ThreadId.make("thread-reaper-stop-failure");
    const reapedThreadId = ThreadId.make("thread-reaper-stop-success");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = createHarness({
      readModel: makeReadModel([
        {
          id: failedThreadId,
          session: {
            threadId: failedThreadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
        {
          id: reapedThreadId,
          session: {
            threadId: reapedThreadId,
            status: "ready",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
      dispatchImplementation: (command) =>
        command.type === "thread.session.stop" && command.threadId === failedThreadId
          ? Effect.fail(
              new OrchestrationCommandInvariantError({
                commandType: command.type,
                detail: "simulated dispatch failure",
              }),
            )
          : Effect.succeed({ sequence: 1 }),
    });
    return Effect.gen(function* () {
      const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      yield* repository.upsert({
        threadId: failedThreadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "1900-01-01T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-failure",
        },
        runtimePayload: null,
      });
      yield* repository.upsert({
        threadId: reapedThreadId,
        providerName: "codex",
        providerInstanceId: null,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "1900-01-01T00:01:00.000Z",
        resumeCursor: {
          opaque: "resume-success",
        },
        runtimePayload: null,
      });
      const reaper = yield* ProviderSessionReaper;
      yield* reaper.start();

      assert.deepStrictEqual(
        harness.dispatch.mock.calls.map(([command]) =>
          command.type === "thread.session.stop" ? command.threadId : null,
        ),
        [failedThreadId, reapedThreadId],
      );
    }).pipe(Effect.provide(harness.layer), Effect.scoped);
  });

  it.effect("continues reaping other sessions when one stop attempt defects", () => {
    const defectThreadId = ThreadId.make("thread-reaper-stop-defect");
    const reapedThreadId = ThreadId.make("thread-reaper-stop-after-defect");
    const now = "2026-01-01T00:00:00.000Z";
    const harness = createHarness({
      readModel: makeReadModel([
        {
          id: defectThreadId,
          session: {
            threadId: defectThreadId,
            status: "ready",
            providerName: "claudeAgent",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
        {
          id: reapedThreadId,
          session: {
            threadId: reapedThreadId,
            status: "ready",
            providerName: "codex",
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: now,
          },
        },
      ]),
      dispatchImplementation: (command) =>
        command.type === "thread.session.stop" && command.threadId === defectThreadId
          ? Effect.die(new Error("simulated stop defect"))
          : Effect.succeed({ sequence: 1 }),
    });
    return Effect.gen(function* () {
      const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      yield* repository.upsert({
        threadId: defectThreadId,
        providerName: "claudeAgent",
        providerInstanceId: null,
        adapterKey: "claudeAgent",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "1900-01-01T00:00:00.000Z",
        resumeCursor: {
          opaque: "resume-defect",
        },
        runtimePayload: null,
      });
      yield* repository.upsert({
        threadId: reapedThreadId,
        providerName: "codex",
        providerInstanceId: null,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "1900-01-01T00:01:00.000Z",
        resumeCursor: {
          opaque: "resume-after-defect",
        },
        runtimePayload: null,
      });
      const reaper = yield* ProviderSessionReaper;
      yield* reaper.start();

      assert.deepStrictEqual(
        harness.dispatch.mock.calls.map(([command]) =>
          command.type === "thread.session.stop" ? command.threadId : null,
        ),
        [defectThreadId, reapedThreadId],
      );
    }).pipe(Effect.provide(harness.layer), Effect.scoped);
  });
});
