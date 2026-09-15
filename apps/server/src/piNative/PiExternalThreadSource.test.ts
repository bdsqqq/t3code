import {
  CommandId,
  ComposerContextId,
  MessageId,
  PiNativeRuntimeId,
  PiNativeSessionKey,
  ProjectId,
  ThreadId,
  type ClientOrchestrationCommand,
  type OrchestrationProjectShell,
  type OrchestrationThreadStreamItem,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { describe, expect } from "vite-plus/test";

import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import type { PiExternalLifecycleOverride } from "../persistence/Services/PiExternalLifecycleOverrides.ts";
import { PiExternalLifecycleOverrideRepository } from "../persistence/Services/PiExternalLifecycleOverrides.ts";
import {
  applyPiExternalAutoSettlement,
  boundExternalCatalog,
  catalogUpdateAfterRead,
  CatalogRuntimeAttachmentGate,
  PiExternalThreadSource,
  PiThreadStreamSequenceGate,
  PiSubagentStreamTracker,
  runtimeSnapshotAtSequence,
  runtimeSequenceStable,
  runtimeCatalogSignature,
  isRuntimeLifecycleEvent,
  planPiExternalTurnStart,
  receiptSessionFile,
  shutdownCreatedRuntime,
  subscribePiThreadInvalidations,
  validExternalLifecycleOverride,
} from "./PiExternalThreadSource.ts";
import { projectPiThread } from "./PiSessionProjection.ts";
import { SessionCatalog } from "./SessionCatalog.ts";
import { SupervisorClient } from "./SupervisorClient.ts";
import {
  GUARDED_RESUME_CAPABILITY,
  type SupervisorRuntimeState,
  type SupervisorStreamEvent,
} from "./SupervisorProtocol.ts";

const takeoverRecord = {
  sourceKey: PiNativeSessionKey.make("source-key"),
  threadId: ThreadId.make("external:pi:path:source-key"),
  canonicalFile: "/sessions/session.jsonl",
  sessionId: "session-1",
  cwd: "/workspace",
  title: "session",
  createdAt: "2026-08-06T00:00:00.000Z",
  updatedAt: "2026-08-06T00:01:00.000Z",
  fileSize: 10,
  fileMtimeMs: 20,
  historyTruncation: { truncated: false },
} as const;

const takeoverTurn = (externalResume?: "takeover", streamingBehavior?: "steer" | "followUp") =>
  ({
    type: "thread.turn.start",
    commandId: CommandId.make("takeover-command"),
    threadId: takeoverRecord.threadId,
    message: {
      messageId: MessageId.make("takeover-message"),
      role: "user",
      text: "continue here",
      attachments: [],
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    ...(externalResume === undefined ? {} : { externalResume }),
    ...(streamingBehavior === undefined ? {} : { streamingBehavior }),
    createdAt: "2026-08-06T00:02:00.000Z",
  }) satisfies Extract<ClientOrchestrationCommand, { readonly type: "thread.turn.start" }>;

describe("PiExternalThreadSource hardening", () => {
  for (const mode of ["takeover", "send", "steer", "followUp"] as const) {
    it.effect(
      `projects structured context for native ${mode} without changing admission identity`,
      () =>
        Effect.gen(function* () {
          const commands: unknown[] = [];
          const runtime = {
            runtimeId: PiNativeRuntimeId.make("runtime-1"),
            sessionFile: takeoverRecord.canonicalFile,
            writerKind: "rpc",
            status: mode === "send" ? "idle" : "streaming",
            sequence: 1,
          } satisfies SupervisorRuntimeState;
          const dependencies = Layer.mergeAll(
            Layer.mock(SessionCatalog)({
              list: () => Effect.succeed([takeoverRecord]),
              omittedCount: () => Effect.succeed(0),
              read: () => Effect.succeed({ record: takeoverRecord, entries: [] }),
            }),
            Layer.mock(SupervisorClient)({
              list: () => Effect.succeed(mode === "takeover" ? [] : [runtime]),
              probeCapabilities: () => Effect.succeed({ guardedResume: GUARDED_RESUME_CAPABILITY }),
              subscribe: () => Stream.empty,
              dispatch: (command) => {
                commands.push(command);
                return Effect.succeed({
                  commandId: CommandId.make("takeover-command"),
                  status: "completed",
                });
              },
            }),
            Layer.mock(ProjectionSnapshotQuery)({
              getShellSnapshot: () =>
                Effect.succeed({
                  snapshotSequence: 0,
                  projects: [],
                  threads: [],
                  updatedAt: takeoverRecord.updatedAt,
                }),
            }),
            Layer.mock(PiExternalLifecycleOverrideRepository)({
              list: () => Effect.succeed([]),
            }),
          );
          const turn = takeoverTurn(
            mode === "takeover" ? "takeover" : undefined,
            mode === "steer" || mode === "followUp" ? mode : undefined,
          );
          const command = {
            ...turn,
            message: {
              ...turn.message,
              text: "Inspect [build](t3-context://v1/terminal/build)",
              context: {
                version: 1,
                records: [
                  {
                    version: 1,
                    kind: "terminal",
                    contextId: ComposerContextId.make("build"),
                    label: "build",
                    terminalId: "terminal-1",
                    terminalLabel: "Build",
                    lineStart: 7,
                    lineEnd: 7,
                    text: "compiled successfully",
                  },
                ],
              },
            },
          } satisfies ClientOrchestrationCommand;
          yield* Effect.gen(function* () {
            const source = yield* PiExternalThreadSource;
            yield* source.dispatch(command);
          }).pipe(
            Effect.provide(PiExternalThreadSource.layerTest.pipe(Layer.provide(dependencies))),
          );
          expect(commands).toHaveLength(1);
          expect(commands[0]).toMatchObject({
            type: mode === "takeover" ? "resumeAndSend" : mode,
            commandId: turn.commandId,
            messageId: turn.message.messageId,
            message: expect.stringContaining("[Terminal: build; ref=build]"),
            ...(mode === "takeover"
              ? { streamingBehavior: "steer" }
              : { runtimeId: runtime.runtimeId }),
          });
          expect(commands[0]).toMatchObject({
            message: expect.stringContaining("7 | compiled successfully"),
          });
          expect(command.message.text).toBe("Inspect [build](t3-context://v1/terminal/build)");
        }).pipe(Effect.scoped),
    );
  }

  it("requires explicit takeover confirmation only when no runtime is present", () => {
    expect(
      planPiExternalTurnStart({
        command: takeoverTurn(),
        record: takeoverRecord,
        runtime: undefined,
        guardedResumeSupported: false,
      }),
    ).toEqual({ type: "takeoverConfirmationRequired" });

    const runtime = {
      runtimeId: PiNativeRuntimeId.make("runtime-1"),
      writerKind: "rpc",
      status: "idle",
      sequence: 1,
    } satisfies SupervisorRuntimeState;
    expect(
      planPiExternalTurnStart({
        command: takeoverTurn(),
        record: takeoverRecord,
        runtime,
        guardedResumeSupported: false,
      }),
    ).toEqual({ type: "runtime", runtime });
  });

  it("keeps guarded takeover on one stable resume-and-send payload after runtime appears", () => {
    const runtime = {
      runtimeId: PiNativeRuntimeId.make("runtime-1"),
      writerKind: "rpc",
      status: "idle",
      sequence: 1,
    } satisfies SupervisorRuntimeState;
    const expected = {
      type: "takeover",
      command: {
        type: "resumeAndSend",
        commandId: "takeover-command",
        sessionKey: "source-key",
        sessionFile: "/sessions/session.jsonl",
        cwd: "/workspace",
        message: "continue here",
        messageId: "takeover-message",
        streamingBehavior: "steer",
      },
    } as const;

    expect(
      planPiExternalTurnStart({
        command: takeoverTurn("takeover"),
        record: takeoverRecord,
        runtime: undefined,
        guardedResumeSupported: true,
      }),
    ).toEqual(expected);
    expect(
      planPiExternalTurnStart({
        command: takeoverTurn("takeover"),
        record: takeoverRecord,
        runtime,
        guardedResumeSupported: true,
      }),
    ).toEqual(expected);
    expect(
      planPiExternalTurnStart({
        command: takeoverTurn("takeover", "followUp"),
        record: takeoverRecord,
        runtime,
        guardedResumeSupported: true,
      }),
    ).toMatchObject({ command: { streamingBehavior: "followUp" } });
    expect(
      planPiExternalTurnStart({
        command: takeoverTurn("takeover"),
        record: takeoverRecord,
        runtime,
        guardedResumeSupported: false,
      }),
    ).toEqual({ type: "supervisorUpgradeRequired" });
  });

  it.effect("shuts down a newly created runtime when session cataloging fails", () =>
    Effect.gen(function* () {
      const commands: unknown[] = [];
      yield* shutdownCreatedRuntime(
        {
          dispatch: (command) => {
            commands.push(command);
            return Effect.succeed({
              commandId: CommandId.make("cleanup-receipt"),
              status: "completed",
              runtimeId: PiNativeRuntimeId.make("runtime-1"),
            });
          },
        },
        PiNativeRuntimeId.make("runtime-1"),
      );

      expect(commands).toMatchObject([{ type: "shutdown", runtimeId: "runtime-1" }]);
    }),
  );

  it("uses the latest runtime state for state-bearing stream snapshots", () => {
    const capturedAtSubscription = {
      runtimeId: PiNativeRuntimeId.make("runtime-1"),
      writerKind: "rpc",
      status: "idle",
      sequence: 1,
    } satisfies SupervisorRuntimeState;
    const current = {
      runtimeId: PiNativeRuntimeId.make("runtime-1"),
      writerKind: "rpc",
      status: "streaming",
      sequence: 7,
    } satisfies SupervisorRuntimeState;

    expect(runtimeSnapshotAtSequence(current, 9)).toMatchObject({
      status: "streaming",
      sequence: 9,
    });
    expect(capturedAtSubscription.status).toBe("idle");
    expect(runtimeSequenceStable(capturedAtSubscription, current)).toBe(false);
    expect(runtimeSequenceStable(current, { ...current })).toBe(true);
  });

  it("drops catalog snapshots that race behind newer runtime items", () => {
    const gate = new PiThreadStreamSequenceGate();
    const snapshot = (snapshotSequence: number) =>
      ({
        kind: "snapshot",
        snapshot: { snapshotSequence },
      }) as unknown as OrchestrationThreadStreamItem;
    const event = (sequence: number) =>
      ({
        kind: "event",
        event: { sequence },
      }) as unknown as OrchestrationThreadStreamItem;

    expect(gate.allows(snapshot(5))).toBe(true);
    expect(gate.allows(event(6))).toBe(true);
    expect(gate.allows(snapshot(5))).toBe(false);
    // Equal-sequence authoritative snapshots can still carry lifecycle
    // changes while preserving the same supervisor overlay.
    expect(gate.allows(snapshot(6))).toBe(true);
  });

  it("stops catalog replacements after a runtime attaches", () => {
    const gate = new CatalogRuntimeAttachmentGate();
    expect(gate.allowsCatalogUpdate()).toBe(true);
    gate.attach();
    expect(gate.allowsCatalogUpdate()).toBe(false);
    expect(catalogUpdateAfterRead(gate, { snapshotSequence: 1 })).toBeUndefined();
  });

  it.effect("buffers lifecycle invalidation while a no-runtime thread snapshot loads", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const initialLifecycleReadStarted = yield* Deferred.make<void>();
        const releaseInitialLifecycleRead = yield* Deferred.make<void>();
        const lifecycle = yield* Ref.make<PiExternalLifecycleOverride | undefined>(undefined);
        let blockNextLifecycleRead = true;

        const dependencies = Layer.mergeAll(
          Layer.mock(SessionCatalog)({
            list: () => Effect.succeed([takeoverRecord]),
            omittedCount: () => Effect.succeed(0),
            read: () => Effect.succeed({ record: takeoverRecord, entries: [] }),
            findLifecycleOperation: () => Effect.succeed(undefined),
          }),
          Layer.mock(SupervisorClient)({
            list: () => Effect.succeed([]),
            probeCapabilities: () => Effect.succeed({}),
            subscribe: () => Stream.empty,
          }),
          Layer.mock(ProjectionSnapshotQuery)({
            getShellSnapshot: () =>
              Effect.succeed({
                snapshotSequence: 0,
                projects: [],
                threads: [],
                updatedAt: "2026-08-06T00:00:00.000Z",
              }),
          }),
          Layer.mock(PiExternalLifecycleOverrideRepository)({
            apply: (value) => Ref.set(lifecycle, value).pipe(Effect.as({ applied: true, value })),
            recordReceipt: (value) => Ref.set(lifecycle, value).pipe(Effect.as(value)),
            list: () =>
              Ref.get(lifecycle).pipe(Effect.map((value) => (value === undefined ? [] : [value]))),
            getBySourceKey: () =>
              Effect.gen(function* () {
                const captured = yield* Ref.get(lifecycle);
                if (blockNextLifecycleRead) {
                  blockNextLifecycleRead = false;
                  yield* Deferred.succeed(initialLifecycleReadStarted, undefined);
                  yield* Deferred.await(releaseInitialLifecycleRead);
                }
                return Option.fromNullishOr(captured);
              }),
            getByCommandId: () => Effect.succeed(Option.none()),
          }),
        );

        yield* Effect.gen(function* () {
          const source = yield* PiExternalThreadSource;
          const subscription = yield* source
            .subscribeThread({
              threadId: takeoverRecord.threadId,
              requestCompletionMarker: true,
            })
            .pipe(Stream.take(3), Stream.runCollect, Effect.forkChild);

          yield* Deferred.await(initialLifecycleReadStarted);
          yield* source.dispatch({
            type: "thread.settle",
            commandId: CommandId.make("settle-during-subscribe"),
            threadId: takeoverRecord.threadId,
          });
          yield* Deferred.succeed(releaseInitialLifecycleRead, undefined);

          const items = Array.from(
            yield* Fiber.join(subscription).pipe(Effect.timeout("1 second"), TestClock.withLive),
          );
          expect(items.map((item) => item.kind)).toEqual(["snapshot", "synchronized", "snapshot"]);
          expect(items[0]).toMatchObject({
            kind: "snapshot",
            snapshot: { thread: { settledOverride: null } },
          });
          expect(items[2]).toMatchObject({
            kind: "snapshot",
            snapshot: { thread: { settledOverride: "settled" } },
          });
        }).pipe(Effect.provide(PiExternalThreadSource.layerTest.pipe(Layer.provide(dependencies))));
      }),
    ),
  );

  it.effect("buffers both invalidation channels and releases them on cancellation", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const catalog = yield* PubSub.sliding<void>(1);
        const lifecycle = yield* PubSub.sliding<ThreadId>(16);
        yield* Effect.addFinalizer(() => PubSub.shutdown(catalog));
        yield* Effect.addFinalizer(() => PubSub.shutdown(lifecycle));
        const attached = yield* Deferred.make<void>();
        const worker = yield* Effect.scoped(
          Effect.gen(function* () {
            const changes = yield* subscribePiThreadInvalidations(
              catalog,
              lifecycle,
              takeoverRecord.threadId,
            );
            yield* PubSub.publish(lifecycle, ThreadId.make("unrelated"));
            yield* PubSub.publish(lifecycle, takeoverRecord.threadId);
            yield* PubSub.publish(catalog, undefined);
            // Both publishes precede consumption: lazy subscriptions would lose them.
            let received = 0;
            yield* changes.pipe(
              Stream.runForEach(() => {
                received += 1;
                return received === 2 ? Deferred.succeed(attached, undefined) : Effect.void;
              }),
            );
          }),
        ).pipe(Effect.forkChild);
        yield* Deferred.await(attached).pipe(Effect.timeout("1 second"), TestClock.withLive);
        yield* Fiber.interrupt(worker);
        yield* PubSub.publish(catalog, undefined);
        yield* PubSub.publish(lifecycle, takeoverRecord.threadId);
        expect(yield* PubSub.size(catalog)).toBe(0);
        expect(yield* PubSub.size(lifecycle)).toBe(0);
      }),
    ),
  );

  it("refreshes state for bridge disconnect and reconnect lifecycle events", () => {
    expect(isRuntimeLifecycleEvent("bridge_disconnected")).toBe(true);
    expect(isRuntimeLifecycleEvent("bridge_reconnected")).toBe(true);
    expect(isRuntimeLifecycleEvent("message_update")).toBe(false);
  });

  it("retains sub-agent metadata through a metadata-free terminal event", () => {
    const tracker = new PiSubagentStreamTracker();
    const update = (sequence: number, text: string) =>
      ({
        type: "event",
        runtimeId: PiNativeRuntimeId.make("runtime-1"),
        sequence,
        eventId: `runtime:${sequence}`,
        event: {
          type: "tool_execution_update",
          toolCallId: "delegate-1",
          toolName: "delegate",
          partialResult: {
            content: [{ type: "text", text }],
            details: {
              agent: "delegate",
              task: "Audit auth",
              lifecycle: { status: "running" },
            },
          },
        },
      }) as SupervisorStreamEvent;
    const terminal = {
      type: "event",
      runtimeId: PiNativeRuntimeId.make("runtime-1"),
      sequence: 3,
      eventId: "runtime:3",
      event: {
        type: "tool_execution_end",
        toolCallId: "delegate-1",
        toolName: "delegate",
        result: { content: [{ type: "text", text: "done" }] },
      },
    } as SupervisorStreamEvent;

    expect(tracker.observe(update(1, "starting"))).toEqual({
      snapshot: true,
      retainedEvents: [expect.objectContaining({ eventId: "runtime:1" })],
    });
    expect(tracker.observe(update(2, "still working"))).toEqual({
      snapshot: false,
      retainedEvents: [],
    });
    const decision = tracker.observe(terminal);
    expect(decision.snapshot).toBe(true);
    expect(decision.retainedEvents.map((event) => event.eventId)).toEqual([
      "runtime:2",
      "runtime:3",
    ]);
  });

  it("recovers a created session after its runtime is evicted", () => {
    expect(
      receiptSessionFile({
        commandId: "create-1" as never,
        status: "completed",
        runtimeId: "evicted" as never,
        result: { sessionFile: "/sessions/created.jsonl" },
      }),
    ).toBe("/sessions/created.jsonl");
  });

  it("rebuilds catalog shells only when runtime-visible state changes", () => {
    const runtime = {
      runtimeId: PiNativeRuntimeId.make("runtime-1"),
      sessionFile: "/sessions/one.jsonl",
      writerKind: "rpc",
      status: "streaming",
      sequence: 4,
    } satisfies SupervisorRuntimeState;

    expect(runtimeCatalogSignature([{ ...runtime, sequence: 5 }])).toBe(
      runtimeCatalogSignature([runtime]),
    );
    expect(runtimeCatalogSignature([{ ...runtime, status: "idle" }])).not.toBe(
      runtimeCatalogSignature([runtime]),
    );
  });

  it("settles inactive Pi history using the configured inactivity window", () => {
    const snapshot = projectPiThread({
      record: {
        ...takeoverRecord,
        lastActivityAt: "2026-08-01T00:00:00.000Z",
      },
      entries: [],
      projectId: ProjectId.make("project-1"),
    });

    expect(
      applyPiExternalAutoSettlement({
        snapshot,
        now: "2026-08-06T00:00:00.000Z",
        autoSettleAfterDays: 3,
      }).thread,
    ).toMatchObject({
      settledOverride: "settled",
      settledAt: "2026-08-01T00:00:00.000Z",
    });
    expect(
      applyPiExternalAutoSettlement({
        snapshot,
        now: "2026-08-06T00:00:00.000Z",
        autoSettleAfterDays: null,
      }).thread.settledOverride,
    ).toBeNull();
  });

  it("does not auto-settle active overrides or running Pi sessions", () => {
    const record = {
      ...takeoverRecord,
      lastActivityAt: "2026-08-01T00:00:00.000Z",
    };
    const input = {
      now: "2026-08-06T00:00:00.000Z",
      autoSettleAfterDays: 3,
    } as const;
    const explicitlyActive = projectPiThread({
      record,
      entries: [],
      projectId: ProjectId.make("project-1"),
      lifecycle: {
        override: "active",
        updatedAt: "2026-08-05T00:00:00.000Z",
      },
    });
    const running = projectPiThread({
      record,
      entries: [],
      projectId: ProjectId.make("project-1"),
      runtime: {
        runtimeId: PiNativeRuntimeId.make("runtime-1"),
        writerKind: "rpc",
        status: "streaming",
        sequence: 1,
      },
    });
    const idle = projectPiThread({
      record,
      entries: [],
      projectId: ProjectId.make("project-1"),
      runtime: {
        runtimeId: PiNativeRuntimeId.make("runtime-2"),
        writerKind: "rpc",
        status: "idle",
        sequence: 1,
      },
    });

    expect(
      applyPiExternalAutoSettlement({ snapshot: explicitlyActive, ...input }).thread
        .settledOverride,
    ).toBe("active");
    expect(
      applyPiExternalAutoSettlement({ snapshot: running, ...input }).thread.settledOverride,
    ).toBeNull();
    expect(applyPiExternalAutoSettlement({ snapshot: idle, ...input }).thread.settledOverride).toBe(
      "settled",
    );
  });

  it("invalidates a lifecycle override after the Pi session file changes", () => {
    const record = {
      sourceKey: PiNativeSessionKey.make("source"),
      threadId: ThreadId.make("external:pi:path:source"),
      canonicalFile: "/sessions/source.jsonl",
      sessionId: "session",
      cwd: "/workspace",
      title: "session",
      createdAt: "2026-08-06T00:00:00.000Z",
      updatedAt: "2026-08-06T00:01:00.000Z",
      fileSize: 10,
      fileMtimeMs: 20,
      historyTruncation: { truncated: false },
    } as const;
    const override = {
      sourceKey: record.sourceKey,
      commandId: CommandId.make("settle"),
      lifecycleOverride: "settled" as const,
      observedFileSize: 10,
      observedFileMtimeMs: 20,
      updatedAt: "2026-08-06T00:01:00.000Z",
    };

    expect(validExternalLifecycleOverride(record, override)?.override).toBe("settled");
    expect(validExternalLifecycleOverride({ ...record, fileSize: 11 }, override)).toBeUndefined();
    expect(
      validExternalLifecycleOverride(
        {
          ...record,
          fileSize: 11,
          jsonlLifecycle: {
            override: "active",
            operationId: "pi-operation",
            updatedAt: "2026-08-06T00:02:00.000Z",
          },
        },
        override,
      )?.override,
    ).toBe("active");
  });

  it("bounds aggregate catalog records and serialized bytes with omission counts", () => {
    const project = {
      id: ProjectId.make("project-1"),
      title: "project",
      workspaceRoot: "/workspace",
      defaultModelSelection: null,
      scripts: [],
      createdAt: "2026-07-30T00:00:00.000Z",
      updatedAt: "2026-07-30T00:00:00.000Z",
    } satisfies OrchestrationProjectShell;
    const threads = Array.from({ length: 20 }, (_, index) => ({
      id: ThreadId.make(`external:pi:${index}`),
      projectId: project.id,
      title: "x".repeat(256),
    })) as unknown as OrchestrationThreadShell[];

    const bounded = boundExternalCatalog({
      projects: [project],
      threads,
      totalThreadCount: threads.length,
      maxThreads: 10,
      maxSerializedBytes: 1_500,
    });

    expect(bounded.threads.length).toBeLessThanOrEqual(10);
    expect(Buffer.byteLength(JSON.stringify(bounded)) + 1_024).toBeLessThanOrEqual(1_500);
    expect(bounded.omittedThreadCount).toBe(threads.length - bounded.threads.length);
  });
});
