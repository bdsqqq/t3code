import {
  CommandId,
  PiNativeRuntimeId,
  ProjectId,
  ThreadId,
  type OrchestrationProjectShell,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { describe, expect } from "vite-plus/test";

import {
  boundExternalCatalog,
  catalogUpdateAfterRead,
  CatalogRuntimeAttachmentGate,
  runtimeSnapshotAtSequence,
  runtimeSequenceStable,
  runtimeCatalogSignature,
  isRuntimeLifecycleEvent,
  receiptSessionFile,
  shutdownCreatedRuntime,
} from "./PiExternalThreadSource.ts";
import type { SupervisorRuntimeState } from "./SupervisorProtocol.ts";

describe("PiExternalThreadSource hardening", () => {
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

  it("stops catalog replacements after a runtime attaches", () => {
    const gate = new CatalogRuntimeAttachmentGate();
    expect(gate.allowsCatalogUpdate()).toBe(true);
    gate.attach();
    expect(gate.allowsCatalogUpdate()).toBe(false);
    expect(catalogUpdateAfterRead(gate, { snapshotSequence: 1 })).toBeUndefined();
  });

  it("refreshes state for bridge disconnect and reconnect lifecycle events", () => {
    expect(isRuntimeLifecycleEvent("bridge_disconnected")).toBe(true);
    expect(isRuntimeLifecycleEvent("bridge_reconnected")).toBe(true);
    expect(isRuntimeLifecycleEvent("message_update")).toBe(false);
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
