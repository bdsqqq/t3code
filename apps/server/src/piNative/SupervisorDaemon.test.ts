// @effect-diagnostics nodeBuiltinImport:off
import { PiNativeEventId, PiNativeRuntimeId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  projectOverlayPayload,
  bridgeCommandFrame,
  projectListedRuntime,
  projectQueuePayload,
  projectQueueValues,
  projectReplayItem,
  piRpcSpawnArgs,
  publishSettlementInOrder,
  releaseBridgeRegistration,
  reserveBridgeRegistration,
  decodeRuntimeChunk,
  createSupervisorLockFile,
  makeManagedAdmissionController,
  type ManagedLedgerEntry,
  queuePayloadHasPending,
  shouldUseSnapshot,
  shouldRestartPersistedSession,
} from "./SupervisorDaemon.ts";
import {
  JsonLineDecoder,
  MANAGED_ADMISSION_PROTOCOL,
  type ManagedClaimRequest,
} from "./SupervisorProtocol.ts";

const managedClaim = (message = "hello"): ManagedClaimRequest => ({
  protocol: MANAGED_ADMISSION_PROTOCOL,
  intent: "execute",
  operationKey: "managed-pi:turn-start:operation-1",
  payload: {
    type: "managed-pi.turn-start",
    providerInstanceId: "pi-work",
    threadId: "thread-1",
    session: {
      schemaVersion: 1,
      sessionFile: "/state/pi/session-1.jsonl",
      sessionId: "session-1",
    },
    message,
    attachments: [],
    model: { provider: "openai", modelId: "gpt-5" },
    thinkingLevel: "high",
    interactionMode: "plan",
  },
});

const makeManagedHarness = (
  entries = new Map<string, ManagedLedgerEntry>(),
  options: { readonly persist?: (snapshot: string, index: number) => Promise<void> } = {},
) => {
  const snapshots: string[] = [];
  const timers: Array<{ work: () => void; cancelled: boolean }> = [];
  const controller = makeManagedAdmissionController<object>({
    store: {
      get: (operationKey) => entries.get(operationKey),
      set: (operationKey, entry) => entries.set(operationKey, entry),
      entries: () =>
        [...entries.entries()].map(([operationKey, entry]) => ({ operationKey, entry })),
      persist: async () => {
        const snapshot = JSON.stringify(Object.fromEntries(entries));
        snapshots.push(snapshot);
        await options.persist?.(snapshot, snapshots.length - 1);
      },
    },
    randomToken: () => `lease-${String(timers.length + 1)}`,
    schedule: (work) => {
      const timer = { work, cancelled: false };
      timers.push(timer);
      return { cancel: () => (timer.cancelled = true) };
    },
  });
  return { controller, entries, snapshots, timers };
};

describe("native Pi replay projection", () => {
  it("forwards lifecycle data to the Pi-owned TUI bridge writer", () => {
    expect(
      bridgeCommandFrame({
        type: "setLifecycle",
        commandId: "operation-1",
        runtimeId: "runtime-1",
        lifecycle: {
          version: 1,
          sessionId: "session-1",
          override: "settled",
          operationId: "operation-1",
        },
      }),
    ).toMatchObject({
      command: "setLifecycle",
      lifecycle: {
        sessionId: "session-1",
        override: "settled",
      },
    });
  });

  it("starts rpc sessions in the cataloged sessions root", () => {
    expect(piRpcSpawnArgs({ sessionsRoot: "/isolated/sessions" })).toEqual([
      "--mode",
      "rpc",
      "--session-dir",
      "/isolated/sessions",
    ]);
    expect(
      piRpcSpawnArgs({
        sessionsRoot: "/isolated/sessions",
        sessionFile: "/isolated/sessions/existing.jsonl",
      }),
    ).toContain("/isolated/sessions/existing.jsonl");
  });

  it("restarts only stale persisted-session start receipts", () => {
    const receipt = {
      commandId: "resume-1" as never,
      status: "completed" as const,
      runtimeId: PiNativeRuntimeId.make("runtime-1"),
    };
    expect(
      shouldRestartPersistedSession(
        { type: "start", sessionFile: "/sessions/existing.jsonl" },
        receipt,
        undefined,
      ),
    ).toBe(true);
    expect(shouldRestartPersistedSession({ type: "start" }, receipt, undefined)).toBe(false);
    expect(
      shouldRestartPersistedSession(
        { type: "start", sessionFile: "/sessions/existing.jsonl" },
        receipt,
        {
          runtimeId: PiNativeRuntimeId.make("runtime-1"),
          writerKind: "rpc",
          status: "idle",
          sequence: 1,
        },
      ),
    ).toBe(false);
  });

  it("forces a snapshot when any item at the cursor sequence was evicted", () => {
    expect(shouldUseSnapshot(5, 5, 5)).toBe(true);
    expect(shouldUseSnapshot(6, 5, 6)).toBe(false);
  });

  it("does not retain cumulative assistant content in replay events", () => {
    const cumulative = "x".repeat(2 * 1024 * 1024);
    const projected = projectReplayItem({
      type: "event",
      runtimeId: PiNativeRuntimeId.make("runtime"),
      sequence: 1,
      eventId: PiNativeEventId.make("event"),
      event: {
        type: "message_update",
        message: { role: "assistant", content: cumulative },
        assistantMessageEvent: {
          type: "text_delta",
          delta: "x",
          partial: { role: "assistant", content: cumulative },
        },
      },
    });

    expect(JSON.stringify(projected).length).toBeLessThan(1_024);
  });

  it("retains only one cumulative assistant representation in snapshots", () => {
    const cumulative = "x".repeat(2 * 1024 * 1024);
    const projected = projectOverlayPayload(
      {
        type: "message_update",
        message: { role: "assistant", content: cumulative },
        assistantMessageEvent: { type: "text_delta", delta: "x", partial: cumulative },
      },
      "message_update",
    );

    expect(JSON.stringify(projected).length).toBeLessThan(3 * 1024 * 1024);
    expect(projected).toMatchObject({
      assistantMessageEvent: { partial: cumulative },
    });
  });

  it("bounds pending queues with explicit omission metadata", () => {
    const projected = projectQueuePayload({
      type: "queue_update",
      steering: Array.from({ length: 40 }, () => "x".repeat(1024 * 1024)),
      followUp: ["after"],
    });

    expect(Buffer.byteLength(JSON.stringify(projected))).toBeLessThan(5 * 1024 * 1024);
    expect(projected).toMatchObject({ omittedSteering: 37, omittedFollowUp: 0 });
    expect(
      Buffer.byteLength(
        JSON.stringify(
          projectQueuePayload({
            type: "queue_update",
            steering: ["\0".repeat(6 * 1024 * 1024)],
            followUp: [],
          }),
        ),
      ),
    ).toBeLessThan(1_024);
    const omissionOnly = projectQueuePayload({
      type: "queue_update",
      steering: ["\0".repeat(6 * 1024 * 1024)],
      followUp: [],
    });
    expect(omissionOnly).toMatchObject({ steering: [], omittedSteering: 1 });
    expect(queuePayloadHasPending(omissionOnly)).toBe(true);
    const manyEmptyValues = projectQueueValues(
      { steering: Array.from({ length: 100 }, () => ""), followUp: [] },
      100,
    );
    expect(Buffer.byteLength(JSON.stringify(manyEmptyValues))).toBeLessThanOrEqual(100);
  });

  it("publishes authoritative jsonl replacement before clearing the live overlay", async () => {
    const order: string[] = [];
    await publishSettlementInOrder({
      read: async () => {
        order.push("read");
        return ["entry"];
      },
      isCurrent: () => true,
      publishReplacement: () => order.push("replace"),
      clearOverlay: () => order.push("clear"),
      publishSynchronized: () => order.push("synchronized"),
    });

    expect(order).toEqual(["read", "replace", "clear", "synchronized"]);
  });

  it("reserves one bridge registration per canonical session", () => {
    expect(reserveBridgeRegistration("/session/one.jsonl")).toBe(true);
    expect(reserveBridgeRegistration("/session/one.jsonl")).toBe(false);
    releaseBridgeRegistration("/session/one.jsonl");
    expect(reserveBridgeRegistration("/session/one.jsonl")).toBe(true);
    releaseBridgeRegistration("/session/one.jsonl");
  });

  it("contains oversized rpc frames to the offending runtime", () => {
    const decoded = decodeRuntimeChunk(new JsonLineDecoder(8), '{"oversized":true}');
    expect(decoded.frames).toEqual([]);
    expect(decoded.error).toBeInstanceOf(Error);
  });

  it("omits arbitrary provider state from bounded runtime listings", () => {
    expect(
      projectListedRuntime({
        runtimeId: PiNativeRuntimeId.make("runtime"),
        writerKind: "rpc",
        status: "idle",
        sequence: 1,
        state: { transcript: "x".repeat(1024) },
      }),
    ).not.toHaveProperty("state");
  });

  it("atomically admits only one supervisor lock owner", async () => {
    const root = await NodeFS.promises.mkdtemp(
      NodePath.join(NodeOS.tmpdir(), "t3-supervisor-lock-"),
    );
    const lockPath = NodePath.join(root, "supervisor.lock");
    try {
      const [first, second] = await Promise.all([
        createSupervisorLockFile(lockPath, 1001),
        createSupervisorLockFile(lockPath, 1002),
      ]);
      expect([first, second].filter(Boolean)).toHaveLength(1);
    } finally {
      await NodeFS.promises.rm(root, { recursive: true, force: true });
    }
  });
});

describe("managed Pi admission ledger", () => {
  it("persists delivering before granting one socket-bound lease", async () => {
    const harness = makeManagedHarness();
    const socket = {};

    const result = await harness.controller.claim(socket, managedClaim());

    expect(result).toMatchObject({ status: "granted", operationKey: managedClaim().operationKey });
    expect(JSON.parse(harness.snapshots[0]!)).toMatchObject({
      [managedClaim().operationKey]: {
        kind: "managedAdmission",
        receipt: {
          commandId: managedClaim().operationKey,
          status: "indeterminate",
        },
        operation: { status: "delivering" },
      },
    });
  });

  it("keeps managed entries terminal to the first-parent daemon during rollback", async () => {
    const harness = makeManagedHarness();
    await harness.controller.claim({}, managedClaim());
    const entry = harness.entries.get(managedClaim().operationKey)!;

    expect(entry.receipt.status).toBe("indeterminate");
    expect(entry.receipt.commandId).toBe(managedClaim().operationKey);
    expect(
      entry.receipt.status === "started" &&
        "phase" in entry &&
        (entry as { readonly phase?: string }).phase === "delivering",
    ).toBe(false);
  });

  it("returns prior delivery for duplicate payloads and conflicts changed payloads", async () => {
    const harness = makeManagedHarness();
    const first = await harness.controller.claim({}, managedClaim());
    const duplicate = await harness.controller.claim({}, managedClaim());
    const conflict = await harness.controller.claim({}, managedClaim("different"));

    expect(first.status).toBe("granted");
    expect(duplicate).toEqual({ status: "delivering" });
    expect(conflict).toEqual({
      status: "conflict",
      error: "managed operation payload conflict",
    });
    expect(harness.timers).toHaveLength(1);
  });

  it("does not expose a transition until its persistence completes", async () => {
    let releasePersist!: () => void;
    const persistGate = new Promise<void>((resolve) => (releasePersist = resolve));
    const harness = makeManagedHarness(new Map(), {
      persist: async (_snapshot, index) => {
        if (index === 0) await persistGate;
      },
    });
    const first = harness.controller.claim({}, managedClaim());
    await new Promise((resolve) => setImmediate(resolve));
    let duplicateSettled = false;
    const duplicate = harness.controller.claim({}, managedClaim()).then((result) => {
      duplicateSettled = true;
      return result;
    });
    await new Promise((resolve) => setImmediate(resolve));

    expect(duplicateSettled).toBe(false);
    releasePersist();
    expect((await first).status).toBe("granted");
    expect(await duplicate).toEqual({ status: "delivering" });
  });

  it("does not expose completed until finalization persistence completes", async () => {
    let releaseFinalize!: () => void;
    const finalizeGate = new Promise<void>((resolve) => (releaseFinalize = resolve));
    const harness = makeManagedHarness(new Map(), {
      persist: async (_snapshot, index) => {
        if (index === 1) await finalizeGate;
      },
    });
    const holder = {};
    const granted = await harness.controller.claim(holder, managedClaim());
    if (granted.status !== "granted") throw new Error("expected managed lease");
    const finalization = harness.controller.finalize(holder, {
      protocol: MANAGED_ADMISSION_PROTOCOL,
      operationKey: granted.operationKey,
      leaseToken: granted.leaseToken,
      finalization: { status: "completed", receipt: { turnId: "turn-1" } },
    });
    await new Promise((resolve) => setImmediate(resolve));
    let duplicateSettled = false;
    const duplicate = harness.controller.claim({}, managedClaim()).then((result) => {
      duplicateSettled = true;
      return result;
    });
    await new Promise((resolve) => setImmediate(resolve));

    expect(duplicateSettled).toBe(false);
    releaseFinalize();
    expect(await finalization).toMatchObject({ status: "finalized" });
    expect(await duplicate).toEqual({
      status: "completed",
      receipt: { turnId: "turn-1" },
    });
  });

  it("fails closed after managed admission persistence fails", async () => {
    const harness = makeManagedHarness(new Map(), {
      persist: async () => Promise.reject(new Error("disk unavailable")),
    });

    await expect(harness.controller.claim({}, managedClaim())).rejects.toThrow(
      "supervisor restart required",
    );
    await expect(harness.controller.claim({}, managedClaim())).rejects.toThrow(
      "supervisor restart required",
    );
  });

  it("makes a disconnect during initial persistence indeterminate before reuse", async () => {
    let releasePersist!: () => void;
    const persistGate = new Promise<void>((resolve) => (releasePersist = resolve));
    const harness = makeManagedHarness(new Map(), {
      persist: async (_snapshot, index) => {
        if (index === 0) await persistGate;
      },
    });
    const socket = {};
    const claim = harness.controller.claim(socket, managedClaim());
    await new Promise((resolve) => setImmediate(resolve));
    const disconnect = harness.controller.disconnect(socket);
    releasePersist();

    expect(await claim).toMatchObject({
      status: "indeterminate",
      error: expect.stringContaining("disconnected"),
    });
    await disconnect;
    expect(await harness.controller.claim({}, managedClaim())).toMatchObject({
      status: "indeterminate",
      error: expect.stringContaining("disconnected"),
    });
    expect(JSON.parse(harness.snapshots.at(-1)!)).toMatchObject({
      [managedClaim().operationKey]: {
        operation: { status: "indeterminate", error: expect.stringContaining("disconnected") },
      },
    });
  });

  it("does not grant when the lease times out during initial persistence", async () => {
    let releasePersist!: () => void;
    const persistGate = new Promise<void>((resolve) => (releasePersist = resolve));
    const harness = makeManagedHarness(new Map(), {
      persist: async (_snapshot, index) => {
        if (index === 0) await persistGate;
      },
    });
    const claim = harness.controller.claim({}, managedClaim());
    await new Promise((resolve) => setImmediate(resolve));

    harness.timers[0]!.work();
    releasePersist();

    expect(await claim).toMatchObject({
      status: "indeterminate",
      error: expect.stringContaining("timed out"),
    });
    expect(JSON.parse(harness.snapshots.at(-1)!)).toMatchObject({
      [managedClaim().operationKey]: {
        operation: { status: "indeterminate", error: expect.stringContaining("timed out") },
      },
    });
  });

  it("does not finalize a lease revoked before finalization enters its transition", async () => {
    const harness = makeManagedHarness();
    const holder = {};
    const granted = await harness.controller.claim(holder, managedClaim());
    if (granted.status !== "granted") throw new Error("expected managed lease");

    const finalization = harness.controller.finalize(holder, {
      protocol: MANAGED_ADMISSION_PROTOCOL,
      operationKey: granted.operationKey,
      leaseToken: granted.leaseToken,
      finalization: { status: "completed", receipt: { turnId: "turn-too-late" } },
    });
    const disconnect = harness.controller.disconnect(holder);

    expect(await finalization).toMatchObject({
      status: "staleLease",
      operation: { status: "indeterminate", error: expect.stringContaining("disconnected") },
    });
    await disconnect;
    expect(await harness.controller.claim({}, managedClaim())).toMatchObject({
      status: "indeterminate",
      error: expect.stringContaining("disconnected"),
    });
  });

  it("observes durable recovery states without claiming legacy absence", async () => {
    const harness = makeManagedHarness();

    expect(
      await harness.controller.claim({}, { ...managedClaim(), intent: "recover-existing" }),
    ).toEqual({ status: "absent" });
    expect(harness.entries.size).toBe(0);
    expect(harness.snapshots).toHaveLength(0);
  });

  it("makes requester disconnect and daemon restart durably indeterminate", async () => {
    const disconnected = makeManagedHarness();
    const socket = {};
    await disconnected.controller.claim(socket, managedClaim());
    await disconnected.controller.disconnect(socket);
    expect(await disconnected.controller.claim({}, managedClaim())).toMatchObject({
      status: "indeterminate",
      error: expect.stringContaining("disconnected"),
    });

    const beforeRestart = makeManagedHarness();
    await beforeRestart.controller.claim({}, managedClaim());
    beforeRestart.controller.dispose();
    const afterRestart = makeManagedHarness(beforeRestart.entries);
    expect(await afterRestart.controller.recoverAfterRestart()).toBe(true);
    expect(await afterRestart.controller.claim({}, managedClaim())).toMatchObject({
      status: "indeterminate",
      error: expect.stringContaining("restarted"),
    });
    expect(afterRestart.snapshots).toHaveLength(1);
  });

  it("persists lease timeout as sticky indeterminate", async () => {
    const harness = makeManagedHarness();
    await harness.controller.claim({}, managedClaim());
    harness.timers[0]!.work();
    await new Promise((resolve) => setImmediate(resolve));

    expect(await harness.controller.claim({}, managedClaim())).toMatchObject({
      status: "indeterminate",
      error: expect.stringContaining("timed out"),
    });
  });

  it("rejects stale finalization but durably returns a holder's completed receipt", async () => {
    const harness = makeManagedHarness();
    const holder = {};
    const granted = await harness.controller.claim(holder, managedClaim());
    expect(granted.status).toBe("granted");
    if (granted.status !== "granted") throw new Error("expected managed lease");

    const stale = await harness.controller.finalize(
      {},
      {
        protocol: MANAGED_ADMISSION_PROTOCOL,
        operationKey: granted.operationKey,
        leaseToken: granted.leaseToken,
        finalization: { status: "completed", receipt: { turnId: "turn-stale" } },
      },
    );
    expect(stale).toEqual({ status: "staleLease", operation: { status: "delivering" } });

    const finalized = await harness.controller.finalize(holder, {
      protocol: MANAGED_ADMISSION_PROTOCOL,
      operationKey: granted.operationKey,
      leaseToken: granted.leaseToken,
      finalization: { status: "completed", receipt: { turnId: "turn-1" } },
    });
    expect(finalized).toEqual({
      status: "finalized",
      operation: { status: "completed", receipt: { turnId: "turn-1" } },
    });
    expect(await harness.controller.claim({}, managedClaim())).toEqual({
      status: "completed",
      receipt: { turnId: "turn-1" },
    });
    harness.timers[0]!.work();
    await new Promise((resolve) => setImmediate(resolve));
    expect(await harness.controller.claim({}, managedClaim())).toEqual({
      status: "completed",
      receipt: { turnId: "turn-1" },
    });
  });
});
