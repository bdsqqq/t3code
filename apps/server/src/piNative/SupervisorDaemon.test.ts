// @effect-diagnostics nodeBuiltinImport:off
import { PiNativeEventId, PiNativeRuntimeId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as NodeCrypto from "node:crypto";
import * as NodeEvents from "node:events";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  projectOverlayPayload,
  acquireResumeAndSendRuntime,
  bridgeRegistrationIsSettled,
  bridgeCommandFrame,
  claimRpcSessionWriter,
  correlateRuntimeUserMessage,
  projectListedRuntime,
  projectQueuePayload,
  projectQueueValues,
  projectReplayItem,
  piRpcSpawnArgs,
  publishSettlementInOrder,
  releaseBridgeRegistration,
  releaseSessionWriterIfOwned,
  RPC_MESSAGE_ID_EXTENSION_SOURCE,
  reserveBridgeRegistration,
  runCommandSingleFlight,
  runKeyedSerialQueue,
  runSerializedResumeAndSendDelivery,
  decodeRuntimeChunk,
  createSupervisorLockFile,
  createBridgeRegistrationSocketGuard,
  existingWriterResumeAndSendCommand,
  makeManagedAdmissionController,
  managedMessageIdCommandName,
  messageIdCorrelationPrompt,
  type ManagedLedgerEntry,
  queuePayloadHasPending,
  shouldUseSnapshot,
  shouldRestartPersistedSession,
  validateExistingPiSessionSpawn,
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

  it("forwards message identity to the Pi-owned TUI bridge writer", () => {
    expect(
      bridgeCommandFrame({
        type: "send",
        commandId: "operation-1",
        runtimeId: "runtime-1",
        message: "hello",
        messageId: "message-1",
      }),
    ).toMatchObject({
      command: "send",
      text: "hello",
      messageId: "message-1",
    });
  });

  it("correlates bridge and rpc user events in command order", () => {
    const pending = [
      { messageId: "message-1", text: "same", behavior: "followUp" as const },
      { messageId: "message-2", text: "same", behavior: "steer" as const },
    ];
    expect(
      correlateRuntimeUserMessage(
        {
          type: "event",
          event: "message_start",
          data: { message: { role: "user", content: [{ type: "text", text: "same" }] } },
        },
        pending,
      ),
    ).toMatchObject({ data: { messageId: "message-2" } });
    expect(
      correlateRuntimeUserMessage(
        {
          type: "message_start",
          messageId: "message-1",
          message: { role: "user", content: [{ type: "text", text: "expanded prompt" }] },
        },
        pending,
      ),
    ).toMatchObject({ messageId: "message-1" });
    expect(pending).toEqual([]);
  });

  it("encodes rpc message identity outside user-visible prompt text", () => {
    const prompt = messageIdCorrelationPrompt({
      messageId: "message-1",
      text: "not encoded",
      behavior: "steer",
    });
    const argument = prompt.slice(prompt.indexOf(" ") + 1);

    expect(prompt).toMatch(/^\/__t3_managed_message_id_v1 /);
    expect(JSON.parse(Buffer.from(argument, "base64url").toString("utf8"))).toEqual({
      version: 1,
      messageId: "message-1",
      behavior: "steer",
      operation: "enqueue",
    });
  });

  it("tags the managed rpc event and durable entry with the client message id", async () => {
    const extensionModule = (await import(
      `data:text/javascript;base64,${Buffer.from(RPC_MESSAGE_ID_EXTENSION_SOURCE).toString("base64")}`
    )) as {
      default: (pi: {
        registerCommand(
          name: string,
          options: { handler: (argument: string) => Promise<void> },
        ): void;
        on(name: string, handler: (event: Record<string, unknown>) => void): void;
        appendEntry(customType: string, data: unknown): void;
      }) => void;
    };
    const handlers = new Map<string, (event: Record<string, unknown>) => void>();
    let commandHandler: ((argument: string) => Promise<void>) | undefined;
    const entries: Array<{ customType: string; data: unknown }> = [];
    extensionModule.default({
      registerCommand: (_name, options) => {
        commandHandler = options.handler;
      },
      on: (name, handler) => handlers.set(name, handler),
      appendEntry: (customType, data) => entries.push({ customType, data }),
    });
    const argument = messageIdCorrelationPrompt({
      messageId: "message-1",
      text: "hello",
      behavior: "send",
    }).split(" ")[1]!;
    await commandHandler!(argument);
    handlers.get("input")!({ source: "rpc", text: "hello" });
    const event: Record<string, unknown> = {
      message: { role: "user", content: [{ type: "text", text: "expanded hello" }] },
    };
    handlers.get("message_start")!(event);

    expect(event.messageId).toBe("message-1");
    expect(entries).toEqual([
      { customType: "t3.message-id.v1", data: { version: 1, messageId: "message-1" } },
    ]);
  });

  it("recognizes a reconnected bridge with no surviving delivery work as settled", () => {
    expect(bridgeRegistrationIsSettled(false, [], [])).toBe(true);
    expect(bridgeRegistrationIsSettled(true, [], [])).toBe(false);
    expect(bridgeRegistrationIsSettled(false, ["steer"], [])).toBe(false);
  });

  it("accepts only the message identity command loaded from the managed extension", () => {
    expect(
      managedMessageIdCommandName(
        [
          {
            name: "__t3_managed_message_id_v1",
            sourceInfo: { path: "/other/extension.mjs" },
          },
          {
            name: "__t3_managed_message_id_v1:1",
            sourceInfo: { path: "/managed/message-id.mjs" },
          },
        ],
        "/managed/message-id.mjs",
      ),
    ).toBe("__t3_managed_message_id_v1:1");
  });

  it("starts rpc sessions in the cataloged sessions root", () => {
    expect(
      piRpcSpawnArgs({
        sessionsRoot: "/isolated/sessions",
        extensionPath: "/isolated/managed-message-id.mjs",
      }),
    ).toEqual([
      "--mode",
      "rpc",
      "--session-dir",
      "/isolated/sessions",
      "--extension",
      "/isolated/managed-message-id.mjs",
    ]);
    expect(
      piRpcSpawnArgs({
        sessionsRoot: "/isolated/sessions",
        extensionPath: "/isolated/managed-message-id.mjs",
        sessionFile: "/isolated/sessions/existing.jsonl",
      }),
    ).toContain("/isolated/sessions/existing.jsonl");
  });

  it("observes bridge socket closure before registration handoff", () => {
    const emitter = new NodeEvents.EventEmitter();
    const socket = {
      destroyed: false,
      once: (event: "close" | "error", listener: () => void) => {
        emitter.once(event, listener);
        return socket;
      },
      off: (event: "close" | "error", listener: () => void) => {
        emitter.off(event, listener);
        return socket;
      },
    };
    const guard = createBridgeRegistrationSocketGuard(socket);
    emitter.emit("close");

    expect(guard.isClosed()).toBe(true);
    let cleanupCount = 0;
    expect(
      guard.handoff(() => {
        cleanupCount += 1;
      }),
    ).toBe(false);
    expect(cleanupCount).toBe(1);
    guard.dispose();

    const liveEmitter = new NodeEvents.EventEmitter();
    const liveSocket = {
      destroyed: false,
      once: (event: "close" | "error", listener: () => void) => {
        liveEmitter.once(event, listener);
        return liveSocket;
      },
      off: (event: "close" | "error", listener: () => void) => {
        liveEmitter.off(event, listener);
        return liveSocket;
      },
    };
    const liveGuard = createBridgeRegistrationSocketGuard(liveSocket);
    expect(
      liveGuard.handoff(() => {
        cleanupCount += 1;
      }),
    ).toBe(true);
    liveEmitter.emit("error", new Error("disconnected"));
    expect(cleanupCount).toBe(2);
    liveGuard.dispose();
  });

  it("blocks rpc writer claims while bridge registration holds its reservation", () => {
    const sessionFile = "/sessions/reserved.jsonl";
    const writers = new Map<string, string>();
    const reservations = new Set([sessionFile]);

    expect(
      claimRpcSessionWriter({
        writers,
        bridgeRegistrations: reservations,
        sessionFile,
        runtimeId: "rpc-runtime",
      }),
    ).toEqual({ status: "bridgeReserved" });
    expect(writers.has(sessionFile)).toBe(false);

    reservations.delete(sessionFile);
    expect(
      claimRpcSessionWriter({
        writers,
        bridgeRegistrations: reservations,
        sessionFile,
        runtimeId: "rpc-runtime",
      }),
    ).toEqual({ status: "claimed" });
    expect(writers.get(sessionFile)).toBe("rpc-runtime");
  });

  it("does not let stale runtime cleanup clear another writer", () => {
    const sessionFile = "/sessions/owned.jsonl";
    const writers = new Map([[sessionFile, "current-runtime"]]);

    expect(
      releaseSessionWriterIfOwned({
        writers,
        sessionFile,
        runtimeId: "stale-runtime",
      }),
    ).toBe(false);
    expect(writers.get(sessionFile)).toBe("current-runtime");
    expect(
      releaseSessionWriterIfOwned({
        writers,
        sessionFile,
        runtimeId: "current-runtime",
      }),
    ).toBe(true);
    expect(writers.has(sessionFile)).toBe(false);
  });

  it("preserves resume-and-send streaming intent for a reused writer", () => {
    const outer = {
      type: "resumeAndSend",
      commandId: "takeover-1",
      sessionKey: "source-key",
      sessionFile: "/sessions/session.jsonl",
      cwd: "/workspace",
      message: "continue",
      messageId: "message-1",
      streamingBehavior: "steer",
    };

    const delivery = existingWriterResumeAndSendCommand(outer, PiNativeRuntimeId.make("runtime-1"));
    expect(delivery).toMatchObject({
      type: "steer",
      commandId: "takeover-1",
      runtimeId: "runtime-1",
      message: "continue",
      messageId: "message-1",
    });
    expect(bridgeCommandFrame(delivery)).toMatchObject({
      command: "steer",
      commandId: "takeover-1",
      text: "continue",
      messageId: "message-1",
    });
    expect(outer.type).toBe("resumeAndSend");

    expect(
      existingWriterResumeAndSendCommand(
        { ...outer, streamingBehavior: "followUp" },
        PiNativeRuntimeId.make("runtime-1"),
      ),
    ).toMatchObject({ type: "followUp" });
    expect(
      existingWriterResumeAndSendCommand(
        { ...outer, streamingBehavior: undefined },
        PiNativeRuntimeId.make("runtime-1"),
      ),
    ).toMatchObject({ type: "steer" });
  });

  it("reuses an existing writer without spawning for resume-and-send", async () => {
    let spawnCount = 0;
    const result = await acquireResumeAndSendRuntime({
      existingWriter: async () => ({ runtimeId: "existing" }),
      spawnWriter: async () => {
        spawnCount += 1;
        return { runtimeId: "spawned" };
      },
      isWriterClaimConflict: () => false,
    });

    expect(result).toEqual({ runtime: { runtimeId: "existing" }, reusedWriter: true });
    expect(spawnCount).toBe(0);
  });

  it("reuses the winner when a writer claims the session during spawn admission", async () => {
    const claimConflict = new Error("writer claimed");
    let lookupCount = 0;
    let spawnCount = 0;
    const result = await acquireResumeAndSendRuntime({
      existingWriter: async () => {
        lookupCount += 1;
        return lookupCount === 1 ? undefined : { runtimeId: "race-winner" };
      },
      spawnWriter: async () => {
        spawnCount += 1;
        throw claimConflict;
      },
      isWriterClaimConflict: (cause) => cause === claimConflict,
    });

    expect(result).toEqual({ runtime: { runtimeId: "race-winner" }, reusedWriter: true });
    expect(spawnCount).toBe(1);
    expect(lookupCount).toBe(2);
  });

  it("validates a canonical existing session before spawning", async () => {
    const root = await NodeFS.promises.mkdtemp(
      NodePath.join(NodeOS.tmpdir(), "t3-supervisor-resume-"),
    );
    try {
      const sessionsRoot = NodePath.join(root, "sessions");
      const cwd = NodePath.join(root, "workspace");
      const sessionDir = NodePath.join(sessionsRoot, "project");
      const sessionFile = NodePath.join(sessionDir, "session.jsonl");
      await NodeFS.promises.mkdir(sessionDir, { recursive: true });
      await NodeFS.promises.mkdir(cwd);
      await NodeFS.promises.writeFile(
        sessionFile,
        `${JSON.stringify({ type: "session", id: "session-1", cwd })}\n`,
      );
      const canonicalFile = await NodeFS.promises.realpath(sessionFile);
      const canonicalCwd = await NodeFS.promises.realpath(cwd);
      const sessionKey = NodeCrypto.createHash("sha256").update(canonicalFile).digest("hex");

      await expect(
        validateExistingPiSessionSpawn({ sessionsRoot, sessionFile, cwd, sessionKey }),
      ).resolves.toEqual({
        sessionFile: canonicalFile,
        cwd: canonicalCwd,
        sessionId: "session-1",
      });
    } finally {
      await NodeFS.promises.rm(root, { recursive: true, force: true });
    }
  });

  it("rejects mismatched existing-session takeover inputs", async () => {
    const root = await NodeFS.promises.mkdtemp(
      NodePath.join(NodeOS.tmpdir(), "t3-supervisor-resume-mismatch-"),
    );
    try {
      const sessionsRoot = NodePath.join(root, "sessions");
      const cwd = NodePath.join(root, "workspace");
      const otherCwd = NodePath.join(root, "other-workspace");
      const sessionFile = NodePath.join(sessionsRoot, "session.jsonl");
      const outsideFile = NodePath.join(root, "outside.jsonl");
      await NodeFS.promises.mkdir(sessionsRoot);
      await NodeFS.promises.mkdir(cwd);
      await NodeFS.promises.mkdir(otherCwd);
      const header = `${JSON.stringify({ type: "session", id: "session-1", cwd })}\n`;
      await NodeFS.promises.writeFile(sessionFile, header);
      await NodeFS.promises.writeFile(outsideFile, header);

      await expect(
        validateExistingPiSessionSpawn({
          sessionsRoot,
          sessionFile,
          cwd,
          sessionKey: "not-the-canonical-path-hash",
        }),
      ).rejects.toThrow("session key does not match canonical session file");
      await expect(
        validateExistingPiSessionSpawn({ sessionsRoot, sessionFile, cwd: otherCwd }),
      ).rejects.toThrow("session header cwd does not match resume cwd");
      await expect(
        validateExistingPiSessionSpawn({ sessionsRoot, sessionFile: outsideFile, cwd }),
      ).rejects.toThrow("session file is outside the pi sessions root");
      await expect(
        validateExistingPiSessionSpawn({ sessionsRoot, sessionFile: otherCwd, cwd }),
      ).rejects.toThrow("session file is outside the pi sessions root");
      const sessionDirectory = NodePath.join(sessionsRoot, "not-a-file");
      await NodeFS.promises.mkdir(sessionDirectory);
      await expect(
        validateExistingPiSessionSpawn({ sessionsRoot, sessionFile: sessionDirectory, cwd }),
      ).rejects.toThrow("session file is not a regular file");

      await NodeFS.promises.writeFile(
        sessionFile,
        `${JSON.stringify({ type: "not-session", id: "session-1", cwd })}\n`,
      );
      await expect(
        validateExistingPiSessionSpawn({ sessionsRoot, sessionFile, cwd }),
      ).rejects.toThrow("invalid session header");
    } finally {
      await NodeFS.promises.rm(root, { recursive: true, force: true });
    }
  });

  it("serializes resume-and-send admission per canonical session", async () => {
    const entries = new Map<string, Promise<void>>();
    let releaseFirst!: () => void;
    const firstAdmission = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let markFirstStarted!: () => void;
    const firstStarted = new Promise<void>((resolve) => {
      markFirstStarted = resolve;
    });
    const order: string[] = [];

    const first = runKeyedSerialQueue({
      entries,
      key: "/sessions/one.jsonl",
      run: async () => {
        order.push("first:start");
        markFirstStarted();
        await firstAdmission;
        order.push("first:admitted");
        return "first";
      },
    });
    await firstStarted;
    const second = runKeyedSerialQueue({
      entries,
      key: "/sessions/one.jsonl",
      run: async () => {
        order.push("second:start");
        return "second";
      },
    });
    const other = runKeyedSerialQueue({
      entries,
      key: "/sessions/two.jsonl",
      run: async () => {
        order.push("other:start");
        return "other";
      },
    });
    await expect(other).resolves.toBe("other");
    expect(order).toEqual(["first:start", "other:start"]);

    releaseFirst();
    await expect(Promise.all([first, second])).resolves.toEqual(["first", "second"]);
    expect(order).toEqual(["first:start", "other:start", "first:admitted", "second:start"]);
    expect(entries.size).toBe(0);
  });

  it("keeps serialized resume-and-send queued until delivery can begin", async () => {
    const entries = new Map<string, Promise<void>>();
    const phases = new Map([
      ["first", "queued"],
      ["second", "queued"],
    ]);
    const order: string[] = [];
    let releaseFirst!: () => void;
    const firstAdmission = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let markFirstWorking!: () => void;
    const firstWorking = new Promise<void>((resolve) => {
      markFirstWorking = resolve;
    });

    const first = runSerializedResumeAndSendDelivery({
      entries,
      sessionFile: "/sessions/one.jsonl",
      startDelivery: async () => {
        phases.set("first", "delivering");
        order.push("first:delivering");
      },
      deliver: async () => {
        expect(phases.get("first")).toBe("delivering");
        order.push("first:work");
        markFirstWorking();
        await firstAdmission;
        return "first";
      },
    });
    await firstWorking;

    const second = runSerializedResumeAndSendDelivery({
      entries,
      sessionFile: "/sessions/one.jsonl",
      startDelivery: async () => {
        phases.set("second", "delivering");
        order.push("second:delivering");
      },
      deliver: async () => {
        expect(phases.get("second")).toBe("delivering");
        order.push("second:work");
        return "second";
      },
    });

    await Promise.resolve();
    expect(phases.get("second")).toBe("queued");
    expect(order).toEqual(["first:delivering", "first:work"]);

    releaseFirst();
    await expect(Promise.all([first, second])).resolves.toEqual(["first", "second"]);
    expect(order).toEqual(["first:delivering", "first:work", "second:delivering", "second:work"]);
    expect(entries.size).toBe(0);
  });

  it("publishes one shared command promise before stale restart persistence", async () => {
    const entries = new Map<string, { readonly hash: string; readonly work: Promise<string> }>();
    let releasePersistence!: () => void;
    const persistence = new Promise<void>((resolve) => {
      releasePersistence = resolve;
    });
    let markStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    let runCount = 0;
    const run = async () => {
      runCount += 1;
      markStarted();
      await persistence;
      return "delivered";
    };

    const first = runCommandSingleFlight({
      entries,
      id: "stale-start",
      hash: "same-payload",
      run,
    });
    const duplicate = runCommandSingleFlight({
      entries,
      id: "stale-start",
      hash: "same-payload",
      run,
    });
    expect(first).toBe(duplicate);
    expect(() =>
      runCommandSingleFlight({
        entries,
        id: "stale-start",
        hash: "different-payload",
        run,
      }),
    ).toThrow("commandId payload conflict");

    await started;
    expect(runCount).toBe(1);
    releasePersistence();
    await expect(Promise.all([first, duplicate])).resolves.toEqual(["delivered", "delivered"]);
    expect(entries.has("stale-start")).toBe(false);
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
