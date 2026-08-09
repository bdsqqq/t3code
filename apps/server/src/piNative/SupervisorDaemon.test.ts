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
  queuePayloadHasPending,
  shouldUseSnapshot,
  shouldRestartPersistedSession,
} from "./SupervisorDaemon.ts";
import { JsonLineDecoder } from "./SupervisorProtocol.ts";

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
