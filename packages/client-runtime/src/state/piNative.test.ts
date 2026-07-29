import { CommandId, PiNativeEventId, PiNativeRuntimeId } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "@effect/vitest";

import {
  buildPiNativeStart,
  EMPTY_PI_NATIVE_RUNTIME_VIEW,
  PI_NATIVE_EVENT_LIMIT,
  reducePiNativeStream,
} from "./piNative.ts";

const testCrypto = Crypto.make({
  randomBytes: (size) => new Uint8Array(size),
  digest: (_algorithm, data) => Effect.succeed(data),
});
const runtimeId = PiNativeRuntimeId.make("runtime-1");
const runtime = {
  runtimeId,
  writerKind: "rpc" as const,
  status: "streaming" as const,
  sequence: 2,
};

describe("pi native runtime reducer", () => {
  it("replaces state on snapshot", () => {
    const state = reducePiNativeStream(
      {
        ...EMPTY_PI_NATIVE_RUNTIME_VIEW,
        events: [
          {
            type: "event",
            runtimeId,
            sequence: 1,
            eventId: PiNativeEventId.make("old"),
            event: {},
          },
        ],
      },
      { type: "snapshot", runtime, entries: [{ type: "message" }], events: [] },
    );
    expect(state.entries).toEqual([{ type: "message" }]);
    expect(state.events).toEqual([]);
  });

  it("deduplicates replayed sequence numbers", () => {
    const state = reducePiNativeStream(
      { ...EMPTY_PI_NATIVE_RUNTIME_VIEW, runtime },
      {
        type: "event",
        runtimeId,
        sequence: 2,
        eventId: PiNativeEventId.make("duplicate"),
        event: {},
      },
    );
    expect(state.events).toEqual([]);
  });

  it("tracks streaming through native lifecycle events", () => {
    const started = reducePiNativeStream(
      { ...EMPTY_PI_NATIVE_RUNTIME_VIEW, runtime: { ...runtime, status: "idle", sequence: 0 } },
      {
        type: "event",
        runtimeId,
        sequence: 1,
        eventId: PiNativeEventId.make("started"),
        event: { type: "agent_start" },
      },
    );
    expect(started.runtime?.status).toBe("streaming");
    const settled = reducePiNativeStream(started, {
      type: "event",
      runtimeId,
      sequence: 2,
      eventId: PiNativeEventId.make("settled"),
      event: { type: "agent_settled" },
    });
    expect(settled.runtime?.status).toBe("idle");
    const reconnected = reducePiNativeStream(settled, {
      type: "event",
      runtimeId,
      sequence: 3,
      eventId: PiNativeEventId.make("bridge-reconnected"),
      event: { type: "bridge_reconnected", isStreaming: true },
    });
    expect(reconnected.runtime?.status).toBe("streaming");
  });

  it("accepts a synchronized marker at the snapshot sequence", () => {
    const state = reducePiNativeStream(
      { ...EMPTY_PI_NATIVE_RUNTIME_VIEW, runtime },
      { type: "synchronized", runtimeId, sequence: runtime.sequence },
    );
    expect(state.synchronized).toBe(true);
  });

  it("deduplicates entries repeated by a concurrent snapshot", () => {
    const state = reducePiNativeStream(
      {
        ...EMPTY_PI_NATIVE_RUNTIME_VIEW,
        runtime: { ...runtime, sequence: 2 },
        entries: [{ type: "message", id: "message-1" }],
      },
      {
        type: "entries",
        runtimeId,
        sequence: 3,
        entries: [
          { type: "message", id: "message-1" },
          { type: "message", id: "message-2" },
        ],
      },
    );
    expect(state.entries).toEqual([
      { type: "message", id: "message-1" },
      { type: "message", id: "message-2" },
    ]);
  });

  it("bounds retained live events", () => {
    let state: import("./piNative.ts").PiNativeRuntimeView = {
      ...EMPTY_PI_NATIVE_RUNTIME_VIEW,
      runtime: { ...runtime, sequence: 0 },
    };
    for (let sequence = 1; sequence <= PI_NATIVE_EVENT_LIMIT + 1; sequence += 1) {
      state = reducePiNativeStream(state, {
        type: "event",
        runtimeId,
        sequence,
        eventId: PiNativeEventId.make(`event-${sequence}`),
        event: { type: "text_delta", delta: "x" },
      });
    }
    expect(state.events).toHaveLength(PI_NATIVE_EVENT_LIMIT);
    expect(state.events[0]?.sequence).toBe(2);
  });

  it("uses a reconnect snapshot as the authoritative replacement", () => {
    const withEvent = reducePiNativeStream(
      { ...EMPTY_PI_NATIVE_RUNTIME_VIEW, runtime: { ...runtime, sequence: 2 } },
      {
        type: "event",
        runtimeId,
        sequence: 3,
        eventId: PiNativeEventId.make("before-reconnect"),
        event: {},
      },
    );
    const reconnected = reducePiNativeStream(withEvent, {
      type: "snapshot",
      runtime: { ...runtime, sequence: 4 },
      entries: [{ type: "message", text: "authoritative" }],
      events: [],
    });
    expect(reconnected.entries).toEqual([{ type: "message", text: "authoritative" }]);
    expect(reconnected.events).toEqual([]);
    expect(reconnected.runtime?.sequence).toBe(4);
  });

  it("marks an exited runtime synchronized", () => {
    const state = reducePiNativeStream(
      { ...EMPTY_PI_NATIVE_RUNTIME_VIEW, runtime },
      { type: "exited", runtimeId, sequence: 3, exitCode: 0 },
    );
    expect(state.runtime?.status).toBe("exited");
    expect(state.synchronized).toBe(true);
  });

  it.effect("preserves a caller-provided command id", () => {
    const commandId = CommandId.make("stable-command");
    return buildPiNativeStart({ cwd: "/tmp", commandId }).pipe(
      Effect.provideService(Crypto.Crypto, testCrypto),
      Effect.tap((command) => Effect.sync(() => expect(command.commandId).toBe(commandId))),
      Effect.asVoid,
    );
  });
});
