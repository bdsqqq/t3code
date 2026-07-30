import { PiNativeEventId, PiNativeRuntimeId } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import {
  projectOverlayPayload,
  projectQueuePayload,
  projectReplayItem,
  queuePayloadHasPending,
  shouldUseSnapshot,
} from "./SupervisorDaemon.ts";

describe("native Pi replay projection", () => {
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
  });
});
