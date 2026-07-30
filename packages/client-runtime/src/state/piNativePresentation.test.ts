import { describe, expect, it } from "@effect/vitest";
import { presentPiNativeUnknown } from "./piNativePresentation.ts";

describe("native pi event presentation", () => {
  it("extracts nested text deltas", () => {
    expect(
      presentPiNativeUnknown({
        type: "message_update",
        event: { type: "text_delta", delta: "hi" },
      }),
    ).toEqual({ label: "text_delta", text: "hi" });
  });

  it("extracts tool results without unsafe casts", () => {
    expect(
      presentPiNativeUnknown({ type: "tool_result", toolName: "read", result: { ok: true } }),
    ).toEqual({ label: "tool · read", text: '{\n  "ok": true\n}' });
  });

  it("presents pending steering and follow-up intents", () => {
    const queue = {
      steering: ["fix the failing test"],
      followUp: ["summarize the change"],
    };
    const expected = {
      label: "pending · 2",
      text: "steer: fix the failing test\nfollow-up: summarize the change",
    };
    expect(presentPiNativeUnknown({ type: "queue_update", ...queue })).toEqual(expected);
    expect(
      presentPiNativeUnknown({
        type: "event",
        event: "queue_update",
        data: queue,
      }),
    ).toEqual(expected);
  });
});
