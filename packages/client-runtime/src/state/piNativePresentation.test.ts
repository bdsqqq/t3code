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
});
