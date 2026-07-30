import { describe, expect, it } from "@effect/vitest";

import {
  CommandDeduper,
  JsonLineDecoder,
  messageText,
  parseBridgeCommand,
} from "./t3-control-v2.ts";

describe("t3-control-v2 extension protocol", () => {
  it("extracts exact delivered user text for queue reconciliation", () => {
    expect(
      messageText({
        role: "user",
        content: [
          { type: "text", text: "first" },
          { type: "text", text: " second" },
        ],
      }),
    ).toBe("first second");
  });

  it("decodes complete LF-delimited JSON while retaining a partial line", () => {
    const decoder = new JsonLineDecoder();

    expect(decoder.push('{"type":"command","commandId":"1",')).toEqual([]);
    expect(decoder.push('"command":"abort"}\ninvalid\n\n')).toEqual([
      { type: "command", commandId: "1", command: "abort" },
      undefined,
    ]);
  });

  it("accepts only supported command shapes", () => {
    expect(
      parseBridgeCommand({ type: "command", commandId: "1", command: "send", text: "hello" }),
    ).toEqual({
      type: "command",
      commandId: "1",
      command: "send",
      text: "hello",
    });
    expect(
      parseBridgeCommand({ type: "command", commandId: "2", command: "steer" }),
    ).toBeUndefined();
    expect(
      parseBridgeCommand({ type: "command", commandId: "3", command: "unknown" }),
    ).toBeUndefined();
  });

  it("accepts each command ID once for the process lifetime", () => {
    const deduper = new CommandDeduper();

    expect(deduper.accept("stable-id")).toBe(true);
    expect(deduper.accept("stable-id")).toBe(false);
    expect(deduper.accept("another-id")).toBe(true);
  });
});
