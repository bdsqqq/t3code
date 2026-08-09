import { describe, expect, it } from "@effect/vitest";

import {
  CommandDeduper,
  JsonLineDecoder,
  lifecycleCommandError,
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
    expect(
      parseBridgeCommand({
        type: "command",
        commandId: "4",
        command: "setLifecycle",
        lifecycle: {
          version: 1,
          sessionId: "session-1",
          override: "settled",
          operationId: "operation-1",
        },
      }),
    ).toMatchObject({
      command: "setLifecycle",
      lifecycle: { override: "settled" },
    });
    expect(
      parseBridgeCommand({
        type: "command",
        commandId: "5",
        command: "setLifecycle",
        lifecycle: {
          version: 2,
          sessionId: "session-1",
          override: "settled",
          operationId: "operation-1",
        },
      }),
    ).toBeUndefined();
  });

  it("accepts each command ID once for the process lifetime", () => {
    const deduper = new CommandDeduper();

    expect(deduper.accept("stable-id")).toBe(true);
    expect(deduper.accept("stable-id")).toBe(false);
    expect(deduper.accept("another-id")).toBe(true);
  });

  it("rejects settling a busy or different Pi session", () => {
    const command = parseBridgeCommand({
      type: "command",
      commandId: "lifecycle",
      command: "setLifecycle",
      lifecycle: {
        version: 1,
        sessionId: "session-1",
        override: "settled",
        operationId: "operation-1",
      },
    });
    if (command?.command !== "setLifecycle") {
      throw new Error("expected lifecycle command");
    }

    expect(lifecycleCommandError(command, "session-1", false)).toContain("running");
    expect(lifecycleCommandError(command, "session-2", true)).toContain("does not match");
    expect(lifecycleCommandError(command, "session-1", true)).toBeUndefined();
  });
});
