import { describe, expect, it } from "@effect/vitest";

import {
  CommandDeduper,
  JsonLineDecoder,
  lifecycleCommandError,
  messageText,
  parseBridgeCommand,
  takePendingMessageId,
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
      parseBridgeCommand({
        type: "command",
        commandId: "1",
        command: "send",
        text: "hello",
        messageId: "message-1",
      }),
    ).toEqual({
      type: "command",
      commandId: "1",
      command: "send",
      text: "hello",
      messageId: "message-1",
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
    expect(
      parseBridgeCommand({
        type: "command",
        commandId: "6",
        command: "send",
        text: "hello",
        messageId: "",
      }),
    ).toBeUndefined();
  });

  it("correlates repeated prompt text in command order", () => {
    const pending = [
      { messageId: "message-1", text: "same", behavior: "followUp" as const },
      { messageId: "message-2", text: "other", behavior: "send" as const },
      { messageId: "message-3", text: "same", behavior: "steer" as const },
    ];

    expect(takePendingMessageId(pending, "same", "steer")).toBe("message-3");
    expect(takePendingMessageId(pending, "same", "followUp")).toBe("message-1");
    expect(takePendingMessageId(pending, "missing")).toBeUndefined();
    expect(pending).toEqual([{ messageId: "message-2", text: "other", behavior: "send" }]);
  });

  it("correlates transformed prompt text by delivery behavior", () => {
    const pending = [{ messageId: "message-1", text: "/template", behavior: "followUp" as const }];

    expect(takePendingMessageId(pending, "expanded prompt", "followUp")).toBe("message-1");
  });

  it("does not assign a later T3 id to an earlier local delivery", () => {
    const pending = [
      { text: "/local-template", behavior: "steer" as const },
      { messageId: "message-1", text: "remote", behavior: "steer" as const },
    ];

    expect(takePendingMessageId(pending, "expanded local prompt", "steer")).toBeUndefined();
    expect(takePendingMessageId(pending, "remote", "steer")).toBe("message-1");
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
