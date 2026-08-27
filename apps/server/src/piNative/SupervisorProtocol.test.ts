import { describe, expect, it } from "@effect/vitest";
import {
  JsonLineDecoder,
  SupervisorStreamBuffer,
  encodeLine,
  type SupervisorCommand,
} from "./SupervisorProtocol.ts";

describe("SupervisorProtocol", () => {
  it("keeps resume-and-send streaming intent in the supervisor command", () => {
    const command = {
      type: "resumeAndSend",
      commandId: "takeover-1" as never,
      sessionKey: "source-key" as never,
      sessionFile: "/sessions/session.jsonl",
      cwd: "/workspace",
      message: "continue",
      streamingBehavior: "steer",
    } satisfies SupervisorCommand;

    expect(command.streamingBehavior).toBe("steer");
  });

  it("decodes fragmented LF JSON frames", () => {
    const decoder = new JsonLineDecoder();
    expect(decoder.push('{"type":"eve')).toEqual([]);
    expect(
      decoder.push('nt","sequence":1}\n' + encodeLine({ type: "synchronized", sequence: 1 })),
    ).toEqual([
      { type: "event", sequence: 1 },
      { type: "synchronized", sequence: 1 },
    ]);
  });

  it("drops malformed frames without corrupting the next frame", () => {
    const decoder = new JsonLineDecoder();
    expect(decoder.push("nope\n" + encodeLine({ type: "receipt", status: "completed" }))).toEqual([
      { type: "receipt", status: "completed" },
    ]);
  });

  it("rejects an unterminated frame beyond the serialized-byte ceiling", () => {
    const decoder = new JsonLineDecoder(8);
    expect(() => decoder.push('{"value":')).toThrow("serialized-byte ceiling");
  });

  it("bounds queued stream items by serialized utf8 bytes", () => {
    const buffer = new SupervisorStreamBuffer(150);
    const item = {
      type: "synchronized",
      runtimeId: "runtime" as never,
      sequence: 1,
    } as const;

    expect(buffer.push(item)).toBe(true);
    expect(buffer.push({ ...item, runtimeId: "🫠".repeat(20) as never })).toBe(false);
    expect(buffer.shift()).toEqual(item);
    expect(buffer.push({ ...item, runtimeId: "🫠".repeat(20) as never })).toBe(true);
  });
});
