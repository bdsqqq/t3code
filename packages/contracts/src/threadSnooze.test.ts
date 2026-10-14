import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { OrchestrationV2Command, ThreadSnoozeCondition } from "./orchestrationV2.ts";

describe("conditional snooze contracts", () => {
  const base = { type: "thread.snooze", commandId: "snooze:1", threadId: "thread:1" };
  const condition = { type: "thread-settled", threadId: "thread:2" };
  const decode = Schema.decodeUnknownSync(OrchestrationV2Command);
  it("accepts either the legacy timer or a condition, never both or neither", () => {
    expect(decode({ ...base, snoozedUntil: "2099-01-01T00:00:00.000Z" })).toMatchObject(base);
    expect(decode({ ...base, condition })).toMatchObject({ condition });
    expect(() => decode(base)).toThrow();
    expect(() =>
      decode({ ...base, condition, snoozedUntil: "2099-01-01T00:00:00.000Z" }),
    ).toThrow();
  });
  it("uses the pull request key validators", () => {
    const decodeCondition = Schema.decodeUnknownSync(ThreadSnoozeCondition);
    const pr = {
      type: "pull-request-merged",
      host: "github.com",
      repository: "org/repo",
      number: 42,
    };
    expect(decodeCondition(pr)).toEqual(pr);
    for (const patch of [{ host: "" }, { repository: "" }, { number: 0 }, { number: 1.5 }]) {
      expect(() => decodeCondition({ ...pr, ...patch })).toThrow();
    }
  });
  it("does not expose the guarded wake as a client command", () => {
    expect(() =>
      decode({
        type: "thread.snooze.wake",
        commandId: "wake:1",
        threadId: "thread:1",
        expectedSnoozeCommandId: "snooze:1",
      }),
    ).toThrow();
  });
});
