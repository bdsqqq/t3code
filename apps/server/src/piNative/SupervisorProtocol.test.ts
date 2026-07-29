import { describe, expect, it } from "@effect/vitest";
import { JsonLineDecoder, encodeLine } from "./SupervisorProtocol.ts";

describe("SupervisorProtocol", () => {
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
});
