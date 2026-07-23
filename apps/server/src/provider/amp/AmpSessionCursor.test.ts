import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import { AmpSessionCursor } from "./AmpSessionCursor.ts";

describe("AmpSessionCursor", () => {
  it("decodes the persisted thread cursor", () => {
    expect(
      Schema.decodeUnknownSync(AmpSessionCursor)({ schemaVersion: 1, threadId: "T-abc_123" }),
    ).toEqual({ schemaVersion: 1, threadId: "T-abc_123" });
  });

  it("rejects non-Amp thread ids", () => {
    expect(() =>
      Schema.decodeUnknownSync(AmpSessionCursor)({ schemaVersion: 1, threadId: "abc" }),
    ).toThrow();
  });
});
