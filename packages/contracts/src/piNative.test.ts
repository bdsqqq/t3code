import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { PI_THREAD_LIFECYCLE_CUSTOM_TYPE, PiThreadLifecycleCustomEntry } from "./piNative.ts";

describe("Pi thread lifecycle contract", () => {
  it("decodes the versioned custom JSONL entry", () => {
    expect(
      Schema.decodeUnknownSync(PiThreadLifecycleCustomEntry)({
        type: "custom",
        id: "entry-1",
        parentId: "message-1",
        timestamp: "2026-08-08T10:00:00.000Z",
        customType: PI_THREAD_LIFECYCLE_CUSTOM_TYPE,
        data: {
          version: 1,
          sessionId: "session-1",
          override: "settled",
          operationId: "operation-1",
        },
      }).data.override,
    ).toBe("settled");
  });

  it("rejects unknown lifecycle versions", () => {
    expect(() =>
      Schema.decodeUnknownSync(PiThreadLifecycleCustomEntry)({
        type: "custom",
        id: "entry-1",
        parentId: null,
        timestamp: "2026-08-08T10:00:00.000Z",
        customType: PI_THREAD_LIFECYCLE_CUSTOM_TYPE,
        data: {
          version: 2,
          sessionId: "session-1",
          override: "settled",
          operationId: "operation-1",
        },
      }),
    ).toThrow();
  });
});
