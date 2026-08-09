import type { OrchestrationThread } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { threadAllows } from "./threads.ts";

const externalPiThread = {
  backing: {
    kind: "external",
    source: "pi",
    sourceKey: "source",
    control: "readOnly",
    capabilities: {
      send: false,
      attachments: false,
      streamingBehaviors: [],
      interrupt: false,
      stop: false,
      rename: false,
      archive: false,
      settle: true,
      unsettle: true,
      delete: false,
      changeModel: false,
      changeRuntimeMode: false,
      changeInteractionMode: false,
      checkpoints: false,
    },
  },
} as Pick<OrchestrationThread, "backing">;

describe("threadAllows", () => {
  it("allows explicit Pi settlement without enabling generic lifecycle actions", () => {
    expect(threadAllows(externalPiThread, "settle")).toBe(true);
    expect(threadAllows(externalPiThread, "unsettle")).toBe(true);
    expect(threadAllows(externalPiThread, "lifecycle")).toBe(false);
  });
});
