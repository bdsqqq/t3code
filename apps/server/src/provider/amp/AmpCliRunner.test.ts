import assert from "node:assert/strict";

import { describe, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import { AmpCliMessage, buildAmpCliArgs } from "./AmpCliRunner.ts";

const base = {
  binaryPath: "/custom/bin/amp",
  cwd: "/repo",
  prompt: "hello",
  mode: "medium",
} as const;

describe("AmpCliRunner", () => {
  it("builds deterministic fresh and continuation commands", () => {
    assert.deepEqual(buildAmpCliArgs(base), [
      "--no-ide",
      "--no-notifications",
      "--execute",
      "--stream-json-thinking",
      "--no-archive-after-execute",
      "--mode",
      "medium",
    ]);
    assert.deepEqual(buildAmpCliArgs({ ...base, continueThreadId: "T-existing", effort: "high" }), [
      "threads",
      "continue",
      "T-existing",
      "--no-ide",
      "--no-notifications",
      "--execute",
      "--stream-json-thinking",
      "--no-archive-after-execute",
      "--mode",
      "medium",
      "--effort",
      "high",
    ]);
  });

  it("decodes the stream-json messages used by the adapter", () => {
    const decode = Schema.decodeUnknownSync(AmpCliMessage);
    assert.equal(
      decode({ type: "system", subtype: "init", session_id: "T-test", cwd: "/repo" }).type,
      "system",
    );
    assert.equal(
      decode({
        type: "assistant",
        session_id: "T-test",
        message: { content: [{ type: "thinking", thinking: "hmm" }] },
      }).type,
      "assistant",
    );
    assert.throws(() => decode({ type: "assistant", session_id: "T-test" }));
  });
});
