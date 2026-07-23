import * as NodeAssert from "node:assert/strict";

import { describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import {
  AmpCliMessage,
  AmpCliTransportError,
  buildAmpCliArgs,
  makeAmpCliRunner,
} from "./AmpCliRunner.ts";

const base = {
  binaryPath: "/custom/bin/amp",
  cwd: "/repo",
  prompt: "hello",
  mode: "medium",
} as const;

const line = (value: unknown) => `${JSON.stringify(value)}\n`;
const init = { type: "system", subtype: "init", session_id: "T-test", cwd: "/repo" };
const result = {
  type: "result",
  subtype: "success",
  session_id: "T-test",
  is_error: false,
  result: "done",
};
const isTransportError = Schema.is(AmpCliTransportError);
const makeSpawner = (stdout: string, code = 0, stderr = "") =>
  ChildProcessSpawner.make(() =>
    Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(code)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.encodeText(Stream.make(stdout)),
        stderr: Stream.encodeText(Stream.make(stderr)),
        all: Stream.empty,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
      }),
    ),
  );
const runWith = (stdout: string, code = 0, stderr = "") =>
  Effect.gen(function* () {
    const runner = yield* makeAmpCliRunner();
    return yield* runner.run(base).pipe(Stream.runCollect, Effect.result);
  }).pipe(
    Effect.provideService(
      ChildProcessSpawner.ChildProcessSpawner,
      makeSpawner(stdout, code, stderr),
    ),
  );

describe("AmpCliRunner", () => {
  it("builds deterministic fresh and continuation commands", () => {
    NodeAssert.deepEqual(buildAmpCliArgs(base), [
      "--no-ide",
      "--no-notifications",
      "--execute",
      "--stream-json-thinking",
      "--no-archive-after-execute",
      "--mode",
      "medium",
    ]);
    NodeAssert.deepEqual(buildAmpCliArgs({ ...base, continueThreadId: "T-existing" }), [
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
    ]);
  });

  it("decodes the stream-json messages used by the adapter", () => {
    const decode = Schema.decodeUnknownSync(AmpCliMessage);
    NodeAssert.equal(
      decode({ type: "system", subtype: "init", session_id: "T-test", cwd: "/repo" }).type,
      "system",
    );
    NodeAssert.equal(
      decode({
        type: "assistant",
        session_id: "T-test",
        message: { content: [{ type: "thinking", thinking: "hmm" }] },
      }).type,
      "assistant",
    );
    NodeAssert.throws(() => decode({ type: "assistant", session_id: "T-test" }));
  });

  it.effect("publishes the result only after validating process completion", () =>
    Effect.gen(function* () {
      const completed = yield* runWith(line(init) + line(result));
      NodeAssert.equal(completed._tag, "Success");
      if (Result.isSuccess(completed))
        NodeAssert.deepEqual(
          Array.from(completed.success).map((message) => message.type),
          ["system", "result"],
        );

      const failed = yield* runWith(line(init) + line(result), 2, "late failure");
      NodeAssert.equal(failed._tag, "Failure");
      if (Result.isFailure(failed)) {
        NodeAssert.equal(isTransportError(failed.failure), true);
        if (isTransportError(failed.failure)) {
          NodeAssert.equal(failed.failure.exitCode, 2);
          NodeAssert.equal(failed.failure.stderr, "late failure");
        }
      }
    }),
  );

  it.effect("rejects missing and duplicate terminal results", () =>
    Effect.gen(function* () {
      const missing = yield* runWith(line(init));
      NodeAssert.equal(missing._tag, "Failure");
      if (Result.isFailure(missing) && isTransportError(missing.failure))
        NodeAssert.equal(missing.failure.detail, "Amp CLI exited without a result");

      const duplicate = yield* runWith(line(init) + line(result) + line(result));
      NodeAssert.equal(duplicate._tag, "Failure");
      if (Result.isFailure(duplicate) && isTransportError(duplicate.failure))
        NodeAssert.equal(duplicate.failure.detail, "Amp CLI emitted multiple results");
    }),
  );
});
