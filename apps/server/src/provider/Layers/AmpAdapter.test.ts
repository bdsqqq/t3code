import * as NodeAssert from "node:assert/strict";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import type { AmpCliMessage, AmpCliRunner, AmpCliRunOptions } from "../amp/AmpCliRunner.ts";
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { makeAmpAdapter } from "./AmpAdapter.ts";

const instanceId = ProviderInstanceId.make("amp-test");
const threadId = ThreadId.make("thread");
const cwd = process.cwd();
const selection = createModelSelection(instanceId, "medium");
type Adapter = ProviderAdapterShape<ProviderAdapterError>;

const init = (id = "T-amp-test"): AmpCliMessage => ({
  type: "system",
  subtype: "init",
  session_id: id,
  cwd,
});
const result = (id = "T-amp-test"): AmpCliMessage => ({
  type: "result",
  subtype: "success",
  session_id: id,
  is_error: false,
  result: "done",
});

class ScriptedRunner implements AmpCliRunner {
  readonly calls: AmpCliRunOptions[] = [];
  private readonly scripts: ReadonlyArray<ReadonlyArray<AmpCliMessage>>;

  constructor(scripts: ReadonlyArray<ReadonlyArray<AmpCliMessage>>) {
    this.scripts = scripts;
  }

  run = (options: AmpCliRunOptions) => {
    const index = this.calls.length;
    this.calls.push(options);
    return Stream.fromIterable(this.scripts[index] ?? []);
  };
}

class QueueRunner implements AmpCliRunner {
  readonly calls: AmpCliRunOptions[] = [];
  readonly queues: Queue.Queue<AmpCliMessage>[] = [];

  run = (options: AmpCliRunOptions) => {
    this.calls.push(options);
    return Stream.unwrap(
      Queue.unbounded<AmpCliMessage>().pipe(
        Effect.tap((queue) => Effect.sync(() => this.queues.push(queue))),
        Effect.map(Stream.fromQueue),
      ),
    );
  };
}

const withAdapter = <A>(
  runner: AmpCliRunner,
  use: (adapter: Adapter) => Effect.Effect<A, ProviderAdapterError>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const adapter = yield* makeAmpAdapter({
        binaryPath: "/custom/bin/amp",
        providerInstanceId: instanceId,
        environment: { AMP_API_KEY: "test" },
        allowedModes: new Set(["low", "medium", "high", "ultra"]),
        runner,
      });
      return yield* use(adapter);
    }),
  ).pipe(Effect.provide(NodeServices.layer));

const start = (adapter: Adapter, resumeCursor?: unknown) =>
  adapter.startSession({
    provider: ProviderDriverKind.make("amp"),
    providerInstanceId: instanceId,
    threadId,
    cwd,
    runtimeMode: "full-access",
    ...(resumeCursor ? { resumeCursor } : {}),
  });

const collectTurn = (adapter: Adapter) =>
  adapter.streamEvents.pipe(
    Stream.takeUntil((event) => event.type === "turn.completed"),
    Stream.runCollect,
    Effect.forkChild,
  );

describe("AmpAdapter", () => {
  it.effect("establishes a cursor and translates text, thinking, and tool lifecycle events", () => {
    const runner = new ScriptedRunner([
      [
        init(),
        {
          type: "assistant",
          session_id: "T-amp-test",
          message: {
            content: [
              { type: "thinking", thinking: "checking" },
              { type: "tool_use", id: "tool-1", name: "shell_command", input: { command: "pwd" } },
            ],
          },
        },
        {
          type: "user",
          session_id: "T-amp-test",
          message: {
            content: [
              {
                type: "tool_result",
                tool_use_id: "tool-1",
                content: "/repo",
                is_error: false,
              },
            ],
          },
        },
        {
          type: "assistant",
          session_id: "T-amp-test",
          message: { content: [{ type: "text", text: "done" }] },
        },
        result(),
      ],
    ]);

    return withAdapter(runner, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const collecting = yield* collectTurn(adapter);
        const turn = yield* adapter.sendTurn({
          threadId,
          input: "inspect the repository",
          modelSelection: selection,
        });
        const events = Array.from(yield* Fiber.join(collecting));

        NodeAssert.deepEqual(turn.resumeCursor, { schemaVersion: 1, threadId: "T-amp-test" });
        NodeAssert.equal(runner.calls[0]?.binaryPath, "/custom/bin/amp");
        NodeAssert.equal(runner.calls[0]?.mode, "medium");
        NodeAssert.equal(runner.calls[0]?.env?.AMP_API_KEY, "test");
        NodeAssert.equal(
          events
            .filter((event) => event.type === "content.delta")
            .map((event) => (event.type === "content.delta" ? event.payload.delta : ""))
            .join(""),
          "checkingdone",
        );
        NodeAssert.equal(
          events.some(
            (event) =>
              event.type === "item.completed" &&
              event.payload.itemType === "dynamic_tool_call" &&
              event.payload.status === "completed",
          ),
          true,
        );
        NodeAssert.equal(events.at(-1)?.type, "turn.completed");
        NodeAssert.equal(
          events.every((event) => event.raw?.source === "amp.cli.stream-json" || !event.raw),
          true,
        );
      }),
    );
  });

  it.effect("continues the exact persisted Amp thread", () => {
    const runner = new ScriptedRunner([[init("T-existing"), result("T-existing")]]);
    return withAdapter(runner, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter, { schemaVersion: 1, threadId: "T-existing" });
        yield* adapter.sendTurn({ threadId, input: "continue", modelSelection: selection });
        NodeAssert.equal(runner.calls[0]?.continueThreadId, "T-existing");
      }),
    );
  });

  it.effect("rejects invalid selections without poisoning the next turn", () => {
    const runner = new ScriptedRunner([[init(), result()]]);
    return withAdapter(runner, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const rejected = yield* adapter
          .sendTurn({
            threadId,
            input: "bad mode",
            modelSelection: createModelSelection(instanceId, "unknown"),
          })
          .pipe(Effect.result);
        NodeAssert.equal(rejected._tag, "Failure");
        NodeAssert.equal(runner.calls.length, 0);

        yield* adapter.sendTurn({ threadId, input: "valid", modelSelection: selection });
        NodeAssert.equal(runner.calls.length, 1);
      }),
    );
  });

  it.effect("fails the handshake when the first message is not system/init", () => {
    const runner = new ScriptedRunner([
      [
        {
          type: "assistant",
          session_id: "T-amp-test",
          message: { content: [{ type: "text", text: "wrong order" }] },
        },
      ],
    ]);
    return withAdapter(runner, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const rejected = yield* adapter
          .sendTurn({ threadId, input: "hello", modelSelection: selection })
          .pipe(Effect.result);
        NodeAssert.equal(rejected._tag, "Failure");
      }),
    );
  });

  it.effect("rejects stale cancellation and interrupts the matching active turn", () => {
    const runner = new QueueRunner();
    return withAdapter(runner, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const collecting = yield* collectTurn(adapter);
        const sending = yield* adapter
          .sendTurn({ threadId, input: "wait", modelSelection: selection })
          .pipe(Effect.forkChild);
        while (!runner.queues[0]) yield* Effect.yieldNow;
        yield* Queue.offer(runner.queues[0]!, init());
        const turn = yield* Fiber.join(sending);

        const stale = yield* adapter
          .interruptTurn(threadId, TurnId.make("stale"))
          .pipe(Effect.result);
        NodeAssert.equal(stale._tag, "Failure");
        yield* adapter.interruptTurn(threadId, turn.turnId);

        const events: ProviderRuntimeEvent[] = Array.from(yield* Fiber.join(collecting));
        const completed = events.find((event) => event.type === "turn.completed");
        NodeAssert.equal(completed?.type, "turn.completed");
        if (completed?.type === "turn.completed")
          NodeAssert.equal(completed.payload.state, "interrupted");
        NodeAssert.equal(
          events.some((event) => event.type === "runtime.error"),
          false,
        );
      }),
    );
  });

  it.effect("treats cancellation before initialization as interruption", () => {
    const runner = new QueueRunner();
    return withAdapter(runner, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const collecting = yield* collectTurn(adapter);
        const sending = yield* adapter
          .sendTurn({ threadId, input: "wait before init", modelSelection: selection })
          .pipe(Effect.forkChild);
        while (!runner.queues[0]) yield* Effect.yieldNow;
        const activeTurnId = (yield* adapter.listSessions())[0]?.activeTurnId;
        NodeAssert.ok(activeTurnId);

        yield* adapter.interruptTurn(threadId, activeTurnId);
        const sendExit = yield* Fiber.await(sending);
        NodeAssert.equal(Exit.isFailure(sendExit), true);
        if (Exit.isFailure(sendExit))
          NodeAssert.equal(Cause.hasInterruptsOnly(sendExit.cause), true);

        const events: ProviderRuntimeEvent[] = Array.from(yield* Fiber.join(collecting));
        const completed = events.filter((event) => event.type === "turn.completed");
        NodeAssert.equal(completed.length, 1);
        if (completed[0]?.type === "turn.completed")
          NodeAssert.equal(completed[0].payload.state, "interrupted");
        NodeAssert.equal(
          events.some((event) => event.type === "runtime.error"),
          false,
        );
      }),
    );
  });
});
