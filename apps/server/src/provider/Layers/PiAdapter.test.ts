// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as NodeAssert from "node:assert/strict";

import {
  PiRpcCommandError,
  type PiRpcClient,
  type PiRpcImage,
  PiRpcProtocolError,
  type PiRpcSpawnOptions,
} from "../pi/PiRpcClient.ts";
import type { PiRpcEvent, PiThinkingLevel } from "../pi/PiRpcSchema.ts";
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { makePiAdapter, type PiRpcClientFactory } from "./PiAdapter.ts";

const assert: typeof NodeAssert = NodeAssert;
const fs = NodeFS;
const os = NodeOS;
const path = NodePath;
const instanceId = ProviderInstanceId.make("pi-test");
const modelSelection = createModelSelection(instanceId, "openai/gpt-5", [
  { id: "thinkingLevel", value: "max" },
]);
type Adapter = ProviderAdapterShape<ProviderAdapterError>;
type ToolLifecycleEvent = Extract<
  ProviderRuntimeEvent,
  { type: "item.started" | "item.updated" | "item.completed" }
>;

const isToolLifecycleEvent = (event: ProviderRuntimeEvent): event is ToolLifecycleEvent =>
  event.type === "item.started" || event.type === "item.updated" || event.type === "item.completed";

const toolCallIdOf = (event: ToolLifecycleEvent): string | undefined => {
  const data = event.payload.data;
  if (typeof data !== "object" || data === null) return undefined;
  const toolCallId = Reflect.get(data, "toolCallId");
  return typeof toolCallId === "string" ? toolCallId : undefined;
};

const toolEventsByCallId = (events: ReadonlyArray<ProviderRuntimeEvent>) => {
  const byCallId = new Map<string, ToolLifecycleEvent[]>();
  for (const event of events) {
    if (!isToolLifecycleEvent(event)) continue;
    const toolCallId = toolCallIdOf(event);
    if (!toolCallId) continue;
    const toolEvents = byCallId.get(toolCallId) ?? [];
    toolEvents.push(event);
    byCallId.set(toolCallId, toolEvents);
  }
  return byCallId;
};

const completesToolCall = (toolCallId: string) => (event: ProviderRuntimeEvent) =>
  event.type === "item.completed" && toolCallIdOf(event) === toolCallId;

class FakeClient implements PiRpcClient {
  // oxlint-disable-next-line t3code/no-manual-effect-runtime-in-tests -- The synchronous fake exposes its queue through the PiRpcClient stream interface.
  input = Effect.runSync(Queue.unbounded<PiRpcEvent>());
  events: Stream.Stream<PiRpcEvent> = Stream.fromQueue(this.input);
  readonly calls = {
    close: 0,
    abort: 0,
    prompt: 0,
    prompts: [] as Array<{
      message: string;
      images: ReadonlyArray<PiRpcImage> | undefined;
      streamingBehavior?: "steer" | "followUp";
    }>,
    thinking: [] as PiThinkingLevel[],
  };
  state: { sessionFile?: string; sessionId?: string; isStreaming?: boolean } = {};
  getStateResults: Array<typeof this.state> = [];
  failPrompt = false;
  fatalPrompt = false;
  abortBeforeSettle = false;
  getStateEntered: Deferred.Deferred<void> | undefined;
  getStateGate: Deferred.Deferred<void> | undefined;
  getAvailableModelsEntered: Deferred.Deferred<void> | undefined;
  getAvailableModelsGate: Deferred.Deferred<void> | undefined;
  promptEntered: Deferred.Deferred<void> | undefined;
  promptGate: Deferred.Deferred<void> | undefined;
  closeEntered: Deferred.Deferred<void> | undefined;
  closeGate: Deferred.Deferred<void> | undefined;

  getState = () => {
    const self = this;
    return Effect.gen(function* () {
      if (self.getStateEntered) yield* Deferred.succeed(self.getStateEntered, undefined);
      if (self.getStateGate) yield* Deferred.await(self.getStateGate);
      return self.getStateResults.shift() ?? self.state;
    });
  };
  getAvailableModels = () => {
    const self = this;
    return Effect.gen(function* () {
      if (self.getAvailableModelsEntered)
        yield* Deferred.succeed(self.getAvailableModelsEntered, undefined);
      if (self.getAvailableModelsGate) yield* Deferred.await(self.getAvailableModelsGate);
      return { models: [{ provider: "openai", id: "gpt-5", reasoning: true }] };
    });
  };
  setModel = (provider: string, id: string) => Effect.succeed({ provider, id });
  setThinkingLevel = (level: PiThinkingLevel) =>
    Effect.sync(() => {
      this.calls.thinking.push(level);
    });
  prompt = (
    message: string,
    images?: ReadonlyArray<PiRpcImage>,
    streamingBehavior?: "steer" | "followUp",
  ) => {
    const self = this;
    return Effect.gen(function* () {
      self.calls.prompt += 1;
      self.calls.prompts.push({
        message,
        images,
        ...(streamingBehavior ? { streamingBehavior } : {}),
      });
      if (self.promptEntered) yield* Deferred.succeed(self.promptEntered, undefined);
      if (self.promptGate) yield* Deferred.await(self.promptGate);
      if (self.failPrompt)
        return yield* new PiRpcCommandError({
          command: "prompt",
          requestId: "test",
          detail: "prompt failed",
        });
      if (self.fatalPrompt)
        return yield* new PiRpcProtocolError({ detail: "prompt transport failed" });
    });
  };
  abort = () => {
    const self = this;
    return Effect.gen(function* () {
      self.calls.abort += 1;
      if (self.abortBeforeSettle) yield* Queue.offer(self.input, { type: "agent_settled" });
    });
  };
  close = () => {
    const self = this;
    return Effect.gen(function* () {
      self.calls.close += 1;
      if (self.closeEntered) yield* Deferred.succeed(self.closeEntered, undefined);
      if (self.closeGate) yield* Deferred.await(self.closeGate);
    });
  };
}

interface Harness {
  readonly client: FakeClient;
  readonly spawns: PiRpcSpawnOptions[];
  readonly stateDir: string;
  readonly attachmentsDir: string;
  readonly makeClient: PiRpcClientFactory;
}

const collectThroughSentinel = Effect.fn("PiAdapterTest.collectThroughSentinel")(function* (
  adapter: Adapter,
) {
  let sentinelTurnId: string | undefined;
  const collected = yield* adapter.streamEvents.pipe(
    Stream.takeUntil((event) => event.type === "turn.completed" && event.turnId === sentinelTurnId),
    Stream.runCollect,
    Effect.forkChild,
  );
  return {
    collected,
    setSentinel: (turnId: string) => {
      sentinelTurnId = turnId;
    },
  };
});

const makeHarness = (harnessOptions: { readonly failStart?: boolean } = {}): Harness => {
  const client = new FakeClient();
  const spawns: PiRpcSpawnOptions[] = [];
  const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "t3-pi-adapter-"));
  const attachmentsDir = path.join(stateDir, "attachments");
  fs.mkdirSync(attachmentsDir, { recursive: true });
  const makeClient: PiRpcClientFactory = (spawnOptions) =>
    Effect.gen(function* () {
      spawns.push(spawnOptions);
      if (harnessOptions.failStart) {
        return yield* new PiRpcCommandError({
          command: "spawn",
          requestId: "test",
          detail: "spawn failed",
        });
      }
      const sessionIndex = spawnOptions.args?.indexOf("--session") ?? -1;
      const sessionFile = spawnOptions.args?.[sessionIndex + 1];
      assert.ok(sessionFile);
      const sessionId = "pi-generated-session-id";
      fs.writeFileSync(
        sessionFile,
        `{"type":"session","id":"${sessionId}","cwd":"${spawnOptions.cwd}"}\n`,
      );
      client.state = { sessionFile, sessionId };
      return client;
    });
  return { client, spawns, stateDir, attachmentsDir, makeClient };
};

const withAdapter = <A>(
  harness: Harness,
  use: (adapter: Adapter) => Effect.Effect<A, ProviderAdapterError>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const adapter = yield* makePiAdapter({
        binaryPath: "pi",
        providerInstanceId: instanceId,
        stateDir: harness.stateDir,
        attachmentsDir: harness.attachmentsDir,
        makeRpcClient: harness.makeClient,
      });
      return yield* use(adapter);
    }),
  ).pipe(Effect.provide(NodeServices.layer));

const start = (adapter: Adapter, id = "thread") =>
  adapter.startSession({
    provider: ProviderDriverKind.make("pi"),
    providerInstanceId: instanceId,
    threadId: ThreadId.make(id),
    cwd: process.cwd(),
    runtimeMode: "full-access",
  });

describe("PiAdapter", () => {
  it.effect("allocates an exact durable session and rejects non-full-access before spawn", () => {
    const h = makeHarness();
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        const rejected = yield* adapter
          .startSession({
            threadId: ThreadId.make("rejected"),
            runtimeMode: "approval-required",
          })
          .pipe(Effect.result);
        assert.equal(rejected._tag, "Failure");
        if (rejected._tag === "Failure")
          assert.equal(rejected.failure._tag, "ProviderAdapterValidationError");
        assert.equal(h.spawns.length, 0);

        const session = yield* start(adapter);
        const cursor = session.resumeCursor as {
          schemaVersion: number;
          sessionFile: string;
          sessionId: string;
        };
        assert.equal(cursor.schemaVersion, 1);
        assert.equal(h.client.state.sessionFile, cursor.sessionFile);
        assert.equal(h.client.state.sessionId, cursor.sessionId);
        const args = h.spawns[0]?.args ?? [];
        assert.deepEqual(args.slice(args.indexOf("--session"), args.indexOf("--session") + 2), [
          "--session",
          cursor.sessionFile,
        ]);
        assert.equal(args.includes("--no-session"), false);
        assert.equal(args.includes("--offline"), true);
        for (const arg of [
          "--no-context-files",
          "--no-extensions",
          "--no-skills",
          "--no-prompt-templates",
        ])
          assert.equal(args.includes(arg), false);
      }),
    );
  });

  it.effect("sends persisted image attachments through Pi RPC", () => {
    const h = makeHarness();
    const attachmentId = "thread-image";
    fs.writeFileSync(path.join(h.attachmentsDir, `${attachmentId}.png`), Buffer.from("image"));
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          attachments: [
            {
              type: "image",
              id: attachmentId,
              name: "screenshot.png",
              mimeType: "image/png",
              sizeBytes: 5,
            },
          ],
          modelSelection,
        });
        assert.deepEqual(h.client.calls.prompts, [
          {
            message: "",
            images: [
              {
                type: "image",
                data: Buffer.from("image").toString("base64"),
                mimeType: "image/png",
              },
            ],
          },
        ]);
        yield* Queue.offer(h.client.input, { type: "agent_settled" });
      }),
    );
  });

  it.effect("projects Pi built-in tools into useful canonical tool call details", () => {
    const h = makeHarness();
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const collected = yield* adapter.streamEvents.pipe(
          Stream.takeUntil(completesToolCall("edit-1")),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          input: "inspect and edit",
          modelSelection,
        });
        yield* Queue.offerAll(h.client.input, [
          {
            type: "tool_execution_start",
            toolCallId: "bash-1",
            toolName: "bash",
            args: { cmd: "git status --short" },
          },
          {
            type: "tool_execution_update",
            toolCallId: "bash-1",
            toolName: "bash",
            args: { cmd: "git status --short" },
            partialResult: {
              content: [{ type: "text", text: " M apps/server/src/provider/Layers/PiAdapter.ts" }],
              details: { truncation: null },
            },
          },
          {
            type: "tool_execution_end",
            toolCallId: "bash-1",
            toolName: "bash",
            result: {
              content: [{ type: "text", text: " M apps/server/src/provider/Layers/PiAdapter.ts" }],
              details: { truncation: null },
            },
            isError: false,
          },
          {
            type: "tool_execution_start",
            toolCallId: "edit-1",
            toolName: "edit",
            args: { path: "src/app.ts", oldText: "old", newText: "new" },
          },
          {
            type: "tool_execution_update",
            toolCallId: "edit-1",
            toolName: "edit",
            args: { path: "src/app.ts", oldText: "old", newText: "new" },
            partialResult: { content: [{ type: "text", text: "Editing src/app.ts" }] },
          },
          {
            type: "tool_execution_end",
            toolCallId: "edit-1",
            toolName: "edit",
            result: {
              content: [{ type: "text", text: "Successfully replaced text in src/app.ts" }],
              details: { diff: "-old\n+new" },
            },
            isError: false,
          },
        ]);

        const events = Array.from(yield* Fiber.join(collected));
        const tools = toolEventsByCallId(events);
        const bashEvents = tools.get("bash-1");
        const editEvents = tools.get("edit-1");
        assert.deepEqual(
          bashEvents?.map((event) => event.type),
          ["item.started", "item.completed"],
        );
        assert.deepEqual(
          editEvents?.map((event) => event.type),
          ["item.started", "item.completed"],
        );
        const bashCompleted = bashEvents?.find((event) => event.type === "item.completed");
        const editCompleted = editEvents?.find((event) => event.type === "item.completed");
        assert.deepEqual(bashCompleted?.payload, {
          itemType: "command_execution",
          title: "Ran command",
          status: "completed",
          data: {
            toolCallId: "bash-1",
            toolName: "bash",
            kind: "execute",
            command: "git status --short",
            rawInput: { cmd: "git status --short" },
            rawOutput: {
              content: "M apps/server/src/provider/Layers/PiAdapter.ts",
              truncation: null,
            },
            item: { input: { cmd: "git status --short" } },
          },
        });
        assert.deepEqual(editCompleted?.payload, {
          itemType: "file_change",
          title: "Edited file",
          detail: "src/app.ts",
          status: "completed",
          data: {
            toolCallId: "edit-1",
            toolName: "edit",
            kind: "edit",
            rawInput: { path: "src/app.ts", oldText: "old", newText: "new" },
            rawOutput: {
              content: "Successfully replaced text in src/app.ts",
              diff: "-old\n+new",
            },
            item: {
              input: { path: "src/app.ts", oldText: "old", newText: "new" },
              changes: [{ path: "src/app.ts" }],
            },
          },
        });
      }),
    );
  });

  it.effect("classifies Pi agent-backed tools as collaboration work", () => {
    const h = makeHarness();
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const collected = yield* adapter.streamEvents.pipe(
          Stream.takeUntil(completesToolCall("agent-1")),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          input: "delegate this",
          modelSelection,
        });
        yield* Queue.offerAll(h.client.input, [
          {
            type: "tool_execution_start",
            toolCallId: "agent-1",
            toolName: "security_scout",
            args: { query: "Locate the authentication flow" },
          },
          {
            type: "tool_execution_update",
            toolCallId: "agent-1",
            toolName: "security_scout",
            partialResult: {
              content: [{ type: "text", text: "Searching" }],
              details: { agent: "security_scout", task: "Locate the authentication flow" },
            },
          },
          {
            type: "tool_execution_end",
            toolCallId: "agent-1",
            toolName: "security_scout",
            result: {
              content: [{ type: "text", text: "Found the flow" }],
              details: { agent: "security_scout", task: "Locate the authentication flow" },
            },
            isError: false,
          },
        ]);

        const events = Array.from(yield* Fiber.join(collected));
        const agentEvents = toolEventsByCallId(events).get("agent-1");
        assert.deepEqual(
          agentEvents?.map((event) => event.type),
          ["item.started", "item.completed"],
        );
        const started = agentEvents?.find((event) => event.type === "item.started");
        const completed = agentEvents?.find((event) => event.type === "item.completed");
        assert.equal(started?.payload.itemType, "dynamic_tool_call");
        assert.equal(completed?.payload.itemType, "collab_agent_tool_call");
        assert.equal(completed?.payload.title, "Security scout agent");
        assert.equal(completed?.payload.detail, "Locate the authentication flow");
        assert.equal(
          (completed?.payload.data as Record<string, unknown> | undefined)?.toolName,
          "security_scout",
        );
      }),
    );
  });

  it.effect("keeps one T3 turn across native cycles and settles only at agent_settled", () => {
    const h = makeHarness();
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const collected = yield* Stream.take(adapter.streamEvents, 5).pipe(
          Stream.runCollect,
          Effect.forkChild,
        );
        const turn = yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          input: "hello",
          modelSelection,
        });
        yield* Queue.offerAll(h.client.input, [
          { type: "message_update", assistantMessageEvent: { type: "text_delta", delta: "hi" } },
          { type: "agent_end" },
          { type: "turn_end" },
          { type: "turn_start" },
          { type: "agent_settled" },
        ]);
        const events = Array.from(yield* Fiber.join(collected));
        assert.deepEqual(
          events.map((event) => event.type),
          ["turn.started", "item.started", "content.delta", "item.completed", "turn.completed"],
        );
        assert.equal(
          events.every((event) => event.turnId === turn.turnId),
          true,
        );
        assert.equal(events[1]?.itemId, events[2]?.itemId);
        assert.equal(events[2]?.itemId, events[3]?.itemId);
        assert.deepEqual(h.client.calls.thinking, ["max"]);
      }),
    );
  });

  it.effect(
    "suppresses blank assistant items and records interruption before abort settles",
    () => {
      const h = makeHarness();
      h.client.abortBeforeSettle = true;
      return withAdapter(h, (adapter) =>
        Effect.gen(function* () {
          yield* start(adapter);
          const collected = yield* Stream.take(adapter.streamEvents, 2).pipe(
            Stream.runCollect,
            Effect.forkChild,
          );
          const turn = yield* adapter.sendTurn({
            threadId: ThreadId.make("thread"),
            input: "hello",
            modelSelection,
          });
          yield* adapter.interruptTurn(ThreadId.make("thread"), turn.turnId);
          const events = Array.from(yield* Fiber.join(collected));
          assert.deepEqual(
            events.map((event) => event.type),
            ["turn.started", "turn.completed"],
          );
          const terminal = events[1] as Extract<ProviderRuntimeEvent, { type: "turn.completed" }>;
          assert.equal(terminal.payload.state, "interrupted");
        }),
      );
    },
  );

  it.effect("terminalizes an accepted turn before explicit stop closes its session", () => {
    const h = makeHarness();
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const fence = yield* collectThroughSentinel(adapter);
        const accepted = yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          input: "hello",
          modelSelection,
        });
        yield* adapter.stopSession(ThreadId.make("thread"));
        yield* start(adapter);
        const sentinel = yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          input: "sentinel",
          modelSelection,
        });
        fence.setSentinel(sentinel.turnId);
        yield* Queue.offer(h.client.input, { type: "agent_settled" });
        const events = Array.from(yield* Fiber.join(fence.collected));
        const acceptedEvents = events.filter((event) => event.turnId === accepted.turnId);
        assert.deepEqual(
          acceptedEvents.map((event) => event.type),
          ["turn.started", "turn.completed"],
        );
        const completed = acceptedEvents[1] as Extract<
          ProviderRuntimeEvent,
          { type: "turn.completed" }
        >;
        assert.equal(completed.turnId, accepted.turnId);
        assert.equal(completed.payload.state, "interrupted");
        assert.equal(completed.payload.stopReason, "abort");
      }),
    );
  });

  it.effect("stop wins settlement blocked in get_state without duplicate completion", () => {
    const h = makeHarness();
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        const session = yield* start(adapter);
        h.client.getStateEntered = yield* Deferred.make<void>();
        h.client.getStateGate = yield* Deferred.make<void>();
        let sentinelTurnId: string | undefined;
        const collected = yield* adapter.streamEvents.pipe(
          Stream.takeUntil(
            (event) => event.type === "turn.completed" && event.turnId === sentinelTurnId,
          ),
          Stream.runCollect,
          Effect.forkChild,
        );
        const first = yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          input: "hello",
          modelSelection,
        });
        yield* Queue.offer(h.client.input, { type: "agent_settled" });
        yield* Deferred.await(h.client.getStateEntered);
        yield* adapter.stopSession(ThreadId.make("thread"));
        yield* Deferred.succeed(h.client.getStateGate, undefined);
        h.client.getStateEntered = undefined;
        h.client.getStateGate = undefined;
        yield* adapter.startSession({
          provider: ProviderDriverKind.make("pi"),
          providerInstanceId: instanceId,
          threadId: ThreadId.make("thread"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
          resumeCursor: session.resumeCursor,
        });
        const sentinel = yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          input: "sentinel",
          modelSelection,
        });
        sentinelTurnId = sentinel.turnId;
        yield* Queue.offer(h.client.input, { type: "agent_settled" });
        const events = Array.from(yield* Fiber.join(collected));
        const completed = events.filter(
          (event) => event.type === "turn.completed" && event.turnId === first.turnId,
        );
        assert.equal(completed.length, 1);
        assert.equal(
          (completed[0] as Extract<ProviderRuntimeEvent, { type: "turn.completed" }>).payload.state,
          "interrupted",
        );
      }),
    );
  });

  it.effect("terminalizes an accepted turn when its blocked prompt fiber is interrupted", () =>
    Effect.gen(function* () {
      const h = makeHarness();
      h.client.promptEntered = yield* Deferred.make<void>();
      h.client.promptGate = yield* Deferred.make<void>();
      yield* withAdapter(h, (adapter) =>
        Effect.gen(function* () {
          yield* start(adapter);
          const fence = yield* collectThroughSentinel(adapter);
          const sending = yield* adapter
            .sendTurn({
              threadId: ThreadId.make("thread"),
              input: "hello",
              modelSelection,
            })
            .pipe(Effect.forkChild);
          yield* Deferred.await(h.client.promptEntered!);
          yield* Fiber.interrupt(sending);
          h.client.promptGate = undefined;
          yield* start(adapter);
          const sentinel = yield* adapter.sendTurn({
            threadId: ThreadId.make("thread"),
            input: "sentinel",
            modelSelection,
          });
          fence.setSentinel(sentinel.turnId);
          yield* Queue.offer(h.client.input, { type: "agent_settled" });
          const events = Array.from(yield* Fiber.join(fence.collected));
          const interruptedTurnId = events[0]?.turnId;
          const interruptedEvents = events.filter((event) => event.turnId === interruptedTurnId);
          assert.deepEqual(
            interruptedEvents.map((event) => event.type),
            ["turn.started", "turn.completed"],
          );
          assert.equal(
            (interruptedEvents[1] as Extract<ProviderRuntimeEvent, { type: "turn.completed" }>)
              .payload.state,
            "interrupted",
          );
        }),
      );
    }),
  );

  it.effect("fails an accepted turn when the transport event stream shuts down", () => {
    const h = makeHarness();
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const fence = yield* collectThroughSentinel(adapter);
        const accepted = yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          input: "hello",
          modelSelection,
        });
        yield* Queue.shutdown(h.client.input);
        while (yield* adapter.hasSession(ThreadId.make("thread"))) yield* Effect.yieldNow;
        h.client.input = yield* Queue.unbounded<PiRpcEvent>();
        h.client.events = Stream.fromQueue(h.client.input);
        yield* start(adapter);
        const sentinel = yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          input: "sentinel",
          modelSelection,
        });
        fence.setSentinel(sentinel.turnId);
        yield* Queue.offer(h.client.input, { type: "agent_settled" });
        const events = Array.from(yield* Fiber.join(fence.collected));
        const acceptedEvents = events.filter((event) => event.turnId === accepted.turnId);
        assert.deepEqual(
          acceptedEvents.map((event) => event.type),
          ["turn.started", "runtime.error", "turn.completed"],
        );
        const completed = acceptedEvents.filter((event) => event.type === "turn.completed");
        assert.equal(completed.length, 1);
        assert.equal(completed[0]?.turnId, accepted.turnId);
        assert.equal(completed[0]?.payload.state, "failed");
      }),
    );
  });

  it.effect("rejects startup when the transport event stream is already closed", () => {
    const h = makeHarness();
    h.client.events = Stream.empty;
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        const result = yield* start(adapter).pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        assert.equal(yield* adapter.hasSession(ThreadId.make("thread")), false);
        assert.equal(h.client.calls.close, 1);
        const files = fs
          .readdirSync(h.stateDir, { recursive: true })
          .filter((entry) => String(entry).endsWith(".jsonl"));
        assert.deepEqual(files, []);
      }),
    );
  });

  it.effect("rejects startup while an ended event stream is blocked closing", () =>
    Effect.gen(function* () {
      const h = makeHarness();
      h.client.events = Stream.empty;
      h.client.closeEntered = yield* Deferred.make<void>();
      h.client.closeGate = yield* Deferred.make<void>();
      yield* withAdapter(h, (adapter) =>
        Effect.gen(function* () {
          const startupCompleted = yield* Deferred.make<void>();
          const starting = yield* start(adapter).pipe(
            Effect.result,
            Effect.ensuring(Deferred.succeed(startupCompleted, undefined)),
            Effect.forkChild,
          );
          yield* Deferred.await(h.client.closeEntered!);
          yield* Effect.yieldNow;
          assert.equal(Option.isNone(yield* Deferred.poll(startupCompleted)), true);
          yield* Deferred.succeed(h.client.closeGate!, undefined);
          const result = yield* Fiber.join(starting);
          assert.equal(result._tag, "Failure");
          assert.equal(yield* adapter.hasSession(ThreadId.make("thread")), false);
        }),
      );
    }),
  );

  it.effect("rejects a send whose preflight races the event stream closing", () =>
    Effect.gen(function* () {
      const h = makeHarness();
      h.client.getAvailableModelsEntered = yield* Deferred.make<void>();
      h.client.getAvailableModelsGate = yield* Deferred.make<void>();
      yield* withAdapter(h, (adapter) =>
        Effect.gen(function* () {
          yield* start(adapter);
          const observed: ProviderRuntimeEvent[] = [];
          const events = yield* adapter.streamEvents.pipe(
            Stream.runForEach((event) =>
              Effect.sync(() => {
                observed.push(event);
              }),
            ),
            Effect.forkChild,
          );
          const sending = yield* adapter
            .sendTurn({
              threadId: ThreadId.make("thread"),
              input: "hello",
              modelSelection,
            })
            .pipe(Effect.result, Effect.forkChild);
          yield* Deferred.await(h.client.getAvailableModelsEntered!);
          yield* Queue.shutdown(h.client.input);
          while (yield* adapter.hasSession(ThreadId.make("thread"))) yield* Effect.yieldNow;
          yield* Deferred.succeed(h.client.getAvailableModelsGate!, undefined);
          const result = yield* Fiber.join(sending);
          assert.equal(result._tag, "Failure");
          if (result._tag === "Failure")
            assert.equal(result.failure._tag, "ProviderAdapterSessionNotFoundError");
          assert.equal(h.client.calls.prompt, 0);
          yield* Effect.yieldNow;
          yield* Fiber.interrupt(events);
          assert.equal(
            observed.some((event) => event.type === "turn.started"),
            false,
          );
        }),
      );
    }),
  );

  it.effect("serializes concurrent sends into one turn with a steering message", () => {
    const h = makeHarness();
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        let sentinelTurnId: string | undefined;
        const firstTerminalSeen = yield* Deferred.make<void>();
        const collected = yield* adapter.streamEvents.pipe(
          Stream.tap((event) =>
            event.type === "turn.completed"
              ? Deferred.succeed(firstTerminalSeen, undefined)
              : Effect.void,
          ),
          Stream.takeUntil(
            (event) => event.type === "turn.completed" && event.turnId === sentinelTurnId,
          ),
          Stream.runCollect,
          Effect.forkChild,
        );
        const send = (input: string) =>
          adapter
            .sendTurn({ threadId: ThreadId.make("thread"), input, modelSelection })
            .pipe(Effect.result);
        const results = yield* Effect.all([send("one"), send("two")], {
          concurrency: "unbounded",
        });
        assert.equal(results.filter((result) => result._tag === "Success").length, 2);
        assert.equal(
          new Set(
            results.flatMap((result) =>
              result._tag === "Success" ? [String(result.success.turnId)] : [],
            ),
          ).size,
          1,
        );
        assert.equal(h.client.calls.prompt, 2);
        assert.deepEqual(
          h.client.calls.prompts.map(({ streamingBehavior }) => streamingBehavior),
          [undefined, "steer"],
        );
        yield* Queue.offer(h.client.input, { type: "agent_settled" });
        yield* Deferred.await(firstTerminalSeen);
        const sentinel = yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          input: "sentinel",
          modelSelection,
        });
        sentinelTurnId = sentinel.turnId;
        yield* Queue.offer(h.client.input, { type: "agent_settled" });
        const events = Array.from(yield* Fiber.join(collected));
        const firstTurnId = results.find((result) => result._tag === "Success")!.success.turnId;
        const firstEvents = events.filter((event) => event.turnId === firstTurnId);
        assert.deepEqual(
          firstEvents.map((event) => event.type),
          ["turn.started", "turn.completed"],
        );
      }),
    );
  });

  it.effect("defers settlement until a steering prompt is accepted", () =>
    Effect.gen(function* () {
      const h = makeHarness();
      h.client.promptEntered = yield* Deferred.make<void>();
      h.client.promptGate = yield* Deferred.make<void>();
      yield* withAdapter(h, (adapter) =>
        Effect.gen(function* () {
          yield* start(adapter);
          yield* Deferred.succeed(h.client.promptGate!, undefined);
          const first = yield* adapter.sendTurn({
            threadId: ThreadId.make("thread"),
            input: "first",
            modelSelection,
          });

          h.client.promptEntered = yield* Deferred.make<void>();
          h.client.promptGate = yield* Deferred.make<void>();
          const steering = yield* adapter
            .sendTurn({
              threadId: ThreadId.make("thread"),
              input: "steer",
              modelSelection,
            })
            .pipe(Effect.forkChild);
          yield* Deferred.await(h.client.promptEntered!);
          yield* Queue.offer(h.client.input, { type: "agent_settled" });
          yield* Effect.yieldNow;
          assert.equal((yield* adapter.listSessions())[0]?.status, "running");
          yield* Deferred.succeed(h.client.promptGate!, undefined);

          const steered = yield* Fiber.join(steering);
          assert.equal(steered.turnId, first.turnId);
          while ((yield* adapter.listSessions())[0]?.status === "running") yield* Effect.yieldNow;
          assert.equal((yield* adapter.listSessions())[0]?.status, "ready");
        }),
      );
    }),
  );

  it.effect("rechecks a settlement snapshot when steering starts during get_state", () =>
    Effect.gen(function* () {
      const h = makeHarness();
      yield* withAdapter(h, (adapter) =>
        Effect.gen(function* () {
          yield* start(adapter);
          const first = yield* adapter.sendTurn({
            threadId: ThreadId.make("thread"),
            input: "first",
            modelSelection,
          });
          h.client.getStateEntered = yield* Deferred.make<void>();
          h.client.getStateGate = yield* Deferred.make<void>();
          h.client.getStateResults.push(
            { ...h.client.state, isStreaming: false },
            { ...h.client.state, isStreaming: true },
          );

          yield* Queue.offer(h.client.input, { type: "agent_settled" });
          yield* Deferred.await(h.client.getStateEntered!);
          const steered = yield* adapter.sendTurn({
            threadId: ThreadId.make("thread"),
            input: "steer",
            modelSelection,
          });
          yield* Deferred.succeed(h.client.getStateGate!, undefined);
          while (h.client.getStateResults.length > 0) yield* Effect.yieldNow;

          assert.equal(steered.turnId, first.turnId);
          const running = (yield* adapter.listSessions())[0];
          assert.equal(running?.status, "running");
          assert.equal(running?.activeTurnId, first.turnId);

          h.client.getStateEntered = undefined;
          h.client.getStateGate = undefined;
          h.client.state.isStreaming = false;
          yield* Queue.offer(h.client.input, { type: "agent_settled" });
        }),
      );
    }),
  );

  it.effect("does not let a blocked steering prompt prevent interruption", () =>
    Effect.gen(function* () {
      const h = makeHarness();
      yield* withAdapter(h, (adapter) =>
        Effect.gen(function* () {
          yield* start(adapter);
          const first = yield* adapter.sendTurn({
            threadId: ThreadId.make("thread"),
            input: "first",
            modelSelection,
          });

          h.client.promptEntered = yield* Deferred.make<void>();
          h.client.promptGate = yield* Deferred.make<void>();
          const steering = yield* adapter
            .sendTurn({
              threadId: ThreadId.make("thread"),
              input: "steer",
              modelSelection,
            })
            .pipe(Effect.forkChild);
          yield* Deferred.await(h.client.promptEntered!);

          yield* adapter.interruptTurn(ThreadId.make("thread"), first.turnId);
          assert.equal(h.client.calls.abort, 1);
          const interruptingSteer = yield* Fiber.interrupt(steering).pipe(Effect.forkChild);
          yield* Deferred.succeed(h.client.promptGate!, undefined);
          yield* Fiber.join(interruptingSteer);
          const running = (yield* adapter.listSessions())[0];
          assert.equal(running?.status, "running");
          assert.equal(running?.activeTurnId, first.turnId);
          assert.equal(h.client.calls.close, 0);
          assert.equal(h.client.calls.abort, 2);

          yield* Queue.offer(h.client.input, { type: "agent_settled" });
        }),
      );
    }),
  );

  it.effect("leaves the active turn running when a steering prompt fails", () => {
    const h = makeHarness();
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const first = yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          input: "first",
          modelSelection,
        });
        h.client.failPrompt = true;

        const failed = yield* adapter
          .sendTurn({
            threadId: ThreadId.make("thread"),
            input: "steer",
            modelSelection,
          })
          .pipe(Effect.result);
        assert.equal(failed._tag, "Failure");
        const running = (yield* adapter.listSessions())[0];
        assert.equal(running?.status, "running");
        assert.equal(running?.activeTurnId, first.turnId);

        h.client.failPrompt = false;
        yield* Queue.offer(h.client.input, { type: "agent_settled" });
      }),
    );
  });

  it.effect("resumes only an exact persisted cursor", () => {
    const h = makeHarness();
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        const first = yield* start(adapter);
        yield* adapter.stopSession(ThreadId.make("thread"));
        const resumed = yield* adapter.startSession({
          provider: ProviderDriverKind.make("pi"),
          providerInstanceId: instanceId,
          threadId: ThreadId.make("thread"),
          cwd: process.cwd(),
          runtimeMode: "full-access",
          resumeCursor: first.resumeCursor,
        });
        assert.deepEqual(resumed.resumeCursor, first.resumeCursor);
        assert.equal(h.spawns.length, 2);
        yield* adapter.stopSession(ThreadId.make("thread"));

        const invalid = yield* adapter
          .startSession({
            provider: ProviderDriverKind.make("pi"),
            providerInstanceId: instanceId,
            threadId: ThreadId.make("thread"),
            cwd: process.cwd(),
            runtimeMode: "full-access",
            resumeCursor: {
              ...(first.resumeCursor as Record<string, unknown>),
              sessionId: "wrong-session",
            },
          })
          .pipe(Effect.result);
        assert.equal(invalid._tag, "Failure");
        assert.equal(h.spawns.length, 2);
      }),
    );
  });

  it.effect("removes a fresh placeholder after startup fails", () => {
    const h = makeHarness({ failStart: true });
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        assert.equal((yield* start(adapter).pipe(Effect.result))._tag, "Failure");
        const files = fs
          .readdirSync(h.stateDir, { recursive: true })
          .filter((entry) => String(entry).endsWith(".jsonl"));
        assert.deepEqual(files, []);
        assert.equal(yield* adapter.hasSession(ThreadId.make("thread")), false);
      }),
    );
  });

  it.effect("serializes concurrent starts for one thread", () => {
    const h = makeHarness();
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        const results = yield* Effect.all(
          [start(adapter).pipe(Effect.result), start(adapter).pipe(Effect.result)],
          { concurrency: "unbounded" },
        );
        assert.equal(results.filter((result) => result._tag === "Success").length, 1);
        assert.equal(results.filter((result) => result._tag === "Failure").length, 1);
        assert.equal(h.spawns.length, 1);
      }),
    );
  });

  it.effect("cleans an interrupted startup before publishing ownership", () =>
    Effect.gen(function* () {
      const spawnEntered = yield* Deferred.make<void>();
      const h = makeHarness();
      const interruptedHarness: Harness = {
        ...h,
        makeClient: () =>
          Deferred.succeed(spawnEntered, undefined).pipe(Effect.andThen(Effect.never)),
      };
      yield* withAdapter(interruptedHarness, (adapter) =>
        Effect.gen(function* () {
          const starting = yield* Effect.forkChild(start(adapter));
          yield* Deferred.await(spawnEntered);
          yield* Fiber.interrupt(starting);
          assert.equal(yield* adapter.hasSession(ThreadId.make("thread")), false);
          const files = fs
            .readdirSync(h.stateDir, { recursive: true })
            .filter((entry) => String(entry).endsWith(".jsonl"));
          assert.deepEqual(files, []);
        }),
      );
    }),
  );

  it.effect("releases published startup ownership when interrupted before transfer", () =>
    Effect.gen(function* () {
      const published = yield* Deferred.make<void>();
      const releasePublication = yield* Deferred.make<void>();
      const h = makeHarness();
      let blockPublication = false;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const adapter = yield* makePiAdapter({
            binaryPath: "pi",
            providerInstanceId: instanceId,
            stateDir: h.stateDir,
            attachmentsDir: h.attachmentsDir,
            makeRpcClient: h.makeClient,
            onSessionPublished: () =>
              blockPublication
                ? Deferred.succeed(published, undefined).pipe(
                    Effect.andThen(Deferred.await(releasePublication)),
                  )
                : Effect.void,
          });
          const durable = yield* start(adapter, "durable");
          yield* adapter.stopSession(ThreadId.make("durable"));
          blockPublication = true;
          const starting = yield* Effect.forkChild(
            adapter.startSession({
              provider: ProviderDriverKind.make("pi"),
              providerInstanceId: instanceId,
              threadId: ThreadId.make("thread"),
              cwd: process.cwd(),
              runtimeMode: "full-access",
              resumeCursor: durable.resumeCursor,
            }),
          );
          yield* Deferred.await(published);
          assert.equal(yield* adapter.hasSession(ThreadId.make("thread")), true);
          yield* Fiber.interrupt(starting);
          assert.equal(yield* adapter.hasSession(ThreadId.make("thread")), false);

          blockPublication = false;
          const reacquired = yield* adapter.startSession({
            provider: ProviderDriverKind.make("pi"),
            providerInstanceId: instanceId,
            threadId: ThreadId.make("replacement"),
            cwd: process.cwd(),
            runtimeMode: "full-access",
            resumeCursor: durable.resumeCursor,
          });
          assert.equal(reacquired.threadId, ThreadId.make("replacement"));
        }),
      ).pipe(Effect.provide(NodeServices.layer));
    }),
  );

  it.effect("leases a durable session file to one live thread", () => {
    const h = makeHarness();
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        const first = yield* start(adapter, "thread-one");
        const second = yield* adapter
          .startSession({
            provider: ProviderDriverKind.make("pi"),
            providerInstanceId: instanceId,
            threadId: ThreadId.make("thread-two"),
            cwd: process.cwd(),
            runtimeMode: "full-access",
            resumeCursor: first.resumeCursor,
          })
          .pipe(Effect.result);
        assert.equal(second._tag, "Failure");
        assert.equal(h.spawns.length, 1);
      }),
    );
  });

  it.effect("fails a rejected prompt and allows the next turn", () => {
    const h = makeHarness();
    h.client.failPrompt = true;
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const collected = yield* Stream.take(adapter.streamEvents, 3).pipe(
          Stream.runCollect,
          Effect.forkChild,
        );
        const failed = yield* adapter
          .sendTurn({
            threadId: ThreadId.make("thread"),
            input: "first",
            modelSelection,
          })
          .pipe(Effect.result);
        assert.equal(failed._tag, "Failure");
        assert.deepEqual(
          Array.from(yield* Fiber.join(collected)).map((event) => event.type),
          ["turn.started", "runtime.error", "turn.completed"],
        );

        h.client.failPrompt = false;
        const next = yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          input: "second",
          modelSelection,
        });
        assert.equal(next.threadId, ThreadId.make("thread"));
        yield* Queue.offer(h.client.input, { type: "agent_settled" });
      }),
    );
  });

  it.effect("closes the session after an ambiguous prompt transport failure", () => {
    const h = makeHarness();
    h.client.fatalPrompt = true;
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const failed = yield* adapter
          .sendTurn({
            threadId: ThreadId.make("thread"),
            input: "ambiguous",
            modelSelection,
          })
          .pipe(Effect.result);
        assert.equal(failed._tag, "Failure");
        assert.equal(yield* adapter.hasSession(ThreadId.make("thread")), false);
        assert.equal(h.client.calls.close, 1);
      }),
    );
  });

  it.effect("fails identity drift, closes once, and makes stop idempotent", () => {
    const h = makeHarness();
    return withAdapter(h, (adapter) =>
      Effect.gen(function* () {
        yield* start(adapter);
        const collected = yield* Stream.take(adapter.streamEvents, 3).pipe(
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* adapter.sendTurn({
          threadId: ThreadId.make("thread"),
          input: "hello",
          modelSelection,
        });
        h.client.state = { ...h.client.state, sessionId: "drift" };
        yield* Queue.offer(h.client.input, { type: "agent_settled" });
        const events = Array.from(yield* Fiber.join(collected));
        assert.deepEqual(
          events.map((event) => event.type),
          ["turn.started", "runtime.error", "turn.completed"],
        );
        yield* adapter.stopSession(ThreadId.make("thread"));
        yield* adapter.stopSession(ThreadId.make("thread"));
        assert.equal(h.client.calls.close, 1);
        assert.equal(yield* adapter.hasSession(ThreadId.make("thread")), false);
      }),
    );
  });
});
