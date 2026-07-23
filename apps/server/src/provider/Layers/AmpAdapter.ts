import {
  EventId,
  ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeItemId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import {
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { makeAmpCliRunner, type AmpCliMessage, type AmpCliRunner } from "../amp/AmpCliRunner.ts";
import { AmpSessionCursor, type AmpSessionCursor as Cursor } from "../amp/AmpSessionCursor.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

const PROVIDER = ProviderDriverKind.make("amp");
const EFFORTS = new Set(["none", "minimal", "low", "medium", "high", "xhigh", "max"]);
interface Turn {
  id: TurnId;
  fiber: Fiber.Fiber<void> | undefined;
  initialized: Deferred.Deferred<Cursor, ProviderAdapterValidationError>;
  terminal: boolean;
  interrupted: boolean;
  first: boolean;
  assistant: boolean;
  reasoning: boolean;
  tools: Map<string, RuntimeItemId>;
}
interface Context {
  session: ProviderSession;
  cursor: Cursor | undefined;
  turn: Turn | undefined;
  closing: boolean;
  stopped: boolean;
  lock: Semaphore.Semaphore;
}
export interface AmpAdapterOptions {
  readonly binaryPath: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly environment?: Readonly<Record<string, string | undefined>>;
  readonly allowedModes: ReadonlySet<string>;
  readonly runner?: AmpCliRunner;
}

export const makeAmpAdapter = Effect.fn("makeAmpAdapter")(function* (options: AmpAdapterOptions) {
  const crypto = yield* Crypto.Crypto;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const runner =
    options.runner ??
    (yield* makeAmpCliRunner().pipe(
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    ));
  const events = yield* Queue.unbounded<ProviderRuntimeEvent>();
  const sessions = new Map<ThreadId, Context>();
  const owners = new Map<string, Context>();
  const sessionStartLock = yield* Semaphore.make(1);
  const now = Effect.map(DateTime.now, DateTime.formatIso);
  const uuid = crypto.randomUUIDv4.pipe(Effect.orDie);
  const validation = (operation: string, issue: string, cause?: unknown) =>
    new ProviderAdapterValidationError({ provider: PROVIDER, operation, issue, cause });
  const requireSession = (id: ThreadId) => {
    const c = sessions.get(id);
    return c && !c.stopped
      ? Effect.succeed(c)
      : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId: id }));
  };
  const base = Effect.fn("AmpAdapter.base")(function* (c: Context, t: Turn) {
    return {
      eventId: EventId.make(yield* uuid),
      createdAt: yield* now,
      provider: PROVIDER,
      providerInstanceId: options.providerInstanceId,
      threadId: c.session.threadId,
      turnId: t.id,
    } as const;
  });
  const offer = (e: ProviderRuntimeEvent) => Queue.offer(events, e).pipe(Effect.asVoid);
  const raw = (m: AmpCliMessage) => ({
    source: "amp.cli.stream-json" as const,
    method: m.type === "system" ? "system/init" : m.type,
    payload: m,
  });
  const terminal = Effect.fn("AmpAdapter.terminal")(function* (
    c: Context,
    t: Turn,
    state: "completed" | "failed" | "interrupted",
    message?: string,
    native?: AmpCliMessage,
  ) {
    if (c.turn !== t || t.terminal) return;
    t.terminal = true;
    const itemStatus =
      state === "completed"
        ? ("completed" as const)
        : state === "failed"
          ? ("failed" as const)
          : ("declined" as const);
    if (t.assistant)
      yield* offer({
        type: "item.completed",
        ...(yield* base(c, t)),
        itemId: RuntimeItemId.make(`amp-assistant:${t.id}`),
        payload: {
          itemType: "assistant_message",
          status: itemStatus,
          title: "Assistant message",
        },
        ...(native ? { raw: raw(native) } : {}),
      });
    if (t.reasoning)
      yield* offer({
        type: "item.completed",
        ...(yield* base(c, t)),
        itemId: RuntimeItemId.make(`amp-reasoning:${t.id}`),
        payload: { itemType: "reasoning", status: itemStatus, title: "Reasoning" },
        ...(native ? { raw: raw(native) } : {}),
      });
    yield* Effect.forEach(
      t.tools.values(),
      (itemId) =>
        Effect.gen(function* () {
          yield* offer({
            type: "item.completed",
            ...(yield* base(c, t)),
            itemId,
            payload: { itemType: "dynamic_tool_call", status: itemStatus, title: "Tool call" },
            ...(native ? { raw: raw(native) } : {}),
          });
        }),
      { discard: true },
    );
    t.tools.clear();
    if (state === "failed")
      yield* offer({
        type: "runtime.error",
        ...(yield* base(c, t)),
        payload: { message: message ?? "Amp failed.", class: "transport_error" },
        ...(native ? { raw: raw(native) } : {}),
      });
    yield* offer({
      type: "turn.completed",
      ...(yield* base(c, t)),
      payload:
        state === "completed"
          ? { state, stopReason: null }
          : state === "interrupted"
            ? { state, stopReason: "abort" }
            : { state, errorMessage: message ?? "Amp failed." },
      ...(native ? { raw: raw(native) } : {}),
    });
    c.turn = undefined;
    const { activeTurnId: _, ...session } = c.session;
    c.session = {
      ...session,
      status: state === "failed" ? "error" : "ready",
      ...(c.cursor ? { resumeCursor: c.cursor } : {}),
      updatedAt: yield* now,
    };
  });
  const consume = Effect.fn("AmpAdapter.consume")(function* (
    c: Context,
    t: Turn,
    stream: Stream.Stream<AmpCliMessage, unknown>,
  ) {
    yield* stream.pipe(
      Stream.runForEach((m) =>
        Effect.gen(function* () {
          if (c.turn !== t || t.terminal || c.closing) return;
          if (t.first && m.type !== "system")
            return yield* validation("stream", "First Amp message was not system/init.");
          t.first = false;
          if (m.type === "system") {
            const cursor = yield* Schema.decodeUnknownEffect(AmpSessionCursor)({
              schemaVersion: 1,
              threadId: m.session_id,
            }).pipe(Effect.mapError(String));
            if (
              (c.cursor && c.cursor.threadId !== cursor.threadId) ||
              (m.cwd && path.resolve(m.cwd) !== c.session.cwd)
            )
              return yield* validation("stream", "Amp session identity mismatch.");
            if (!c.cursor) {
              const owner = owners.get(cursor.threadId);
              if (owner && owner !== c)
                return yield* validation("stream", "Amp thread already owned.");
              c.cursor = cursor;
              owners.set(cursor.threadId, c);
            }
            yield* Deferred.succeed(t.initialized, cursor);
            return;
          }
          if (m.session_id !== c.cursor?.threadId)
            return yield* validation("stream", "Amp session id changed.");
          if (m.type === "assistant")
            for (const p of m.message.content) {
              if (p.type === "text" || p.type === "thinking") {
                const reasoning = p.type === "thinking";
                const id = RuntimeItemId.make(
                  `amp-${reasoning ? "reasoning" : "assistant"}:${t.id}`,
                );
                const started = reasoning ? t.reasoning : t.assistant;
                if (!started) {
                  if (reasoning) t.reasoning = true;
                  else t.assistant = true;
                  yield* offer({
                    type: "item.started",
                    ...(yield* base(c, t)),
                    itemId: id,
                    payload: {
                      itemType: reasoning ? "reasoning" : "assistant_message",
                      status: "inProgress",
                      title: reasoning ? "Reasoning" : "Assistant message",
                    },
                    raw: raw(m),
                  });
                }
                yield* offer({
                  type: "content.delta",
                  ...(yield* base(c, t)),
                  itemId: id,
                  payload: {
                    streamKind: reasoning ? "reasoning_text" : "assistant_text",
                    delta: reasoning ? p.thinking : p.text,
                  },
                  raw: raw(m),
                });
              } else if (p.type === "tool_use") {
                const id = RuntimeItemId.make(`amp-tool:${t.id}:${p.id}`);
                t.tools.set(p.id, id);
                yield* offer({
                  type: "item.started",
                  ...(yield* base(c, t)),
                  itemId: id,
                  payload: {
                    itemType: "dynamic_tool_call",
                    status: "inProgress",
                    title: p.name,
                    data: p.input,
                  },
                  raw: raw(m),
                });
              }
            }
          else if (m.type === "user") {
            for (const p of m.message.content)
              if (p.type === "tool_result" && t.tools.has(p.tool_use_id)) {
                yield* offer({
                  type: "item.completed",
                  ...(yield* base(c, t)),
                  itemId: t.tools.get(p.tool_use_id)!,
                  payload: {
                    itemType: "dynamic_tool_call",
                    status: p.is_error ? "failed" : "completed",
                    title: "Tool call",
                    detail: String(p.content),
                  },
                  raw: raw(m),
                });
                t.tools.delete(p.tool_use_id);
              }
          } else if (m.type === "result") {
            if (m.is_error) yield* terminal(c, t, "failed", m.error ?? "Amp returned an error.", m);
            else yield* terminal(c, t, "completed", undefined, m);
          }
        }),
      ),
      Effect.catch((cause) =>
        terminal(c, t, t.interrupted ? "interrupted" : "failed", String(cause)),
      ),
      Effect.ensuring(
        Deferred.fail(
          t.initialized,
          validation("sendTurn", "Amp failed before initialization."),
        ).pipe(Effect.ignore),
      ),
    );
    if (!t.terminal)
      yield* terminal(
        c,
        t,
        t.interrupted ? "interrupted" : "failed",
        "Amp stream ended without a result.",
      );
  });
  const startSession: ProviderAdapterShape<ProviderAdapterError>["startSession"] = (input) =>
    sessionStartLock.withPermit(
      Effect.gen(function* () {
        if (input.runtimeMode !== "full-access")
          return yield* validation("startSession", "Amp supports only full-access runtime mode.");
        if (input.provider && input.provider !== PROVIDER)
          return yield* validation("startSession", "Wrong provider.");
        if (input.providerInstanceId && input.providerInstanceId !== options.providerInstanceId)
          return yield* validation("startSession", "Wrong provider instance.");
        if (sessions.has(input.threadId))
          return yield* validation("startSession", "Thread already active.");
        const cwd = yield* fs
          .realPath(path.resolve(input.cwd ?? process.cwd()))
          .pipe(
            Effect.mapError((c) => validation("startSession", "Invalid Amp working directory.", c)),
          );
        const cursor = input.resumeCursor
          ? yield* Schema.decodeUnknownEffect(AmpSessionCursor)(input.resumeCursor).pipe(
              Effect.mapError((c) => validation("startSession", "Invalid Amp resume cursor.", c)),
            )
          : undefined;
        if (cursor && owners.has(cursor.threadId))
          return yield* validation("startSession", "Amp thread already owned.");
        const createdAt = yield* now;
        const lock = yield* Semaphore.make(1);
        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: options.providerInstanceId,
          threadId: input.threadId,
          status: "ready",
          runtimeMode: "full-access",
          cwd,
          ...(cursor ? { resumeCursor: cursor } : {}),
          createdAt,
          updatedAt: createdAt,
        };
        const c: Context = {
          session,
          cursor,
          turn: undefined,
          closing: false,
          stopped: false,
          lock,
        };
        sessions.set(input.threadId, c);
        if (cursor) owners.set(cursor.threadId, c);
        return session;
      }),
    );
  const sendTurn: ProviderAdapterShape<ProviderAdapterError>["sendTurn"] = (input) =>
    Effect.flatMap(requireSession(input.threadId), (c) =>
      c.lock
        .withPermit(
          Effect.gen(function* () {
            if (c.turn || c.session.status !== "ready")
              return yield* validation("sendTurn", "Amp session is not idle.");
            if (!input.input?.trim())
              return yield* validation("sendTurn", "Amp requires text input.");
            if (input.attachments?.length)
              return yield* validation("sendTurn", "Amp attachments are unsupported.");
            const s = input.modelSelection;
            if (!s || s.instanceId !== options.providerInstanceId)
              return yield* validation(
                "sendTurn",
                "A model selection for this Amp instance is required.",
              );
            if (!options.allowedModes.has(s.model))
              return yield* validation("sendTurn", "Invalid Amp mode.");
            const effort = getModelSelectionStringOptionValue(s, "effort");
            if (effort && !EFFORTS.has(effort))
              return yield* validation("sendTurn", "Invalid Amp effort selection.");
            const initialized = yield* Deferred.make<Cursor, ProviderAdapterValidationError>();
            const t: Turn = {
              id: TurnId.make(yield* uuid),
              fiber: undefined,
              initialized,
              terminal: false,
              interrupted: false,
              first: true,
              assistant: false,
              reasoning: false,
              tools: new Map(),
            };
            c.turn = t;
            c.session = {
              ...c.session,
              status: "running",
              activeTurnId: t.id,
              updatedAt: yield* now,
            };
            yield* offer({
              type: "turn.started",
              ...(yield* base(c, t)),
              payload: { model: s.model, ...(effort ? { effort } : {}) },
            });
            t.fiber = yield* consume(
              c,
              t,
              runner.run({
                binaryPath: options.binaryPath,
                cwd: c.session.cwd!,
                ...(options.environment ? { env: options.environment } : {}),
                prompt: input.input!,
                mode: s.model,
                ...(effort ? { effort } : {}),
                ...(c.cursor ? { continueThreadId: c.cursor.threadId } : {}),
              }),
            ).pipe(Effect.forkDetach);
            return t;
          }),
        )
        .pipe(
          Effect.flatMap((t) =>
            Deferred.await(t.initialized).pipe(
              Effect.map((cursor) => ({
                threadId: input.threadId,
                turnId: t.id,
                resumeCursor: cursor,
              })),
            ),
          ),
          Effect.onInterrupt(() =>
            Effect.suspend(() => {
              const turn = c.turn;
              return turn
                ? interruptTurn(input.threadId, turn.id).pipe(Effect.ignore)
                : Effect.void;
            }),
          ),
        ),
    );
  const interruptTurn: ProviderAdapterShape<ProviderAdapterError>["interruptTurn"] = (id, turnId) =>
    Effect.flatMap(requireSession(id), (c) =>
      c.lock.withPermit(
        Effect.gen(function* () {
          const t = c.turn;
          if (!t || (turnId && turnId !== t.id))
            return yield* validation("interruptTurn", "No matching active Amp turn.");
          t.interrupted = true;
          if (t.fiber) yield* Fiber.interrupt(t.fiber);
          yield* terminal(c, t, "interrupted");
        }),
      ),
    );
  const stopSession = (id: ThreadId) =>
    Effect.flatMap(requireSession(id), (c) =>
      c.lock.withPermit(
        Effect.gen(function* () {
          c.closing = true;
          if (c.turn?.fiber) yield* Fiber.interrupt(c.turn.fiber);
          if (c.cursor && owners.get(c.cursor.threadId) === c) owners.delete(c.cursor.threadId);
          sessions.delete(id);
          c.stopped = true;
        }),
      ),
    ).pipe(Effect.catchTag("ProviderAdapterSessionNotFoundError", () => Effect.void));
  const stopAll = () =>
    Effect.forEach([...sessions.keys()], stopSession, { discard: true, concurrency: "unbounded" });
  yield* Effect.addFinalizer(() => stopAll());
  const unsupported = (op: string, id: ThreadId) =>
    requireSession(id).pipe(Effect.andThen(validation(op, `Amp does not support ${op}.`)));
  return {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "in-session" },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest: (id) => unsupported("respondToRequest", id),
    respondToUserInput: (id) => unsupported("respondToUserInput", id),
    readThread: (id) => unsupported("readThread", id),
    rollbackThread: (id) => unsupported("rollbackThread", id),
    stopSession,
    listSessions: () => Effect.sync(() => [...sessions.values()].map((c) => ({ ...c.session }))),
    hasSession: (id) => Effect.sync(() => sessions.has(id)),
    stopAll,
    streamEvents: Stream.fromQueue(events),
  } satisfies ProviderAdapterShape<ProviderAdapterError>;
});
