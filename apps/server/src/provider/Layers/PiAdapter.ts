import {
  DEFAULT_PROVIDER_INTERACTION_MODE,
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
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { ChildProcessSpawner } from "effect/unstable/process";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import type { SupervisorClient } from "../../piNative/SupervisorClient.ts";
import { MANAGED_ADMISSION_PROTOCOL } from "../../piNative/SupervisorProtocol.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { decodePiModelSlug } from "../pi/PiModel.ts";
import {
  makePiRpcClient,
  type PiRpcClient,
  type PiRpcError,
  type PiRpcSpawnOptions,
} from "../pi/PiRpcClient.ts";
import { PiThinkingLevel, type PiRpcEvent } from "../pi/PiRpcSchema.ts";
import {
  allocateFreshPiSessionFile,
  cleanupFreshPiSessionFile,
  piInstanceStateRoot,
  PiSessionCursor,
  piStateMatchesCursor,
  validatePiResumeSessionFile,
} from "../pi/PiSessionFile.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

const PROVIDER = ProviderDriverKind.make("pi");
const DETERMINISTIC_ARGS = ["--offline"] as const;

export type PiRpcClientFactory = (
  options: PiRpcSpawnOptions,
) => Effect.Effect<PiRpcClient, PiRpcError, ChildProcessSpawner.ChildProcessSpawner | Scope.Scope>;

export interface PiAdapterOptions {
  readonly binaryPath: string;
  readonly args?: ReadonlyArray<string>;
  readonly providerInstanceId: ProviderInstanceId;
  readonly stateDir: string;
  readonly attachmentsDir: string;
  readonly environment?: Readonly<Record<string, string | undefined>>;
  readonly makeRpcClient?: PiRpcClientFactory;
  readonly onSessionPublished?: () => Effect.Effect<void>;
  readonly supervisor: Pick<SupervisorClient["Service"], "claimManaged">;
}

interface ActiveTurn {
  readonly id: TurnId;
  readonly assistantItemId: RuntimeItemId;
  readonly reasoningItemId: RuntimeItemId;
  readonly toolItemIds: Map<string, RuntimeItemId>;
  readonly toolArgs: Map<string, Record<string, unknown>>;
  assistantText: string;
  assistantStarted: boolean;
  reasoningStarted: boolean;
  interruptRequested: boolean;
  terminal: boolean;
}

interface SessionContext {
  session: ProviderSession;
  readonly cursor: PiSessionCursor;
  readonly lease: SessionFileLease;
  readonly client: PiRpcClient;
  readonly scope: Scope.Closeable;
  eventFiber: Fiber.Fiber<void>;
  activeTurn: ActiveTurn | undefined;
  readonly managedOperations: Map<string, TurnId>;
  steeringPromptsInFlight: number;
  steeringGeneration: number;
  deferredSettlement: PiRpcEvent | undefined;
  closing: boolean;
  stopped: boolean;
}

interface SessionFileLease {
  startupOwned: boolean;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const string = (value: unknown): string | undefined =>
  typeof value === "string" && value.length > 0 ? value : undefined;
const trimmedString = (value: unknown): string | undefined =>
  typeof value === "string" ? value.trim() || undefined : undefined;

const piToolText = (value: unknown): string | undefined => {
  const record = isRecord(value) ? value : undefined;
  if (!record) return trimmedString(value);
  if (!Array.isArray(record.content)) return undefined;
  const text = record.content
    .flatMap((part) => (isRecord(part) && typeof part.text === "string" ? [part.text] : []))
    .join("\n")
    .trim();
  return text || undefined;
};

const piToolPath = (args: Record<string, unknown>): string | undefined =>
  trimmedString(args.path) ?? trimmedString(args.file_path);

const piToolCommand = (args: Record<string, unknown>): string | undefined =>
  trimmedString(args.command) ?? trimmedString(args.cmd);

/**
 * Pi's agent-backed extensions share `details.agent` and `details.task`.
 * Classifying that result metadata keeps new agents working without a T3 tool-name allowlist.
 */
const piSubagentPresentation = (
  args: Record<string, unknown>,
  output: Record<string, unknown> | undefined,
) => {
  const outputDetails = isRecord(output?.details) ? output.details : undefined;
  const agent = trimmedString(outputDetails?.agent);
  if (!agent) return undefined;
  const label = agent.replace(/[_-]+/gu, " ");
  const detail =
    trimmedString(outputDetails?.task) ??
    trimmedString(args.description) ??
    trimmedString(args.task) ??
    trimmedString(args.query) ??
    trimmedString(args.objective) ??
    trimmedString(args.goal) ??
    trimmedString(args.prompt) ??
    trimmedString(args.diff_description) ??
    trimmedString(args.name) ??
    trimmedString(args.scriptPath);

  return {
    title: `${label.charAt(0).toUpperCase()}${label.slice(1)} agent`,
    detail,
  };
};

const piToolPresentation = (event: Record<string, unknown>) => {
  const toolName = string(event.toolName) ?? "tool";
  const normalizedName = toolName.toLowerCase();
  const args = isRecord(event.args) ? event.args : {};
  const output = event.result ?? event.partialResult;
  const outputRecord = isRecord(output) ? output : undefined;
  const subagent = piSubagentPresentation(args, outputRecord);
  const outputText = piToolText(output);
  const path = piToolPath(args);
  const toolCallId = string(event.toolCallId) ?? string(event.toolCallID);
  const itemType = subagent
    ? ("collab_agent_tool_call" as const)
    : normalizedName === "bash"
      ? ("command_execution" as const)
      : normalizedName === "write" || normalizedName === "edit"
        ? ("file_change" as const)
        : ("dynamic_tool_call" as const);
  const title = subagent
    ? subagent.title
    : normalizedName === "bash"
      ? "Ran command"
      : normalizedName === "read"
        ? "Read file"
        : normalizedName === "write"
          ? "Wrote file"
          : normalizedName === "edit"
            ? "Edited file"
            : normalizedName === "grep"
              ? "Searched files"
              : normalizedName === "find"
                ? "Found files"
                : normalizedName === "ls"
                  ? "Listed directory"
                  : toolName;
  const invocationDetail = subagent
    ? subagent.detail
    : normalizedName === "grep"
      ? `${trimmedString(args.pattern) ? `/${trimmedString(args.pattern)}/` : "pattern"} in ${path ?? trimmedString(args.glob) ?? "."}`
      : normalizedName === "find"
        ? `${trimmedString(args.filePattern) ?? trimmedString(args.pattern) ?? "files"} in ${path ?? "."}`
        : normalizedName === "ls"
          ? (path ?? ".")
          : path;
  const detail = event.isError === true ? outputText?.split(/\r?\n/u)[0] : invocationDetail;
  const command = normalizedName === "bash" ? piToolCommand(args) : undefined;
  const changes =
    (normalizedName === "write" || normalizedName === "edit") && path ? [{ path }] : undefined;

  return {
    itemType,
    title,
    ...(detail ? { detail } : {}),
    data: {
      ...(toolCallId ? { toolCallId } : {}),
      toolName,
      kind:
        normalizedName === "bash"
          ? "execute"
          : normalizedName === "read"
            ? "read"
            : normalizedName === "write" || normalizedName === "edit"
              ? "edit"
              : "other",
      ...(command ? { command } : {}),
      rawInput: args,
      ...(outputRecord
        ? {
            rawOutput: {
              ...(outputText ? { content: outputText } : {}),
              ...(isRecord(outputRecord.details) ? outputRecord.details : {}),
            },
          }
        : {}),
      item: {
        input: args,
        ...(changes ? { changes } : {}),
      },
    },
  };
};

export const makePiAdapter = Effect.fn("makePiAdapter")(function* (options: PiAdapterOptions) {
  const crypto = yield* Crypto.Crypto;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const provideFiles = <A, E>(effect: Effect.Effect<A, E, FileSystem.FileSystem | Path.Path>) =>
    effect.pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
    );
  const root = yield* piInstanceStateRoot({
    stateDir: options.stateDir,
    instanceId: options.providerInstanceId,
  }).pipe(Effect.mapError((cause) => validation("startSession", "Invalid Pi state root.", cause)));
  const sessions = new Map<ThreadId, SessionContext>();
  const sessionFileLeases = new Map<string, SessionFileLease>();
  const threadLocks = yield* SynchronizedRef.make(new Map<ThreadId, Semaphore.Semaphore>());
  const getThreadLock = (threadId: ThreadId) =>
    SynchronizedRef.modifyEffect(threadLocks, (locks) => {
      const existing = locks.get(threadId);
      if (existing) return Effect.succeed([existing, locks] as const);
      return Semaphore.make(1).pipe(
        Effect.map((lock) => [lock, new Map(locks).set(threadId, lock)] as const),
      );
    });
  const withThreadLock = <A, E, R>(threadId: ThreadId, effect: Effect.Effect<A, E, R>) =>
    Effect.flatMap(getThreadLock(threadId), (lock) => lock.withPermit(effect));
  const events = yield* Queue.unbounded<ProviderRuntimeEvent>();
  const now = Effect.map(DateTime.now, DateTime.formatIso);
  const uuid = crypto.randomUUIDv4.pipe(
    Effect.mapError((cause) => request("crypto/randomUUIDv4", cause)),
  );
  const stamp = Effect.all({ eventId: Effect.map(uuid, EventId.make), createdAt: now });

  function validation(operation: string, issue: string, cause?: unknown) {
    return new ProviderAdapterValidationError({ provider: PROVIDER, operation, issue, cause });
  }
  function request(method: string, cause: unknown) {
    return new ProviderAdapterRequestError({
      provider: PROVIDER,
      method,
      detail: isRecord(cause) && typeof cause.detail === "string" ? cause.detail : String(cause),
      cause,
    });
  }
  const offer = (event: ProviderRuntimeEvent) => Queue.offer(events, event).pipe(Effect.asVoid);
  const base = Effect.fn("PiAdapter.eventBase")(function* (ctx: SessionContext, turn?: ActiveTurn) {
    return {
      ...(yield* stamp),
      provider: PROVIDER,
      providerInstanceId: options.providerInstanceId,
      threadId: ctx.session.threadId,
      ...(turn ? { turnId: turn.id } : {}),
    } as const;
  });
  const raw = (event: PiRpcEvent) => ({
    source: "pi.rpc.notification" as const,
    method: isRecord(event) ? string(event.type) : undefined,
    payload: event,
  });

  const publishTerminal = Effect.fn("PiAdapter.publishTerminal")(function* (
    ctx: SessionContext,
    expected: ActiveTurn,
    terminalEvents: ReadonlyArray<ProviderRuntimeEvent>,
    status: "ready" | "error",
    errorMessage?: string,
  ) {
    return yield* Effect.uninterruptible(
      Effect.gen(function* () {
        if (ctx.activeTurn !== expected || expected.terminal) return false;
        expected.terminal = true;
        ctx.activeTurn = undefined;
        yield* Effect.forEach(terminalEvents, offer, { discard: true });
        const { activeTurnId: _, ...session } = ctx.session;
        ctx.session = {
          ...session,
          status,
          ...(status === "ready" ? { resumeCursor: ctx.cursor } : {}),
          ...(errorMessage ? { lastError: errorMessage } : {}),
          updatedAt: yield* now,
        };
        return true;
      }),
    );
  });

  const close = Effect.fn("PiAdapter.close")(function* (ctx: SessionContext) {
    yield* Effect.uninterruptible(
      Effect.gen(function* () {
        if (ctx.stopped || ctx.closing) return;
        ctx.closing = true;
        const turn = ctx.activeTurn;
        if (turn && !turn.terminal) {
          const completedEvent = {
            type: "turn.completed",
            ...(yield* base(ctx, turn)),
            payload: { state: "interrupted", stopReason: "abort" },
          } as const;
          yield* publishTerminal(ctx, turn, [completedEvent], "ready");
        }
        const { activeTurnId: _, ...session } = ctx.session;
        ctx.session = { ...session, status: "closed", updatedAt: yield* now };
        yield* ctx.client.close().pipe(Effect.ignore);
        yield* Scope.close(ctx.scope, Exit.void).pipe(Effect.ignore);
        if (sessions.get(ctx.session.threadId) === ctx) {
          sessions.delete(ctx.session.threadId);
        }
        if (!ctx.lease.startupOwned && sessionFileLeases.get(ctx.cursor.sessionFile) === ctx.lease)
          sessionFileLeases.delete(ctx.cursor.sessionFile);
        ctx.stopped = true;
      }),
    );
  });

  const failActive = Effect.fn("PiAdapter.failActive")(function* (
    ctx: SessionContext,
    message: string,
    event?: PiRpcEvent,
    fatal = true,
  ) {
    const turn = ctx.activeTurn;
    if (!turn || turn.terminal) return;
    const errorEvent = {
      type: "runtime.error",
      ...(yield* base(ctx, turn)),
      payload: { message, class: "transport_error", ...(event ? { detail: event } : {}) },
      ...(event ? { raw: raw(event) } : {}),
    } as const;
    const completedEvent = {
      type: "turn.completed",
      ...(yield* base(ctx, turn)),
      payload: { state: "failed", errorMessage: message },
    } as const;
    yield* publishTerminal(
      ctx,
      turn,
      [errorEvent, completedEvent],
      fatal ? "error" : "ready",
      fatal ? message : undefined,
    );
  });

  const toolEventKey = (event: Record<string, unknown>) =>
    string(event.toolCallId) ?? string(event.toolCallID) ?? string(event.toolName) ?? "tool";

  const itemForTool = Effect.fn("PiAdapter.itemForTool")(function* (
    turn: ActiveTurn,
    event: Record<string, unknown>,
  ) {
    const key = toolEventKey(event);
    const existing = turn.toolItemIds.get(key);
    if (existing) return existing;
    const id = RuntimeItemId.make(`pi-tool:${turn.id}:${key}`);
    turn.toolItemIds.set(key, id);
    return id;
  });

  const handleEvent = Effect.fn("PiAdapter.handleEvent")(function* (
    ctx: SessionContext,
    native: PiRpcEvent,
  ) {
    if ("_tag" in native && native._tag === "PiRpcProtocolFailureEvent") {
      yield* failActive(ctx, String(native.detail), native);
      yield* close(ctx);
      return;
    }
    const event = native as Record<string, unknown>;
    const type = string(event.type);
    const turn = ctx.activeTurn;
    if (!turn || turn.terminal) return;
    if (type === "message_update") {
      const update = isRecord(event.assistantMessageEvent)
        ? event.assistantMessageEvent
        : undefined;
      const updateType = string(update?.type);
      const delta = string(update?.delta);
      if ((updateType === "text_delta" || updateType === "thinking_delta") && delta) {
        const isAssistant = updateType === "text_delta";
        const itemId = isAssistant ? turn.assistantItemId : turn.reasoningItemId;
        const started = isAssistant ? turn.assistantStarted : turn.reasoningStarted;
        if (!started) {
          if (isAssistant) turn.assistantStarted = true;
          else turn.reasoningStarted = true;
          yield* offer({
            type: "item.started",
            ...(yield* base(ctx, turn)),
            itemId,
            payload: {
              itemType: isAssistant ? "assistant_message" : "reasoning",
              status: "inProgress",
              title: isAssistant ? "Assistant message" : "Reasoning",
            },
            raw: raw(native),
          });
        }
        if (isAssistant) turn.assistantText += delta;
        yield* offer({
          type: "content.delta",
          ...(yield* base(ctx, turn)),
          itemId,
          payload: {
            streamKind: isAssistant ? "assistant_text" : "reasoning_text",
            delta,
          },
          raw: raw(native),
        });
      } else if (updateType === "error") {
        yield* failActive(
          ctx,
          string(update?.reason) ?? string(update?.error) ?? "Pi assistant failed.",
          native,
        );
      }
      return;
    }
    if (type?.startsWith("tool_execution_")) {
      // pi's partial result is a cumulative snapshot, not a delta. persisting
      // every snapshot duplicates all prior output and can outrun the serial
      // ingestion worker; start/end preserve lifecycle and final output.
      if (type === "tool_execution_update") return;
      const lifecycle = type === "tool_execution_start" ? "item.started" : "item.completed";
      const itemId = yield* itemForTool(turn, event);
      const toolKey = toolEventKey(event);
      const eventArgs = isRecord(event.args) ? event.args : undefined;
      if (eventArgs) turn.toolArgs.set(toolKey, eventArgs);
      const presentationEvent =
        eventArgs || !turn.toolArgs.has(toolKey)
          ? event
          : { ...event, args: turn.toolArgs.get(toolKey) };
      const isError = event.isError === true;
      yield* offer({
        type: lifecycle,
        ...(yield* base(ctx, turn)),
        itemId,
        payload: {
          ...piToolPresentation(presentationEvent),
          status:
            lifecycle === "item.completed" ? (isError ? "failed" : "completed") : "inProgress",
        },
        raw: raw(native),
      } as ProviderRuntimeEvent);
      return;
    }
    if (type === "agent_settled") {
      if (ctx.steeringPromptsInFlight > 0) {
        ctx.deferredSettlement = native;
        return;
      }
      const state = yield* Effect.gen(function* () {
        while (ctx.steeringPromptsInFlight === 0) {
          const steeringGeneration = ctx.steeringGeneration;
          const snapshot = yield* ctx.client.getState().pipe(
            Effect.mapError((cause) => request("get_state", cause)),
            Effect.exit,
          );
          if (ctx.steeringGeneration === steeringGeneration) return snapshot;
        }
        return undefined;
      });
      if (state === undefined) {
        ctx.deferredSettlement = native;
        return;
      }
      if (ctx.activeTurn !== turn || turn.terminal || ctx.closing || ctx.stopped) return;
      if (Exit.isFailure(state) || !piStateMatchesCursor(state.value, ctx.cursor)) {
        yield* failActive(ctx, "Pi session identity drifted during settlement.", native);
        yield* close(ctx);
        return;
      }
      if (state.value.isStreaming === true) return;
      const terminalEvents: ProviderRuntimeEvent[] = [];
      if (turn.assistantText.trim().length > 0) {
        terminalEvents.push({
          type: "item.completed",
          ...(yield* base(ctx, turn)),
          itemId: turn.assistantItemId,
          payload: {
            itemType: "assistant_message",
            status: "completed",
            title: "Assistant message",
          },
          raw: raw(native),
        });
      }
      if (turn.reasoningStarted) {
        terminalEvents.push({
          type: "item.completed",
          ...(yield* base(ctx, turn)),
          itemId: turn.reasoningItemId,
          payload: {
            itemType: "reasoning",
            status: "completed",
            title: "Reasoning",
          },
          raw: raw(native),
        });
      }
      terminalEvents.push({
        type: "turn.completed",
        ...(yield* base(ctx, turn)),
        payload: {
          state: turn.interruptRequested ? "interrupted" : "completed",
          stopReason: turn.interruptRequested ? "abort" : null,
        },
        raw: raw(native),
      });
      yield* publishTerminal(ctx, turn, terminalEvents, "ready");
    }
    // agent_end and turn_end are native cycle boundaries, not T3 settlement.
  });

  const requireSession = (threadId: ThreadId) => {
    const ctx = sessions.get(threadId);
    return ctx && !ctx.stopped
      ? Effect.succeed(ctx)
      : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
  };

  const startSession: ProviderAdapterShape<ProviderAdapterError>["startSession"] = (input) =>
    withThreadLock(
      input.threadId,
      Effect.scoped(
        Effect.gen(function* () {
          if (input.runtimeMode !== "full-access")
            return yield* validation("startSession", "Pi supports only full-access runtime mode.");
          if (input.provider && input.provider !== PROVIDER)
            return yield* validation("startSession", `Expected provider '${PROVIDER}'.`);
          if (input.providerInstanceId && input.providerInstanceId !== options.providerInstanceId)
            return yield* validation(
              "startSession",
              "Provider instance does not match this Pi adapter.",
            );
          if (sessions.has(input.threadId))
            return yield* validation(
              "startSession",
              `Thread '${input.threadId}' is already active.`,
            );
          const cwd = yield* provideFiles(
            fs.realPath(path.resolve(input.cwd ?? process.cwd())),
          ).pipe(
            Effect.mapError((cause) =>
              validation("startSession", "Invalid Pi working directory.", cause),
            ),
          );
          const fresh = input.resumeCursor === undefined;
          let cursor: PiSessionCursor | undefined;
          let freshFile: { readonly sessionFile: string } | undefined;
          const scope = yield* Scope.make();
          let transferred = false;
          let leasedFile: string | undefined;
          let startupLease: SessionFileLease | undefined;
          let candidateCtx: SessionContext | undefined;
          yield* Effect.addFinalizer(() =>
            transferred
              ? Effect.void
              : Effect.uninterruptible(
                  Effect.gen(function* () {
                    yield* Scope.close(scope, Exit.void).pipe(Effect.ignore);
                    if (candidateCtx && sessions.get(input.threadId) === candidateCtx)
                      sessions.delete(input.threadId);
                    const ownsStartupLease =
                      leasedFile !== undefined &&
                      startupLease !== undefined &&
                      sessionFileLeases.get(leasedFile) === startupLease;
                    const releaseStartupLease = Effect.sync(() => {
                      if (ownsStartupLease && sessionFileLeases.get(leasedFile!) === startupLease)
                        sessionFileLeases.delete(leasedFile!);
                    });
                    yield* freshFile && (leasedFile === undefined || ownsStartupLease)
                      ? provideFiles(cleanupFreshPiSessionFile(freshFile)).pipe(
                          Effect.ignore,
                          Effect.ensuring(releaseStartupLease),
                        )
                      : releaseStartupLease;
                  }),
                ),
          );
          if (fresh) {
            freshFile = yield* provideFiles(
              allocateFreshPiSessionFile({ stateRoot: root, fileId: yield* uuid }),
            ).pipe(Effect.mapError((cause) => request("session/allocate", cause)));
          } else {
            cursor = yield* Schema.decodeUnknownEffect(PiSessionCursor)(input.resumeCursor).pipe(
              Effect.mapError((cause) =>
                validation("startSession", "Invalid Pi resume cursor.", cause),
              ),
            );
            cursor = yield* provideFiles(
              validatePiResumeSessionFile({ stateRoot: root, cursor, cwd }),
            ).pipe(
              Effect.mapError((cause) =>
                validation("startSession", "Invalid Pi resume session file.", cause),
              ),
            );
          }
          const candidateFile = cursor?.sessionFile ?? freshFile!.sessionFile;
          const leaseOwner = sessionFileLeases.get(candidateFile);
          if (leaseOwner !== undefined)
            return yield* validation("startSession", "Pi session file already has a live writer.");
          startupLease = { startupOwned: true };
          sessionFileLeases.set(candidateFile, startupLease);
          leasedFile = candidateFile;
          const factory: PiRpcClientFactory = options.makeRpcClient ?? makePiRpcClient;
          const spawn = factory({
            command: options.binaryPath,
            args: [
              ...(options.args ?? []),
              "--session",
              cursor?.sessionFile ?? freshFile!.sessionFile,
              ...DETERMINISTIC_ARGS,
            ],
            cwd,
            ...(options.environment ? { env: options.environment } : {}),
          }).pipe(
            Effect.provideService(Scope.Scope, scope),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          );
          const started = yield* spawn.pipe(
            Effect.flatMap((client) =>
              client.getState().pipe(Effect.map((state) => ({ client, state }))),
            ),
            Effect.mapError((cause) => request("session/start", cause)),
            Effect.result,
          );
          if (
            fresh &&
            Result.isSuccess(started) &&
            started.success.state.sessionFile === freshFile!.sessionFile &&
            typeof started.success.state.sessionId === "string" &&
            started.success.state.sessionId.length > 0 &&
            started.success.state.sessionId.trim() === started.success.state.sessionId
          ) {
            cursor = {
              schemaVersion: 1,
              sessionFile: freshFile!.sessionFile,
              sessionId: started.success.state.sessionId,
            };
          }
          if (
            Result.isFailure(started) ||
            cursor === undefined ||
            !piStateMatchesCursor(started.success.state, cursor)
          ) {
            if (Result.isSuccess(started)) yield* started.success.client.close();
            yield* Scope.close(scope, Exit.void).pipe(Effect.ignore);
            if (Result.isFailure(started)) return yield* started.failure;
            return yield* validation("startSession", "Pi reported a different session path or id.");
          }
          // Fresh Pi must have replaced the private placeholder with its exact header.
          cursor = yield* provideFiles(
            validatePiResumeSessionFile({ stateRoot: root, cursor, cwd }),
          ).pipe(
            Effect.mapError((cause) =>
              validation("startSession", "Pi session header validation failed.", cause),
            ),
            Effect.onError(() =>
              started.success.client
                .close()
                .pipe(
                  Effect.andThen(Scope.close(scope, Exit.void)),
                  Effect.andThen(
                    freshFile ? provideFiles(cleanupFreshPiSessionFile(freshFile)) : Effect.void,
                  ),
                  Effect.ignore,
                ),
            ),
          );
          const createdAt = yield* now;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: options.providerInstanceId,
            threadId: input.threadId,
            status: "ready",
            runtimeMode: "full-access",
            cwd,
            ...(input.modelSelection ? { model: input.modelSelection.model } : {}),
            resumeCursor: cursor,
            createdAt,
            updatedAt: createdAt,
          };
          const ctx: SessionContext = {
            session,
            cursor,
            lease: startupLease,
            client: started.success.client,
            scope,
            eventFiber: undefined as never,
            activeTurn: undefined,
            managedOperations: new Map(),
            steeringPromptsInFlight: 0,
            steeringGeneration: 0,
            deferredSettlement: undefined,
            closing: false,
            stopped: false,
          };
          candidateCtx = ctx;
          sessions.set(input.threadId, ctx);
          if (options.onSessionPublished) yield* options.onSessionPublished();
          ctx.eventFiber = yield* started.success.client.events.pipe(
            Stream.runForEach((event) => handleEvent(ctx, event)),
            Effect.ensuring(
              Effect.suspend(() =>
                ctx.closing || ctx.stopped
                  ? Effect.void
                  : failActive(ctx, "Pi RPC event stream ended unexpectedly.").pipe(
                      Effect.andThen(close(ctx)),
                      Effect.orDie,
                    ),
              ),
            ),
            Effect.orDie,
            Effect.forkIn(scope),
          );
          yield* Effect.yieldNow;
          if (ctx.closing || ctx.stopped)
            return yield* validation("startSession", "Pi RPC event stream ended during startup.");
          startupLease.startupOwned = false;
          transferred = true;
          return session;
        }),
      ),
    );

  const makeActiveTurn = (turnId: TurnId): ActiveTurn => ({
    id: turnId,
    assistantItemId: RuntimeItemId.make(`pi-assistant:${turnId}`),
    reasoningItemId: RuntimeItemId.make(`pi-reasoning:${turnId}`),
    toolItemIds: new Map(),
    toolArgs: new Map(),
    assistantText: "",
    assistantStarted: false,
    reasoningStarted: false,
    interruptRequested: false,
    terminal: false,
  });

  const sendTurn: ProviderAdapterShape<ProviderAdapterError>["sendTurn"] = (input) => {
    let createdTurn: ActiveTurn | undefined;
    return Effect.scoped(
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          const ctx = yield* requireSession(input.threadId);
          const steeringTurn = ctx.activeTurn?.terminal === false ? ctx.activeTurn : undefined;
          if (!steeringTurn && ctx.session.status !== "ready")
            return yield* validation("sendTurn", "Pi session must be idle before prompting.");
          if (!input.operationId)
            return yield* validation("sendTurn", "Managed Pi turns require a stable operation id.");
          if (!input.input && (!input.attachments || input.attachments.length === 0))
            return yield* validation("sendTurn", "Pi requires non-empty text or attachments.");
          const selection = input.modelSelection;
          if (selection && selection.instanceId !== options.providerInstanceId)
            return yield* validation(
              "sendTurn",
              "Model selection belongs to another provider instance.",
            );
          const parsed = selection ? decodePiModelSlug(selection.model) : undefined;
          if (!parsed)
            return yield* validation("sendTurn", "A valid Pi model selection is required.");
          const rawThinking = getModelSelectionStringOptionValue(selection, "thinkingLevel");
          const thinking =
            rawThinking === undefined
              ? undefined
              : yield* Schema.decodeUnknownEffect(PiThinkingLevel)(rawThinking).pipe(
                  Effect.mapError((cause) =>
                    validation("sendTurn", "Invalid Pi thinking level.", cause),
                  ),
                );
          if (
            ctx.closing ||
            ctx.stopped ||
            sessions.get(input.threadId) !== ctx ||
            (!steeringTurn && ctx.session.status !== "ready")
          )
            return yield* new ProviderAdapterSessionNotFoundError({
              provider: PROVIDER,
              threadId: input.threadId,
            });

          const operationKey = `managed-pi:turn-start:${input.operationId}`;
          const managedClaim = yield* options.supervisor
            .claimManaged({
              protocol: MANAGED_ADMISSION_PROTOCOL,
              intent: input.admissionMode === "recover-durable" ? "recover-existing" : "execute",
              operationKey,
              payload: {
                type: "managed-pi.turn-start",
                providerInstanceId: options.providerInstanceId,
                threadId: input.threadId,
                session: ctx.cursor,
                message: input.input ?? "",
                attachments: input.attachments ?? [],
                model: { provider: parsed.provider, modelId: parsed.modelId },
                thinkingLevel: thinking ?? null,
                interactionMode: input.interactionMode ?? DEFAULT_PROVIDER_INTERACTION_MODE,
              },
            })
            .pipe(Effect.mapError((cause) => request("supervisor.claimManaged", cause)));

          if (managedClaim.status === "completed") {
            const turnId = TurnId.make(managedClaim.receipt.turnId);
            const knownTurnId = ctx.managedOperations.get(operationKey);
            if (knownTurnId && knownTurnId !== turnId)
              return yield* validation(
                "sendTurn",
                "Managed Pi admission receipt changed its provider turn id.",
              );
            // The durable receipt reconciles prompt admission only. PiAdapter still owns
            // provider events, and does not synthesize tool, model, or terminal events here.
            if (!knownTurnId && !ctx.activeTurn && ctx.session.status === "ready") {
              ctx.managedOperations.set(operationKey, turnId);
              const reconciled = makeActiveTurn(turnId);
              createdTurn = reconciled;
              ctx.activeTurn = reconciled;
              ctx.session = {
                ...ctx.session,
                status: "running",
                activeTurnId: turnId,
                updatedAt: yield* now,
              };
              yield* offer({
                type: "turn.started",
                ...(yield* base(ctx, reconciled)),
                payload: {
                  model: selection!.model,
                  ...(thinking ? { effort: thinking } : {}),
                },
              });
            }
            return {
              _tag: "Result" as const,
              result: { threadId: input.threadId, turnId, resumeCursor: ctx.cursor },
            };
          }
          if (managedClaim.status === "conflict")
            return yield* validation("sendTurn", managedClaim.error);
          if (managedClaim.status === "absent")
            return yield* request(
              "supervisor.claimManaged",
              "Recovered Pi admission has no durable managed claim. It may predate the admission fence, so the prompt was not resent.",
            );
          if (managedClaim.status === "rejected")
            return yield* request("supervisor.claimManaged", managedClaim.error);
          if (managedClaim.status === "delivering")
            return yield* request(
              "supervisor.claimManaged",
              "Managed Pi admission is already delivering; the prompt was not resent.",
            );
          if (managedClaim.status === "indeterminate")
            return yield* request(
              "supervisor.claimManaged",
              `${managedClaim.error} The prompt was not resent.`,
            );

          const finalize = (finalization: Parameters<typeof managedClaim.finalize>[0]) =>
            managedClaim.finalize(finalization).pipe(
              Effect.mapError((cause) => request("supervisor.finalizeManaged", cause)),
              Effect.flatMap((result) =>
                result.status === "finalized"
                  ? Effect.void
                  : Effect.fail(
                      request(
                        "supervisor.finalizeManaged",
                        `Managed Pi admission lease is stale (${result.operation.status}).`,
                      ),
                    ),
              ),
            );
          const finalizeFailure = (cause: unknown, indeterminate: boolean) =>
            finalize({
              status: indeterminate ? "indeterminate" : "rejected",
              error: cause instanceof Error ? cause.message : String(cause),
            });

          const materializedImages = yield* Effect.forEach(
            input.attachments ?? [],
            (attachment) => {
              const attachmentPath = resolveAttachmentPath({
                attachmentsDir: options.attachmentsDir,
                attachment,
              });
              if (!attachmentPath)
                return Effect.fail(request("prompt", `Invalid attachment id '${attachment.id}'.`));
              return provideFiles(fs.readFile(attachmentPath)).pipe(
                Effect.map((bytes) => ({
                  type: "image" as const,
                  data: Buffer.from(bytes).toString("base64"),
                  mimeType: attachment.mimeType,
                })),
                Effect.mapError((cause) => request("prompt", cause)),
              );
            },
            { concurrency: 1 },
          ).pipe(Effect.result);
          if (Result.isFailure(materializedImages)) {
            yield* finalizeFailure(materializedImages.failure, false);
            return yield* materializedImages.failure;
          }
          const images = materializedImages.success;

          const available = yield* ctx.client.getAvailableModels().pipe(
            Effect.mapError((cause) => request("get_available_models", cause)),
            Effect.result,
          );
          if (Result.isFailure(available)) {
            yield* finalizeFailure(available.failure, false);
            return yield* available.failure;
          }
          if (
            !available.success.models.some(
              (model) => model.provider === parsed.provider && model.id === parsed.modelId,
            )
          ) {
            const unavailable = validation(
              "sendTurn",
              "Selected Pi model is not currently available.",
            );
            yield* finalizeFailure(unavailable, false);
            return yield* unavailable;
          }

          if (steeringTurn && ctx.activeTurn === steeringTurn) {
            ctx.steeringPromptsInFlight += 1;
            ctx.steeringGeneration += 1;
            return {
              _tag: "Steer" as const,
              effect: Effect.gen(function* () {
                const prompted = yield* ctx.client
                  .prompt(input.input ?? "", images, "steer")
                  .pipe(Effect.result);
                if (Result.isFailure(prompted))
                  return yield* Effect.gen(function* () {
                    const finalized = yield* finalizeFailure(prompted.failure, true).pipe(
                      Effect.result,
                    );
                    if (Result.isFailure(finalized)) return yield* finalized.failure;
                    return yield* request("prompt", prompted.failure);
                  }).pipe(Effect.ensuring(close(ctx).pipe(Effect.orDie)));
                const finalized = yield* finalize({
                  status: "completed",
                  receipt: { turnId: steeringTurn.id },
                }).pipe(Effect.result);
                if (Result.isFailure(finalized)) {
                  yield* close(ctx);
                  return yield* finalized.failure;
                }
                ctx.managedOperations.set(operationKey, steeringTurn.id);
                if (steeringTurn.interruptRequested)
                  yield* ctx.client
                    .abort()
                    .pipe(Effect.mapError((cause) => request("abort", cause)));
                return {
                  threadId: input.threadId,
                  turnId: steeringTurn.id,
                  resumeCursor: ctx.cursor,
                };
              }).pipe(
                Effect.ensuring(
                  Effect.gen(function* () {
                    ctx.steeringPromptsInFlight -= 1;
                    const deferredSettlement = ctx.deferredSettlement;
                    ctx.deferredSettlement = undefined;
                    if (deferredSettlement)
                      yield* handleEvent(ctx, deferredSettlement).pipe(Effect.orDie);
                  }),
                ),
                Effect.uninterruptible,
              ),
            };
          }

          const configured = yield* Effect.gen(function* () {
            if (
              ctx.closing ||
              ctx.stopped ||
              sessions.get(input.threadId) !== ctx ||
              ctx.session.status !== "ready"
            )
              return yield* new ProviderAdapterSessionNotFoundError({
                provider: PROVIDER,
                threadId: input.threadId,
              });
            yield* ctx.client
              .setModel(parsed.provider, parsed.modelId)
              .pipe(Effect.mapError((cause) => request("set_model", cause)));
            if (thinking !== undefined)
              yield* ctx.client
                .setThinkingLevel(thinking)
                .pipe(Effect.mapError((cause) => request("set_thinking_level", cause)));
            if (
              ctx.closing ||
              ctx.stopped ||
              sessions.get(input.threadId) !== ctx ||
              ctx.session.status !== "ready"
            )
              return yield* new ProviderAdapterSessionNotFoundError({
                provider: PROVIDER,
                threadId: input.threadId,
              });
          }).pipe(Effect.result);
          if (Result.isFailure(configured)) {
            yield* finalizeFailure(configured.failure, false);
            return yield* configured.failure;
          }

          const turnId = TurnId.make(yield* uuid);
          const turn = makeActiveTurn(turnId);
          createdTurn = turn;
          ctx.activeTurn = turn;
          ctx.session = {
            ...ctx.session,
            status: "running",
            activeTurnId: turnId,
            updatedAt: yield* now,
          };
          yield* offer({
            type: "turn.started",
            ...(yield* base(ctx, turn)),
            payload: {
              model: selection!.model,
              ...(thinking ? { effort: thinking } : {}),
            },
          });
          const prompted = yield* ctx.client.prompt(input.input ?? "", images).pipe(Effect.result);
          if (Result.isFailure(prompted))
            return yield* Effect.gen(function* () {
              const finalized = yield* finalizeFailure(prompted.failure, true).pipe(Effect.result);
              yield* failActive(ctx, "Pi prompt failed.");
              if (Result.isFailure(finalized)) return yield* finalized.failure;
              return yield* request("prompt", prompted.failure);
            }).pipe(Effect.ensuring(close(ctx).pipe(Effect.orDie)));
          const finalized = yield* finalize({
            status: "completed",
            receipt: { turnId },
          }).pipe(Effect.result);
          if (Result.isFailure(finalized)) {
            yield* failActive(ctx, "Pi prompt admission finalization failed.", undefined, true);
            yield* close(ctx);
            return yield* finalized.failure;
          }
          ctx.managedOperations.set(operationKey, turnId);
          return {
            _tag: "Result" as const,
            result: { threadId: input.threadId, turnId, resumeCursor: ctx.cursor },
          };
        }),
      ).pipe(
        Effect.flatMap((action) =>
          action._tag === "Steer" ? action.effect : Effect.succeed(action.result),
        ),
      ),
    ).pipe(
      Effect.onInterrupt(() =>
        Effect.suspend(() => {
          const ctx = sessions.get(input.threadId);
          return ctx && ctx.activeTurn === createdTurn ? close(ctx) : Effect.void;
        }),
      ),
    );
  };

  const interruptTurn: ProviderAdapterShape<ProviderAdapterError>["interruptTurn"] = (
    threadId,
    turnId,
  ) =>
    withThreadLock(
      threadId,
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const turn = ctx.activeTurn;
        if (!turn || (turnId && turn.id !== turnId))
          return yield* validation("interruptTurn", "No matching active Pi turn.");
        turn.interruptRequested = true;
        yield* ctx.client.abort().pipe(Effect.mapError((cause) => request("abort", cause)));
      }),
    );
  const unsupported = (operation: string, threadId: ThreadId) =>
    requireSession(threadId).pipe(
      Effect.andThen(validation(operation, `Pi does not support ${operation}.`)),
    );
  const stopSession = (threadId: ThreadId) =>
    withThreadLock(threadId, sessions.has(threadId) ? close(sessions.get(threadId)!) : Effect.void);
  const stopAll = () =>
    Effect.forEach([...sessions.keys()], stopSession, { discard: true, concurrency: "unbounded" });
  yield* Effect.addFinalizer(() => stopAll().pipe(Effect.ignore));

  return {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "in-session" },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest: (threadId) => unsupported("respondToRequest", threadId),
    respondToUserInput: (threadId) => unsupported("respondToUserInput", threadId),
    readThread: (threadId) => unsupported("readThread", threadId),
    rollbackThread: (threadId) => unsupported("rollbackThread", threadId),
    stopSession,
    listSessions: () =>
      Effect.sync(() => [...sessions.values()].map((ctx) => ({ ...ctx.session }))),
    hasSession: (threadId) => Effect.sync(() => sessions.has(threadId)),
    stopAll,
    streamEvents: Stream.fromQueue(events),
  } satisfies ProviderAdapterShape<ProviderAdapterError>;
});
