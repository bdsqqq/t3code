import {
  CommandId,
  type PiNativeCommand,
  type PiNativeJsonlEntry,
  type PiNativeRuntimeId,
  type PiNativeRuntimeState,
  type PiNativeSessionKey,
  type PiNativeStreamItem,
  WS_METHODS,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { Atom } from "effect/unstable/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import { request, subscribe } from "../rpc/client.ts";
import {
  createEnvironmentCommand,
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
  createEnvironmentSubscriptionAtomFamily,
} from "./runtime.ts";

export interface PiNativeRuntimeView {
  readonly runtime: PiNativeRuntimeState | null;
  readonly entries: ReadonlyArray<PiNativeJsonlEntry>;
  readonly events: ReadonlyArray<Extract<PiNativeStreamItem, { readonly type: "event" }>>;
  readonly synchronized: boolean;
  readonly exitCode?: number;
}

export const PI_NATIVE_EVENT_LIMIT = 500;
export const PI_NATIVE_ENTRY_LIMIT = 1_000;

export const EMPTY_PI_NATIVE_RUNTIME_VIEW: PiNativeRuntimeView = {
  runtime: null,
  entries: [],
  events: [],
  synchronized: false,
};

function piNativeEventType(event: unknown): string | null {
  if (typeof event !== "object" || event === null || Array.isArray(event)) return null;
  const record = event as Readonly<Record<string, unknown>>;
  if (typeof record.type === "string" && record.type !== "event") return record.type;
  return typeof record.event === "string" ? record.event : null;
}

function bridgeReconnectStreaming(event: unknown): boolean | null {
  if (typeof event !== "object" || event === null || Array.isArray(event)) return null;
  const value = (event as Readonly<Record<string, unknown>>).isStreaming;
  return typeof value === "boolean" ? value : null;
}

function piNativeEntryIdentity(entry: PiNativeJsonlEntry): string {
  if (typeof entry.id === "string") return `id:${entry.id}`;
  return `json:${JSON.stringify(entry)}`;
}

export function reducePiNativeStream(
  state: PiNativeRuntimeView,
  item: PiNativeStreamItem,
): PiNativeRuntimeView {
  if (item.type === "snapshot") {
    return {
      runtime: item.runtime,
      entries: item.entries,
      events: item.events.slice(-PI_NATIVE_EVENT_LIMIT),
      synchronized: false,
    };
  }
  const sequence = state.runtime?.sequence ?? 0;
  if (item.type === "synchronized" && item.sequence === sequence) {
    return state.synchronized ? state : { ...state, synchronized: true };
  }
  if (item.sequence <= sequence) return state;
  const runtime = state.runtime === null ? null : { ...state.runtime, sequence: item.sequence };
  if (item.type === "event") {
    const eventType = piNativeEventType(item.event);
    const reconnectStreaming =
      eventType === "bridge_reconnected" ? bridgeReconnectStreaming(item.event) : null;
    const nextRuntime =
      runtime === null
        ? null
        : {
            ...runtime,
            ...(eventType === "agent_start"
              ? { status: "streaming" as const }
              : eventType === "agent_settled"
                ? { status: "idle" as const }
                : eventType === "bridge_disconnected"
                  ? { status: "starting" as const }
                  : eventType === "bridge_reconnected"
                    ? {
                        status:
                          reconnectStreaming === true ? ("streaming" as const) : ("idle" as const),
                      }
                    : {}),
            overlay: {
              ...(runtime.overlay ?? { isStreaming: false, pendingMessageCount: 0 }),
              isStreaming:
                eventType === "agent_start"
                  ? true
                  : eventType === "agent_settled"
                    ? false
                    : eventType === "bridge_reconnected" && reconnectStreaming !== null
                      ? reconnectStreaming
                      : (runtime.overlay?.isStreaming ?? false),
              ...(eventType === null ? {} : { lastEventType: eventType }),
            },
          };
    const events = [...state.events, item];
    return {
      ...state,
      runtime: nextRuntime,
      events: events.length > PI_NATIVE_EVENT_LIMIT ? events.slice(-PI_NATIVE_EVENT_LIMIT) : events,
    };
  }
  if (item.type === "entries") {
    const identities = new Set(state.entries.map(piNativeEntryIdentity));
    const entries = [
      ...state.entries,
      ...item.entries.filter((entry) => {
        const identity = piNativeEntryIdentity(entry);
        if (identities.has(identity)) return false;
        identities.add(identity);
        return true;
      }),
    ];
    return {
      ...state,
      runtime,
      entries:
        entries.length <= PI_NATIVE_ENTRY_LIMIT
          ? entries
          : [entries[0]!, ...entries.slice(-(PI_NATIVE_ENTRY_LIMIT - 1))],
      events: [],
    };
  }
  if (item.type === "synchronized") {
    return { ...state, runtime, synchronized: true };
  }
  return {
    ...state,
    runtime: runtime === null ? null : { ...runtime, status: "exited" },
    synchronized: true,
    ...(item.exitCode === undefined ? {} : { exitCode: item.exitCode }),
  };
}

function makeCommandId(commandId?: CommandId) {
  if (commandId !== undefined) return Effect.succeed(commandId);
  return Crypto.Crypto.pipe(
    Effect.flatMap((crypto) => crypto.randomUUIDv4),
    Effect.orDie,
    Effect.map(CommandId.make),
  );
}

export const buildPiNativeStart = (input: {
  readonly cwd: string;
  readonly commandId?: CommandId;
}) =>
  makeCommandId(input.commandId).pipe(
    Effect.map(
      (commandId) => ({ type: "start", commandId, cwd: input.cwd }) satisfies PiNativeCommand,
    ),
  );
export const buildPiNativeResume = (input: {
  readonly sessionKey: PiNativeSessionKey;
  readonly commandId?: CommandId;
}) =>
  makeCommandId(input.commandId).pipe(
    Effect.map(
      (commandId) =>
        ({ type: "resume", commandId, sessionKey: input.sessionKey }) satisfies PiNativeCommand,
    ),
  );
export const buildPiNativeMessage = (input: {
  readonly type: "send" | "steer" | "followUp";
  readonly runtimeId: PiNativeRuntimeId;
  readonly message: string;
  readonly commandId?: CommandId;
}) =>
  makeCommandId(input.commandId).pipe(
    Effect.map((commandId) => ({ ...input, commandId }) satisfies PiNativeCommand),
  );
export const buildPiNativeControl = (input: {
  readonly type: "abort" | "shutdown";
  readonly runtimeId: PiNativeRuntimeId;
  readonly commandId?: CommandId;
}) =>
  makeCommandId(input.commandId).pipe(
    Effect.map((commandId) => ({ ...input, commandId }) satisfies PiNativeCommand),
  );

export function createPiNativeEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | Crypto.Crypto | R, E>,
) {
  return {
    commandId: createEnvironmentCommand(runtime, {
      label: "environment-data:pi-native:command-id",
      execute: (_input: void) => makeCommandId(),
    }),
    list: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:pi-native:list",
      tag: WS_METHODS.piNativeList,
      staleTimeMs: 2_000,
    }),
    read: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:pi-native:read",
      tag: WS_METHODS.piNativeRead,
      staleTimeMs: 2_000,
    }),
    runtime: createEnvironmentSubscriptionAtomFamily(runtime, {
      label: "environment-data:pi-native:runtime",
      idleTtlMs: 0,
      subscribe: (input: { readonly runtimeId: PiNativeRuntimeId }) =>
        // The atom cannot feed its reduced cursor back into a reconnecting subscription.
        // Reconnect therefore requests an authoritative snapshot; snapshot replacement is
        // intentional and prevents gaps when transport loss happens between events.
        subscribe(WS_METHODS.piNativeSubscribe, input, {
          onExpectedFailure: () => Effect.void,
          retryExpectedFailureAfter: "500 millis",
        }).pipe(Stream.scan(EMPTY_PI_NATIVE_RUNTIME_VIEW, reducePiNativeStream)),
    }),
    dispatch: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:pi-native:dispatch",
      tag: WS_METHODS.piNativeDispatch,
    }),
    start: createEnvironmentCommand(runtime, {
      label: "environment-data:pi-native:start",
      execute: (input: Parameters<typeof buildPiNativeStart>[0]) =>
        buildPiNativeStart(input).pipe(
          Effect.flatMap((command) => request(WS_METHODS.piNativeDispatch, command)),
        ),
    }),
    resume: createEnvironmentCommand(runtime, {
      label: "environment-data:pi-native:resume",
      execute: (input: Parameters<typeof buildPiNativeResume>[0]) =>
        buildPiNativeResume(input).pipe(
          Effect.flatMap((command) => request(WS_METHODS.piNativeDispatch, command)),
        ),
    }),
    message: createEnvironmentCommand(runtime, {
      label: "environment-data:pi-native:message",
      execute: (input: Parameters<typeof buildPiNativeMessage>[0]) =>
        buildPiNativeMessage(input).pipe(
          Effect.flatMap((command) => request(WS_METHODS.piNativeDispatch, command)),
        ),
    }),
    control: createEnvironmentCommand(runtime, {
      label: "environment-data:pi-native:control",
      execute: (input: Parameters<typeof buildPiNativeControl>[0]) =>
        buildPiNativeControl(input).pipe(
          Effect.flatMap((command) => request(WS_METHODS.piNativeDispatch, command)),
        ),
    }),
  };
}
