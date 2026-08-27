// @effect-diagnostics globalTimers:off
// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics preferSchemaOverJson:off
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type {
  ManagedClaimRequest,
  ManagedClaimResponse,
  ManagedFinalization,
  ManagedFinalizeRequest,
  ManagedFinalizeResponse,
  ManagedOperationState,
  ManagedPiTurnStartPayload,
  SupervisorCommandReceipt,
  SupervisorRuntimeState,
  SupervisorStreamEvent,
  SupervisorStreamItem,
} from "./SupervisorProtocol.ts";
import {
  GUARDED_RESUME_CAPABILITY,
  JsonLineDecoder,
  MANAGED_ADMISSION_PROTOCOL,
  SUPERVISOR_MAX_STREAM_ITEM_BYTES,
  SUPERVISOR_PROTOCOL,
  encodeLine,
  isRecord,
} from "./SupervisorProtocol.ts";
import { defaultPiSessionsRoot } from "./PiSessionsRoot.ts";

const { createHash, randomUUID } = NodeCrypto;
const { spawn } = NodeChildProcess;
const fs = NodeFS.promises;
const { createConnection, createServer } = NodeNet;
const { homedir } = NodeOS;
const path = NodePath;
type ChildProcessWithoutNullStreams = NodeChildProcess.ChildProcessWithoutNullStreams;
type Socket = NodeNet.Socket;
export function decodeRuntimeChunk(decoder: JsonLineDecoder, chunk: string) {
  try {
    return { frames: decoder.push(chunk), error: undefined };
  } catch (error) {
    return { frames: [] as unknown[], error };
  }
}
const ROOT =
  process.env.T3_PI_SUPERVISOR_ROOT ?? path.join(homedir(), ".pi", "agent", SUPERVISOR_PROTOCOL);
export const supervisorSocketPath = path.join(ROOT, "supervisor.sock");
const LEDGER = path.join(ROOT, "commands.json");
const RING_SIZE = 1_000;
const EXITED_RETENTION_MS = 60_000;
const MAX_SOCKET_QUEUE_BYTES = 16 * 1024 * 1024;
const MAX_OVERLAY_BYTES = 8 * 1024 * 1024;
const MAX_RING_BYTES = 16 * 1024 * 1024;
const MAX_PENDING_QUEUE_BYTES = 4 * 1024 * 1024;
const PENDING_QUEUE_ENVELOPE_RESERVE_BYTES = 1_024;
const SNAPSHOT_ENTRY_LIMIT = 1_000;
const SNAPSHOT_HEAD_BYTES = 256 * 1024;
const SNAPSHOT_TAIL_BYTES = 16 * 1024 * 1024;
class IndeterminateCommandError extends Error {}
class RpcCommandRejectedError extends Error {}
class SessionWriterClaimConflictError extends Error {
  readonly sessionFile: string;
  readonly runtimeId: string;

  constructor(sessionFile: string, runtimeId: string) {
    super("session already has a writer");
    this.sessionFile = sessionFile;
    this.runtimeId = runtimeId;
  }
}
type CommandLedgerEntry = {
  hash: string;
  command?: Record<string, unknown>;
  phase?: "queued" | "delivering";
  receipt: SupervisorCommandReceipt;
};
export type ManagedLedgerEntry = {
  readonly kind: "managedAdmission";
  readonly protocol: typeof MANAGED_ADMISSION_PROTOCOL;
  readonly hash: string;
  readonly receipt: SupervisorCommandReceipt;
  readonly operation: Exclude<ManagedOperationState, { readonly status: "absent" }>;
};
type LedgerEntry = CommandLedgerEntry | ManagedLedgerEntry;
type Runtime = {
  state: SupervisorRuntimeState;
  child?: ChildProcessWithoutNullStreams;
  bridge?: Socket;
  bridgeExpiry?: NodeJS.Timeout;
  ring: SupervisorStreamItem[];
  ringBytes: number;
  ringEvictedThrough: number;
  overlayEvents: SupervisorStreamEvent[];
  overlayBytes: number;
  overlayOmittedCount: number;
  pendingQueueEvent?: SupervisorStreamEvent;
  sessionReadOffset: number;
  subscribers: Set<{
    socket: Socket;
    requestId: string;
    ready: boolean;
    buffer: SupervisorStreamItem[];
    bufferBytes: number;
  }>;
  nextRpcId: number;
  pending: Map<
    string,
    { resolve: (response: Record<string, unknown>) => void; reject: (cause: Error) => void }
  >;
  bridgePending: Map<string, (frame: Record<string, unknown>) => void>;
};
const runtimes = new Map<string, Runtime>();
const writers = new Map<string, string>();
/** lets a racing resume await the writer claim that won before its child is ready. */
const startingRuntimes = new Map<string, Promise<Runtime | undefined>>();
const bridgeRegistrations = new Set<string>();
const liveCommands = new Map<string, { hash: string; work: Promise<SupervisorCommandReceipt> }>();
const resumeAndSendQueues = new Map<string, Promise<void>>();
let ledger: Record<string, LedgerEntry> = {};
let ledgerWrite = Promise.resolve();
const socketWrites = new WeakMap<
  Socket,
  { blocked: boolean; queue: string[]; queuedBytes: number }
>();
const canonicalJson = (value: unknown): string => {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (isRecord(value))
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(",")}}`;
  return JSON.stringify(value);
};

const hasOnlyKeys = (value: Record<string, unknown>, allowed: ReadonlySet<string>): boolean =>
  Object.keys(value).every((key) => allowed.has(key));

const requireStableString = (value: unknown, field: string): string => {
  if (typeof value !== "string" || value.length === 0 || value.trim() !== value)
    throw new Error(`invalid managed admission ${field}`);
  return value;
};

const decodeManagedPayload = (value: unknown): ManagedPiTurnStartPayload => {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(
      value,
      new Set([
        "type",
        "providerInstanceId",
        "threadId",
        "session",
        "message",
        "attachments",
        "model",
        "thinkingLevel",
        "interactionMode",
      ]),
    ) ||
    value.type !== "managed-pi.turn-start" ||
    typeof value.message !== "string" ||
    !Array.isArray(value.attachments) ||
    !isRecord(value.session) ||
    !hasOnlyKeys(value.session, new Set(["schemaVersion", "sessionFile", "sessionId"])) ||
    value.session.schemaVersion !== 1 ||
    !isRecord(value.model) ||
    !hasOnlyKeys(value.model, new Set(["provider", "modelId"])) ||
    (value.interactionMode !== "default" && value.interactionMode !== "plan")
  )
    throw new Error("invalid managed Pi turn-start payload");
  const thinkingLevels = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);
  if (value.thinkingLevel !== null && !thinkingLevels.has(String(value.thinkingLevel)))
    throw new Error("invalid managed Pi thinking level");
  const attachments = value.attachments.map((attachment) => {
    if (
      !isRecord(attachment) ||
      !hasOnlyKeys(attachment, new Set(["type", "id", "name", "mimeType", "sizeBytes"])) ||
      attachment.type !== "image" ||
      typeof attachment.sizeBytes !== "number" ||
      !Number.isSafeInteger(attachment.sizeBytes) ||
      attachment.sizeBytes < 0
    )
      throw new Error("invalid managed Pi attachment identity");
    return {
      type: "image" as const,
      id: requireStableString(attachment.id, "attachment id"),
      name: requireStableString(attachment.name, "attachment name"),
      mimeType: requireStableString(attachment.mimeType, "attachment mime type"),
      sizeBytes: attachment.sizeBytes,
    };
  });
  return {
    type: "managed-pi.turn-start",
    providerInstanceId: requireStableString(value.providerInstanceId, "provider instance id"),
    threadId: requireStableString(value.threadId, "thread id"),
    session: {
      schemaVersion: 1,
      sessionFile: requireStableString(value.session.sessionFile, "session file"),
      sessionId: requireStableString(value.session.sessionId, "session id"),
    },
    message: value.message,
    attachments,
    model: {
      provider: requireStableString(value.model.provider, "model provider"),
      modelId: requireStableString(value.model.modelId, "model id"),
    },
    thinkingLevel: value.thinkingLevel as ManagedPiTurnStartPayload["thinkingLevel"],
    interactionMode: value.interactionMode,
  };
};

const decodeManagedClaim = (value: unknown): ManagedClaimRequest => {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, new Set(["protocol", "intent", "operationKey", "payload"])) ||
    value.protocol !== MANAGED_ADMISSION_PROTOCOL ||
    (value.intent !== "execute" && value.intent !== "recover-existing")
  )
    throw new Error("unsupported managed admission claim protocol");
  const operationKey = requireStableString(value.operationKey, "operation key");
  if (!operationKey.startsWith("managed-pi:turn-start:") || operationKey.length > 1_024)
    throw new Error("invalid managed admission operation namespace");
  return {
    protocol: MANAGED_ADMISSION_PROTOCOL,
    intent: value.intent,
    operationKey,
    payload: decodeManagedPayload(value.payload),
  };
};

const decodeManagedFinalization = (value: unknown): ManagedFinalizeRequest => {
  if (
    !isRecord(value) ||
    !hasOnlyKeys(value, new Set(["protocol", "operationKey", "leaseToken", "finalization"])) ||
    value.protocol !== MANAGED_ADMISSION_PROTOCOL ||
    !isRecord(value.finalization)
  )
    throw new Error("invalid managed admission finalization");
  const operationKey = requireStableString(value.operationKey, "operation key");
  if (!operationKey.startsWith("managed-pi:turn-start:") || operationKey.length > 1_024)
    throw new Error("invalid managed admission operation namespace");
  const leaseToken = requireStableString(value.leaseToken, "lease token");
  let finalization: ManagedFinalization;
  if (value.finalization.status === "completed") {
    if (
      !hasOnlyKeys(value.finalization, new Set(["status", "receipt"])) ||
      !isRecord(value.finalization.receipt) ||
      !hasOnlyKeys(value.finalization.receipt, new Set(["turnId"]))
    )
      throw new Error("invalid managed admission completed receipt");
    finalization = {
      status: "completed",
      receipt: {
        turnId: requireStableString(value.finalization.receipt.turnId, "provider turn id"),
      },
    };
  } else if (
    value.finalization.status === "rejected" ||
    value.finalization.status === "indeterminate"
  ) {
    if (!hasOnlyKeys(value.finalization, new Set(["status", "error"])))
      throw new Error("invalid managed admission failure receipt");
    finalization = {
      status: value.finalization.status,
      error: requireStableString(value.finalization.error, "finalization error"),
    };
  } else {
    throw new Error("invalid managed admission finalization status");
  }
  return { protocol: MANAGED_ADMISSION_PROTOCOL, operationKey, leaseToken, finalization };
};

export function piRpcSpawnArgs(input: {
  readonly sessionsRoot: string;
  readonly sessionFile?: string;
}): string[] {
  return [
    "--mode",
    "rpc",
    "--session-dir",
    input.sessionsRoot,
    ...(input.sessionFile ? ["--session", input.sessionFile] : []),
  ];
}
export function shouldRestartPersistedSession(
  command: Record<string, unknown>,
  receipt: SupervisorCommandReceipt,
  runtime: SupervisorRuntimeState | undefined,
): boolean {
  return (
    command.type === "start" &&
    typeof command.sessionFile === "string" &&
    receipt.status === "completed" &&
    receipt.runtimeId !== undefined &&
    (runtime === undefined || runtime.status === "exited")
  );
}
const hashCommand = (command: unknown) =>
  createHash("sha256").update(canonicalJson(command)).digest("hex");
const atomicLedger = async () => {
  const snapshot = JSON.stringify(ledger);
  const write = ledgerWrite.then(async () => {
    const temp = `${LEDGER}.${process.pid}.${randomUUID()}.tmp`;
    try {
      await fs.writeFile(temp, snapshot, { mode: 0o600 });
      await fs.rename(temp, LEDGER);
      await fs.chmod(LEDGER, 0o600);
    } finally {
      await fs.rm(temp, { force: true });
    }
  });
  ledgerWrite = write.catch(() => {});
  await write;
};

const isManagedLedgerEntry = (value: unknown): value is ManagedLedgerEntry =>
  isRecord(value) &&
  value.kind === "managedAdmission" &&
  value.protocol === MANAGED_ADMISSION_PROTOCOL &&
  typeof value.hash === "string" &&
  isRecord(value.operation) &&
  (value.operation.status === "delivering" ||
    (value.operation.status === "completed" &&
      isRecord(value.operation.receipt) &&
      typeof value.operation.receipt.turnId === "string" &&
      value.operation.receipt.turnId.length > 0) ||
    ((value.operation.status === "rejected" || value.operation.status === "indeterminate") &&
      typeof value.operation.error === "string" &&
      value.operation.error.length > 0));

interface ManagedAdmissionStore {
  readonly get: (operationKey: string) => unknown;
  readonly set: (operationKey: string, entry: ManagedLedgerEntry) => void;
  readonly entries: () => ReadonlyArray<{
    readonly operationKey: string;
    readonly entry: unknown;
  }>;
  readonly persist: () => Promise<void>;
}

interface ManagedLeaseTimer {
  readonly cancel: () => void;
}

export interface ManagedAdmissionController<TSocket extends object> {
  readonly claim: (socket: TSocket, claim: ManagedClaimRequest) => Promise<ManagedClaimResponse>;
  readonly finalize: (
    socket: TSocket,
    request: ManagedFinalizeRequest,
  ) => Promise<ManagedFinalizeResponse>;
  readonly disconnect: (socket: TSocket) => Promise<void>;
  readonly recoverAfterRestart: () => Promise<boolean>;
  readonly dispose: () => void;
}

export function makeManagedAdmissionController<TSocket extends object>(options: {
  readonly store: ManagedAdmissionStore;
  readonly leaseTimeoutMs?: number;
  readonly randomToken?: () => string;
  readonly schedule?: (work: () => void, delayMs: number) => ManagedLeaseTimer;
}): ManagedAdmissionController<TSocket> {
  const leaseTimeoutMs = options.leaseTimeoutMs ?? 5 * 60_000;
  const randomToken = options.randomToken ?? randomUUID;
  const schedule =
    options.schedule ??
    ((work: () => void, delayMs: number) => {
      const timer = setTimeout(work, delayMs);
      timer.unref();
      return { cancel: () => clearTimeout(timer) };
    });
  type ManagedLease = {
    readonly socket: TSocket;
    readonly leaseToken: string;
    readonly timer: ManagedLeaseTimer;
    revocationError?: string;
  };
  const leases = new Map<string, ManagedLease>();
  const transitions = new Map<string, Promise<void>>();
  let poisoned: Error | undefined;

  const poison = (cause: unknown): Error => {
    poisoned ??= new Error("managed admission persistence failed; supervisor restart required", {
      cause,
    });
    for (const lease of leases.values()) lease.timer.cancel();
    leases.clear();
    return poisoned;
  };

  const ensureHealthy = () => {
    if (poisoned) throw poisoned;
  };

  const persist = async () => {
    try {
      await options.store.persist();
    } catch (cause) {
      throw poison(cause);
    }
  };

  const withOperationLock = async <A>(operationKey: string, work: () => Promise<A>): Promise<A> => {
    const previous = transitions.get(operationKey) ?? Promise.resolve();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const current = previous.catch(() => {}).then(() => gate);
    transitions.set(operationKey, current);
    await previous.catch(() => {});
    try {
      ensureHealthy();
      return await work();
    } finally {
      release();
      if (transitions.get(operationKey) === current) transitions.delete(operationKey);
    }
  };

  const visibleOperation = (operationKey: string): ManagedOperationState => {
    const entry = options.store.get(operationKey);
    return isManagedLedgerEntry(entry) ? entry.operation : { status: "absent" };
  };

  const persistRevokedLease = async (
    operationKey: string,
    lease: ManagedLease,
  ): Promise<ManagedOperationState | undefined> => {
    if (lease.revocationError === undefined) return undefined;
    lease.timer.cancel();
    if (leases.get(operationKey) === lease) leases.delete(operationKey);
    const entry = options.store.get(operationKey);
    if (!isManagedLedgerEntry(entry) || entry.operation.status !== "delivering")
      return visibleOperation(operationKey);
    const indeterminate = {
      status: "indeterminate" as const,
      error: lease.revocationError,
    };
    options.store.set(operationKey, { ...entry, operation: indeterminate });
    await persist();
    return indeterminate;
  };

  const markIndeterminate = (operationKey: string, leaseToken: string, error: string) =>
    withOperationLock(operationKey, async () => {
      const lease = leases.get(operationKey);
      if (!lease || lease.leaseToken !== leaseToken) return;
      lease.revocationError ??= error;
      await persistRevokedLease(operationKey, lease);
    });

  const claim = async (
    socket: TSocket,
    request: ManagedClaimRequest,
  ): Promise<ManagedClaimResponse> =>
    withOperationLock(request.operationKey, async () => {
      const hash = createHash("sha256").update(canonicalJson(request.payload)).digest("hex");
      const prior = options.store.get(request.operationKey);
      if (prior !== undefined) {
        if (!isManagedLedgerEntry(prior) || prior.hash !== hash)
          return { status: "conflict", error: "managed operation payload conflict" };
        return prior.operation;
      }
      if (request.intent === "recover-existing") return { status: "absent" };

      const leaseToken = randomToken();
      const timeoutError = "managed admission lease timed out while delivery was in progress";
      const timer = schedule(() => {
        const lease = leases.get(request.operationKey);
        if (!lease || lease.leaseToken !== leaseToken) return;
        lease.revocationError ??= timeoutError;
        void markIndeterminate(request.operationKey, leaseToken, timeoutError).catch(() => {});
      }, leaseTimeoutMs);
      const entry: ManagedLedgerEntry = {
        kind: "managedAdmission",
        protocol: MANAGED_ADMISSION_PROTOCOL,
        hash,
        receipt: {
          commandId: request.operationKey as SupervisorCommandReceipt["commandId"],
          status: "indeterminate",
          error: "managed admission entry is not a legacy supervisor command",
        },
        operation: { status: "delivering" },
      };
      options.store.set(request.operationKey, entry);
      leases.set(request.operationKey, { socket, leaseToken, timer });
      await persist();

      const lease = leases.get(request.operationKey);
      const operation = visibleOperation(request.operationKey);
      if (
        lease?.leaseToken === leaseToken &&
        lease.revocationError !== undefined &&
        operation.status === "delivering"
      ) {
        const revoked = await persistRevokedLease(request.operationKey, lease);
        if (revoked) return revoked;
      }
      if (
        !lease ||
        lease.socket !== socket ||
        lease.leaseToken !== leaseToken ||
        operation.status !== "delivering"
      )
        return operation.status === "absent"
          ? { status: "indeterminate", error: "managed admission lease was lost" }
          : operation;
      return { status: "granted", operationKey: request.operationKey, leaseToken };
    });

  const finalize = async (
    socket: TSocket,
    request: ManagedFinalizeRequest,
  ): Promise<ManagedFinalizeResponse> =>
    withOperationLock(request.operationKey, async () => {
      const lease = leases.get(request.operationKey);
      if (
        lease?.socket === socket &&
        lease.leaseToken === request.leaseToken &&
        lease.revocationError !== undefined
      ) {
        const revoked = await persistRevokedLease(request.operationKey, lease);
        return {
          status: "staleLease",
          operation: revoked ?? visibleOperation(request.operationKey),
        };
      }
      if (
        !lease ||
        lease.socket !== socket ||
        lease.leaseToken !== request.leaseToken ||
        visibleOperation(request.operationKey).status !== "delivering"
      )
        return { status: "staleLease", operation: visibleOperation(request.operationKey) };

      lease.timer.cancel();
      leases.delete(request.operationKey);
      const entry = options.store.get(request.operationKey);
      if (!isManagedLedgerEntry(entry))
        return { status: "staleLease", operation: { status: "absent" } };
      options.store.set(request.operationKey, { ...entry, operation: request.finalization });
      await persist();
      return { status: "finalized", operation: request.finalization };
    });

  const disconnect = async (socket: TSocket): Promise<void> => {
    const abandoned = [...leases.entries()].flatMap(([operationKey, lease]) =>
      lease.socket === socket ? [{ operationKey, lease }] : [],
    );
    if (abandoned.length === 0) return;
    const disconnectError =
      "managed admission requester disconnected while delivery was in progress";
    for (const { lease } of abandoned) lease.revocationError ??= disconnectError;
    for (const { operationKey, lease } of abandoned) {
      await markIndeterminate(operationKey, lease.leaseToken, disconnectError);
    }
  };

  const recoverAfterRestart = async (): Promise<boolean> => {
    let recovered = false;
    for (const { operationKey, entry } of options.store.entries()) {
      if (!isManagedLedgerEntry(entry) || entry.operation.status !== "delivering") continue;
      options.store.set(operationKey, {
        ...entry,
        operation: {
          status: "indeterminate",
          error: "supervisor restarted while managed delivery was in progress",
        },
      });
      recovered = true;
    }
    if (recovered) await persist();
    return recovered;
  };

  return {
    claim,
    finalize,
    disconnect,
    recoverAfterRestart,
    dispose: () => {
      for (const lease of leases.values()) lease.timer.cancel();
      leases.clear();
    },
  };
}

const managedAdmissions = makeManagedAdmissionController<Socket>({
  store: {
    get: (operationKey) => ledger[operationKey],
    set: (operationKey, entry) => {
      ledger[operationKey] = entry;
    },
    entries: () => Object.entries(ledger).map(([operationKey, entry]) => ({ operationKey, entry })),
    persist: atomicLedger,
  },
});

const writeBounded = (socket: Socket, value: unknown): boolean => {
  if (socket.destroyed) return false;
  const line = encodeLine(value);
  if (Buffer.byteLength(line) > SUPERVISOR_MAX_STREAM_ITEM_BYTES) {
    socket.destroy(new Error("supervisor stream item exceeds byte limit"));
    return false;
  }
  const state = socketWrites.get(socket) ?? { blocked: false, queue: [], queuedBytes: 0 };
  socketWrites.set(socket, state);
  const flush = () => {
    state.blocked = false;
    while (state.queue.length > 0 && !socket.destroyed) {
      const queued = state.queue.shift()!;
      state.queuedBytes -= Buffer.byteLength(queued);
      if (!socket.write(queued)) {
        state.blocked = true;
        socket.once("drain", flush);
        break;
      }
    }
  };
  if (state.blocked) {
    const bytes = Buffer.byteLength(line);
    if (state.queuedBytes + bytes > MAX_SOCKET_QUEUE_BYTES) {
      socket.destroy();
      return false;
    }
    state.queue.push(line);
    state.queuedBytes += bytes;
    return true;
  }
  if (!socket.write(line)) {
    state.blocked = true;
    socket.once("drain", flush);
  }
  return true;
};
export const projectReplayItem = (item: SupervisorStreamItem): SupervisorStreamItem => {
  if (item.type !== "event" || !isRecord(item.event)) return item;
  const eventType = overlayEventType(item.event);
  if (eventType === "message_update") {
    if (
      item.event.type === "event" &&
      isRecord(item.event.data) &&
      isRecord(item.event.data.update)
    ) {
      const { partial: _partial, ...update } = item.event.data.update;
      return { ...item, event: { ...item.event, data: { ...item.event.data, update } } };
    }
    const { message: _message, assistantMessageEvent, ...eventPayload } = item.event;
    if (!isRecord(assistantMessageEvent)) return { ...item, event: eventPayload };
    const { partial: _partial, ...delta } = assistantMessageEvent;
    return { ...item, event: { ...eventPayload, assistantMessageEvent: delta } };
  }
  if (eventType === "tool_execution_update") {
    if (item.event.type === "event" && isRecord(item.event.data)) {
      const { partialResult: _partialResult, ...data } = item.event.data;
      return { ...item, event: { ...item.event, data } };
    }
    const { partialResult: _partialResult, ...eventPayload } = item.event;
    return { ...item, event: eventPayload };
  }
  return item;
};
const emit = (runtime: Runtime, item: SupervisorStreamItem) => {
  const replayItem = projectReplayItem(item);
  const replayBytes = Buffer.byteLength(JSON.stringify(replayItem));
  runtime.ring.push(replayItem);
  runtime.ringBytes += replayBytes;
  while (runtime.ring.length > RING_SIZE || runtime.ringBytes > MAX_RING_BYTES) {
    const removed = runtime.ring.shift();
    if (!removed) break;
    runtime.ringEvictedThrough = Math.max(runtime.ringEvictedThrough, streamItemSequence(removed));
    runtime.ringBytes -= Buffer.byteLength(JSON.stringify(removed));
  }
  for (const subscriber of runtime.subscribers) {
    if (!subscriber.ready) {
      const buffered = projectReplayItem(item);
      subscriber.buffer.push(buffered);
      subscriber.bufferBytes += Buffer.byteLength(JSON.stringify(buffered));
      if (subscriber.buffer.length > RING_SIZE || subscriber.bufferBytes > MAX_RING_BYTES)
        subscriber.socket.destroy(new Error("slow supervisor client"));
    } else if (
      !writeBounded(subscriber.socket, { type: "stream", requestId: subscriber.requestId, item })
    )
      runtime.subscribers.delete(subscriber);
  }
};
const streamItemSequence = (item: SupervisorStreamItem): number =>
  item.type === "snapshot" ? item.runtime.sequence : item.sequence;
export const projectListedRuntime = (runtime: SupervisorRuntimeState): SupervisorRuntimeState => {
  const { state: _state, ...listed } = runtime;
  return listed;
};
export const shouldUseSnapshot = (
  cursor: number | undefined,
  ringEvictedThrough: number,
  oldestSequence: number | undefined,
): boolean =>
  cursor === undefined ||
  cursor <= ringEvictedThrough ||
  (cursor !== undefined && oldestSequence !== undefined && cursor < oldestSequence - 1);
const overlayEventType = (payload: unknown): string | undefined => {
  if (!isRecord(payload)) return;
  if (payload.type === "event" && typeof payload.event === "string") return payload.event;
  return typeof payload.type === "string" ? payload.type : undefined;
};
const encodedBytes = (value: unknown): number => Buffer.byteLength(JSON.stringify(value));
export const projectQueueValues = (
  queue: { readonly steering?: unknown; readonly followUp?: unknown },
  maxBytes = MAX_PENDING_QUEUE_BYTES - PENDING_QUEUE_ENVELOPE_RESERVE_BYTES,
) => {
  const allSteering = Array.isArray(queue.steering)
    ? queue.steering.filter((value): value is string => typeof value === "string")
    : [];
  const allFollowUp = Array.isArray(queue.followUp)
    ? queue.followUp.filter((value): value is string => typeof value === "string")
    : [];
  const steering: string[] = [];
  const followUp: string[] = [];
  let bytes = encodedBytes({
    steering,
    followUp,
    omittedSteering: allSteering.length,
    omittedFollowUp: allFollowUp.length,
  });
  const append = (target: string[], value: string) => {
    const addedBytes = encodedBytes(value) + (target.length > 0 ? 1 : 0);
    if (bytes + addedBytes > maxBytes) return false;
    target.push(value);
    bytes += addedBytes;
    return true;
  };
  for (const value of allSteering) if (!append(steering, value)) break;
  for (const value of allFollowUp) if (!append(followUp, value)) break;
  return {
    steering,
    followUp,
    omittedSteering: allSteering.length - steering.length,
    omittedFollowUp: allFollowUp.length - followUp.length,
  };
};
export const projectQueuePayload = (payload: unknown): unknown => {
  if (!isRecord(payload) || overlayEventType(payload) !== "queue_update") return payload;
  const queue = payload.type === "event" && isRecord(payload.data) ? payload.data : payload;
  const projected = projectQueueValues(queue);
  return payload.type === "event" ? { ...payload, data: projected } : { ...payload, ...projected };
};
export const queuePayloadHasPending = (payload: unknown): boolean =>
  isRecord(payload) &&
  ((Array.isArray(payload.steering) && payload.steering.length > 0) ||
    (Array.isArray(payload.followUp) && payload.followUp.length > 0) ||
    (typeof payload.omittedSteering === "number" && payload.omittedSteering > 0) ||
    (typeof payload.omittedFollowUp === "number" && payload.omittedFollowUp > 0));
export function reserveBridgeRegistration(sessionFile: string): boolean {
  if (bridgeRegistrations.has(sessionFile)) return false;
  bridgeRegistrations.add(sessionFile);
  return true;
}
export function releaseBridgeRegistration(sessionFile: string): void {
  bridgeRegistrations.delete(sessionFile);
}

interface BridgeRegistrationSocketTarget {
  readonly destroyed: boolean;
  readonly once: (event: "close" | "error", listener: () => void) => unknown;
  readonly off: (event: "close" | "error", listener: () => void) => unknown;
}

export function createBridgeRegistrationSocketGuard(socket: BridgeRegistrationSocketTarget) {
  let closed = socket.destroyed;
  let handedOff = false;
  const markClosed = () => {
    closed = true;
  };
  socket.once("close", markClosed);
  socket.once("error", markClosed);
  const removeRegistrationListeners = () => {
    socket.off("close", markClosed);
    socket.off("error", markClosed);
  };
  return {
    isClosed: () => closed || socket.destroyed,
    handoff: (cleanup: () => void): boolean => {
      if (handedOff) return !closed;
      handedOff = true;
      removeRegistrationListeners();
      if (closed || socket.destroyed) {
        cleanup();
        return false;
      }
      socket.once("close", cleanup);
      socket.once("error", cleanup);
      return true;
    },
    dispose: () => {
      if (!handedOff) removeRegistrationListeners();
    },
  };
}

export function claimRpcSessionWriter(input: {
  readonly writers: Map<string, string>;
  readonly bridgeRegistrations: ReadonlySet<string>;
  readonly sessionFile: string;
  readonly runtimeId: string;
}):
  | { readonly status: "claimed" }
  | { readonly status: "bridgeReserved" }
  | { readonly status: "owned"; readonly runtimeId: string } {
  if (input.bridgeRegistrations.has(input.sessionFile)) return { status: "bridgeReserved" };
  const owner = input.writers.get(input.sessionFile);
  if (owner !== undefined) return { status: "owned", runtimeId: owner };
  input.writers.set(input.sessionFile, input.runtimeId);
  return { status: "claimed" };
}

export function releaseSessionWriterIfOwned(input: {
  readonly writers: Map<string, string>;
  readonly sessionFile: string;
  readonly runtimeId: string;
}): boolean {
  if (input.writers.get(input.sessionFile) !== input.runtimeId) return false;
  input.writers.delete(input.sessionFile);
  return true;
}
export const projectOverlayPayload = (payload: unknown, eventType: string | undefined): unknown => {
  if (!isRecord(payload) || eventType !== "message_update") return payload;
  if (payload.type === "event" && isRecord(payload.data) && isRecord(payload.data.update)) {
    const { partial, ...update } = payload.data.update;
    return {
      ...payload,
      data: { ...payload.data, update: partial === undefined ? update : { partial } },
    };
  }
  if (!isRecord(payload.assistantMessageEvent)) return payload;
  const partial = payload.assistantMessageEvent.partial;
  if (partial === undefined) return payload;
  const { message: _message, ...withoutMessage } = payload;
  return {
    ...withoutMessage,
    assistantMessageEvent: { partial },
  };
};
const eventToolCallId = (payload: unknown): string | undefined => {
  if (!isRecord(payload)) return;
  if (typeof payload.toolCallId === "string") return payload.toolCallId;
  return isRecord(payload.data) && typeof payload.data.toolCallId === "string"
    ? payload.data.toolCallId
    : undefined;
};
const retainOverlayEvent = (
  runtime: Runtime,
  item: SupervisorStreamEvent,
  eventType: string | undefined,
) => {
  if (eventType === "queue_update") {
    const payload =
      isRecord(item.event) && item.event.type === "event" && isRecord(item.event.data)
        ? item.event.data
        : item.event;
    const hasPending = queuePayloadHasPending(payload);
    if (hasPending) runtime.pendingQueueEvent = item;
    else delete runtime.pendingQueueEvent;
    return;
  }
  const projected = { ...item, event: projectOverlayPayload(item.event, eventType) };
  const toolCallId = eventToolCallId(item.event);
  const replace = (candidate: SupervisorStreamEvent) => {
    const candidateType = overlayEventType(candidate.event);
    if (eventType === "message_update") return candidateType === eventType;
    if (eventType === "tool_execution_update" && toolCallId)
      return candidateType === eventType && eventToolCallId(candidate.event) === toolCallId;
    if (eventType === "tool_execution_end" && toolCallId)
      return (
        candidateType === "tool_execution_update" && eventToolCallId(candidate.event) === toolCallId
      );
    return false;
  };
  const retained = runtime.overlayEvents.filter((candidate) => !replace(candidate));
  retained.push(projected);
  runtime.overlayEvents = retained;
  runtime.overlayBytes = retained.reduce(
    (total, candidate) => total + Buffer.byteLength(JSON.stringify(candidate)),
    0,
  );
  while (runtime.overlayEvents.length > RING_SIZE || runtime.overlayBytes > MAX_OVERLAY_BYTES) {
    const removed = runtime.overlayEvents.shift();
    if (!removed) break;
    runtime.overlayBytes -= Buffer.byteLength(JSON.stringify(removed));
    runtime.overlayOmittedCount += 1;
  }
};
const clearOverlay = (runtime: Runtime, clearPendingQueue = false) => {
  runtime.overlayEvents.length = 0;
  runtime.overlayBytes = 0;
  runtime.overlayOmittedCount = 0;
  if (clearPendingQueue) delete runtime.pendingQueueEvent;
};
const event = (runtime: Runtime, payload: unknown) => {
  payload = projectQueuePayload(payload);
  const sequence = runtime.state.sequence + 1;
  runtime.state = { ...runtime.state, sequence };
  const item: SupervisorStreamEvent = {
    type: "event",
    runtimeId: runtime.state.runtimeId,
    sequence,
    eventId: `${runtime.state.runtimeId}:${sequence}` as never,
    event: payload,
  };
  retainOverlayEvent(runtime, item, overlayEventType(payload));
  emit(runtime, item);
};
export async function publishSettlementInOrder<T>(input: {
  readonly read: () => Promise<T>;
  readonly isCurrent: () => boolean;
  readonly publishReplacement: (value: T) => void;
  readonly clearOverlay: () => void;
  readonly publishSynchronized: () => void;
}): Promise<boolean> {
  const value = await input.read();
  if (!input.isCurrent()) return false;
  input.publishReplacement(value);
  input.clearOverlay();
  input.publishSynchronized();
  return true;
}
const publishSettledSnapshot = async (runtime: Runtime, settledSequence: number) => {
  await publishSettlementInOrder({
    read: () => readAppendedEntries(runtime.state.sessionFile, runtime.sessionReadOffset),
    isCurrent: () => runtime.state.sequence === settledSequence && runtime.state.status === "idle",
    publishReplacement: (appended) => {
      runtime.sessionReadOffset = appended.offset;
      runtime.state = { ...runtime.state, sequence: settledSequence + 1 };
      emit(runtime, {
        type: "entries",
        runtimeId: runtime.state.runtimeId,
        sequence: runtime.state.sequence,
        entries: appended.entries,
      });
    },
    clearOverlay: () => clearOverlay(runtime, true),
    publishSynchronized: () =>
      emit(runtime, {
        type: "synchronized",
        runtimeId: runtime.state.runtimeId,
        sequence: runtime.state.sequence,
      }),
  });
};
const setExited = (runtime: Runtime, exitCode?: number) => {
  if (runtime.state.status === "exited") return;
  if (runtime.bridgeExpiry) clearTimeout(runtime.bridgeExpiry);
  const sequence = runtime.state.sequence + 1;
  runtime.state = { ...runtime.state, sequence, status: "exited" };
  if (runtime.state.sessionFile) {
    releaseSessionWriterIfOwned({
      writers,
      sessionFile: runtime.state.sessionFile,
      runtimeId: runtime.state.runtimeId,
    });
  }
  emit(runtime, {
    type: "exited",
    runtimeId: runtime.state.runtimeId,
    sequence,
    ...(exitCode === undefined ? {} : { exitCode }),
  });
  const eviction = setTimeout(() => {
    if (runtimes.get(runtime.state.runtimeId) === runtime) {
      runtimes.delete(runtime.state.runtimeId);
      runtime.ring.length = 0;
      runtime.ringBytes = 0;
      clearOverlay(runtime, true);
      runtime.subscribers.clear();
    }
  }, EXITED_RETENTION_MS);
  eviction.unref();
};
const rpc = (runtime: Runtime, type: string, fields: Record<string, unknown> = {}) =>
  new Promise<Record<string, unknown>>((resolve, reject) => {
    const id = `supervisor-${++runtime.nextRpcId}`;
    const timer = setTimeout(() => {
      runtime.pending.delete(id);
      reject(new Error(`${type} timed out`));
    }, 120_000);
    timer.unref();
    runtime.pending.set(id, {
      resolve: (response) => {
        clearTimeout(timer);
        if (response.success !== true)
          reject(
            new RpcCommandRejectedError(
              typeof response.error === "string" ? response.error : `${type} failed`,
            ),
          );
        else resolve(response);
      },
      reject: (cause) => {
        clearTimeout(timer);
        reject(cause);
      },
    });
    runtime.child!.stdin.write(encodeLine({ type, id, ...fields }));
  });
const attachRpc = (runtime: Runtime) => {
  const decoder = new JsonLineDecoder();
  const rejectPending = (cause: Error) => {
    for (const pending of runtime.pending.values()) pending.reject(cause);
    runtime.pending.clear();
  };
  runtime.child!.stdin.on("error", rejectPending);
  runtime.child!.stdout.setEncoding("utf8");
  runtime.child!.stdout.on("data", (chunk: string) => {
    const decoded = decodeRuntimeChunk(decoder, chunk);
    if (decoded.error !== undefined) {
      rejectPending(
        decoded.error instanceof Error
          ? decoded.error
          : new Error("pi rpc frame exceeded its byte ceiling"),
      );
      runtime.child?.kill("SIGTERM");
      setExited(runtime);
      return;
    }
    for (const value of decoded.frames) {
      if (isRecord(value) && value.type === "response" && typeof value.id === "string") {
        const pending = runtime.pending.get(value.id);
        runtime.pending.delete(value.id);
        pending?.resolve(value);
      } else {
        if (
          isRecord(value) &&
          value.type === "extension_ui_request" &&
          typeof value.id === "string" &&
          (value.method === "select" ||
            value.method === "confirm" ||
            value.method === "input" ||
            value.method === "editor")
        ) {
          runtime.child!.stdin.write(
            encodeLine({ type: "extension_ui_response", id: value.id, cancelled: true }),
          );
        }
        const eventType =
          isRecord(value) && typeof value.type === "string" ? value.type : undefined;
        if (eventType === "agent_start") clearOverlay(runtime);
        const pendingMessageCount =
          eventType === "queue_update" && isRecord(value)
            ? (Array.isArray(value.steering) ? value.steering.length : 0) +
              (Array.isArray(value.followUp) ? value.followUp.length : 0)
            : undefined;
        const streaming =
          eventType === "agent_start"
            ? true
            : eventType === "agent_settled"
              ? false
              : (runtime.state.overlay?.isStreaming ?? false);
        runtime.state = {
          ...runtime.state,
          status: streaming ? "streaming" : "idle",
          overlay: {
            ...(runtime.state.overlay ?? { isStreaming: false, pendingMessageCount: 0 }),
            isStreaming: streaming,
            ...(pendingMessageCount === undefined ? {} : { pendingMessageCount }),
            ...(eventType ? { lastEventType: eventType } : {}),
          },
        };
        if (eventType === "agent_settled") {
          void publishSettledSnapshot(runtime, runtime.state.sequence);
        } else {
          event(runtime, value);
        }
      }
    }
  });
  runtime.child!.stderr.resume();
  runtime.child!.on("error", rejectPending);
  runtime.child!.on("exit", (code) => {
    rejectPending(new Error("pi exited"));
    setExited(runtime, code ?? undefined);
  });
};
export async function validateExistingPiSessionSpawn(input: {
  readonly sessionsRoot: string;
  readonly sessionFile: string;
  readonly cwd: string;
  readonly sessionKey?: string;
}): Promise<{ readonly sessionFile: string; readonly cwd: string; readonly sessionId: string }> {
  const sessionsRoot = await fs.realpath(input.sessionsRoot);
  const sessionFile = await fs.realpath(input.sessionFile);
  const relativeSessionFile = path.relative(sessionsRoot, sessionFile);
  if (
    relativeSessionFile === "" ||
    relativeSessionFile.startsWith(`..${path.sep}`) ||
    relativeSessionFile === ".." ||
    path.isAbsolute(relativeSessionFile)
  ) {
    throw new Error("session file is outside the pi sessions root");
  }
  const sessionStat = await fs.stat(sessionFile);
  if (!sessionStat.isFile()) throw new Error("session file is not a regular file");

  const cwd = await fs.realpath(input.cwd);
  const cwdStat = await fs.stat(cwd);
  if (!cwdStat.isDirectory()) throw new Error("cwd is not a directory");

  const firstLine = await readSessionHeaderLine(sessionFile);
  let header: unknown;
  try {
    header = JSON.parse(firstLine ?? "");
  } catch {
    header = undefined;
  }
  if (
    !isRecord(header) ||
    header.type !== "session" ||
    typeof header.id !== "string" ||
    header.id.length === 0 ||
    typeof header.cwd !== "string"
  ) {
    throw new Error("invalid session header");
  }
  const headerCwd = await fs.realpath(header.cwd).catch(() => undefined);
  if (headerCwd !== cwd) throw new Error("session header cwd does not match resume cwd");

  if (input.sessionKey !== undefined) {
    const expectedSessionKey = createHash("sha256").update(sessionFile).digest("hex");
    if (input.sessionKey !== expectedSessionKey) {
      throw new Error("session key does not match canonical session file");
    }
  }
  return { sessionFile, cwd, sessionId: header.id };
}

export async function acquireResumeAndSendRuntime<T>(input: {
  readonly existingWriter: () => Promise<T | undefined>;
  readonly spawnWriter: () => Promise<T>;
  readonly isWriterClaimConflict: (cause: unknown) => boolean;
}): Promise<{ readonly runtime: T; readonly reusedWriter: boolean }> {
  const existing = await input.existingWriter();
  if (existing !== undefined) return { runtime: existing, reusedWriter: true };
  try {
    return { runtime: await input.spawnWriter(), reusedWriter: false };
  } catch (cause) {
    if (!input.isWriterClaimConflict(cause)) throw cause;
    const raced = await input.existingWriter();
    if (raced !== undefined) return { runtime: raced, reusedWriter: true };
    throw cause;
  }
}

async function liveWriterForSession(sessionFile: string): Promise<Runtime | undefined> {
  const runtimeId = writers.get(sessionFile);
  if (runtimeId === undefined) return undefined;
  const starting = startingRuntimes.get(runtimeId);
  const runtime = starting === undefined ? runtimes.get(runtimeId) : await starting;
  return runtime?.state.status === "exited" ? undefined : runtime;
}

type ValidatedExistingPiSessionSpawn = Awaited<ReturnType<typeof validateExistingPiSessionSpawn>>;

async function spawnRuntime(
  command: Record<string, unknown>,
  existingValidation?: ValidatedExistingPiSessionSpawn,
): Promise<Runtime> {
  const runtimeId = randomUUID() as never;
  let cwd = String(command.cwd);
  let sessionFile = typeof command.sessionFile === "string" ? command.sessionFile : undefined;
  const sessionKey = command.type === "resumeAndSend" ? String(command.sessionKey) : undefined;
  if (sessionFile) {
    const validated =
      existingValidation ??
      (await validateExistingPiSessionSpawn({
        sessionsRoot: defaultPiSessionsRoot(),
        sessionFile,
        cwd,
        ...(sessionKey === undefined ? {} : { sessionKey }),
      }));
    sessionFile = validated.sessionFile;
    cwd = validated.cwd;
  } else {
    const cwdStat = await fs.stat(cwd);
    if (!cwdStat.isDirectory()) throw new Error("cwd is not a directory");
  }

  let resolveStarting: ((runtime: Runtime | undefined) => void) | undefined;
  if (sessionFile) {
    const readiness = new Promise<Runtime | undefined>((resolve) => {
      resolveStarting = resolve;
    });
    startingRuntimes.set(runtimeId, readiness);
    const claim = claimRpcSessionWriter({
      writers,
      bridgeRegistrations,
      sessionFile,
      runtimeId,
    });
    if (claim.status !== "claimed") {
      startingRuntimes.delete(runtimeId);
      resolveStarting?.(undefined);
      if (claim.status === "bridgeReserved") {
        throw new Error("session bridge registration is in progress");
      }
      throw new SessionWriterClaimConflictError(sessionFile, claim.runtimeId);
    }
  }

  try {
    const args = piRpcSpawnArgs({
      sessionsRoot: defaultPiSessionsRoot(),
      ...(sessionFile ? { sessionFile } : {}),
    });
    const child = spawn(process.env.T3_PI_EXECUTABLE ?? "pi", args, {
      cwd,
      stdio: ["pipe", "pipe", "pipe"],
    });
    await new Promise<void>((resolve, reject) => {
      child.once("spawn", resolve);
      child.once("error", reject);
    });
    const runtime: Runtime = {
      state: {
        runtimeId,
        ...(sessionKey ? { sessionKey: sessionKey as never } : {}),
        ...(sessionFile ? { sessionFile } : {}),
        cwd,
        writerKind: "rpc",
        status: "starting",
        sequence: 0,
        overlay: { isStreaming: false, pendingMessageCount: 0 },
      },
      child,
      ring: [],
      ringBytes: 0,
      ringEvictedThrough: -1,
      overlayEvents: [],
      overlayBytes: 0,
      overlayOmittedCount: 0,
      sessionReadOffset: 0,
      subscribers: new Set(),
      nextRpcId: 0,
      pending: new Map(),
      bridgePending: new Map(),
    };
    runtimes.set(runtimeId, runtime);
    attachRpc(runtime);
    const response = await rpc(runtime, "get_state").catch(async (cause) => {
      await stopChild(runtime);
      throw cause;
    });
    const state = isRecord(response.data) ? response.data : {};
    const discoveredFile =
      typeof state.sessionFile === "string"
        ? await fs.realpath(state.sessionFile).catch(() => state.sessionFile as string)
        : sessionFile;
    if (discoveredFile) {
      const owner = writers.get(discoveredFile);
      if (owner && owner !== runtimeId) {
        child.kill("SIGTERM");
        throw new SessionWriterClaimConflictError(discoveredFile, owner);
      }
      writers.set(discoveredFile, runtimeId);
    }
    runtime.state = {
      ...runtime.state,
      ...(discoveredFile ? { sessionFile: discoveredFile } : {}),
      status: state.isStreaming === true ? "streaming" : "idle",
      state,
      overlay: {
        isStreaming: state.isStreaming === true,
        pendingMessageCount:
          typeof state.pendingMessageCount === "number"
            ? Math.max(0, Math.trunc(state.pendingMessageCount))
            : 0,
        lastEventType: "get_state",
      },
    };
    runtime.sessionReadOffset = (await readSessionFile(discoveredFile)).offset;
    resolveStarting?.(runtime);
    return runtime;
  } catch (cause) {
    if (sessionFile) {
      releaseSessionWriterIfOwned({ writers, sessionFile, runtimeId });
    }
    runtimes.delete(runtimeId);
    resolveStarting?.(undefined);
    throw cause;
  } finally {
    startingRuntimes.delete(runtimeId);
  }
}
async function stopChild(runtime: Runtime): Promise<void> {
  const child = runtime.child;
  if (!child || child.exitCode !== null) return;
  await new Promise<void>((resolve) => {
    const forced = setTimeout(() => child.kill("SIGKILL"), 2_000);
    forced.unref();
    child.once("exit", () => {
      clearTimeout(forced);
      resolve();
    });
    child.kill("SIGTERM");
  });
}

export function bridgeCommandFrame(command: Record<string, unknown>) {
  return {
    type: "command",
    commandId: String(command.commandId),
    command: command.type,
    ...(typeof command.message === "string" ? { text: command.message } : {}),
    ...(isRecord(command.lifecycle) ? { lifecycle: command.lifecycle } : {}),
  };
}

export function existingWriterResumeAndSendCommand(
  command: Record<string, unknown>,
  runtimeId: SupervisorRuntimeState["runtimeId"],
): Record<string, unknown> {
  const streamingBehavior = command.streamingBehavior === "followUp" ? "followUp" : "steer";
  return { ...command, type: streamingBehavior, runtimeId };
}

async function deliverRuntimeCommand(
  runtime: Runtime,
  command: Record<string, unknown>,
): Promise<SupervisorCommandReceipt> {
  const commandId = String(command.commandId);
  if (runtime.state.status === "exited") throw new Error("runtime is not live");
  if (runtime.state.writerKind === "tuiBridge" && !runtime.bridge)
    throw new Error("bridge is reconnecting");
  if (runtime.bridge) {
    const receipt = await new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => {
        runtime.bridgePending.delete(commandId);
        reject(new IndeterminateCommandError("bridge receipt timed out"));
      }, 30_000);
      timer.unref();
      runtime.bridgePending.set(commandId, (frame) => {
        clearTimeout(timer);
        resolve(frame);
      });
      runtime.bridge!.write(encodeLine(bridgeCommandFrame(command)));
    });
    if (receipt.status === "submitted")
      throw new IndeterminateCommandError(
        "pi accepted the bridge handoff but exposes no message-acceptance receipt",
      );
    if (receipt.status === "indeterminate")
      throw new IndeterminateCommandError(String(receipt.error ?? "bridge disconnected"));
    if (receipt.status === "error") throw new Error(String(receipt.error));
    return {
      commandId: commandId as never,
      status: "completed",
      runtimeId: runtime.state.runtimeId,
    };
  }
  if (command.type === "setLifecycle") {
    throw new Error("pi rpc does not support lifecycle entry emission");
  }
  if (command.type === "shutdown") {
    await stopChild(runtime);
    return {
      commandId: commandId as never,
      status: "completed",
      runtimeId: runtime.state.runtimeId,
    };
  }
  const rpcType =
    command.type === "send"
      ? "prompt"
      : command.type === "followUp"
        ? "prompt"
        : command.type === "steer"
          ? "prompt"
          : "abort";
  try {
    await rpc(
      runtime,
      rpcType,
      typeof command.message === "string"
        ? {
            message: command.message,
            ...(Array.isArray(command.images) ? { images: command.images } : {}),
            ...(command.type === "send" ? {} : { streamingBehavior: command.type }),
          }
        : {},
    );
  } catch (cause) {
    if (cause instanceof RpcCommandRejectedError) throw cause;
    throw new IndeterminateCommandError(
      cause instanceof Error ? cause.message : "pi command outcome is unknown",
    );
  }
  const stateResponse = await rpc(runtime, "get_state").catch(() => undefined);
  if (!stateResponse)
    return {
      commandId: commandId as never,
      status: "completed",
      runtimeId: runtime.state.runtimeId,
    };
  const state = isRecord(stateResponse.data) ? stateResponse.data : {};
  const isStreaming = state.isStreaming === true;
  runtime.state = {
    ...runtime.state,
    status: isStreaming ? "streaming" : "idle",
    state,
    overlay: {
      isStreaming,
      pendingMessageCount:
        typeof state.pendingMessageCount === "number"
          ? Math.max(0, Math.trunc(state.pendingMessageCount))
          : 0,
      lastEventType: "get_state",
    },
  };
  return { commandId: commandId as never, status: "completed", runtimeId: runtime.state.runtimeId };
}

export function runKeyedSerialQueue<T>(input: {
  readonly entries: Map<string, Promise<void>>;
  readonly key: string;
  readonly run: () => Promise<T>;
}): Promise<T> {
  const prior = input.entries.get(input.key) ?? Promise.resolve();
  const work = prior.catch(() => undefined).then(() => input.run());
  const tail = work.then(
    () => undefined,
    () => undefined,
  );
  input.entries.set(input.key, tail);
  void tail.then(() => {
    if (input.entries.get(input.key) === tail) input.entries.delete(input.key);
  });
  return work;
}

/**
 * Keeps a queued resume replayable until its session slot is ready, then persists
 * delivery before any writer acquisition or prompt handoff can begin.
 */
export function runSerializedResumeAndSendDelivery<T>(input: {
  readonly entries: Map<string, Promise<void>>;
  readonly sessionFile: string;
  readonly startDelivery: () => Promise<void>;
  readonly deliver: () => Promise<T>;
}): Promise<T> {
  return runKeyedSerialQueue({
    entries: input.entries,
    key: input.sessionFile,
    run: async () => {
      await input.startDelivery();
      return input.deliver();
    },
  });
}

async function execute(
  command: Record<string, unknown>,
  startResumeAndSendDelivery?: () => Promise<void>,
): Promise<SupervisorCommandReceipt> {
  const commandId = String(command.commandId);
  if (command.type === "start") {
    const runtime = await spawnRuntime(command);
    return {
      commandId: commandId as never,
      status: "completed",
      runtimeId: runtime.state.runtimeId,
      result: runtime.state.sessionFile ? { sessionFile: runtime.state.sessionFile } : undefined,
    };
  }
  if (command.type === "resumeAndSend") {
    const validated = await validateExistingPiSessionSpawn({
      sessionsRoot: defaultPiSessionsRoot(),
      sessionFile: String(command.sessionFile),
      cwd: String(command.cwd),
      sessionKey: String(command.sessionKey),
    });
    if (startResumeAndSendDelivery === undefined)
      throw new Error("resume-and-send delivery start callback is required");
    return runSerializedResumeAndSendDelivery({
      entries: resumeAndSendQueues,
      sessionFile: validated.sessionFile,
      startDelivery: startResumeAndSendDelivery,
      deliver: async () => {
        const acquisition = await acquireResumeAndSendRuntime({
          existingWriter: () => liveWriterForSession(validated.sessionFile),
          spawnWriter: () =>
            spawnRuntime(
              { ...command, sessionFile: validated.sessionFile, cwd: validated.cwd },
              validated,
            ),
          isWriterClaimConflict: (cause) => cause instanceof SessionWriterClaimConflictError,
        });
        if (acquisition.reusedWriter) {
          return deliverRuntimeCommand(
            acquisition.runtime,
            existingWriterResumeAndSendCommand(command, acquisition.runtime.state.runtimeId),
          );
        }
        try {
          await rpc(acquisition.runtime, "prompt", {
            message: String(command.message),
            ...(Array.isArray(command.images) ? { images: command.images } : {}),
          });
        } catch (cause) {
          if (cause instanceof RpcCommandRejectedError) throw cause;
          throw new IndeterminateCommandError(
            cause instanceof Error ? cause.message : "pi command outcome is unknown",
          );
        }
        return {
          commandId: commandId as never,
          status: "completed",
          runtimeId: acquisition.runtime.state.runtimeId,
        };
      },
    });
  }
  const runtime = runtimes.get(String(command.runtimeId));
  if (!runtime) throw new Error("runtime is not live");
  return deliverRuntimeCommand(runtime, command);
}
export function runCommandSingleFlight<T>(input: {
  readonly entries: Map<string, { readonly hash: string; readonly work: Promise<T> }>;
  readonly id: string;
  readonly hash: string;
  readonly run: () => Promise<T>;
}): Promise<T> {
  const active = input.entries.get(input.id);
  if (active !== undefined) {
    if (active.hash !== input.hash) throw new Error("commandId payload conflict");
    return active.work;
  }
  let work!: Promise<T>;
  work = Promise.resolve()
    .then(() => input.run())
    .finally(() => {
      if (input.entries.get(input.id)?.work === work) input.entries.delete(input.id);
    });
  input.entries.set(input.id, { hash: input.hash, work });
  return work;
}

async function dispatch(command: Record<string, unknown>): Promise<SupervisorCommandReceipt> {
  const id = String(command.commandId);
  const hash = hashCommand(command);
  return runCommandSingleFlight({
    entries: liveCommands,
    id,
    hash,
    run: async () => {
      let prior = ledger[id];
      if (prior) {
        if (isManagedLedgerEntry(prior)) throw new Error("commandId payload conflict");
        if (prior.hash !== hash) throw new Error("commandId payload conflict");
        if (prior.receipt.status !== "started") {
          if (
            !shouldRestartPersistedSession(
              command,
              prior.receipt,
              prior.receipt.runtimeId === undefined
                ? undefined
                : runtimes.get(prior.receipt.runtimeId)?.state,
            )
          ) {
            return prior.receipt;
          }
          delete ledger[id];
          await atomicLedger();
          prior = undefined;
        }
      }
      if (prior) {
        if (isManagedLedgerEntry(prior)) throw new Error("commandId payload conflict");
        if (prior.phase !== "queued" || !prior.command)
          return { ...prior.receipt, status: "indeterminate" };
        command = prior.command;
      }
      const started = { commandId: id as never, status: "started" as const };
      let queuedWrite = Promise.resolve();
      if (!prior) {
        ledger[id] = { hash, command, phase: "queued", receipt: started };
        queuedWrite = atomicLedger();
      }
      const startDelivery = async () => {
        ledger[id] = { hash, command, phase: "delivering", receipt: started };
        await atomicLedger();
      };
      return queuedWrite
        .then(async () => {
          if (command.type === "resumeAndSend") return execute(command, startDelivery);
          await startDelivery();
          return execute(command);
        })
        .then(
          async (receipt) => {
            ledger[id] = { hash, receipt };
            await atomicLedger();
            return receipt;
          },
          async (cause) => {
            const receipt = {
              commandId: id as never,
              status:
                cause instanceof IndeterminateCommandError
                  ? ("indeterminate" as const)
                  : ("rejected" as const),
              error: String(cause),
            };
            ledger[id] = { hash, receipt };
            await atomicLedger();
            return receipt;
          },
        );
    },
  });
}
async function readSessionHeaderLine(sessionFile: string): Promise<string | undefined> {
  const handle = await fs.open(sessionFile, "r");
  try {
    const buffer = Buffer.alloc(64 * 1024);
    const { bytesRead } = await handle.read(buffer, 0, buffer.byteLength, 0);
    const contents = buffer.subarray(0, bytesRead).toString("utf8");
    const newline = contents.indexOf("\n");
    if (newline < 0) return bytesRead < buffer.byteLength ? contents : undefined;
    return contents.slice(0, newline).replace(/\r$/, "");
  } finally {
    await handle.close();
  }
}
async function registerBridge(socket: Socket, frame: Record<string, unknown>) {
  const socketGuard = createBridgeRegistrationSocketGuard(socket);
  try {
    const sessionId = String(frame.sessionId);
    if (typeof frame.sessionFile !== "string") {
      socket.end(encodeLine({ type: "error", error: "bridge sessionFile is required" }));
      return;
    }
    const sessionsRoot = await fs.realpath(defaultPiSessionsRoot()).catch(() => undefined);
    const sessionFile = await fs.realpath(frame.sessionFile).catch(() => undefined);
    if (!sessionsRoot || !sessionFile || !sessionFile.startsWith(`${sessionsRoot}${path.sep}`)) {
      socket.end(
        encodeLine({ type: "error", error: "bridge sessionFile is outside the pi sessions root" }),
      );
      return;
    }
    if (!reserveBridgeRegistration(sessionFile)) {
      socket.end(
        encodeLine({ type: "error", error: "session registration is already in progress" }),
      );
      return;
    }
    try {
      const firstLine = await readSessionHeaderLine(sessionFile);
      let header: unknown;
      try {
        header = JSON.parse(firstLine ?? "");
      } catch {
        header = undefined;
      }
      if (
        !isRecord(header) ||
        header.type !== "session" ||
        header.id !== sessionId ||
        typeof header.cwd !== "string" ||
        header.cwd !== frame.cwd
      ) {
        socket.end(
          encodeLine({ type: "error", error: "bridge session header does not match registration" }),
        );
        return;
      }
      if (socketGuard.isClosed()) return;
      const writerId = writers.get(sessionFile);
      const existing = writerId ? runtimes.get(writerId) : undefined;
      if (
        writerId &&
        (!existing ||
          existing.state.writerKind !== "tuiBridge" ||
          existing.bridge !== undefined ||
          !isRecord(existing.state.state) ||
          existing.state.state.sessionId !== sessionId)
      ) {
        socket.end(encodeLine({ type: "error", error: "session already has a writer" }));
        return;
      }
      const runtime: Runtime =
        existing ??
        ({
          state: {
            runtimeId: randomUUID() as never,
            sessionFile,
            cwd: String(frame.cwd),
            writerKind: "tuiBridge",
            status: "idle",
            sequence: 0,
            overlay: { isStreaming: false, pendingMessageCount: 0 },
            state: { sessionId, cwd: frame.cwd, pid: frame.pid },
          },
          ring: [],
          ringBytes: 0,
          ringEvictedThrough: -1,
          overlayEvents: [],
          overlayBytes: 0,
          overlayOmittedCount: 0,
          sessionReadOffset: (await readSessionFile(sessionFile)).offset,
          subscribers: new Set(),
          nextRpcId: 0,
          pending: new Map(),
          bridgePending: new Map(),
        } satisfies Runtime);
      if (socketGuard.isClosed()) return;
      if (runtime.bridgeExpiry) clearTimeout(runtime.bridgeExpiry);
      delete runtime.bridgeExpiry;
      runtime.bridge = socket;
      const bridgeIsStreaming =
        typeof frame.isStreaming === "boolean"
          ? frame.isStreaming
          : runtime.state.overlay?.isStreaming === true;
      const steering = Array.isArray(frame.steering)
        ? frame.steering.filter((value): value is string => typeof value === "string")
        : [];
      const followUp = Array.isArray(frame.followUp)
        ? frame.followUp.filter((value): value is string => typeof value === "string")
        : [];
      runtime.state = {
        ...runtime.state,
        status: bridgeIsStreaming ? "streaming" : "idle",
        overlay: {
          ...(runtime.state.overlay ?? { pendingMessageCount: 0 }),
          isStreaming: bridgeIsStreaming,
          pendingMessageCount: steering.length + followUp.length,
          lastEventType: existing ? "bridge_reconnected" : "bridge_registered",
        },
        state: { sessionId, cwd: frame.cwd, pid: frame.pid },
        cwd: String(frame.cwd),
      };
      runtimes.set(runtime.state.runtimeId, runtime);
      writers.set(sessionFile, runtime.state.runtimeId);
      event(runtime, {
        type: "event",
        event: "queue_update",
        data: { steering, followUp },
      });
      if (existing) event(runtime, { type: "bridge_reconnected", isStreaming: bridgeIsStreaming });
      if (existing && !bridgeIsStreaming)
        void publishSettledSnapshot(runtime, runtime.state.sequence);
      const cleanup = () => {
        if (runtime.state.status === "exited" || runtime.bridge !== socket) return;
        delete runtime.bridge;
        for (const pending of runtime.bridgePending.values())
          pending({ status: "indeterminate", error: "bridge disconnected" });
        runtime.bridgePending.clear();
        runtime.state = { ...runtime.state, status: "starting" };
        event(runtime, { type: "bridge_disconnected" });
        runtime.bridgeExpiry = setTimeout(() => {
          delete runtime.bridgeExpiry;
          if (!runtime.bridge) setExited(runtime);
        }, 30_000);
        runtime.bridgeExpiry.unref();
      };
      socketGuard.handoff(cleanup);
    } finally {
      releaseBridgeRegistration(sessionFile);
    }
  } finally {
    socketGuard.dispose();
  }
}
function parseSessionEntries(text: string): ReadonlyArray<Record<string, unknown>> {
  return text
    .split(/\r?\n/)
    .filter(Boolean)
    .flatMap((line) => {
      try {
        const value: unknown = JSON.parse(line);
        return isRecord(value) ? [value] : [];
      } catch {
        return [];
      }
    });
}
async function readSessionFile(sessionFile: string | undefined) {
  if (!sessionFile) return { entries: [], offset: 0 } as const;
  let handle: NodeFS.promises.FileHandle | undefined;
  try {
    handle = await fs.open(sessionFile, "r");
    const stat = await handle.stat();
    const readRange = async (start: number, length: number) => {
      const buffer = Buffer.alloc(length);
      const { bytesRead } = await handle!.read(buffer, 0, length, start);
      return buffer.subarray(0, bytesRead).toString("utf8");
    };
    const headLength = Math.min(stat.size, SNAPSHOT_HEAD_BYTES);
    const tailStart = Math.max(0, stat.size - SNAPSHOT_TAIL_BYTES);
    const [rawHead, rawTail] = await Promise.all([
      readRange(0, headLength),
      readRange(tailStart, stat.size - tailStart),
    ]);
    const head =
      headLength < stat.size && !rawHead.endsWith("\n")
        ? rawHead.slice(0, rawHead.lastIndexOf("\n") + 1)
        : rawHead;
    const tail =
      tailStart > 0 && !rawTail.startsWith("\n")
        ? rawTail.slice(rawTail.indexOf("\n") + 1)
        : rawTail;
    const headEntries = parseSessionEntries(head);
    const tailEntries = parseSessionEntries(tail).slice(-(SNAPSHOT_ENTRY_LIMIT - 1));
    const header = headEntries.find((entry) => entry.type === "session");
    return {
      entries: header
        ? [
            header,
            ...tailEntries.filter((entry) => entry.type !== "session" || entry.id !== header.id),
          ]
        : tailEntries,
      offset: stat.size,
    };
  } catch {
    return { entries: [], offset: 0 } as const;
  } finally {
    await handle?.close();
  }
}
async function readAppendedEntries(sessionFile: string | undefined, offset: number) {
  if (!sessionFile) return { entries: [], offset } as const;
  let handle: NodeFS.promises.FileHandle | undefined;
  try {
    handle = await fs.open(sessionFile, "r");
    const stat = await handle.stat();
    if (stat.size < offset) return readSessionFile(sessionFile);
    const start = Math.max(offset, stat.size - SNAPSHOT_TAIL_BYTES);
    const buffer = Buffer.alloc(stat.size - start);
    const { bytesRead } = await handle.read(buffer, 0, buffer.byteLength, start);
    const raw = buffer.subarray(0, bytesRead).toString("utf8");
    const complete = start > offset ? raw.slice(raw.indexOf("\n") + 1) : raw;
    return {
      entries: parseSessionEntries(complete).slice(-SNAPSHOT_ENTRY_LIMIT),
      offset: stat.size,
    };
  } catch {
    return { entries: [], offset } as const;
  } finally {
    await handle?.close();
  }
}
export async function createSupervisorLockFile(lockPath: string, pid: number): Promise<boolean> {
  const candidate = `${lockPath}.candidate-${pid}-${randomUUID()}`;
  const handle = await fs.open(candidate, "wx", 0o600);
  try {
    await handle.writeFile(String(pid));
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await fs.link(candidate, lockPath);
    return true;
  } catch (cause) {
    if (cause instanceof Error && "code" in cause && cause.code === "EEXIST") return false;
    throw cause;
  } finally {
    await fs.rm(candidate, { force: true });
  }
}

export async function runSupervisorDaemon(): Promise<never> {
  await fs.mkdir(ROOT, { recursive: true, mode: 0o700 });
  await fs.chmod(ROOT, 0o700);
  const lockPath = path.join(ROOT, "supervisor.lock");
  while (!(await createSupervisorLockFile(lockPath, process.pid))) {
    const ownerText = await fs.readFile(lockPath, "utf8").catch(() => "");
    const ownerPid = Number.parseInt(ownerText, 10);
    if (Number.isSafeInteger(ownerPid) && ownerPid > 0) {
      try {
        process.kill(ownerPid, 0);
        throw new Error("pi supervisor already running");
      } catch (cause) {
        if (!(cause instanceof Error) || !("code" in cause) || cause.code !== "ESRCH") throw cause;
      }
    }
    let live = false;
    for (let attempt = 0; attempt < 40 && !live; attempt++) {
      live = await new Promise<boolean>((resolve) => {
        const probe = createConnection(supervisorSocketPath);
        probe.once("connect", () => {
          probe.end();
          resolve(true);
        });
        probe.once("error", () => resolve(false));
      });
      if (!live) await new Promise((resolve) => setTimeout(resolve, 50));
    }
    if (live) throw new Error("pi supervisor already running");
    const staleClaim = `${lockPath}.stale-${process.pid}-${randomUUID()}`;
    try {
      await fs.rename(lockPath, staleClaim);
    } catch (cause) {
      if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") continue;
      throw cause;
    }
    const claimedOwner = await fs.readFile(staleClaim, "utf8").catch(() => "");
    if (claimedOwner !== ownerText) {
      await fs.link(staleClaim, lockPath).catch(() => undefined);
      await fs.rm(staleClaim, { force: true });
      throw new Error("pi supervisor already running");
    }
    await fs.rm(staleClaim, { force: true });
  }
  try {
    ledger = JSON.parse(await fs.readFile(LEDGER, "utf8")) as typeof ledger;
  } catch (cause) {
    if (!(cause instanceof Error && "code" in cause && cause.code === "ENOENT")) throw cause;
    ledger = {};
  }
  let recoveredIndeterminate = false;
  for (const [commandId, entry] of Object.entries(ledger)) {
    if (isManagedLedgerEntry(entry)) continue;
    if (entry.receipt.status !== "started" || entry.phase !== "delivering") continue;
    ledger[commandId] = {
      hash: entry.hash,
      receipt: {
        commandId: commandId as never,
        status: "indeterminate",
        error: "supervisor restarted after command delivery began",
      },
    };
    recoveredIndeterminate = true;
  }
  if (recoveredIndeterminate) await atomicLedger();
  await managedAdmissions.recoverAfterRestart();
  await fs.rm(supervisorSocketPath, { force: true });
  const server = createServer((socket) => {
    socket.setEncoding("utf8");
    socket.on("error", () => socket.destroy());
    const decoder = new JsonLineDecoder();
    let handling = Promise.resolve();
    socket.on("close", () => {
      socketWrites.delete(socket);
      void managedAdmissions.disconnect(socket).catch(() => {});
      for (const runtime of runtimes.values())
        for (const subscriber of runtime.subscribers)
          if (subscriber.socket === socket) runtime.subscribers.delete(subscriber);
    });
    socket.on("data", (chunk: string) => {
      try {
        for (const value of decoder.push(chunk))
          handling = handling.then(
            () =>
              handleFrame(socket, value).catch((cause) => {
                writeBounded(socket, {
                  type: "error",
                  error: cause instanceof Error ? cause.message : String(cause),
                });
              }),
            () => {
              socket.destroy();
            },
          );
      } catch {
        socket.destroy();
      }
    });
  });
  await new Promise<void>((resolve, reject) =>
    server.listen(supervisorSocketPath, resolve).once("error", reject),
  );
  await fs.chmod(supervisorSocketPath, 0o600);
  return await new Promise<never>(() => {});
}

async function handleFrame(socket: Socket, value: unknown): Promise<void> {
  if (!isRecord(value)) return;
  if (value.type === "register" && value.protocol === SUPERVISOR_PROTOCOL) {
    try {
      await registerBridge(socket, value);
    } catch (cause) {
      socket.end(
        encodeLine({
          type: "error",
          error: cause instanceof Error ? cause.message : String(cause),
        }),
      );
    }
    return;
  }
  const bridged = [...runtimes.values()].find((candidate) => candidate.bridge === socket);
  if (value.type === "event") {
    if (bridged) {
      if (value.event === "agent_start") clearOverlay(bridged);
      const streaming =
        value.event === "agent_start"
          ? true
          : value.event === "agent_settled"
            ? false
            : (bridged.state.overlay?.isStreaming ?? false);
      const queueData = value.event === "queue_update" && isRecord(value.data) ? value.data : {};
      const pendingMessageCount =
        (Array.isArray(queueData.steering) ? queueData.steering.length : 0) +
        (Array.isArray(queueData.followUp) ? queueData.followUp.length : 0);
      bridged.state = {
        ...bridged.state,
        overlay: {
          ...(bridged.state.overlay ?? { isStreaming: false, pendingMessageCount: 0 }),
          isStreaming: streaming,
          ...(value.event === "queue_update" ? { pendingMessageCount } : {}),
          ...(typeof value.event === "string" ? { lastEventType: value.event } : {}),
        },
        status:
          value.event === "agent_start"
            ? "streaming"
            : value.event === "agent_settled"
              ? "idle"
              : bridged.state.status,
      };
      if (value.event === "agent_settled") {
        void publishSettledSnapshot(bridged, bridged.state.sequence);
      } else {
        event(bridged, value);
      }
    }
    return;
  }
  if (value.type === "unregister") {
    if (bridged) setExited(bridged);
    return;
  }
  if (value.type === "receipt") {
    if (bridged) {
      const pending = bridged.bridgePending.get(String(value.commandId));
      bridged.bridgePending.delete(String(value.commandId));
      pending?.(value);
      event(bridged, value);
    }
    return;
  }
  if (value.type !== "request" || typeof value.requestId !== "string") return;
  try {
    if (value.method === "list") {
      writeBounded(socket, {
        type: "response",
        requestId: value.requestId,
        ok: true,
        result: [...runtimes.values()].map((runtime) => projectListedRuntime(runtime.state)),
        capabilities: {
          managedAdmission: MANAGED_ADMISSION_PROTOCOL,
          guardedResume: GUARDED_RESUME_CAPABILITY,
        },
      });
    } else if (value.method === "dispatch" && isRecord(value.command)) {
      writeBounded(socket, {
        type: "response",
        requestId: value.requestId,
        ok: true,
        result: await dispatch(value.command),
      });
    } else if (value.method === "claimManaged") {
      writeBounded(socket, {
        type: "response",
        requestId: value.requestId,
        ok: true,
        result: await managedAdmissions.claim(socket, decodeManagedClaim(value.claim)),
      });
    } else if (value.method === "finalizeManaged") {
      writeBounded(socket, {
        type: "response",
        requestId: value.requestId,
        ok: true,
        result: await managedAdmissions.finalize(
          socket,
          decodeManagedFinalization(value.finalization),
        ),
      });
    } else if (value.method === "subscribe" && typeof value.runtimeId === "string") {
      const runtime = runtimes.get(value.runtimeId);
      if (!runtime) throw new Error("unknown runtime");
      const subscriber = {
        socket,
        requestId: value.requestId,
        ready: false,
        buffer: [] as SupervisorStreamItem[],
        bufferBytes: 0,
      };
      runtime.subscribers.add(subscriber);
      const cursor = typeof value.cursor === "number" ? value.cursor : undefined;
      const oldest = runtime.ring[0];
      const needsSnapshot = shouldUseSnapshot(
        cursor,
        runtime.ringEvictedThrough,
        oldest ? streamItemSequence(oldest) : undefined,
      );
      let snapshotState: SupervisorRuntimeState;
      if (needsSnapshot) {
        snapshotState = projectListedRuntime(runtime.state);
        const snapshotOverlayEvents = [
          ...runtime.overlayEvents,
          ...(runtime.pendingQueueEvent ? [runtime.pendingQueueEvent] : []),
        ]
          .filter((item) => item.sequence <= snapshotState.sequence)
          .sort((a, b) => a.sequence - b.sequence);
        const snapshot = await readSessionFile(runtime.state.sessionFile);
        const entries = snapshot.entries;
        const boundedEntries =
          entries.length <= SNAPSHOT_ENTRY_LIMIT
            ? entries
            : [entries[0]!, ...entries.slice(-(SNAPSHOT_ENTRY_LIMIT - 1))];
        if (
          !writeBounded(socket, {
            type: "stream",
            requestId: value.requestId,
            item: {
              type: "snapshot",
              runtime: snapshotState,
              entries: boundedEntries,
              events: snapshotOverlayEvents,
              omittedOverlayEventCount: runtime.overlayOmittedCount,
            },
          })
        )
          return;
      } else {
        snapshotState = runtime.state;
        const replayCursor = cursor ?? -1;
        for (const item of runtime.ring) {
          const itemSequence = streamItemSequence(item);
          if (
            itemSequence > replayCursor &&
            itemSequence <= snapshotState.sequence &&
            !writeBounded(socket, { type: "stream", requestId: value.requestId, item })
          )
            return;
        }
      }
      for (const item of subscriber.buffer) {
        const itemSequence = streamItemSequence(item);
        if (
          itemSequence > snapshotState.sequence &&
          !writeBounded(socket, { type: "stream", requestId: value.requestId, item })
        )
          return;
      }
      subscriber.buffer.length = 0;
      subscriber.bufferBytes = 0;
      subscriber.ready = true;
      writeBounded(socket, {
        type: "stream",
        requestId: value.requestId,
        item: {
          type: "synchronized",
          runtimeId: runtime.state.runtimeId,
          sequence: runtime.state.sequence,
        },
      });
    }
  } catch (cause) {
    writeBounded(socket, {
      type: "response",
      requestId: value.requestId,
      ok: false,
      error: String(cause),
    });
  }
}
