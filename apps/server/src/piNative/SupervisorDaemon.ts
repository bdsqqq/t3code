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
  PiNativeCommandReceipt,
  PiNativeRuntimeState,
  PiNativeStreamEvent,
  PiNativeStreamItem,
} from "@t3tools/contracts";
import {
  JsonLineDecoder,
  SUPERVISOR_PROTOCOL,
  encodeLine,
  isRecord,
} from "./SupervisorProtocol.ts";

const { createHash, randomUUID } = NodeCrypto;
const { spawn } = NodeChildProcess;
const fs = NodeFS.promises;
const { createConnection, createServer } = NodeNet;
const { homedir } = NodeOS;
const path = NodePath;
type ChildProcessWithoutNullStreams = NodeChildProcess.ChildProcessWithoutNullStreams;
type Socket = NodeNet.Socket;
const ROOT =
  process.env.T3_PI_SUPERVISOR_ROOT ?? path.join(homedir(), ".pi", "agent", "t3-control-v1");
export const supervisorSocketPath = path.join(ROOT, "supervisor.sock");
const LEDGER = path.join(ROOT, "commands.json");
const RING_SIZE = 1_000;
const EXITED_RETENTION_MS = 60_000;
const MAX_SOCKET_QUEUE_BYTES = 16 * 1024 * 1024;
const SNAPSHOT_ENTRY_LIMIT = 1_000;
const SNAPSHOT_HEAD_BYTES = 256 * 1024;
const SNAPSHOT_TAIL_BYTES = 16 * 1024 * 1024;
class IndeterminateCommandError extends Error {}
class RpcCommandRejectedError extends Error {}
type LedgerEntry = { hash: string; receipt: PiNativeCommandReceipt };
type Runtime = {
  state: PiNativeRuntimeState;
  child?: ChildProcessWithoutNullStreams;
  bridge?: Socket;
  bridgeExpiry?: NodeJS.Timeout;
  ring: PiNativeStreamItem[];
  overlayEvents: PiNativeStreamEvent[];
  sessionReadOffset: number;
  subscribers: Set<{
    socket: Socket;
    requestId: string;
    ready: boolean;
    buffer: PiNativeStreamItem[];
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
const liveCommands = new Map<string, { hash: string; work: Promise<PiNativeCommandReceipt> }>();
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
const writeBounded = (socket: Socket, value: unknown): boolean => {
  if (socket.destroyed) return false;
  const line = encodeLine(value);
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
const emit = (runtime: Runtime, item: PiNativeStreamItem) => {
  runtime.ring.push(item);
  if (runtime.ring.length > RING_SIZE) runtime.ring.shift();
  for (const subscriber of runtime.subscribers) {
    if (!subscriber.ready) {
      subscriber.buffer.push(item);
      if (subscriber.buffer.length > RING_SIZE)
        subscriber.socket.destroy(new Error("slow supervisor client"));
    } else if (
      !writeBounded(subscriber.socket, { type: "stream", requestId: subscriber.requestId, item })
    )
      runtime.subscribers.delete(subscriber);
  }
};
const streamItemSequence = (item: PiNativeStreamItem): number =>
  item.type === "snapshot" ? item.runtime.sequence : item.sequence;
const event = (runtime: Runtime, payload: unknown) => {
  const sequence = runtime.state.sequence + 1;
  runtime.state = { ...runtime.state, sequence };
  const item: PiNativeStreamEvent = {
    type: "event",
    runtimeId: runtime.state.runtimeId,
    sequence,
    eventId: `${runtime.state.runtimeId}:${sequence}` as never,
    event: payload,
  };
  runtime.overlayEvents.push(item);
  if (runtime.overlayEvents.length > RING_SIZE) runtime.overlayEvents.shift();
  emit(runtime, item);
};
const publishSettledSnapshot = async (runtime: Runtime, settledSequence: number) => {
  const appended = await readAppendedEntries(runtime.state.sessionFile, runtime.sessionReadOffset);
  if (runtime.state.sequence !== settledSequence || runtime.state.status !== "idle") return;
  runtime.sessionReadOffset = appended.offset;
  runtime.state = { ...runtime.state, sequence: settledSequence + 1 };
  emit(runtime, {
    type: "entries",
    runtimeId: runtime.state.runtimeId,
    sequence: runtime.state.sequence,
    entries: appended.entries,
  });
  emit(runtime, {
    type: "synchronized",
    runtimeId: runtime.state.runtimeId,
    sequence: runtime.state.sequence,
  });
};
const setExited = (runtime: Runtime, exitCode?: number) => {
  if (runtime.state.status === "exited") return;
  if (runtime.bridgeExpiry) clearTimeout(runtime.bridgeExpiry);
  const sequence = runtime.state.sequence + 1;
  runtime.state = { ...runtime.state, sequence, status: "exited" };
  if (runtime.state.sessionFile) writers.delete(runtime.state.sessionFile);
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
      runtime.overlayEvents.length = 0;
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
    for (const value of decoder.push(chunk)) {
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
        if (eventType === "agent_start") runtime.overlayEvents.length = 0;
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
            ...(eventType ? { lastEventType: eventType } : {}),
          },
        };
        event(runtime, value);
        if (eventType === "agent_settled") {
          runtime.overlayEvents.length = 0;
          void publishSettledSnapshot(runtime, runtime.state.sequence);
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
async function spawnRuntime(command: Record<string, unknown>): Promise<Runtime> {
  const runtimeId = randomUUID() as never;
  const cwd = String(command.cwd);
  const cwdStat = await fs.stat(cwd);
  if (!cwdStat.isDirectory()) throw new Error("cwd is not a directory");
  const sessionFile =
    typeof command.sessionFile === "string" ? await fs.realpath(command.sessionFile) : undefined;
  if (sessionFile && writers.has(sessionFile)) throw new Error("session already has a writer");
  if (sessionFile) writers.set(sessionFile, runtimeId);
  const args = ["--mode", "rpc", ...(sessionFile ? ["--session", sessionFile] : [])];
  const child = spawn(process.env.T3_PI_EXECUTABLE ?? "pi", args, {
    cwd,
    stdio: ["pipe", "pipe", "pipe"],
  });
  await new Promise<void>((resolve, reject) => {
    child.once("spawn", resolve);
    child.once("error", reject);
  }).catch((cause) => {
    if (sessionFile && writers.get(sessionFile) === runtimeId) writers.delete(sessionFile);
    throw cause;
  });
  const runtime: Runtime = {
    state: {
      runtimeId,
      ...(sessionFile ? { sessionFile } : {}),
      cwd,
      writerKind: "rpc",
      status: "starting",
      sequence: 0,
      overlay: { isStreaming: false, pendingMessageCount: 0 },
    },
    child,
    ring: [],
    overlayEvents: [],
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
    runtimes.delete(runtimeId);
    if (sessionFile) writers.delete(sessionFile);
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
      throw new Error("session already has a writer");
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
  return runtime;
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

async function execute(command: Record<string, unknown>): Promise<PiNativeCommandReceipt> {
  const commandId = String(command.commandId);
  if (command.type === "start" || command.type === "resume") {
    const runtime = await spawnRuntime(command);
    return {
      commandId: commandId as never,
      status: "completed",
      runtimeId: runtime.state.runtimeId,
    };
  }
  const runtime = runtimes.get(String(command.runtimeId));
  if (!runtime || runtime.state.status === "exited") throw new Error("runtime is not live");
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
      runtime.bridge!.write(
        encodeLine({
          type: "command",
          commandId,
          command: command.type,
          ...(typeof command.message === "string" ? { text: command.message } : {}),
        }),
      );
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
async function dispatch(command: Record<string, unknown>): Promise<PiNativeCommandReceipt> {
  const id = String(command.commandId);
  const hash = hashCommand(command);
  const active = liveCommands.get(id);
  if (active) {
    if (active.hash !== hash) throw new Error("commandId payload conflict");
    return active.work;
  }
  const prior = ledger[id];
  if (prior) {
    if (prior.hash !== hash) throw new Error("commandId payload conflict");
    return prior.receipt.status === "started"
      ? { ...prior.receipt, status: "indeterminate" }
      : prior.receipt;
  }
  const started = { commandId: id as never, status: "started" as const };
  ledger[id] = { hash, receipt: started };
  const work = atomicLedger()
    .then(() => execute(command))
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
    )
    .finally(() => liveCommands.delete(id));
  liveCommands.set(id, { hash, work });
  return work;
}
async function registerBridge(socket: Socket, frame: Record<string, unknown>) {
  const sessionId = String(frame.sessionId);
  if (typeof frame.sessionFile !== "string") {
    socket.end(encodeLine({ type: "error", error: "bridge sessionFile is required" }));
    return;
  }
  const sessionsRoot = await fs
    .realpath(process.env.T3_PI_SESSIONS_ROOT ?? path.join(homedir(), ".pi", "agent", "sessions"))
    .catch(() => undefined);
  const sessionFile = await fs.realpath(frame.sessionFile).catch(() => undefined);
  if (!sessionsRoot || !sessionFile || !sessionFile.startsWith(`${sessionsRoot}${path.sep}`)) {
    socket.end(
      encodeLine({ type: "error", error: "bridge sessionFile is outside the pi sessions root" }),
    );
    return;
  }
  const firstLine = (await fs.readFile(sessionFile, "utf8")).split(/\r?\n/, 1)[0];
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
  if (socket.destroyed) return;
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
      overlayEvents: [],
      sessionReadOffset: (await readSessionFile(sessionFile)).offset,
      subscribers: new Set(),
      nextRpcId: 0,
      pending: new Map(),
      bridgePending: new Map(),
    } satisfies Runtime);
  if (runtime.bridgeExpiry) clearTimeout(runtime.bridgeExpiry);
  delete runtime.bridgeExpiry;
  runtime.bridge = socket;
  const bridgeIsStreaming =
    typeof frame.isStreaming === "boolean"
      ? frame.isStreaming
      : runtime.state.overlay?.isStreaming === true;
  runtime.state = {
    ...runtime.state,
    status: bridgeIsStreaming ? "streaming" : "idle",
    overlay: {
      ...(runtime.state.overlay ?? { pendingMessageCount: 0 }),
      isStreaming: bridgeIsStreaming,
      lastEventType: existing ? "bridge_reconnected" : "bridge_registered",
    },
    state: { sessionId, cwd: frame.cwd, pid: frame.pid },
    cwd: String(frame.cwd),
  };
  runtimes.set(runtime.state.runtimeId, runtime);
  writers.set(sessionFile, runtime.state.runtimeId);
  if (existing) event(runtime, { type: "bridge_reconnected", isStreaming: bridgeIsStreaming });
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
  socket.once("close", cleanup);
  socket.once("error", cleanup);
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
    const start = stat.size < offset ? 0 : offset;
    const buffer = Buffer.alloc(Math.max(0, stat.size - start));
    const { bytesRead } = await handle.read(buffer, 0, buffer.byteLength, start);
    return {
      entries: parseSessionEntries(buffer.subarray(0, bytesRead).toString("utf8")),
      offset: start + bytesRead,
    };
  } catch {
    return { entries: [], offset } as const;
  } finally {
    await handle?.close();
  }
}

export async function runSupervisorDaemon(): Promise<never> {
  await fs.mkdir(ROOT, { recursive: true, mode: 0o700 });
  await fs.chmod(ROOT, 0o700);
  try {
    ledger = JSON.parse(await fs.readFile(LEDGER, "utf8")) as typeof ledger;
  } catch {
    ledger = {};
  }
  const lockPath = path.join(ROOT, "supervisor.lock");
  let lock: NodeFS.promises.FileHandle;
  try {
    lock = await fs.open(lockPath, "wx", 0o600);
  } catch {
    const ownerPid = Number.parseInt(await fs.readFile(lockPath, "utf8").catch(() => ""), 10);
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
    await fs.rm(lockPath, { force: true });
    await fs.rm(supervisorSocketPath, { force: true });
    lock = await fs.open(lockPath, "wx", 0o600);
  }
  await lock.writeFile(String(process.pid));
  await lock.sync();
  await fs.rm(supervisorSocketPath, { force: true });
  const server = createServer((socket) => {
    socket.setEncoding("utf8");
    socket.on("error", () => socket.destroy());
    const decoder = new JsonLineDecoder();
    let handling = Promise.resolve();
    socket.on("close", () => {
      socketWrites.delete(socket);
      for (const runtime of runtimes.values())
        for (const subscriber of runtime.subscribers)
          if (subscriber.socket === socket) runtime.subscribers.delete(subscriber);
    });
    socket.on("data", (chunk: string) => {
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
    });
  });
  await new Promise<void>((resolve, reject) =>
    server.listen(supervisorSocketPath, resolve).once("error", reject),
  );
  await fs.chmod(supervisorSocketPath, 0o600);
  void lock;
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
      if (value.event === "agent_start") bridged.overlayEvents.length = 0;
      const streaming =
        value.event === "agent_start"
          ? true
          : value.event === "agent_settled"
            ? false
            : (bridged.state.overlay?.isStreaming ?? false);
      bridged.state = {
        ...bridged.state,
        overlay: {
          ...(bridged.state.overlay ?? { isStreaming: false, pendingMessageCount: 0 }),
          isStreaming: streaming,
          ...(typeof value.event === "string" ? { lastEventType: value.event } : {}),
        },
        status:
          value.event === "agent_start"
            ? "streaming"
            : value.event === "agent_settled"
              ? "idle"
              : bridged.state.status,
      };
      event(bridged, value);
      if (value.event === "agent_settled") {
        bridged.overlayEvents.length = 0;
        void publishSettledSnapshot(bridged, bridged.state.sequence);
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
    if (value.method === "list")
      socket.write(
        encodeLine({
          type: "response",
          requestId: value.requestId,
          ok: true,
          result: [...runtimes.values()].map((runtime) => runtime.state),
        }),
      );
    else if (value.method === "dispatch" && isRecord(value.command))
      socket.write(
        encodeLine({
          type: "response",
          requestId: value.requestId,
          ok: true,
          result: await dispatch(value.command),
        }),
      );
    else if (value.method === "subscribe" && typeof value.runtimeId === "string") {
      const runtime = runtimes.get(value.runtimeId);
      if (!runtime) throw new Error("unknown runtime");
      const subscriber = {
        socket,
        requestId: value.requestId,
        ready: false,
        buffer: [] as PiNativeStreamItem[],
      };
      runtime.subscribers.add(subscriber);
      const cursor = typeof value.cursor === "number" ? value.cursor : undefined;
      const oldest = runtime.ring[0];
      const snapshotState = runtime.state;
      const snapshotOverlayEvents = runtime.overlayEvents.filter(
        (item) => item.sequence <= snapshotState.sequence,
      );
      const needsSnapshot =
        cursor === undefined || (oldest && cursor < streamItemSequence(oldest) - 1);
      if (needsSnapshot) {
        const snapshot = await readSessionFile(snapshotState.sessionFile);
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
            },
          })
        )
          return;
      } else {
        for (const item of runtime.ring) {
          const itemSequence = streamItemSequence(item);
          if (
            itemSequence > cursor &&
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
    socket.write(
      encodeLine({ type: "response", requestId: value.requestId, ok: false, error: String(cause) }),
    );
  }
}
