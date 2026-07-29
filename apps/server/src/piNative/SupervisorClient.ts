// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeNet from "node:net";
import type {
  PiNativeCommand,
  PiNativeCommandReceipt,
  PiNativeRuntimeState,
  PiNativeStreamItem,
} from "@t3tools/contracts";
import { PiNativeError } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { JsonLineDecoder, encodeLine, isRecord } from "./SupervisorProtocol.ts";
import { supervisorSocketPath } from "./SupervisorDaemon.ts";

const { randomUUID } = NodeCrypto;
const { spawn } = NodeChildProcess;
const { createConnection } = NodeNet;
type Socket = NodeNet.Socket;
const connect = () =>
  new Promise<Socket>((resolve, reject) => {
    const socket = createConnection(supervisorSocketPath);
    socket.once("connect", () => resolve(socket));
    socket.once("error", reject);
  });
const spawnDaemon = () => {
  const entry = process.argv[1];
  if (!entry) throw new Error("cannot resolve t3 executable");
  const child = spawn(process.execPath, [entry, "pi-supervisor"], {
    detached: true,
    stdio: "ignore",
    env: process.env,
  });
  child.unref();
};
let daemonReady: Promise<void> | undefined;
async function connectOrSpawn(): Promise<Socket> {
  try {
    return await connect();
  } catch {}
  daemonReady ??= (async () => {
    spawnDaemon();
    for (let attempt = 0; attempt < 100; attempt++) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      try {
        const socket = await connect();
        socket.end();
        return;
      } catch {}
    }
    throw new Error("pi supervisor did not start");
  })().finally(() => {
    daemonReady = undefined;
  });
  await daemonReady;
  return connect();
}
async function request(method: string, fields: Record<string, unknown> = {}): Promise<unknown> {
  const socket = await connectOrSpawn();
  const requestId = randomUUID();
  const decoder = new JsonLineDecoder();
  return await new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      settled = true;
      socket.destroy();
      reject(new Error("supervisor request timed out"));
    }, 120_000);
    const fail = (cause: unknown) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(cause instanceof Error ? cause : new Error(String(cause)));
    };
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      for (const value of decoder.push(chunk))
        if (isRecord(value) && value.type === "response" && value.requestId === requestId) {
          if (settled) continue;
          settled = true;
          clearTimeout(timer);
          socket.end();
          if (value.ok === true) resolve(value.result);
          else reject(new Error(String(value.error)));
        }
    });
    socket.once("error", fail);
    socket.once("close", () => fail(new Error("pi supervisor request closed")));
    socket.write(encodeLine({ type: "request", requestId, method, ...fields }));
  });
}
async function* subscribe(runtimeId: string, cursor?: number): AsyncGenerator<PiNativeStreamItem> {
  const socket = await connectOrSpawn();
  const requestId = randomUUID();
  const decoder = new JsonLineDecoder();
  const values: PiNativeStreamItem[] = [];
  let wake: (() => void) | undefined;
  let done = false;
  let failure: unknown;
  socket.setEncoding("utf8");
  socket.on("data", (chunk: string) => {
    for (const value of decoder.push(chunk)) {
      if (isRecord(value) && value.type === "stream" && value.requestId === requestId) {
        values.push(value.item as PiNativeStreamItem);
        if (values.length > 1_000) {
          failure = new Error("supervisor stream consumer is too slow");
          done = true;
          socket.destroy();
        }
      } else if (
        isRecord(value) &&
        value.type === "response" &&
        value.requestId === requestId &&
        value.ok === false
      ) {
        failure = new Error(String(value.error));
        done = true;
      }
    }
    wake?.();
  });
  socket.on("error", (cause) => {
    failure = cause;
    done = true;
    wake?.();
  });
  socket.on("close", () => {
    if (!done) failure = new Error("pi supervisor subscription closed");
    done = true;
    wake?.();
  });
  socket.write(
    encodeLine({
      type: "request",
      requestId,
      method: "subscribe",
      runtimeId,
      ...(cursor === undefined ? {} : { cursor }),
    }),
  );
  try {
    while (true) {
      if (done && values.length === 0) break;
      if (values.length === 0)
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
      wake = undefined;
      while (values.length > 0) yield values.shift()!;
      if (failure) throw failure;
    }
  } finally {
    socket.destroy();
  }
}
const mapError = (cause: unknown) =>
  new PiNativeError({
    code: "supervisor",
    message: cause instanceof Error ? cause.message : String(cause),
  });
export class SupervisorClient extends Context.Service<
  SupervisorClient,
  {
    readonly list: () => Effect.Effect<ReadonlyArray<PiNativeRuntimeState>, PiNativeError>;
    readonly dispatch: (
      command: PiNativeCommand | Record<string, unknown>,
    ) => Effect.Effect<PiNativeCommandReceipt, PiNativeError>;
    readonly subscribe: (
      runtimeId: string,
      cursor?: number,
    ) => Stream.Stream<PiNativeStreamItem, PiNativeError>;
  }
>()("t3/piNative/SupervisorClient") {
  static readonly layer = Layer.sync(SupervisorClient, makeSupervisorClient);
}
export function makeSupervisorClient(): SupervisorClient["Service"] {
  return SupervisorClient.of({
    list: () =>
      Effect.tryPromise({
        try: () => request("list") as Promise<ReadonlyArray<PiNativeRuntimeState>>,
        catch: mapError,
      }),
    dispatch: (command) =>
      Effect.tryPromise({
        try: () => request("dispatch", { command }) as Promise<PiNativeCommandReceipt>,
        catch: mapError,
      }),
    subscribe: (runtimeId, cursor) =>
      Stream.fromAsyncIterable(subscribe(runtimeId, cursor), mapError),
  });
}
