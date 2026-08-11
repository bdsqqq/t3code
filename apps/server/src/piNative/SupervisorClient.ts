// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeNet from "node:net";
import { PiNativeError } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import type {
  ManagedClaimRequest,
  ManagedClaimResponse,
  ManagedFinalization,
  ManagedFinalizeResponse,
  SupervisorCommand,
  SupervisorCommandReceipt,
  SupervisorRuntimeState,
  SupervisorStreamItem,
} from "./SupervisorProtocol.ts";
import {
  JsonLineDecoder,
  MANAGED_ADMISSION_PROTOCOL,
  SupervisorStreamBuffer,
  encodeLine,
  isRecord,
} from "./SupervisorProtocol.ts";
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
      try {
        for (const value of decoder.push(chunk))
          if (isRecord(value) && value.type === "response" && value.requestId === requestId) {
            if (settled) continue;
            settled = true;
            clearTimeout(timer);
            socket.end();
            if (value.ok === true) resolve(value.result);
            else reject(new Error(String(value.error)));
          }
      } catch (cause) {
        socket.destroy();
        fail(cause);
      }
    });
    socket.once("error", fail);
    socket.once("close", () => fail(new Error("pi supervisor request closed")));
    socket.write(encodeLine({ type: "request", requestId, method, ...fields }));
  });
}

interface ManagedConnection {
  readonly request: (
    method: string,
    fields: Record<string, unknown>,
  ) => Promise<Record<string, unknown>>;
  readonly close: () => void;
}

async function openManagedConnection(): Promise<ManagedConnection> {
  const socket = await connectOrSpawn();
  const decoder = new JsonLineDecoder();
  let pending:
    | {
        readonly requestId: string;
        readonly resolve: (value: Record<string, unknown>) => void;
        readonly reject: (cause: Error) => void;
        readonly timer: NodeJS.Timeout;
      }
    | undefined;
  const fail = (cause: unknown) => {
    const current = pending;
    pending = undefined;
    if (!current) return;
    clearTimeout(current.timer);
    current.reject(cause instanceof Error ? cause : new Error(String(cause)));
  };
  socket.setEncoding("utf8");
  socket.on("data", (chunk: string) => {
    try {
      for (const value of decoder.push(chunk)) {
        if (
          !pending ||
          !isRecord(value) ||
          value.type !== "response" ||
          value.requestId !== pending.requestId
        )
          continue;
        const current = pending;
        pending = undefined;
        clearTimeout(current.timer);
        if (value.ok === true) current.resolve(value);
        else current.reject(new Error(String(value.error)));
      }
    } catch (cause) {
      socket.destroy();
      fail(cause);
    }
  });
  socket.on("error", fail);
  socket.on("close", () => fail(new Error("pi supervisor managed admission socket closed")));
  return {
    request: (method, fields) => {
      if (pending) return Promise.reject(new Error("managed admission request already in flight"));
      if (socket.destroyed)
        return Promise.reject(new Error("managed admission socket is not connected"));
      const requestId = randomUUID();
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          fail(new Error("supervisor managed admission request timed out"));
          socket.destroy();
        }, 120_000);
        pending = { requestId, resolve, reject, timer };
        socket.write(encodeLine({ type: "request", requestId, method, ...fields }));
      });
    },
    close: () => socket.destroy(),
  };
}
async function* subscribe(
  runtimeId: string,
  cursor?: number,
): AsyncGenerator<SupervisorStreamItem> {
  const socket = await connectOrSpawn();
  const requestId = randomUUID();
  const decoder = new JsonLineDecoder();
  const values = new SupervisorStreamBuffer();
  let wake: (() => void) | undefined;
  let done = false;
  let failure: unknown;
  socket.setEncoding("utf8");
  socket.on("data", (chunk: string) => {
    try {
      for (const value of decoder.push(chunk)) {
        if (isRecord(value) && value.type === "stream" && value.requestId === requestId) {
          if (!values.push(value.item as SupervisorStreamItem)) {
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
    } catch (cause) {
      failure = cause;
      done = true;
      socket.destroy();
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
    message:
      cause instanceof Error
        ? cause.message
        : `Native Pi supervisor request failed: ${String(cause)}`,
  });

const decodeManagedOperation = (
  value: unknown,
): Exclude<
  ManagedClaimResponse,
  { readonly status: "granted" } | { readonly status: "conflict" }
> => {
  if (!isRecord(value) || typeof value.status !== "string")
    throw new Error("invalid managed admission operation response");
  if (value.status === "absent" || value.status === "delivering") return { status: value.status };
  if (
    value.status === "completed" &&
    isRecord(value.receipt) &&
    typeof value.receipt.turnId === "string" &&
    value.receipt.turnId.length > 0
  )
    return { status: "completed", receipt: { turnId: value.receipt.turnId } };
  if (
    (value.status === "rejected" || value.status === "indeterminate") &&
    typeof value.error === "string" &&
    value.error.length > 0
  )
    return { status: value.status, error: value.error };
  throw new Error("invalid managed admission operation response");
};

const decodeManagedClaimResponse = (value: unknown, operationKey: string): ManagedClaimResponse => {
  if (!isRecord(value) || typeof value.status !== "string")
    throw new Error("invalid managed admission claim response");
  if (value.status === "granted") {
    if (
      value.operationKey !== operationKey ||
      typeof value.leaseToken !== "string" ||
      value.leaseToken.length === 0
    )
      throw new Error("invalid managed admission grant");
    return { status: "granted", operationKey, leaseToken: value.leaseToken };
  }
  if (value.status === "conflict") {
    if (typeof value.error !== "string" || value.error.length === 0)
      throw new Error("invalid managed admission conflict response");
    return { status: "conflict", error: value.error };
  }
  return decodeManagedOperation(value);
};

const decodeManagedFinalizeResponse = (value: unknown): ManagedFinalizeResponse => {
  if (!isRecord(value) || (value.status !== "finalized" && value.status !== "staleLease"))
    throw new Error("invalid managed admission finalization response");
  const operation = decodeManagedOperation(value.operation);
  if (value.status === "finalized") {
    if (operation.status === "absent")
      throw new Error("invalid managed admission finalized operation");
    return { status: "finalized", operation };
  }
  return { status: "staleLease", operation };
};

export type SupervisorManagedClaim =
  | Exclude<ManagedClaimResponse, { readonly status: "granted" }>
  | {
      readonly status: "granted";
      readonly operationKey: string;
      readonly finalize: (
        finalization: ManagedFinalization,
      ) => Effect.Effect<ManagedFinalizeResponse, PiNativeError>;
    };

export class SupervisorClient extends Context.Service<
  SupervisorClient,
  {
    readonly list: () => Effect.Effect<ReadonlyArray<SupervisorRuntimeState>, PiNativeError>;
    readonly dispatch: (
      command: SupervisorCommand | Record<string, unknown>,
    ) => Effect.Effect<SupervisorCommandReceipt, PiNativeError>;
    readonly claimManaged: (
      claim: ManagedClaimRequest,
    ) => Effect.Effect<SupervisorManagedClaim, PiNativeError, Scope.Scope>;
    readonly subscribe: (
      runtimeId: string,
      cursor?: number,
    ) => Stream.Stream<SupervisorStreamItem, PiNativeError>;
  }
>()("t3/piNative/SupervisorClient") {
  static readonly layer = Layer.sync(SupervisorClient, makeSupervisorClient);
}
export function makeSupervisorClient(): SupervisorClient["Service"] {
  return SupervisorClient.of({
    list: () =>
      Effect.tryPromise({
        try: () => request("list") as Promise<ReadonlyArray<SupervisorRuntimeState>>,
        catch: mapError,
      }),
    dispatch: (command) =>
      Effect.tryPromise({
        try: () => request("dispatch", { command }) as Promise<SupervisorCommandReceipt>,
        catch: mapError,
      }),
    claimManaged: (claim) =>
      Effect.acquireRelease(
        Effect.tryPromise({ try: openManagedConnection, catch: mapError }),
        (connection) => Effect.sync(connection.close),
      ).pipe(
        Effect.flatMap((connection) =>
          Effect.tryPromise({
            try: async (): Promise<SupervisorManagedClaim> => {
              const probe = await connection.request("list", {});
              if (
                !isRecord(probe.capabilities) ||
                probe.capabilities.managedAdmission !== MANAGED_ADMISSION_PROTOCOL
              )
                throw new Error(
                  "The running Pi supervisor predates managed admission. Restart it after external Pi sessions finish, then retry.",
                );
              const response = await connection.request("claimManaged", {
                claim,
              });
              const result = decodeManagedClaimResponse(response.result, claim.operationKey);
              if (result.status !== "granted") return result;
              return {
                status: "granted",
                operationKey: result.operationKey,
                finalize: (finalization) =>
                  Effect.tryPromise({
                    try: async () => {
                      const response = await connection.request("finalizeManaged", {
                        finalization: {
                          protocol: claim.protocol,
                          operationKey: result.operationKey,
                          leaseToken: result.leaseToken,
                          finalization,
                        },
                      });
                      return decodeManagedFinalizeResponse(response.result);
                    },
                    catch: mapError,
                  }),
              };
            },
            catch: mapError,
          }),
        ),
      ),
    subscribe: (runtimeId, cursor) =>
      Stream.fromAsyncIterable(subscribe(runtimeId, cursor), mapError),
  });
}
