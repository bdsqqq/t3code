import MermaidRendererWorker from "./mermaid-renderer.worker.ts?worker";
import { isMermaidRenderResponse, type MermaidRenderRequest } from "./mermaid-renderer-protocol";

const DEFAULT_TIMEOUT_MS = 3_000;

interface WorkerLike {
  addEventListener(type: "error", listener: (event: ErrorEvent) => void): void;
  addEventListener(type: "message", listener: (event: MessageEvent<unknown>) => void): void;
  addEventListener(type: "messageerror", listener: (event: MessageEvent<unknown>) => void): void;
  postMessage(message: MermaidRenderRequest): void;
  terminate(): void;
}

interface PendingRender {
  readonly code: string;
  readonly namespace: string;
  readonly requestId: number;
  readonly enqueuedAt: number;
  readonly resolve: (svg: string) => void;
  readonly reject: (cause: Error) => void;
  readonly signal?: AbortSignal;
  abortListener?: () => void;
  startedAt?: number;
  settled: boolean;
  timeout?: ReturnType<typeof setTimeout>;
}

function abortError(): Error {
  const error = new Error("Mermaid rendering was aborted");
  error.name = "AbortError";
  return error;
}

function measure(name: string, start: number, detail: Record<string, unknown>): void {
  if (typeof performance === "undefined" || typeof performance.measure !== "function") return;
  performance.clearMeasures(name);
  performance.measure(name, { start, end: performance.now(), detail });
}

export function createMermaidRendererClient(
  workerFactory: () => WorkerLike,
  timeoutMs = DEFAULT_TIMEOUT_MS,
) {
  let worker: WorkerLike | null = null;
  let active: PendingRender | null = null;
  let disabled = false;
  let generation = 0;
  let nextRequestId = 1;
  const queue: PendingRender[] = [];

  function settle(pending: PendingRender, result: { svg: string } | { error: Error }): void {
    if (pending.settled) return;
    pending.settled = true;
    pending.signal?.removeEventListener("abort", pending.abortListener ?? (() => undefined));
    if ("svg" in result) pending.resolve(result.svg);
    else pending.reject(result.error);
  }

  function terminateWorker(): void {
    worker?.terminate();
    worker = null;
    generation += 1;
  }

  function failActive(cause: Error, restart: boolean): void {
    const pending = active;
    if (!pending) return;
    if (pending.timeout) clearTimeout(pending.timeout);
    active = null;
    settle(pending, { error: cause });
    if (restart) terminateWorker();
    dispatch();
  }

  function handleWorkerFailure(cause: Error): void {
    if (active) {
      failActive(cause, true);
      return;
    }
    terminateWorker();
    dispatch();
  }

  function ensureWorker(): WorkerLike | null {
    if (worker || disabled) return worker;
    try {
      const created = workerFactory();
      const workerGeneration = generation;
      created.addEventListener("message", (event) => {
        if (workerGeneration !== generation || !active) return;
        if (!isMermaidRenderResponse(event.data) || event.data.requestId !== active.requestId) {
          failActive(new Error("Mermaid worker returned an invalid response"), true);
          return;
        }

        const pending = active;
        if (pending.timeout) clearTimeout(pending.timeout);
        active = null;
        measure("t3.mermaid.worker.roundtrip", pending.startedAt ?? pending.enqueuedAt, {
          generation: workerGeneration,
          outcome: event.data.ok ? "success" : "render-error",
          renderDurationMs: event.data.renderDurationMs,
        });
        if (event.data.ok) {
          settle(pending, { svg: event.data.svg });
          dispatch();
        } else {
          settle(pending, { error: new Error(event.data.error) });
          terminateWorker();
          dispatch();
        }
      });
      created.addEventListener("error", () => {
        if (workerGeneration !== generation) return;
        handleWorkerFailure(new Error("Mermaid worker crashed"));
      });
      created.addEventListener("messageerror", () => {
        if (workerGeneration !== generation) return;
        handleWorkerFailure(new Error("Mermaid worker returned an unreadable response"));
      });
      worker = created;
      return worker;
    } catch {
      disabled = true;
      const cause = new Error("Mermaid worker is unavailable");
      if (active) {
        settle(active, { error: cause });
        active = null;
      }
      for (const pending of queue.splice(0)) settle(pending, { error: cause });
      return null;
    }
  }

  function dispatch(): void {
    if (active || disabled) return;
    while (queue.length > 0 && queue[0]?.settled) queue.shift();
    const pending = queue.shift();
    if (!pending) return;

    active = pending;
    const currentWorker = ensureWorker();
    if (!currentWorker || active !== pending) return;

    pending.startedAt = performance.now();
    measure("t3.mermaid.worker.queue", pending.enqueuedAt, {
      generation,
      outcome: "dispatched",
    });
    pending.timeout = setTimeout(() => {
      failActive(new Error("Mermaid rendering timed out"), true);
    }, timeoutMs);
    // Dedicated workers do not accept a targetOrigin argument.
    /* oxlint-disable unicorn/require-post-message-target-origin */
    try {
      currentWorker.postMessage({
        type: "render",
        requestId: pending.requestId,
        code: pending.code,
        namespace: pending.namespace,
      });
    } catch {
      failActive(new Error("Mermaid worker rejected the request"), true);
    }
    /* oxlint-enable unicorn/require-post-message-target-origin */
  }

  function render(code: string, namespace: string, signal?: AbortSignal): Promise<string> {
    if (signal?.aborted) return Promise.reject(abortError());
    if (disabled) return Promise.reject(new Error("Mermaid worker is unavailable"));

    return new Promise((resolve, reject) => {
      const pending: PendingRender = {
        code,
        namespace,
        requestId: nextRequestId,
        enqueuedAt: performance.now(),
        resolve,
        reject,
        ...(signal ? { signal } : {}),
        settled: false,
      };
      nextRequestId += 1;
      pending.abortListener = () => {
        settle(pending, { error: abortError() });
      };
      signal?.addEventListener("abort", pending.abortListener, { once: true });
      queue.push(pending);
      dispatch();
    });
  }

  function dispose(): void {
    const cause = new Error("Mermaid renderer was disposed");
    if (active) {
      if (active.timeout) clearTimeout(active.timeout);
      settle(active, { error: cause });
      active = null;
    }
    for (const pending of queue.splice(0)) settle(pending, { error: cause });
    disabled = true;
    terminateWorker();
  }

  return { dispose, render };
}

const client = createMermaidRendererClient(() => new MermaidRendererWorker());

export function renderMermaidInWorker(
  code: string,
  namespace: string,
  signal?: AbortSignal,
): Promise<string> {
  return client.render(code, namespace, signal);
}

if (import.meta.hot) {
  import.meta.hot.dispose(() => client.dispose());
}
