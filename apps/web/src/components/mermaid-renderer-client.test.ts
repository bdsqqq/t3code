import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { createMermaidRendererClient } from "./mermaid-renderer-client";
import type { MermaidRenderRequest, MermaidRenderResponse } from "./mermaid-renderer-protocol";

class FakeWorker {
  static instances: FakeWorker[] = [];

  private errorListener: ((event: ErrorEvent) => void) | null = null;
  private messageListener: ((event: MessageEvent<unknown>) => void) | null = null;
  private messageErrorListener: ((event: MessageEvent<unknown>) => void) | null = null;
  readonly messages: MermaidRenderRequest[] = [];
  terminated = false;

  constructor() {
    FakeWorker.instances.push(this);
  }

  postMessage(message: MermaidRenderRequest): void {
    this.messages.push(message);
  }

  addEventListener(
    type: "error" | "message" | "messageerror",
    listener: ((event: ErrorEvent) => void) | ((event: MessageEvent<unknown>) => void),
  ): void {
    if (type === "error") this.errorListener = listener as (event: ErrorEvent) => void;
    else if (type === "message") {
      this.messageListener = listener as (event: MessageEvent<unknown>) => void;
    } else {
      this.messageErrorListener = listener as (event: MessageEvent<unknown>) => void;
    }
  }

  terminate(): void {
    this.terminated = true;
  }

  respond(response: MermaidRenderResponse): void {
    this.messageListener?.({ data: response } as MessageEvent<unknown>);
  }

  emitError(): void {
    this.errorListener?.({ type: "error" } as ErrorEvent);
  }

  emitMessageError(): void {
    this.messageErrorListener?.({ data: null } as MessageEvent<unknown>);
  }

  emitMessage(data: unknown): void {
    this.messageListener?.({ data } as MessageEvent<unknown>);
  }
}

function success(request: MermaidRenderRequest, svg = "<svg />"): MermaidRenderResponse {
  return {
    type: "render-result",
    requestId: request.requestId,
    ok: true,
    svg,
    renderDurationMs: 4,
  };
}

afterEach(() => {
  vi.useRealTimers();
  FakeWorker.instances = [];
});

describe("createMermaidRendererClient", () => {
  it("serializes requests and reuses one worker", async () => {
    const client = createMermaidRendererClient(() => new FakeWorker());
    const first = client.render("graph LR\nA --> B", "first");
    const second = client.render("graph LR\nB --> C", "second");
    const worker = FakeWorker.instances[0]!;

    expect(worker.messages).toHaveLength(1);
    worker.respond(success(worker.messages[0]!, "<svg id='first' />"));
    await expect(first).resolves.toContain("first");
    expect(worker.messages).toHaveLength(2);
    worker.respond(success(worker.messages[1]!, "<svg id='second' />"));
    await expect(second).resolves.toContain("second");
    expect(FakeWorker.instances).toHaveLength(1);
  });

  it("removes aborted queued work without disrupting the active render", async () => {
    const client = createMermaidRendererClient(() => new FakeWorker());
    const first = client.render("graph LR\nA --> B", "first");
    const controller = new AbortController();
    const aborted = client.render("graph LR\nB --> C", "aborted", controller.signal);
    const worker = FakeWorker.instances[0]!;

    controller.abort();
    await expect(aborted).rejects.toMatchObject({ name: "AbortError" });
    worker.respond(success(worker.messages[0]!));
    await expect(first).resolves.toBe("<svg />");
    expect(worker.messages).toHaveLength(1);
    expect(worker.terminated).toBe(false);
  });

  it("lets aborted active work finish before dispatching the queue", async () => {
    const client = createMermaidRendererClient(() => new FakeWorker());
    const controller = new AbortController();
    const aborted = client.render("graph LR\nA --> B", "aborted", controller.signal);
    const next = client.render("graph LR\nB --> C", "next");
    const worker = FakeWorker.instances[0]!;
    const abortedAssertion = expect(aborted).rejects.toMatchObject({ name: "AbortError" });

    controller.abort();
    await abortedAssertion;
    expect(worker.messages).toHaveLength(1);
    expect(worker.terminated).toBe(false);

    worker.respond(success(worker.messages[0]!));
    expect(worker.messages).toHaveLength(2);
    worker.respond(success(worker.messages[1]!));
    await expect(next).resolves.toBe("<svg />");
    expect(FakeWorker.instances).toHaveLength(1);
  });

  it("restarts after malformed and renderer-error responses", async () => {
    const client = createMermaidRendererClient(() => new FakeWorker());
    const malformed = client.render("graph LR\nA --> B", "malformed");
    const next = client.render("graph LR\nB --> C", "next");
    const firstWorker = FakeWorker.instances[0]!;

    firstWorker.emitMessage({ requestId: 999 });
    await expect(malformed).rejects.toThrow("invalid response");
    expect(firstWorker.terminated).toBe(true);

    const secondWorker = FakeWorker.instances[1]!;
    secondWorker.respond({
      type: "render-result",
      requestId: secondWorker.messages[0]!.requestId,
      ok: false,
      error: "unsafe diagram",
      renderDurationMs: 2,
    });
    await expect(next).rejects.toThrow("unsafe diagram");
    expect(secondWorker.terminated).toBe(true);
  });

  it.each(["error", "messageerror"] as const)("restarts after worker %s events", async (event) => {
    const client = createMermaidRendererClient(() => new FakeWorker());
    const failed = client.render("graph LR\nA --> B", event);
    const worker = FakeWorker.instances[0]!;

    if (event === "error") worker.emitError();
    else worker.emitMessageError();

    await expect(failed).rejects.toThrow(/crashed|unreadable/);
    expect(worker.terminated).toBe(true);
  });

  it("terminates timed-out work and uses a fresh worker for the queue", async () => {
    vi.useFakeTimers();
    const client = createMermaidRendererClient(() => new FakeWorker(), 3_000);
    const timedOut = client.render("graph LR\nA --> B", "timeout");
    const next = client.render("graph LR\nB --> C", "next");
    const firstWorker = FakeWorker.instances[0]!;
    const timedOutAssertion = expect(timedOut).rejects.toThrow("timed out");

    await vi.advanceTimersByTimeAsync(3_000);
    await timedOutAssertion;
    expect(firstWorker.terminated).toBe(true);

    const secondWorker = FakeWorker.instances[1]!;
    firstWorker.emitError();
    expect(secondWorker.terminated).toBe(false);
    secondWorker.respond(success(secondWorker.messages[0]!));
    await expect(next).resolves.toBe("<svg />");
  });

  it("disables the client after worker construction fails", async () => {
    const factory = vi.fn(() => {
      throw new Error("unsupported");
    });
    const client = createMermaidRendererClient(factory);

    await expect(client.render("graph LR\nA --> B", "first")).rejects.toThrow("unavailable");
    await expect(client.render("graph LR\nB --> C", "second")).rejects.toThrow("unavailable");
    expect(factory).toHaveBeenCalledTimes(1);
  });

  it("terminates and rejects active and queued work when disposed", async () => {
    const client = createMermaidRendererClient(() => new FakeWorker());
    const active = client.render("graph LR\nA --> B", "active");
    const queued = client.render("graph LR\nB --> C", "queued");
    const worker = FakeWorker.instances[0]!;

    client.dispose();

    await expect(active).rejects.toThrow("disposed");
    await expect(queued).rejects.toThrow("disposed");
    expect(worker.terminated).toBe(true);
  });
});
