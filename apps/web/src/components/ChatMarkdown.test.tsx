import { renderToStaticMarkup } from "react-dom/server";
import { act, create, type ReactTestRenderer } from "react-test-renderer";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import ChatMarkdown, { MermaidDiagram } from "./ChatMarkdown";
import { renderSafeMermaidSvg } from "./mermaid-renderer";

describe("MermaidDiagram", () => {
  it("renders Mermaid source as a themed SVG", () => {
    const source = "graph LR\n  A[Start] --> B[Finish]";
    const html = renderToStaticMarkup(<MermaidDiagram code={source} />);

    expect(html).toContain('class="chat-markdown-mermaid"');
    expect(html).toContain('aria-label="Mermaid diagram"');
    expect(html).toContain('aria-describedby="mermaid-');
    expect(html).toContain("Mermaid diagram source:");
    expect(html).toContain("<svg");
    expect(html).toContain("var(--foreground)");
    expect(html).toContain('aria-hidden="true"');
    expect(html).not.toContain("fonts.googleapis.com");
    expect(html).toContain("```mermaid");
    expect(html).not.toContain("<pre>");
  });

  it("supports Mermaid's single-line semicolon syntax", () => {
    const html = renderToStaticMarkup(<MermaidDiagram code="graph LR; A --> B; B --> C" />);

    expect(html).toContain("<svg");
    expect(html).toContain('data-id="C"');
  });

  it("does not turn semicolons in comments into live statements", () => {
    const html = renderToStaticMarkup(
      <MermaidDiagram code={"graph LR; A --> B; %% hidden; C --> D"} />,
    );

    expect(html).toContain('data-id="A"');
    expect(html).not.toContain('data-id="C"');
    expect(html).not.toContain('data-id="D"');
  });

  it("preserves percent pairs inside node labels", () => {
    const html = renderToStaticMarkup(
      <MermaidDiagram code={"graph LR; A[100%% coverage]; B --> C"} />,
    );

    expect(html).toContain('data-label="100%% coverage"');
    expect(html).toContain('data-id="C"');
  });

  it("rejects diagrams with external SVG resource references", () => {
    const source = "graph LR\n A --> B\n style A fill:url(https://example.com/pixel.svg#x)";
    const html = renderToStaticMarkup(<MermaidDiagram code={source} />);

    expect(html).toContain("<pre>");
    expect(html).not.toContain("<svg");
    expect(html).not.toContain('fill="url(https://example.com');
  });

  it("rejects CSS-escaped external SVG resource references", () => {
    const source = String.raw`graph LR
 A --> B
 style A fill:u\72l(http://127.0.0.1/pixel.svg#x)`;
    const html = renderToStaticMarkup(<MermaidDiagram code={source} />);

    expect(html).toContain("<pre>");
    expect(html).not.toContain("<svg");
  });

  it("shows source while a diagram is streaming", () => {
    const html = renderToStaticMarkup(
      <MermaidDiagram code="graph LR\n A --> B" isStreaming={true} />,
    );

    expect(html).toContain("<pre>");
    expect(html).not.toContain("<svg");
  });

  it("bounds diagram complexity before rendering", () => {
    const source = ["graph TD", ...Array.from({ length: 201 }, (_, index) => `N${index}`)].join(
      "\n",
    );
    const html = renderToStaticMarkup(<MermaidDiagram code={source} />);

    expect(html).toContain("<pre>");
    expect(html).not.toContain("<svg");
  });

  it("rejects Cartesian edge expansion before layout", () => {
    const left = Array.from({ length: 15 }, (_, index) => `A${index}`).join(" & ");
    const right = Array.from({ length: 15 }, (_, index) => `B${index}`).join(" & ");
    const html = renderToStaticMarkup(<MermaidDiagram code={`graph LR\n${left} --> ${right}`} />);

    expect(html).toContain("<pre>");
    expect(html).not.toContain("<svg");
  });

  it("rejects oversized XY chart datasets before rendering", () => {
    const points = Array.from({ length: 201 }, (_, index) => index).join(",");
    const html = renderToStaticMarkup(<MermaidDiagram code={`xychart-beta\nbar [${points}]`} />);

    expect(html).toContain("<pre>");
    expect(html).not.toContain("<svg");
  });

  it("preserves cylinder geometry through SVG sanitization", () => {
    const svg = renderSafeMermaidSvg("graph LR\n A[(Database)]");

    expect(svg.match(/<ellipse/g)).toHaveLength(2);
  });

  it("namespaces defs independently without changing diagram data IDs", () => {
    const html = renderToStaticMarkup(
      <>
        <MermaidDiagram code={"graph LR\n A --> B"} />
        <MermaidDiagram code={"graph LR\n A --> B"} />
      </>,
    );
    const markerIds = [...html.matchAll(/<marker[^>]+id="([^"]+-arrowhead)"/g)].map(
      (match) => match[1],
    );

    expect(html).toContain("<marker");
    expect(new Set(markerIds).size).toBe(2);
    expect(html.match(/data-id="A"/g)).toHaveLength(2);
  });

  it("routes Mermaid fences through the full markdown renderer", () => {
    const html = renderToStaticMarkup(
      <ChatMarkdown text={"```mermaid\ngraph LR\n A --> B\n```"} cwd={undefined} />,
    );

    expect(html).toContain('class="chat-markdown-mermaid"');
    expect(html).toContain("<svg");
  });

  it("falls back to the source when the diagram is unsupported", () => {
    const html = renderToStaticMarkup(<MermaidDiagram code="not a diagram" />);

    expect(html).toContain("<pre>");
    expect(html).toContain('class="language-mermaid"');
    expect(html).toContain("not a diagram");
  });
});

interface MermaidWorkerResponse {
  readonly svg?: string;
  readonly error?: string;
}

class FakeWorker {
  static readonly instances: FakeWorker[] = [];
  readonly listeners = new Map<
    string,
    Array<(event: MessageEvent<MermaidWorkerResponse>) => void>
  >();
  postedMessage: unknown;
  terminated = false;

  constructor() {
    FakeWorker.instances.push(this);
  }

  addEventListener(type: string, listener: (event: MessageEvent<MermaidWorkerResponse>) => void) {
    const listeners = this.listeners.get(type) ?? [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }

  postMessage(message: unknown) {
    this.postedMessage = message;
  }

  terminate() {
    this.terminated = true;
  }

  emit(type: "message" | "error", data: MermaidWorkerResponse = {}) {
    for (const listener of this.listeners.get(type) ?? []) {
      listener({ data } as MessageEvent<MermaidWorkerResponse>);
    }
  }
}

const originalWorker = globalThis.Worker;
const reactActEnvironment = globalThis as typeof globalThis & {
  IS_REACT_ACT_ENVIRONMENT?: boolean;
};

afterEach(() => {
  vi.useRealTimers();
  FakeWorker.instances.length = 0;
  globalThis.Worker = originalWorker;
  reactActEnvironment.IS_REACT_ACT_ENVIRONMENT = false;
});

describe("MermaidDiagram worker lifecycle", () => {
  it("posts source and renders a successful worker response", async () => {
    reactActEnvironment.IS_REACT_ACT_ENVIRONMENT = true;
    globalThis.Worker = FakeWorker as unknown as typeof Worker;
    const code = "graph LR\n WorkerA --> WorkerB";
    let renderer: ReactTestRenderer | undefined;

    await act(async () => {
      renderer = create(<MermaidDiagram code={code} />);
    });
    const worker = FakeWorker.instances[0];
    expect(worker?.postedMessage).toEqual({ code });

    await act(async () => {
      worker?.emit("message", { svg: renderSafeMermaidSvg(code) });
    });
    expect(renderer?.root.findByProps({ className: "chat-markdown-mermaid" })).toBeDefined();
    expect(worker?.terminated).toBe(true);

    await act(async () => renderer?.unmount());
  });

  it("falls back and terminates after a worker error", async () => {
    reactActEnvironment.IS_REACT_ACT_ENVIRONMENT = true;
    globalThis.Worker = FakeWorker as unknown as typeof Worker;
    let renderer: ReactTestRenderer | undefined;

    await act(async () => {
      renderer = create(<MermaidDiagram code={"graph LR\n ErrorA --> ErrorB"} />);
    });
    const worker = FakeWorker.instances[0];
    await act(async () => worker?.emit("error"));

    expect(renderer?.root.findByType("pre")).toBeDefined();
    expect(worker?.terminated).toBe(true);
    await act(async () => renderer?.unmount());
  });

  it("times out stalled workers and terminates workers on cleanup", async () => {
    vi.useFakeTimers();
    reactActEnvironment.IS_REACT_ACT_ENVIRONMENT = true;
    globalThis.Worker = FakeWorker as unknown as typeof Worker;
    let renderer: ReactTestRenderer | undefined;

    await act(async () => {
      renderer = create(<MermaidDiagram code={"graph LR\n TimeoutA --> TimeoutB"} />);
    });
    const timedOutWorker = FakeWorker.instances[0];
    await act(async () => {
      vi.advanceTimersByTime(3_000);
    });
    expect(timedOutWorker?.terminated).toBe(true);
    expect(renderer?.root.findByType("pre")).toBeDefined();

    await act(async () => {
      renderer?.update(<MermaidDiagram code={"graph LR\n CleanupA --> CleanupB"} />);
    });
    const cleanupWorker = FakeWorker.instances[1];
    await act(async () => renderer?.unmount());
    expect(cleanupWorker?.terminated).toBe(true);
  });
});
