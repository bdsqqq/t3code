import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import ChatMarkdown, { MermaidDiagram } from "./ChatMarkdown";

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

  it("rejects diagrams with external SVG resource references", () => {
    const source = "graph LR\n A --> B\n style A fill:url(https://example.com/pixel.svg#x)";
    const html = renderToStaticMarkup(<MermaidDiagram code={source} />);

    expect(html).toContain("<pre>");
    expect(html).not.toContain("<svg");
    expect(html).not.toContain('fill="url(https://example.com');
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
