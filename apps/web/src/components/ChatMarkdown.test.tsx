import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import { MermaidDiagram } from "./ChatMarkdown";

describe("MermaidDiagram", () => {
  it("renders Mermaid source as a themed SVG", () => {
    const source = "graph LR\n  A[Start] --> B[Finish]";
    const html = renderToStaticMarkup(<MermaidDiagram code={source} />);

    expect(html).toContain('class="chat-markdown-mermaid"');
    expect(html).toContain('aria-label="Mermaid diagram"');
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

  it("falls back to the source when the diagram is unsupported", () => {
    const html = renderToStaticMarkup(<MermaidDiagram code="not a diagram" />);

    expect(html).toContain("<pre>");
    expect(html).toContain('class="language-mermaid"');
    expect(html).toContain("not a diagram");
  });
});
