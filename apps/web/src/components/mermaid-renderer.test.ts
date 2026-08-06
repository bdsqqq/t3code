import { describe, expect, it } from "vite-plus/test";

import { renderSafeMermaidSvg } from "./mermaid-renderer";

describe("renderSafeMermaidSvg", () => {
  it("renders themed, accessible SVG with isolated definition IDs", () => {
    const svg = renderSafeMermaidSvg("graph LR; A[Start] --> B[Finish]", "diagram-one");

    expect(svg).toContain("<svg");
    expect(svg).toContain('aria-hidden="true"');
    expect(svg).toContain(
      'style="--bg:var(--background);--fg:var(--foreground);--line:var(--muted-foreground);' +
        "--accent:var(--primary);--muted:var(--muted-foreground);--surface:var(--card);" +
        '--border:var(--input)"',
    );
    expect(svg).toContain('id="diagram-one-arrowhead"');
    expect(svg).toContain('data-id="A"');
    expect(svg).not.toContain("fonts.googleapis.com");
  });

  it.each([
    ["sequence", "sequenceDiagram\nAlice->>Bob: Hello"],
    ["class", "classDiagram\nAnimal <|-- Dog"],
    ["entity relationship", "erDiagram\nCUSTOMER ||--o{ ORDER : places"],
    ["chart", 'xychart-beta\ntitle "Sales"\nx-axis [Jan, Feb]\nbar [10, 20]'],
  ])("renders and sanitizes %s diagrams", (_type, source) => {
    const svg = renderSafeMermaidSvg(source, "supported-type");

    expect(svg).toMatch(/^<svg /);
    expect(svg).not.toMatch(/@import|fonts\.googleapis|<script|foreignObject|\son\w+=/i);
  });

  it("rejects literal and CSS-escaped external paint URLs", () => {
    expect(() =>
      renderSafeMermaidSvg(
        "graph LR\n A --> B\n style A fill:url(https://example.com/pixel.svg#x)",
        "literal-url",
      ),
    ).toThrow(/unsafe paint|external resource/);

    const escaped = String.raw`graph LR
 A --> B
 style A fill:u\72l(http://127.0.0.1/pixel.svg#x)`;
    expect(() => renderSafeMermaidSvg(escaped, "escaped-url")).toThrow("unsafe paint");
  });

  it("rejects oversized encoded source before entity decoding", () => {
    const encodedLabel = `graph LR\nA["${"&amp;".repeat(4_000)}"]`;

    expect(() => renderSafeMermaidSvg(encodedLabel, "oversized-encoded")).toThrow(
      "source is too large",
    );
  });

  it("applies limits after decoding XML entities", () => {
    const nodes = Array.from({ length: 151 }, (_, index) => `N${index}`).join(" &amp; ");
    const points = Array.from({ length: 201 }, (_, index) => index).join("&#44;");

    expect(() => renderSafeMermaidSvg(`graph LR\n${nodes}`, "encoded-nodes")).toThrow(
      "too many nodes",
    );
    expect(() => renderSafeMermaidSvg(`xychart-beta\nbar [${points}]`, "encoded-chart")).toThrow(
      "too many data points",
    );
    expect(renderSafeMermaidSvg('graph LR\nA["R &amp; D"]', "encoded-label")).toContain(
      "R &amp; D",
    );
  });

  it("rejects expensive Cartesian edge expansion before layout", () => {
    const left = Array.from({ length: 11 }, (_, index) => `A${index}`).join(" & ");
    const right = Array.from({ length: 11 }, (_, index) => `B${index}`).join(" & ");

    expect(() => renderSafeMermaidSvg(`graph LR\n${left} --> ${right}`, "too-many-edges")).toThrow(
      "too many edges",
    );
  });

  it("does not count ampersands inside labels as parallel nodes", () => {
    const edges = Array.from({ length: 51 }, (_, index) => `A${index}["R & D"] --> B${index}`).join(
      "\n",
    );

    expect(renderSafeMermaidSvg(`graph LR\n${edges}`, "quoted-ampersands")).toContain("<svg ");
  });

  it("rejects oversized disconnected flowcharts before layout", () => {
    const nodes = Array.from({ length: 151 }, (_, index) => `N${index}`).join(" & ");

    expect(() => renderSafeMermaidSvg(`graph LR\n${nodes}`, "too-many-nodes")).toThrow(
      "too many nodes",
    );
    expect(() =>
      renderSafeMermaidSvg(`\n%% comment\ngraph LR\n${nodes}`, "prefixed-too-many-nodes"),
    ).toThrow("too many nodes");
  });

  it("rejects unsafe SVG namespaces before interpolation", () => {
    expect(() => renderSafeMermaidSvg("graph LR\nA --> B", 'x" onload="alert(1)')).toThrow(
      "Invalid Mermaid SVG namespace",
    );
  });

  it("preserves cylinder geometry through sanitization", () => {
    const svg = renderSafeMermaidSvg("graph LR\n A[(Database)]", "cylinder");

    expect(svg.match(/<ellipse/g)).toHaveLength(2);
  });
});
