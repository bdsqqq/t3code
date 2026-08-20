import { parseMermaid, renderMermaidSVG } from "beautiful-mermaid";
import { decodeXML } from "entities";
import sanitizeHtml from "sanitize-html";

import { prepareMermaidSource } from "./mermaid-source";

const MAX_SVG_LENGTH = 2_000_000;
const MAX_FLOWCHART_NODES = 150;
const MAX_FLOWCHART_EDGES = 100;
const MERMAID_THEME = {
  bg: "var(--background)",
  fg: "var(--foreground)",
  line: "var(--muted-foreground)",
  accent: "var(--primary)",
  muted: "var(--muted-foreground)",
  surface: "var(--card)",
  // beautiful-mermaid names its own variable --border, so referencing T3's
  // --border here would create a self-reference on the generated SVG.
  border: "var(--input)",
} as const;
const SVG_TAGS = [
  "svg",
  "style",
  "defs",
  "marker",
  "polygon",
  "polyline",
  "rect",
  "ellipse",
  "text",
  "tspan",
  "g",
  "circle",
  "line",
  "path",
  "title",
];
const SVG_ATTRIBUTES = [
  "xmlns",
  "viewBox",
  "width",
  "height",
  "style",
  "aria-hidden",
  "focusable",
  "id",
  "class",
  "data-*",
  "markerWidth",
  "markerHeight",
  "refX",
  "refY",
  "orient",
  "marker-start",
  "marker-end",
  "points",
  "fill",
  "fill-opacity",
  "stroke",
  "stroke-width",
  "stroke-linejoin",
  "stroke-linecap",
  "stroke-dasharray",
  "opacity",
  "x",
  "y",
  "x1",
  "y1",
  "x2",
  "y2",
  "cx",
  "cy",
  "r",
  "rx",
  "ry",
  "d",
  "dy",
  "transform",
  "text-anchor",
  "font-size",
  "font-weight",
  "font-style",
  "text-decoration",
];
const SAFE_PAINT =
  /^(?:none|transparent|currentColor|var\(--[\w-]+\)|#[\da-f]{3,8}|(?:rgb|rgba|hsl|hsla)\([\d.,%+\-\s]+\)|[a-z]+)$/i;

function assertSafePaintValues(svg: string): void {
  for (const match of svg.matchAll(/\s(?:fill|stroke)="([^"]*)"/gi)) {
    if (!SAFE_PAINT.test(match[1]?.trim() ?? "")) {
      throw new Error("Mermaid diagram contains an unsafe paint value");
    }
  }
  for (const match of svg.matchAll(/url\(\s*(['"]?)(.*?)\1\s*\)/gi)) {
    if (!match[2]?.trim().startsWith("#")) {
      throw new Error("Mermaid diagram contains an external resource reference");
    }
  }
}

function namespaceSvgIds(svg: string, namespace: string): string {
  const ids = [...svg.matchAll(/\sid="([^"]+)"/g)].flatMap((match) => (match[1] ? [match[1]] : []));
  return ids.reduce((result, id) => {
    const namespacedId = `${namespace}-${id}`;
    return result
      .replaceAll(` id="${id}"`, ` id="${namespacedId}"`)
      .replaceAll(`url(#${id})`, `url(#${namespacedId})`);
  }, svg);
}

function sanitizeSvg(svg: string): string {
  if (svg.length > MAX_SVG_LENGTH) throw new Error("Rendered Mermaid diagram is too large");
  const withoutRemoteFonts = svg
    .replace(/\s*@import url\('[^']+'\);/g, "")
    .replace("<svg ", '<svg aria-hidden="true" focusable="false" ');
  assertSafePaintValues(withoutRemoteFonts);
  const sanitized = sanitizeHtml(withoutRemoteFonts, {
    allowedTags: SVG_TAGS,
    allowedAttributes: {
      "*": SVG_ATTRIBUTES.filter((attribute) => attribute !== "style"),
      svg: SVG_ATTRIBUTES,
    },
    allowedSchemes: [],
    allowProtocolRelative: false,
    // The renderer owns this static theme block; resource URLs are rejected above.
    allowVulnerableTags: true,
    parser: { lowerCaseAttributeNames: false, lowerCaseTags: false },
  });
  if (!sanitized.startsWith("<svg ")) throw new Error("Mermaid renderer did not return a safe SVG");
  return sanitized;
}

export function renderSafeMermaidSvg(code: string, namespace: string): string {
  if (!/^[a-zA-Z0-9_-]+$/.test(namespace)) throw new Error("Invalid Mermaid SVG namespace");
  const encodedSource = prepareMermaidSource(code);
  const decoded = decodeXML(code);
  const source = decoded === code ? encodedSource : prepareMermaidSource(decoded);
  const header =
    source
      .split("\n")
      .map((line) => line.trim())
      .find((line) => line.length > 0 && !line.startsWith("%%")) ?? "";
  if (/^(?:(?:graph|flowchart)\s+(?:TD|TB|LR|BT|RL)|stateDiagram(?:-v2)?)$/i.test(header)) {
    const graph = parseMermaid(source);
    if (graph.nodes.size > MAX_FLOWCHART_NODES) {
      throw new Error("Mermaid diagram has too many nodes");
    }
    if (graph.edges.length > MAX_FLOWCHART_EDGES) {
      throw new Error("Mermaid diagram has too many edges");
    }
  }

  // beautiful-mermaid decodes XML entities internally. Re-encode ampersands so the
  // source validated above is exactly the source its parser and renderer receive.
  const rendered = renderMermaidSVG(source.replaceAll("&", "&amp;"), {
    ...MERMAID_THEME,
    font: "var(--font-sans)",
    transparent: true,
    interactive: false,
  });
  return namespaceSvgIds(sanitizeSvg(rendered), namespace);
}
