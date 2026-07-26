import { parseMermaid, renderMermaidSVG } from "beautiful-mermaid";
import sanitizeHtml from "sanitize-html";

const MAX_SOURCE_LENGTH = 20_000;
const MAX_STATEMENTS = 200;
const MAX_CONNECTORS = 200;
const MAX_FLOWCHART_NODES = 150;
const MAX_FLOWCHART_EDGES = 100;
const MAX_CHART_SERIES = 10;
const MAX_CHART_DATA_POINTS = 200;
const MAX_CHART_CATEGORIES = 100;
const MAX_SVG_LENGTH = 2_000_000;

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

export function normalizeMermaidStatements(code: string): string {
  let normalized = "";
  let quoted = false;
  let escaped = false;
  let bracketDepth = 0;
  let atStatementStart = true;

  for (let index = 0; index < code.length; index += 1) {
    const character = code[index] ?? "";
    if (escaped) {
      normalized += character;
      escaped = false;
      atStatementStart = false;
      continue;
    }
    if (character === "\\" && quoted) {
      normalized += character;
      escaped = true;
      continue;
    }
    if (character === '"') {
      quoted = !quoted;
      normalized += character;
      atStatementStart = false;
      continue;
    }
    if (!quoted && atStatementStart && character === "%" && code[index + 1] === "%") {
      const lineEnd = code.indexOf("\n", index);
      if (lineEnd === -1) return normalized + code.slice(index);
      normalized += code.slice(index, lineEnd + 1);
      index = lineEnd;
      continue;
    }
    if (!quoted && "([{".includes(character)) bracketDepth += 1;
    if (!quoted && ")]}".includes(character)) bracketDepth = Math.max(0, bracketDepth - 1);
    if (character === ";" && !quoted && bracketDepth === 0) {
      normalized += "\n";
      atStatementStart = true;
      continue;
    }
    normalized += character;
    if (character === "\n") atStatementStart = true;
    else if (!/\s/.test(character)) atStatementStart = false;
  }

  return normalized;
}

function assertChartComplexity(code: string): void {
  if (!/^xychart(?:-beta)?\b/i.test(code.trimStart())) return;

  const series = [...code.matchAll(/(?:^|\n)\s*(?:bar|line)\s*\[([^\]]*)\]/gi)];
  const dataPointCount = series.reduce(
    (total, match) => total + (match[1]?.split(",").filter((value) => value.trim()).length ?? 0),
    0,
  );
  const categories = /(?:^|\n)\s*x-axis(?:\s+"[^"]*")?\s*\[([^\]]*)\]/i.exec(code)?.[1];
  const categoryCount = categories?.split(",").filter((value) => value.trim()).length ?? 0;

  if (series.length > MAX_CHART_SERIES || dataPointCount > MAX_CHART_DATA_POINTS) {
    throw new Error("Mermaid chart has too many data points");
  }
  if (categoryCount > MAX_CHART_CATEGORIES) {
    throw new Error("Mermaid chart has too many categories");
  }
}

function assertParallelEdgeExpansion(code: string): void {
  const arrow = /<?(?:-->|-\.->|==>|---|-\.-|===)(?:\|[^|]*\|)?/g;
  let estimatedEdges = 0;
  for (const line of code.split("\n")) {
    const groups = line.split(arrow);
    if (groups.length < 2) continue;
    for (let index = 1; index < groups.length; index += 1) {
      const sourceCount = groups[index - 1]?.split("&").length ?? 1;
      const targetCount = groups[index]?.split("&").length ?? 1;
      estimatedEdges += sourceCount * targetCount;
      if (estimatedEdges > MAX_FLOWCHART_EDGES) {
        throw new Error("Mermaid diagram expands to too many edges");
      }
    }
  }
}

function assertFlowchartComplexity(code: string): void {
  if (!/^(?:graph|flowchart)\s+(?:TD|TB|LR|BT|RL)\s*$|^stateDiagram(?:-v2)?\s*$/im.test(code)) {
    return;
  }
  assertParallelEdgeExpansion(code);
  const graph = parseMermaid(code);
  if (graph.nodes.size > MAX_FLOWCHART_NODES || graph.edges.length > MAX_FLOWCHART_EDGES) {
    throw new Error("Mermaid diagram expands to too many nodes or edges");
  }
}

export function assertMermaidComplexity(code: string): void {
  if (code.length > MAX_SOURCE_LENGTH) throw new Error("Mermaid diagram source is too large");
  const statements = code
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("%%"));
  if (statements.length > MAX_STATEMENTS)
    throw new Error("Mermaid diagram has too many statements");
  if ((code.match(/--|->|<-|==/g)?.length ?? 0) > MAX_CONNECTORS) {
    throw new Error("Mermaid diagram has too many connectors");
  }
  assertChartComplexity(code);
  assertFlowchartComplexity(code);
}

function assertSafePaintValues(svg: string): void {
  const safePaint =
    /^(?:none|transparent|currentColor|var\(--[\w-]+\)|#[\da-f]{3,8}|(?:rgb|rgba|hsl|hsla)\([\d.,%+\-\s]+\)|[a-z]+)$/i;
  for (const match of svg.matchAll(/\s(?:fill|stroke)="([^"]*)"/gi)) {
    if (!safePaint.test(match[1]?.trim() ?? "")) {
      throw new Error("Mermaid diagram contains an unsafe paint value");
    }
  }
  for (const match of svg.matchAll(/url\(\s*(['"]?)(.*?)\1\s*\)/gi)) {
    if (!match[2]?.trim().startsWith("#")) {
      throw new Error("Mermaid diagram contains an external resource reference");
    }
  }
}

export function sanitizeMermaidSvg(svg: string): string {
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
    parser: { lowerCaseAttributeNames: false, lowerCaseTags: false },
  });
  if (!sanitized.startsWith("<svg ")) throw new Error("Mermaid renderer did not return a safe SVG");
  return sanitized;
}

export function renderSafeMermaidSvg(code: string): string {
  const normalized = normalizeMermaidStatements(code);
  assertMermaidComplexity(normalized);
  const rendered = renderMermaidSVG(normalized, {
    bg: "color-mix(in srgb, var(--muted) 78%, var(--background))",
    fg: "var(--foreground)",
    line: "var(--muted-foreground)",
    accent: "var(--foreground)",
    border: "var(--border)",
    font: "DM Sans Variable",
    transparent: true,
    interactive: false,
  });
  return sanitizeMermaidSvg(rendered);
}
