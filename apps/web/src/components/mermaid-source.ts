const MAX_SOURCE_LENGTH = 20_000;
const MAX_STATEMENTS = 100;
const MAX_CHART_SERIES = 10;
const MAX_CHART_DATA_POINTS = 200;
const MAX_CHART_CATEGORIES = 100;

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

function assertChartSize(code: string): void {
  if (!/^xychart(?:-beta)?\b/i.test(code.trimStart())) return;

  const series = [...code.matchAll(/(?:^|\n)\s*(?:bar|line)\s*\[([^\]]*)\]/gi)];
  const dataPoints = series.reduce(
    (total, match) => total + (match[1]?.split(",").filter((value) => value.trim()).length ?? 0),
    0,
  );
  const categories = /(?:^|\n)\s*x-axis(?:\s+"[^"]*")?\s*\[([^\]]*)\]/i.exec(code)?.[1];
  const categoryCount = categories?.split(",").filter((value) => value.trim()).length ?? 0;

  if (series.length > MAX_CHART_SERIES || dataPoints > MAX_CHART_DATA_POINTS) {
    throw new Error("Mermaid chart has too many data points");
  }
  if (categoryCount > MAX_CHART_CATEGORIES) {
    throw new Error("Mermaid chart has too many categories");
  }
}

export function prepareMermaidSource(code: string): string {
  const normalized = normalizeMermaidStatements(code);
  if (normalized.length > MAX_SOURCE_LENGTH) throw new Error("Mermaid diagram source is too large");

  const statements = normalized
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("%%"));
  if (statements.length > MAX_STATEMENTS) {
    throw new Error("Mermaid diagram has too many statements");
  }

  assertChartSize(normalized);
  return normalized;
}
