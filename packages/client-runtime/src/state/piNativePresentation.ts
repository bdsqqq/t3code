export interface PiNativePresentation {
  readonly label: string;
  readonly text: string | null;
}

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringAt(
  value: Readonly<Record<string, unknown>>,
  keys: ReadonlyArray<string>,
): string | null {
  for (const key of keys) {
    if (typeof value[key] === "string") return value[key];
  }
  return null;
}

function contentText(value: unknown): string | null {
  if (typeof value === "string") return value;
  if (!Array.isArray(value)) return null;
  const parts = value.flatMap((part) => {
    if (typeof part === "string") return [part];
    if (!isRecord(part)) return [];
    const text = stringAt(part, ["text", "delta", "content"]);
    return text === null ? [] : [text];
  });
  return parts.length === 0 ? null : parts.join("");
}

export function presentPiNativeUnknown(value: unknown): PiNativePresentation {
  if (!isRecord(value)) return { label: "event", text: typeof value === "string" ? value : null };
  const nested = value.event ?? value.message ?? value.assistantMessageEvent;
  const type = stringAt(value, ["type", "eventType", "kind"]);
  const role = stringAt(value, ["role"]);
  const direct = stringAt(value, ["text_delta", "delta", "text"]);
  const content = contentText(value.content);
  if (direct !== null || content !== null) {
    return { label: role ?? type ?? "message", text: direct ?? content };
  }
  if (type?.includes("tool") || value.toolName !== undefined || value.tool !== undefined) {
    const name = stringAt(value, ["toolName", "tool", "name"]);
    const detail = value.result ?? value.output ?? value.input ?? value.arguments;
    return {
      label: name === null ? (type ?? "tool") : `tool · ${name}`,
      text:
        typeof detail === "string"
          ? detail
          : detail === undefined
            ? null
            : JSON.stringify(detail, null, 2),
    };
  }
  if (nested !== undefined && nested !== value) {
    const presented = presentPiNativeUnknown(nested);
    if (presented.text !== null) return presented;
  }
  return { label: role ?? type ?? "event", text: null };
}
