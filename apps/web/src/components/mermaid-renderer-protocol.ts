export interface MermaidRenderRequest {
  readonly type: "render";
  readonly requestId: number;
  readonly code: string;
  readonly namespace: string;
}

export type MermaidRenderResponse =
  | {
      readonly type: "render-result";
      readonly requestId: number;
      readonly ok: true;
      readonly svg: string;
      readonly renderDurationMs: number;
    }
  | {
      readonly type: "render-result";
      readonly requestId: number;
      readonly ok: false;
      readonly error: string;
      readonly renderDurationMs: number;
    };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export function isMermaidRenderRequest(value: unknown): value is MermaidRenderRequest {
  return (
    isRecord(value) &&
    value.type === "render" &&
    Number.isSafeInteger(value.requestId) &&
    typeof value.code === "string" &&
    typeof value.namespace === "string"
  );
}

export function isMermaidRenderResponse(value: unknown): value is MermaidRenderResponse {
  if (
    !isRecord(value) ||
    value.type !== "render-result" ||
    !Number.isSafeInteger(value.requestId) ||
    typeof value.ok !== "boolean" ||
    typeof value.renderDurationMs !== "number" ||
    !Number.isFinite(value.renderDurationMs)
  ) {
    return false;
  }
  return value.ok ? typeof value.svg === "string" : typeof value.error === "string";
}
