import { RuntimeTaskId, type RuntimeTaskStatus, type RuntimeTaskUsage } from "@t3tools/contracts";

type JsonRecord = Readonly<Record<string, unknown>>;

const SUMMARY_MAX_CHARS = 180;

const isRecord = (value: unknown): value is JsonRecord =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const trimmedString = (value: unknown): string | undefined =>
  typeof value === "string" ? value.trim() || undefined : undefined;

const count = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : undefined;

const compact = (value: string | undefined): string | undefined => {
  const normalized = value
    ?.replace(/\n\n---\nrouting:\n[\s\S]*$/u, "")
    .replace(/\s+/gu, " ")
    .trim();
  if (!normalized) return undefined;
  return normalized.length <= SUMMARY_MAX_CHARS
    ? normalized
    : `${normalized.slice(0, SUMMARY_MAX_CHARS - 1)}…`;
};

function contentText(value: unknown): string | undefined {
  if (!Array.isArray(value)) return trimmedString(value);
  const text = value
    .flatMap((part) => (isRecord(part) && typeof part.text === "string" ? [part.text] : []))
    .join("\n");
  return trimmedString(text);
}

function modelLabel(value: unknown): string | undefined {
  const direct = trimmedString(value);
  if (direct) return direct;
  if (!isRecord(value)) return undefined;
  const provider = trimmedString(value.provider);
  const id = trimmedString(value.id) ?? trimmedString(value.modelId);
  return provider && id ? `${provider}/${id}` : (id ?? trimmedString(value.name));
}

function typedUsage(details: JsonRecord): RuntimeTaskUsage | undefined {
  if (!isRecord(details.usage)) return undefined;
  const inputTokens = count(details.usage.input);
  const outputTokens = count(details.usage.output);
  const cachedInputTokens = count(details.usage.cacheRead);
  const cacheWriteTokens = count(details.usage.cacheWrite);
  const reasoningOutputTokens = count(details.usage.reasoning);
  const toolUses = Array.isArray(details.toolCalls) ? details.toolCalls.length : undefined;
  const hasTokens =
    inputTokens !== undefined ||
    outputTokens !== undefined ||
    cachedInputTokens !== undefined ||
    cacheWriteTokens !== undefined;
  if (!hasTokens && toolUses === undefined) return undefined;

  const lifecycle = isRecord(details.lifecycle) ? details.lifecycle : undefined;
  const startedAt = lifecycle ? Date.parse(String(lifecycle.startedAt ?? "")) : Number.NaN;
  const endedAt = lifecycle ? Date.parse(String(lifecycle.endedAt ?? "")) : Number.NaN;
  const durationMs =
    Number.isFinite(startedAt) && Number.isFinite(endedAt) && endedAt >= startedAt
      ? Math.round(endedAt - startedAt)
      : undefined;

  return {
    totalTokens:
      (inputTokens ?? 0) + (outputTokens ?? 0) + (cachedInputTokens ?? 0) + (cacheWriteTokens ?? 0),
    ...(inputTokens === undefined ? {} : { inputTokens }),
    ...(cachedInputTokens === undefined ? {} : { cachedInputTokens }),
    ...(outputTokens === undefined ? {} : { outputTokens }),
    ...(reasoningOutputTokens === undefined ? {} : { reasoningOutputTokens }),
    ...(toolUses === undefined ? {} : { toolUses }),
    ...(durationMs === undefined ? {} : { durationMs }),
  };
}

function runtimeStatus(details: JsonRecord): RuntimeTaskStatus | undefined {
  const lifecycle = isRecord(details.lifecycle) ? details.lifecycle : undefined;
  const lifecycleStatus = trimmedString(lifecycle?.status);
  if (lifecycleStatus === "starting") return "pending";
  if (lifecycleStatus === "running") return "running";
  if (lifecycleStatus === "succeeded") return "completed";
  if (lifecycleStatus === "failed" || lifecycleStatus === "timed_out") return "failed";
  if (lifecycleStatus === "cancelled") return "cancelled";
  if (lifecycleStatus === "interrupted") return "interrupted";
  if (details.stopReason === "aborted") return "cancelled";
  if (
    details.stopReason === "error" ||
    (typeof details.exitCode === "number" && details.exitCode !== 0)
  ) {
    return "failed";
  }
  return undefined;
}

/**
 * Pi keeps extension-specific state in tool-result details. Our agent tools
 * share this small shape, so the adapter can expose them without a tool-name
 * allowlist or a dependency on the extension package.
 */
export interface PiSubagentMetadata {
  readonly title: string;
  readonly role: string;
  readonly model?: string;
  readonly progress?: string;
  readonly summary?: string;
  readonly error?: string;
  readonly typedUsage?: RuntimeTaskUsage;
  readonly status?: RuntimeTaskStatus;
}

export function parsePiSubagent(
  args: JsonRecord,
  output: JsonRecord | undefined,
): PiSubagentMetadata | undefined {
  const details = isRecord(output?.details) ? output.details : undefined;
  const agent = trimmedString(details?.agent);
  if (!details || !agent) return undefined;

  const role = agent.replace(/[_-]+/gu, " ");
  const task =
    trimmedString(details.task) ??
    trimmedString(args.description) ??
    trimmedString(args.task) ??
    trimmedString(args.query) ??
    trimmedString(args.objective) ??
    trimmedString(args.goal) ??
    trimmedString(args.prompt) ??
    trimmedString(args.diff_description) ??
    trimmedString(args.name) ??
    trimmedString(args.scriptPath);
  const fallbackTitle = `${role.charAt(0).toUpperCase()}${role.slice(1)} agent`;
  const text = contentText(output?.content);
  const error = trimmedString(details.errorMessage);
  const savedOutput = trimmedString(details.output);
  const status = runtimeStatus(details);
  const model = modelLabel(details.model);
  const progress = compact(text);
  const summary = compact(status === "failed" ? (error ?? text) : (savedOutput ?? text));
  const usage = typedUsage(details);

  return {
    title: compact(task) ?? fallbackTitle,
    role,
    ...(model ? { model } : {}),
    ...(progress ? { progress } : {}),
    ...(summary ? { summary } : {}),
    ...(error ? { error } : {}),
    ...(usage ? { typedUsage: usage } : {}),
    ...(status ? { status } : {}),
  };
}

export function piSubagentTaskId(ownerSessionId: string, toolCallId: string): RuntimeTaskId {
  return RuntimeTaskId.make(`pi-agent:${ownerSessionId}:${toolCallId}`);
}

export function piSubagentCompletionStatus(
  metadata: PiSubagentMetadata,
  isError: boolean,
): "completed" | "failed" | "stopped" {
  if (metadata.status === "cancelled" || metadata.status === "interrupted") return "stopped";
  if (isError || metadata.status === "failed") return "failed";
  return "completed";
}
