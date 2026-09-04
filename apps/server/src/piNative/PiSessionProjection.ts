import type {
  EventId,
  ExternalThreadBacking,
  MessageId,
  OrchestrationEvent,
  OrchestrationMessage,
  OrchestrationProjectShell,
  OrchestrationSession,
  OrchestrationThread,
  OrchestrationThreadActivity,
  OrchestrationThreadDetailSnapshot,
  OrchestrationThreadShell,
  ProjectId,
  RuntimeTaskId,
  TurnId,
} from "@t3tools/contracts";
import {
  EventId as EventIdSchema,
  MessageId as MessageIdSchema,
  ProviderInstanceId,
  TurnId as TurnIdSchema,
} from "@t3tools/contracts";

import type { PiSessionCatalogRecord } from "./SessionCatalog.ts";
import type { SupervisorRuntimeState, SupervisorStreamEvent } from "./SupervisorProtocol.ts";
import {
  parsePiSubagent,
  piSubagentCompletionStatus,
  piSubagentTaskId,
  type PiSubagentMetadata,
} from "../provider/pi/PiSubagent.ts";

type JsonRecord = Readonly<Record<string, unknown>>;
const MESSAGE_ID_CUSTOM_TYPE = "t3.message-id.v1";

const isRecord = (value: unknown): value is JsonRecord =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const timestamp = (value: unknown, fallback: string): string =>
  typeof value === "string" && !Number.isNaN(Date.parse(value)) ? value : fallback;

function contentText(value: unknown): string {
  if (typeof value === "string") return value;
  if (!Array.isArray(value)) return "";
  return value
    .flatMap((block) =>
      isRecord(block) && block.type === "text" && typeof block.text === "string"
        ? [block.text]
        : [],
    )
    .join("");
}

const trimmedString = (value: unknown): string | undefined =>
  typeof value === "string" ? value.trim() || undefined : undefined;

function toolOutput(output: unknown): JsonRecord | undefined {
  const record = isRecord(output) ? output : undefined;
  if (!record) {
    const content = trimmedString(output);
    return content ? { content } : undefined;
  }

  const content = trimmedString(record.content) ?? trimmedString(contentText(record.content));
  const stdout = trimmedString(record.stdout);
  const details = isRecord(record.details) ? record.details : undefined;
  if (!content && !stdout && !details) return undefined;
  return {
    ...(content ? { content } : {}),
    ...(stdout ? { stdout } : {}),
    ...details,
  };
}

function activeBranch(entries: ReadonlyArray<JsonRecord>) {
  const treeEntries = entries.filter(
    (entry) => typeof entry.id === "string" && "parentId" in entry,
  );
  const byId = new Map(treeEntries.map((entry) => [entry.id as string, entry] as const));
  const branch: JsonRecord[] = [];
  let current = treeEntries.at(-1);
  let missingParentId: string | undefined;
  const seen = new Set<string>();
  while (current && typeof current.id === "string" && !seen.has(current.id)) {
    seen.add(current.id);
    branch.push(current);
    if (current.parentId === null) break;
    if (typeof current.parentId !== "string") break;
    const parent = byId.get(current.parentId);
    if (!parent) {
      missingParentId = current.parentId;
      break;
    }
    current = parent;
  }
  return { entries: branch.toReversed(), missingParentId };
}

export function projectPiActiveBranch(entries: ReadonlyArray<JsonRecord>) {
  return activeBranch(entries);
}

const turnId = (record: PiSessionCatalogRecord, entryId: string): TurnId =>
  TurnIdSchema.make(`${record.sessionId}:${entryId}`);
const messageId = (record: PiSessionCatalogRecord, entryId: string): MessageId =>
  MessageIdSchema.make(`${record.sessionId}:${entryId}`);
const activityId = (record: PiSessionCatalogRecord, entryId: string): EventId =>
  EventIdSchema.make(`${record.sessionId}:${entryId}`);

function runtimeSession(
  record: PiSessionCatalogRecord,
  runtime: SupervisorRuntimeState | undefined,
  activeTurnId: TurnId | null,
): OrchestrationSession | null {
  if (!runtime) return null;
  const status =
    runtime.status === "streaming"
      ? "running"
      : runtime.status === "starting"
        ? "starting"
        : runtime.status === "idle"
          ? "ready"
          : "stopped";
  return {
    threadId: record.threadId,
    status,
    providerName: "pi",
    providerInstanceId: ProviderInstanceId.make("pi"),
    runtimeMode: "full-access",
    activeTurnId: status === "running" ? activeTurnId : null,
    lastError: null,
    updatedAt: record.updatedAt,
  };
}

function backingFor(
  runtime: SupervisorRuntimeState | undefined,
  catalogResumeSupported: boolean,
): ExternalThreadBacking {
  const reconnecting = runtime?.status === "starting";
  const controlled = runtime !== undefined && runtime.status !== "exited" && !reconnecting;
  const streaming = runtime?.status === "streaming";
  return {
    kind: "external",
    source: "pi",
    sourceKey: runtime?.sessionKey ?? "catalog",
    control:
      runtime === undefined
        ? catalogResumeSupported
          ? "resumable"
          : "readOnly"
        : reconnecting
          ? "live"
          : controlled
            ? "live"
            : "readOnly",
    capabilities: {
      send: (runtime === undefined && catalogResumeSupported) || controlled,
      attachments: false,
      streamingBehaviors: streaming ? ["steer", "followUp"] : [],
      interrupt: controlled && streaming,
      stop: controlled,
      rename: false,
      archive: false,
      settle: true,
      unsettle: true,
      delete: false,
      changeModel: false,
      changeRuntimeMode: false,
      changeInteractionMode: false,
      checkpoints: false,
    },
  };
}

export function projectPiBacking(
  record: PiSessionCatalogRecord,
  runtime: SupervisorRuntimeState | undefined,
  catalogResumeSupported = false,
): ExternalThreadBacking {
  return {
    ...backingFor(runtime, catalogResumeSupported),
    sourceKey: record.sourceKey,
  };
}

function toolPresentation(toolName: string, args: JsonRecord, output?: unknown) {
  const normalized = toolName.toLowerCase();
  const path = trimmedString(args.path) ?? trimmedString(args.file_path);
  const command = trimmedString(args.command) ?? trimmedString(args.cmd);
  const rawOutput = toolOutput(output);
  const outputRecord = isRecord(output) ? output : undefined;
  const subagent = parsePiSubagent(args, outputRecord);
  const title = subagent
    ? `${subagent.role.charAt(0).toUpperCase()}${subagent.role.slice(1)} agent`
    : normalized === "bash"
      ? "Ran command"
      : normalized === "read"
        ? "Read file"
        : normalized === "write"
          ? "Wrote file"
          : normalized === "edit"
            ? "Edited file"
            : normalized === "grep"
              ? "Searched files"
              : normalized === "find"
                ? "Found files"
                : normalized === "ls"
                  ? "Listed directory"
                  : toolName;
  const detail = subagent
    ? subagent.title
    : normalized === "grep"
      ? `${trimmedString(args.pattern) ? `/${trimmedString(args.pattern)}/` : "pattern"} in ${path ?? trimmedString(args.glob) ?? "."}`
      : normalized === "find"
        ? `${trimmedString(args.filePattern) ?? trimmedString(args.pattern) ?? "files"} in ${path ?? "."}`
        : normalized === "ls"
          ? (path ?? ".")
          : path;
  const changes =
    (normalized === "write" || normalized === "edit") && path ? [{ path }] : undefined;

  return {
    itemType: subagent
      ? "collab_agent_tool_call"
      : normalized === "bash"
        ? "command_execution"
        : normalized === "write" || normalized === "edit"
          ? "file_change"
          : "dynamic_tool_call",
    title,
    ...(detail ? { detail } : {}),
    data: {
      toolName,
      kind:
        normalized === "bash"
          ? "execute"
          : normalized === "read"
            ? "read"
            : normalized === "write" || normalized === "edit"
              ? "edit"
              : "other",
      ...(command ? { command } : {}),
      rawInput: args,
      ...(rawOutput ? { rawOutput } : {}),
      item: {
        input: args,
        ...(changes ? { changes } : {}),
      },
    },
  };
}

const subagentLinkage = (metadata: PiSubagentMetadata, toolUseId: string) => ({
  taskType: "local_agent",
  agentKind: "agent",
  title: metadata.title,
  role: metadata.role,
  toolUseId,
  ...(metadata.model ? { model: metadata.model } : {}),
});

function subagentCompletionPresentation(
  metadata: PiSubagentMetadata,
  isError: boolean,
): {
  readonly status: "completed" | "failed" | "stopped";
  readonly tone: "info" | "error";
  readonly summary: string;
} {
  const status = piSubagentCompletionStatus(metadata, isError);
  return status === "failed"
    ? { status, tone: "error", summary: "Sub-agent failed" }
    : status === "stopped"
      ? { status, tone: "info", summary: "Sub-agent stopped" }
      : { status, tone: "info", summary: "Sub-agent completed" };
}

function projectedSubagentActivities(input: {
  readonly record: PiSessionCatalogRecord;
  readonly toolCallId: string;
  readonly turnId: TurnId | null;
  readonly startedAt: string;
  readonly completedAt: string;
  readonly metadata: PiSubagentMetadata;
  readonly isError: boolean;
}): ReadonlyArray<OrchestrationThreadActivity> {
  const taskId = piSubagentTaskId(input.record.sessionId, input.toolCallId);
  const linkage = subagentLinkage(input.metadata, input.toolCallId);
  const completion = subagentCompletionPresentation(input.metadata, input.isError);
  return [
    {
      id: activityId(input.record, `task:${input.toolCallId}:started`),
      tone: "info",
      kind: "task.started",
      summary: "Sub-agent started",
      payload: {
        taskId,
        detail: input.metadata.title,
        ...linkage,
      },
      turnId: input.turnId,
      createdAt: input.startedAt,
    },
    {
      id: activityId(input.record, `task:${input.toolCallId}:completed`),
      tone: completion.tone,
      kind: "task.completed",
      summary: completion.summary,
      payload: {
        taskId,
        status: completion.status,
        ...linkage,
        ...(input.metadata.summary ? { summary: input.metadata.summary } : {}),
        ...(input.metadata.typedUsage ? { typedUsage: input.metadata.typedUsage } : {}),
      },
      turnId: input.turnId,
      createdAt: input.completedAt,
    },
  ];
}

function projectHistory(record: PiSessionCatalogRecord, entries: ReadonlyArray<JsonRecord>) {
  const branch = activeBranch(entries);
  const messages: OrchestrationMessage[] = [];
  const activities: OrchestrationThreadActivity[] = [];
  const toolActivityIndex = new Map<string, number>();
  let currentTurnId: TurnId | null = null;
  let nextUserMessageId: MessageId | undefined;
  let model: string | undefined;

  for (const entry of branch.entries) {
    const entryId = String(entry.id);
    const createdAt = timestamp(entry.timestamp, record.updatedAt);
    if (
      entry.type === "custom" &&
      entry.customType === MESSAGE_ID_CUSTOM_TYPE &&
      isRecord(entry.data) &&
      entry.data.version === 1 &&
      typeof entry.data.messageId === "string"
    ) {
      nextUserMessageId = MessageIdSchema.make(entry.data.messageId);
      continue;
    }
    if (entry.type === "model_change" && typeof entry.modelId === "string") {
      nextUserMessageId = undefined;
      model =
        typeof entry.provider === "string" ? `${entry.provider}/${entry.modelId}` : entry.modelId;
      continue;
    }
    if (entry.type === "custom") continue;
    if (entry.type !== "message" || !isRecord(entry.message)) {
      nextUserMessageId = undefined;
      continue;
    }
    const message = entry.message;
    const role = message.role;
    if (role === "user") {
      currentTurnId = turnId(record, entryId);
      messages.push({
        id: nextUserMessageId ?? messageId(record, entryId),
        role: "user",
        text: contentText(message.content),
        turnId: currentTurnId,
        streaming: false,
        createdAt,
        updatedAt: createdAt,
      });
      nextUserMessageId = undefined;
      continue;
    }
    nextUserMessageId = undefined;
    if (role === "assistant") {
      if (typeof message.model === "string") {
        model =
          typeof message.provider === "string"
            ? `${message.provider}/${message.model}`
            : message.model;
      }
      const content = Array.isArray(message.content) ? message.content : [];
      const text = contentText(content);
      if (text.length > 0) {
        messages.push({
          id: messageId(record, entryId),
          role: "assistant",
          text,
          turnId: currentTurnId,
          streaming: false,
          createdAt,
          updatedAt: createdAt,
        });
      }
      for (let index = 0; index < content.length; index += 1) {
        const block = content[index];
        if (!isRecord(block)) continue;
        if (block.type === "thinking" && typeof block.thinking === "string") {
          activities.push({
            id: activityId(record, `${entryId}:thinking:${index}`),
            tone: "info",
            kind: "reasoning",
            summary: "Reasoned",
            payload: { text: block.thinking },
            turnId: currentTurnId,
            createdAt,
          });
        }
        if (
          block.type === "toolCall" &&
          typeof block.id === "string" &&
          typeof block.name === "string"
        ) {
          const args = isRecord(block.arguments) ? block.arguments : {};
          toolActivityIndex.set(block.id, activities.length);
          activities.push({
            id: activityId(record, `tool:${block.id}`),
            tone: "tool",
            kind: "item.started",
            summary: toolPresentation(block.name, args).title,
            payload: {
              ...toolPresentation(block.name, args),
              status: "inProgress",
              data: { ...toolPresentation(block.name, args).data, toolCallId: block.id },
            },
            turnId: currentTurnId,
            createdAt,
          });
        }
      }
      continue;
    }
    if (
      role === "toolResult" &&
      typeof message.toolCallId === "string" &&
      typeof message.toolName === "string"
    ) {
      const index = toolActivityIndex.get(message.toolCallId);
      const prior = index === undefined ? undefined : activities[index];
      const priorPayload = isRecord(prior?.payload) ? prior.payload : undefined;
      const priorData = isRecord(priorPayload?.data) ? priorPayload.data : undefined;
      const priorArgs = isRecord(priorData?.rawInput) ? priorData.rawInput : {};
      const presentation = toolPresentation(message.toolName, priorArgs, message);
      const metadata = parsePiSubagent(priorArgs, message);
      const completed: OrchestrationThreadActivity = {
        id: prior?.id ?? activityId(record, `tool:${message.toolCallId}`),
        tone: message.isError === true ? "error" : "tool",
        kind: "item.completed",
        summary: prior?.summary ?? presentation.title,
        payload: {
          ...presentation,
          status: message.isError === true ? "failed" : "completed",
          data: { ...presentation.data, toolCallId: message.toolCallId },
        },
        turnId: prior?.turnId ?? currentTurnId,
        createdAt: prior?.createdAt ?? createdAt,
      };
      if (index === undefined) activities.push(completed);
      else activities[index] = completed;
      if (metadata) {
        activities.push(
          ...projectedSubagentActivities({
            record,
            toolCallId: message.toolCallId,
            turnId: prior?.turnId ?? currentTurnId,
            startedAt: prior?.createdAt ?? createdAt,
            completedAt: createdAt,
            metadata,
            isError: message.isError === true,
          }),
        );
      }
    }
    if (role === "bashExecution") {
      activities.push({
        id: activityId(record, entryId),
        tone: message.exitCode === 0 ? "tool" : "error",
        kind: "item.completed",
        summary: "Ran command",
        payload: {
          itemType: "command_execution",
          title: "Ran command",
          status: message.exitCode === 0 ? "completed" : "failed",
          data: { command: message.command, rawOutput: message.output },
        },
        turnId: currentTurnId,
        createdAt,
      });
    }
  }

  return {
    messages,
    activities,
    model,
    activeTurnId: currentTurnId,
    missingParentId: branch.missingParentId,
  };
}

export function projectPiThread(input: {
  readonly record: PiSessionCatalogRecord;
  readonly entries: ReadonlyArray<JsonRecord>;
  readonly projectId: ProjectId;
  readonly runtime?: SupervisorRuntimeState;
  readonly catalogResumeSupported?: boolean;
  readonly lifecycle?: {
    readonly override: "settled" | "active";
    readonly updatedAt: string;
  };
}): OrchestrationThreadDetailSnapshot {
  const history = projectHistory(input.record, input.entries);
  const session = runtimeSession(input.record, input.runtime, history.activeTurnId);
  const running = session?.status === "running";
  const lifecycle = running ? undefined : input.lifecycle;
  const lastAssistant = history.messages.findLast(
    (message) => message.role === "assistant" && message.turnId === history.activeTurnId,
  );
  const latestTurn =
    history.activeTurnId === null
      ? input.record.lastActivityAt === undefined
        ? null
        : {
            turnId: turnId(input.record, "catalog-activity"),
            state: "completed" as const,
            requestedAt: input.record.lastActivityAt,
            startedAt: input.record.lastActivityAt,
            completedAt: input.record.lastActivityAt,
            assistantMessageId: null,
          }
      : {
          turnId: history.activeTurnId,
          state: running ? ("running" as const) : ("completed" as const),
          requestedAt:
            history.messages.findLast((message) => message.role === "user")?.createdAt ??
            input.record.updatedAt,
          startedAt:
            history.messages.findLast((message) => message.role === "user")?.createdAt ?? null,
          completedAt: running ? null : (lastAssistant?.updatedAt ?? input.record.updatedAt),
          assistantMessageId: lastAssistant?.id ?? null,
        };
  const thread: OrchestrationThread = {
    id: input.record.threadId,
    projectId: input.projectId,
    title: input.record.title,
    modelSelection: {
      instanceId: ProviderInstanceId.make("pi"),
      model: history.model ?? input.record.model ?? "unknown",
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: input.record.cwd,
    latestTurn,
    createdAt: input.record.createdAt,
    updatedAt: input.record.updatedAt,
    archivedAt: null,
    settledOverride:
      lifecycle?.override === "settled"
        ? "settled"
        : lifecycle?.override === "active"
          ? "active"
          : null,
    settledAt:
      lifecycle?.override === "settled"
        ? lifecycle.updatedAt
        : running || lifecycle?.override === "active"
          ? null
          : input.record.updatedAt,
    snoozedUntil: null,
    snoozedAt: null,
    deletedAt: null,
    messages: history.messages,
    proposedPlans: [],
    activities: history.activities,
    checkpoints: [],
    session,
    backing: projectPiBacking(input.record, input.runtime, input.catalogResumeSupported),
    historyTruncation: {
      ...input.record.historyTruncation,
      truncated: input.record.historyTruncation.truncated || history.missingParentId !== undefined,
      ...(history.missingParentId === undefined
        ? {}
        : { missingParentId: history.missingParentId }),
    },
  };
  return {
    snapshotSequence: input.runtime?.sequence ?? 0,
    thread,
  };
}

export function projectPiThreadOverlay(
  snapshot: OrchestrationThreadDetailSnapshot,
  record: PiSessionCatalogRecord,
  events: ReadonlyArray<SupervisorStreamEvent>,
  occurredAt = record.updatedAt,
  omittedOverlayEventCount = 0,
): OrchestrationThreadDetailSnapshot {
  let messages = [...snapshot.thread.messages];
  let activities = [...snapshot.thread.activities];
  let pendingComposerIntents = [...(snapshot.thread.pendingComposerIntents ?? [])];
  let pendingComposerIntentOmittedCount = snapshot.thread.pendingComposerIntentOmittedCount ?? 0;
  let latestTurn = snapshot.thread.latestTurn;
  const session = snapshot.thread.session;
  let liveSessionTurnId: TurnId | null = null;
  let activeTurnId = snapshot.thread.latestTurn?.turnId ?? null;
  const subagentMetadataByToolCallId = new Map<string, PiSubagentMetadata>();
  const subagentTaskIdByToolCallId = new Map<string, RuntimeTaskId>();
  for (const item of events) {
    const payload = livePayload(item.event);
    if (!payload?.type) continue;
    if (payload.type === "message_start") {
      const userMessage = liveUserMessage(payload);
      if (!userMessage) continue;
      const liveTurnId = turnId(record, `live-user:${item.eventId}`);
      const userText = contentText(userMessage.content);
      const correlatedMessageId = liveMessageId(payload);
      const persistedUserById =
        correlatedMessageId === undefined
          ? undefined
          : messages.findLast((message) => message.id === correlatedMessageId);
      const latestPersistedUser = messages.findLast((message) => message.role === "user");
      const persistedTurnId =
        persistedUserById !== undefined && latestTurn?.turnId === persistedUserById.turnId
          ? persistedUserById.turnId
          : correlatedMessageId === undefined &&
              latestPersistedUser?.text === userText &&
              latestTurn?.turnId === latestPersistedUser.turnId &&
              latestTurn.assistantMessageId === null
            ? latestPersistedUser.turnId
            : null;
      activeTurnId = persistedTurnId ?? liveTurnId;
      const id = correlatedMessageId ?? messageId(record, `live-user:${item.eventId}`);
      if (persistedTurnId === null) {
        messages = [
          ...messages.filter((candidate) => candidate.id !== id),
          {
            id,
            role: "user",
            text: userText,
            turnId: liveTurnId,
            streaming: false,
            createdAt: occurredAt,
            updatedAt: occurredAt,
          },
        ];
      }
      latestTurn = {
        turnId: activeTurnId,
        state: "running",
        requestedAt: occurredAt,
        startedAt: occurredAt,
        completedAt: null,
        assistantMessageId: null,
      };
      if (session) {
        liveSessionTurnId = activeTurnId;
      }
      continue;
    }
    if (payload.type === "queue_update") {
      const steering = Array.isArray(payload.data.steering)
        ? payload.data.steering.filter((value): value is string => typeof value === "string")
        : [];
      const followUp = Array.isArray(payload.data.followUp)
        ? payload.data.followUp.filter((value): value is string => typeof value === "string")
        : [];
      pendingComposerIntents = [
        ...steering.map((text) => ({ behavior: "steer" as const, text })),
        ...followUp.map((text) => ({ behavior: "followUp" as const, text })),
      ];
      pendingComposerIntentOmittedCount =
        (typeof payload.data.omittedSteering === "number" ? payload.data.omittedSteering : 0) +
        (typeof payload.data.omittedFollowUp === "number" ? payload.data.omittedFollowUp : 0);
      continue;
    }
    if (payload.type === "message_update") {
      const update = isRecord(payload.data.update) ? payload.data.update : undefined;
      const partial =
        update?.partial ??
        payload.data.message ??
        (isRecord(payload.data.assistantMessageEvent)
          ? payload.data.assistantMessageEvent.partial
          : undefined);
      const text = isRecord(partial)
        ? contentText(partial.content)
        : typeof partial === "string"
          ? partial
          : "";
      if (text.length === 0) continue;
      const id = MessageIdSchema.make(
        `${record.sessionId}:live-assistant:${activeTurnId ?? "turn"}`,
      );
      const message: OrchestrationMessage = {
        id,
        role: "assistant",
        text,
        turnId: activeTurnId,
        streaming: true,
        createdAt: occurredAt,
        updatedAt: occurredAt,
      };
      messages = [...messages.filter((candidate) => candidate.id !== id), message];
      continue;
    }
    if (payload.type.startsWith("tool_execution_")) {
      const toolCallId =
        typeof payload.data.toolCallId === "string"
          ? payload.data.toolCallId
          : `sequence-${item.sequence}`;
      const toolName = typeof payload.data.toolName === "string" ? payload.data.toolName : "tool";
      const id = activityId(record, `tool:${toolCallId}`);
      const prior = activities.find((activity) => activity.id === id);
      const priorPayload = isRecord(prior?.payload) ? prior.payload : undefined;
      const priorData = isRecord(priorPayload?.data) ? priorPayload.data : undefined;
      const priorArgs = isRecord(priorData?.rawInput) ? priorData.rawInput : {};
      const args = isRecord(payload.data.args) ? payload.data.args : priorArgs;
      const output = payload.data.result ?? payload.data.partialResult;
      const presentation = toolPresentation(toolName, args, output);
      const parsedMetadata = parsePiSubagent(args, isRecord(output) ? output : undefined);
      if (parsedMetadata) subagentMetadataByToolCallId.set(toolCallId, parsedMetadata);
      const metadata = parsedMetadata ?? subagentMetadataByToolCallId.get(toolCallId);
      const completed = payload.type === "tool_execution_end";
      const activity: OrchestrationThreadActivity = {
        id,
        tone: payload.data.isError === true ? "error" : "tool",
        kind: completed
          ? "item.completed"
          : payload.type === "tool_execution_start"
            ? "item.started"
            : "item.updated",
        summary: prior?.summary ?? presentation.title,
        payload: {
          ...presentation,
          status: completed
            ? payload.data.isError === true
              ? "failed"
              : "completed"
            : "inProgress",
          data: { ...presentation.data, toolCallId },
        },
        turnId: activeTurnId,
        sequence: item.sequence,
        createdAt: prior?.createdAt ?? occurredAt,
      };
      activities = [...activities.filter((candidate) => candidate.id !== id), activity];
      if (metadata) {
        const taskId =
          subagentTaskIdByToolCallId.get(toolCallId) ??
          piSubagentTaskId(record.sessionId, toolCallId);
        subagentTaskIdByToolCallId.set(toolCallId, taskId);
        const linkage = subagentLinkage(metadata, toolCallId);
        const startedId = activityId(record, `task:${toolCallId}:started`);
        if (!activities.some((candidate) => candidate.id === startedId)) {
          activities.push({
            id: startedId,
            tone: "info",
            kind: "task.started",
            summary: "Sub-agent started",
            payload: { taskId, detail: metadata.title, ...linkage },
            turnId: activeTurnId,
            sequence: item.sequence,
            createdAt: prior?.createdAt ?? occurredAt,
          });
        }
        const completion = subagentCompletionPresentation(metadata, payload.data.isError === true);
        const taskActivity: OrchestrationThreadActivity = completed
          ? {
              id: activityId(record, `task:${toolCallId}:completed`),
              tone: completion.tone,
              kind: "task.completed",
              summary: completion.summary,
              payload: {
                taskId,
                status: completion.status,
                ...linkage,
                ...(metadata.summary ? { summary: metadata.summary } : {}),
                ...(metadata.typedUsage ? { typedUsage: metadata.typedUsage } : {}),
              },
              turnId: activeTurnId,
              sequence: item.sequence,
              createdAt: occurredAt,
            }
          : {
              id: activityId(record, `task:${toolCallId}:progress`),
              tone: metadata.status === "failed" ? "error" : "info",
              kind: "task.progress",
              summary: metadata.progress ?? metadata.title,
              payload: {
                taskId,
                detail: metadata.progress ?? metadata.title,
                ...linkage,
                ...(metadata.progress ? { summary: metadata.progress } : {}),
                ...(metadata.typedUsage ? { typedUsage: metadata.typedUsage } : {}),
                ...(metadata.status ? { status: metadata.status } : {}),
                ...(metadata.error ? { error: metadata.error } : {}),
              },
              turnId: activeTurnId,
              sequence: item.sequence,
              createdAt: occurredAt,
            };
        activities = [
          ...activities.filter((candidate) => candidate.id !== taskActivity.id),
          taskActivity,
        ];
      }
    }
  }
  return {
    ...snapshot,
    thread: {
      ...snapshot.thread,
      messages,
      activities,
      latestTurn,
      historyTruncation: {
        ...snapshot.thread.historyTruncation,
        truncated:
          snapshot.thread.historyTruncation?.truncated === true || omittedOverlayEventCount > 0,
        omittedEntryCount:
          (snapshot.thread.historyTruncation?.omittedEntryCount ?? 0) + omittedOverlayEventCount,
      },
      pendingComposerIntents,
      pendingComposerIntentOmittedCount,
      session:
        session && liveSessionTurnId
          ? {
              ...session,
              status: "running",
              activeTurnId: liveSessionTurnId,
              updatedAt: occurredAt,
            }
          : session,
    },
  };
}

export function projectPiThreadShell(
  detail: OrchestrationThreadDetailSnapshot,
): OrchestrationThreadShell {
  const { thread } = detail;
  return {
    id: thread.id,
    projectId: thread.projectId,
    title: thread.title,
    modelSelection: thread.modelSelection,
    runtimeMode: thread.runtimeMode,
    interactionMode: thread.interactionMode,
    branch: thread.branch,
    worktreePath: thread.worktreePath,
    latestTurn: thread.latestTurn,
    createdAt: thread.createdAt,
    updatedAt: thread.updatedAt,
    archivedAt: thread.archivedAt,
    settledOverride: thread.settledOverride,
    settledAt: thread.settledAt,
    snoozedUntil: thread.snoozedUntil,
    snoozedAt: thread.snoozedAt,
    session: thread.session,
    latestUserMessageAt:
      thread.messages.findLast((message) => message.role === "user")?.createdAt ?? null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    backing: thread.backing,
    pendingComposerIntentCount:
      (thread.pendingComposerIntents?.length ?? 0) +
      (thread.pendingComposerIntentOmittedCount ?? 0),
  };
}

export function projectPiExternalProject(input: {
  readonly projectId: ProjectId;
  readonly cwd: string;
  readonly records: ReadonlyArray<PiSessionCatalogRecord>;
}): OrchestrationProjectShell {
  const title = input.cwd.split(/[\\/]/u).findLast(Boolean) ?? input.cwd;
  const sorted = [...input.records].sort((a, b) => a.createdAt.localeCompare(b.createdAt));
  return {
    id: input.projectId,
    title,
    workspaceRoot: input.cwd,
    repositoryIdentity: null,
    defaultModelSelection: null,
    scripts: [],
    createdAt: sorted[0]?.createdAt ?? "1970-01-01T00:00:00.000Z",
    updatedAt: sorted.at(-1)?.updatedAt ?? "1970-01-01T00:00:00.000Z",
  };
}

function livePayload(event: unknown) {
  if (!isRecord(event)) return undefined;
  if (event.type === "event" && typeof event.event === "string") {
    return {
      type: event.event,
      data: isRecord(event.data) ? event.data : {},
    };
  }
  return {
    type: typeof event.type === "string" ? event.type : undefined,
    data: event,
  };
}

export function isPiSubagentLiveEvent(event: unknown): boolean {
  const payload = livePayload(event);
  if (!payload?.type || !payload.type.startsWith("tool_execution_")) return false;
  const args = isRecord(payload.data.args) ? payload.data.args : {};
  const output = payload.data.result ?? payload.data.partialResult;
  return parsePiSubagent(args, isRecord(output) ? output : undefined) !== undefined;
}

export function piLiveToolCallId(event: unknown): string | undefined {
  const payload = livePayload(event);
  if (!payload?.type || !payload.type.startsWith("tool_execution_")) return undefined;
  return typeof payload.data.toolCallId === "string" ? payload.data.toolCallId : undefined;
}

function liveUserMessage(payload: NonNullable<ReturnType<typeof livePayload>>) {
  const message = isRecord(payload.data.message)
    ? payload.data.message
    : payload.data.role === "user"
      ? payload.data
      : undefined;
  return message?.role === "user" ? message : undefined;
}

function liveMessageId(
  payload: NonNullable<ReturnType<typeof livePayload>>,
): MessageId | undefined {
  return typeof payload.data.messageId === "string"
    ? MessageIdSchema.make(payload.data.messageId)
    : undefined;
}

export function projectPiLiveEvent(input: {
  readonly record: PiSessionCatalogRecord;
  readonly runtime: SupervisorRuntimeState;
  readonly item: SupervisorStreamEvent;
  readonly activeTurnId: TurnId | null;
  readonly occurredAt: string;
}): OrchestrationEvent | null {
  const payload = livePayload(input.item.event);
  if (!payload?.type) return null;
  const occurredAt = input.occurredAt;
  const base = {
    sequence: input.item.sequence,
    eventId: EventIdSchema.make(`${input.record.sessionId}:runtime:${input.item.eventId}`),
    aggregateKind: "thread" as const,
    aggregateId: input.record.threadId,
    occurredAt,
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
  };
  if (payload.type === "agent_start") {
    return {
      ...base,
      type: "thread.session-set",
      payload: {
        threadId: input.record.threadId,
        session: runtimeSession(input.record, input.runtime, input.activeTurnId)!,
      },
    };
  }
  if (payload.type === "message_start") {
    const userMessage = liveUserMessage(payload);
    if (!userMessage) return null;
    const liveTurnId = turnId(input.record, `live-user:${input.item.eventId}`);
    const correlatedMessageId = liveMessageId(payload);
    return {
      ...base,
      type: "thread.message-sent",
      payload: {
        threadId: input.record.threadId,
        messageId:
          correlatedMessageId ?? messageId(input.record, `live-user:${input.item.eventId}`),
        role: "user",
        text: contentText(userMessage.content),
        turnId: liveTurnId,
        streaming: false,
        createdAt: occurredAt,
        updatedAt: occurredAt,
      },
    };
  }
  if (payload.type === "message_update") {
    const update = isRecord(payload.data.update)
      ? payload.data.update
      : isRecord(payload.data.assistantMessageEvent)
        ? payload.data.assistantMessageEvent
        : undefined;
    if (update?.type !== "text_delta" || typeof update.delta !== "string") return null;
    return {
      ...base,
      type: "thread.message-sent",
      payload: {
        threadId: input.record.threadId,
        messageId: MessageIdSchema.make(
          `${input.record.sessionId}:live-assistant:${input.activeTurnId ?? "turn"}`,
        ),
        role: "assistant",
        text: update.delta,
        turnId: input.activeTurnId,
        streaming: true,
        createdAt: occurredAt,
        updatedAt: occurredAt,
      },
    };
  }
  if (payload.type.startsWith("tool_execution_")) {
    const toolCallId =
      typeof payload.data.toolCallId === "string"
        ? payload.data.toolCallId
        : `sequence-${input.item.sequence}`;
    const toolName = typeof payload.data.toolName === "string" ? payload.data.toolName : "tool";
    const args = isRecord(payload.data.args) ? payload.data.args : {};
    const output = payload.data.result ?? payload.data.partialResult;
    const presentation = toolPresentation(toolName, args, output);
    const metadata = parsePiSubagent(args, isRecord(output) ? output : undefined);
    const completed = payload.type === "tool_execution_end";
    if (metadata && !completed) {
      const taskId = piSubagentTaskId(input.record.sessionId, toolCallId);
      return {
        ...base,
        type: "thread.activity-appended",
        payload: {
          threadId: input.record.threadId,
          activity: {
            id: activityId(input.record, `task:${toolCallId}:progress`),
            tone: metadata.status === "failed" ? "error" : "info",
            kind: "task.progress",
            summary: metadata.progress ?? metadata.title,
            payload: {
              taskId,
              detail: metadata.progress ?? metadata.title,
              ...subagentLinkage(metadata, toolCallId),
              ...(metadata.progress ? { summary: metadata.progress } : {}),
              ...(metadata.typedUsage ? { typedUsage: metadata.typedUsage } : {}),
              ...(metadata.status ? { status: metadata.status } : {}),
              ...(metadata.error ? { error: metadata.error } : {}),
            },
            turnId: input.activeTurnId,
            sequence: input.item.sequence,
            createdAt: occurredAt,
          },
        },
      };
    }
    return {
      ...base,
      type: "thread.activity-appended",
      payload: {
        threadId: input.record.threadId,
        activity: {
          id: activityId(input.record, `tool:${toolCallId}`),
          tone: payload.data.isError === true ? "error" : "tool",
          kind: completed
            ? "item.completed"
            : payload.type === "tool_execution_start"
              ? "item.started"
              : "item.updated",
          summary: presentation.title,
          payload: {
            ...presentation,
            status: completed
              ? payload.data.isError === true
                ? "failed"
                : "completed"
              : "inProgress",
            data: { ...presentation.data, toolCallId },
          },
          turnId: input.activeTurnId,
          sequence: input.item.sequence,
          createdAt: occurredAt,
        },
      },
    };
  }
  return null;
}
