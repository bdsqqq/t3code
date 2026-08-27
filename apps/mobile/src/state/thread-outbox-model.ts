import { isTransportConnectionErrorMessage } from "@t3tools/client-runtime/errors";
import type { EnvironmentShellStatus } from "@t3tools/client-runtime/state/shell";
import { threadAllows } from "@t3tools/client-runtime/state/threads";
import {
  CommandId,
  EnvironmentId,
  IsoDateTime,
  MessageId,
  ModelSelection,
  ProjectId,
  ProviderInteractionMode,
  RuntimeMode,
  ThreadId,
  type ModelSelection as ModelSelectionType,
  type ProjectId as ProjectIdType,
  type ProviderInteractionMode as ProviderInteractionModeType,
  type RuntimeMode as RuntimeModeType,
  type OrchestrationThread,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import { DraftComposerImageAttachmentSchema } from "../lib/composer-image-schema";
import type { DraftComposerImageAttachment } from "../lib/composerImages";
import { scopedThreadKey } from "../lib/scopedEntities";

const THREAD_OUTBOX_SCHEMA_VERSION = 8;
const THREAD_OUTBOX_MAX_RETRY_DELAY_MS = 16_000;

const QueuedThreadCreationSchema = Schema.Struct({
  projectId: ProjectId,
  // Snapshot of the project's display metadata so a pending task stays
  // presentable in the thread list even when the project shell is not loaded.
  projectTitle: Schema.optional(Schema.String),
  projectCwd: Schema.optional(Schema.String),
  workspaceMode: Schema.Literals(["local", "worktree"]),
  branch: Schema.NullOr(Schema.String),
  worktreePath: Schema.NullOr(Schema.String),
  startFromOrigin: Schema.optional(Schema.Boolean),
});

export const QueuedThreadMessageSchema = Schema.Struct({
  schemaVersion: Schema.Literals([1, 2, 3, 4, 5, 6, 7, THREAD_OUTBOX_SCHEMA_VERSION]),
  environmentId: EnvironmentId,
  threadId: ThreadId,
  messageId: MessageId,
  commandId: CommandId,
  text: Schema.String,
  attachments: Schema.Array(DraftComposerImageAttachmentSchema),
  modelSelection: Schema.optional(ModelSelection),
  runtimeMode: Schema.optional(RuntimeMode),
  interactionMode: Schema.optional(ProviderInteractionMode),
  streamingBehavior: Schema.optional(Schema.Literals(["steer", "followUp"])),
  externalResume: Schema.optional(Schema.Literals(["needsConfirmation", "takeover"])),
  deliveryStatus: Schema.optional(Schema.Literal("indeterminate")),
  awaitThreadVisibility: Schema.optional(Schema.Boolean),
  // Present when the queued item creates a brand-new thread (pending task)
  // instead of appending a turn to an existing one.
  creation: Schema.optional(QueuedThreadCreationSchema),
  createdAt: IsoDateTime,
});

const decodeStoredQueuedThreadMessage = Schema.decodeUnknownSync(QueuedThreadMessageSchema);
const encodeStoredQueuedThreadMessage = Schema.encodeUnknownSync(QueuedThreadMessageSchema);

export interface QueuedThreadCreation {
  readonly projectId: ProjectIdType;
  readonly projectTitle?: string;
  readonly projectCwd?: string;
  readonly workspaceMode: "local" | "worktree";
  readonly branch: string | null;
  readonly worktreePath: string | null;
  readonly startFromOrigin?: boolean;
}

export interface QueuedThreadMessage {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly commandId: CommandId;
  readonly text: string;
  readonly attachments: ReadonlyArray<DraftComposerImageAttachment>;
  readonly modelSelection?: ModelSelectionType;
  readonly runtimeMode?: RuntimeModeType;
  readonly interactionMode?: ProviderInteractionModeType;
  readonly streamingBehavior?: "steer" | "followUp";
  readonly externalResume?: "needsConfirmation" | "takeover";
  readonly deliveryStatus?: "indeterminate";
  readonly awaitThreadVisibility?: boolean;
  readonly creation?: QueuedThreadCreation;
  readonly createdAt: string;
}

export interface ThreadSettingsSnapshot {
  readonly modelSelection: ModelSelectionType;
  readonly runtimeMode: RuntimeModeType;
  readonly interactionMode: ProviderInteractionModeType;
}
export type QueuedExternalResumeDrainAction = "mark-needs-confirmation" | "send" | "wait";

export function resolveQueuedExternalResumeDrainAction(
  message: Pick<QueuedThreadMessage, "externalResume">,
  backingControl: "live" | "readOnly" | "resumable" | undefined,
): QueuedExternalResumeDrainAction {
  if (backingControl !== "resumable" || message.externalResume === "takeover") {
    return "send";
  }
  return message.externalResume === "needsConfirmation" ? "wait" : "mark-needs-confirmation";
}

export function renewQueuedExternalResumeTakeover(
  message: QueuedThreadMessage,
  commandId: CommandId,
): QueuedThreadMessage {
  return { ...message, commandId, externalResume: "takeover" };
}

/** Local confirmation state must never cross the wire. */
export function queuedExternalResumeForWire(
  message: Pick<QueuedThreadMessage, "externalResume">,
): "takeover" | undefined {
  return message.externalResume === "takeover" ? "takeover" : undefined;
}

export function queuedMessageRequiresExplicitDiscard(
  message: Pick<QueuedThreadMessage, "deliveryStatus">,
): boolean {
  return message.deliveryStatus === "indeterminate";
}

export type ThreadOutboxDeliverySuccessAction = "mark-indeterminate" | "remove";

export function resolveThreadOutboxDeliverySuccessAction(
  deliveryStatus: "completed" | "indeterminate" | undefined,
): ThreadOutboxDeliverySuccessAction {
  return deliveryStatus === "indeterminate" ? "mark-indeterminate" : "remove";
}

export function threadComposerQueueCount(input: {
  readonly localCount: number;
  readonly detailIntentCount?: number;
  readonly detailOmittedCount?: number;
  readonly shellIntentCount?: number;
  readonly hasDetail: boolean;
}): number {
  return (
    input.localCount +
    (input.hasDetail
      ? (input.detailIntentCount ?? 0) + (input.detailOmittedCount ?? 0)
      : (input.shellIntentCount ?? 0))
  );
}
export function waitsForQueuedThreadVisibility(
  message: Pick<QueuedThreadMessage, "awaitThreadVisibility">,
  threadExists: boolean,
): boolean {
  return message.awaitThreadVisibility === true && !threadExists;
}
export const newTaskTargetRequiresProvider = (target: "t3" | "pi"): boolean => target === "t3";
export function threadComposerAllowsSend(
  thread: OrchestrationThread | OrchestrationThreadShell,
  hasContent: boolean,
  behavior?: "steer" | "followUp",
): boolean {
  return (
    hasContent &&
    threadAllows(thread, "send") &&
    (behavior === undefined || threadAllows(thread, behavior))
  );
}
export function queuedMessageBlockedByCapabilities(
  message: Pick<QueuedThreadMessage, "attachments" | "streamingBehavior">,
  thread: OrchestrationThread | OrchestrationThreadShell,
): boolean {
  return (
    !threadAllows(thread, "send") ||
    (message.attachments.length > 0 && !threadAllows(thread, "attachments")) ||
    (thread.session?.status === "running" &&
      message.streamingBehavior !== undefined &&
      !threadAllows(thread, message.streamingBehavior))
  );
}

export function resolveQueuedThreadSettings(
  message: QueuedThreadMessage,
  thread: ThreadSettingsSnapshot,
): ThreadSettingsSnapshot {
  return {
    modelSelection: message.modelSelection ?? thread.modelSelection,
    runtimeMode: message.runtimeMode ?? thread.runtimeMode,
    interactionMode: message.interactionMode ?? thread.interactionMode,
  };
}

export function resolveCapabilityAllowedQueuedThreadSettings(
  message: QueuedThreadMessage,
  thread: OrchestrationThread | OrchestrationThreadShell,
): ThreadSettingsSnapshot {
  const queued = resolveQueuedThreadSettings(message, thread);
  return {
    modelSelection: threadAllows(thread, "changeModel")
      ? queued.modelSelection
      : thread.modelSelection,
    runtimeMode: threadAllows(thread, "changeRuntimeMode")
      ? queued.runtimeMode
      : thread.runtimeMode,
    interactionMode: threadAllows(thread, "changeInteractionMode")
      ? queued.interactionMode
      : thread.interactionMode,
  };
}

export function modelSelectionsEqual(left: ModelSelectionType, right: ModelSelectionType): boolean {
  return (
    left.instanceId === right.instanceId &&
    left.model === right.model &&
    JSON.stringify(left.options ?? null) === JSON.stringify(right.options ?? null)
  );
}

export function encodeQueuedThreadMessage(message: QueuedThreadMessage): unknown {
  return encodeStoredQueuedThreadMessage({
    schemaVersion: THREAD_OUTBOX_SCHEMA_VERSION,
    ...message,
  });
}

export function decodeQueuedThreadMessage(value: unknown): QueuedThreadMessage {
  const { schemaVersion: _, ...message } = decodeStoredQueuedThreadMessage(value);
  return message;
}

export function groupQueuedThreadMessages(
  messages: ReadonlyArray<QueuedThreadMessage>,
): Record<string, ReadonlyArray<QueuedThreadMessage>> {
  const deduplicated = new Map<MessageId, QueuedThreadMessage>();
  for (const message of messages) {
    deduplicated.set(message.messageId, message);
  }

  const grouped: Record<string, Array<QueuedThreadMessage>> = {};
  for (const message of deduplicated.values()) {
    const threadKey = scopedThreadKey(message.environmentId, message.threadId);
    (grouped[threadKey] ??= []).push(message);
  }
  for (const queue of Object.values(grouped)) {
    queue.sort((left, right) => left.createdAt.localeCompare(right.createdAt));
  }
  return grouped;
}

export function flattenQueuedThreadMessages(
  queues: Record<string, ReadonlyArray<QueuedThreadMessage>>,
): ReadonlyArray<QueuedThreadMessage> {
  return Object.values(queues).flat();
}

export function threadOutboxRetryDelayMs(attempt: number): number {
  return Math.min(1_000 * 2 ** Math.max(0, attempt - 1), THREAD_OUTBOX_MAX_RETRY_DELAY_MS);
}

export type ThreadOutboxDeliveryAction = "wait" | "remove" | "send";

export function resolveThreadOutboxDeliveryAction(input: {
  readonly isCreation: boolean;
  readonly threadExists: boolean;
  readonly shellStatus: EnvironmentShellStatus;
  readonly environmentConnected: boolean;
  readonly threadBusy: boolean;
  readonly isExternalPiThread?: boolean;
}): ThreadOutboxDeliveryAction {
  if (input.isCreation) {
    // A pending task creates its thread on delivery. If the thread already
    // exists the creation command went through and only cleanup remains.
    if (input.threadExists) {
      return "remove";
    }
    // Wait for the shell to be live before sending: until the thread list has
    // synchronized, a previously delivered creation whose cleanup failed would
    // look missing and get re-issued, duplicating the thread.
    return input.environmentConnected && input.shellStatus === "live" ? "send" : "wait";
  }
  if (!input.threadExists) {
    if (input.isExternalPiThread === true) {
      return "wait";
    }
    return input.shellStatus === "live" ? "remove" : "wait";
  }
  return input.environmentConnected ? "send" : "wait";
}

/**
 * A queued creation can only be dispatched once its payload would pass server
 * validation; incomplete payloads stay pending until the user edits them.
 */
export function isQueuedThreadCreationSendable(message: QueuedThreadMessage): boolean {
  if (!message.creation) {
    return false;
  }
  if (message.text.trim().length === 0 || message.modelSelection === undefined) {
    return false;
  }
  return message.creation.workspaceMode !== "worktree" || Boolean(message.creation.branch);
}

function errorMessage(error: unknown): string | null {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === "object" && error !== null && "message" in error) {
    return typeof error.message === "string" ? error.message : null;
  }
  return typeof error === "string" ? error : null;
}

export function shouldRetryThreadOutboxDelivery(error: unknown): boolean {
  if (
    typeof error === "object" &&
    error !== null &&
    "_tag" in error &&
    error._tag === "ConnectionTransientError"
  ) {
    return true;
  }
  return isTransportConnectionErrorMessage(errorMessage(error));
}

export type ThreadOutboxCommandStage = "settings-sync" | "start-turn";
export type ThreadOutboxFailureAction = "discard" | "needs-confirmation" | "retry";

export function resolveThreadOutboxFailureAction(input: {
  readonly stage: ThreadOutboxCommandStage;
  readonly error: unknown;
  readonly interrupted: boolean;
  readonly externalResume?: QueuedThreadMessage["externalResume"];
}): ThreadOutboxFailureAction {
  const code =
    typeof input.error === "object" &&
    input.error !== null &&
    "code" in input.error &&
    typeof input.error.code === "string"
      ? input.error.code
      : undefined;
  if (input.stage === "settings-sync") {
    return "retry";
  }
  if (code === "takeover_confirmation_required") {
    return "needs-confirmation";
  }
  if (code === "command_rejected") {
    return input.externalResume === "takeover" ? "needs-confirmation" : "discard";
  }
  if (code === "read_only" && input.externalResume !== undefined) {
    return "needs-confirmation";
  }
  if (
    input.interrupted ||
    code === "runtime_starting" ||
    code === "supervisor_upgrade_required" ||
    code === "streaming_behavior_required" ||
    code === "read_only" ||
    code === "supervisor" ||
    shouldRetryThreadOutboxDelivery(input.error)
  ) {
    return "retry";
  }
  return "discard";
}
