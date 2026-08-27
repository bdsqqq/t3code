import { useAtomValue } from "@effect/atom-react";
import type {
  EnvironmentProject,
  EnvironmentThreadShell,
} from "@t3tools/client-runtime/state/shell";
import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  type DispatchResult,
  type MessageId,
} from "@t3tools/contracts";
import { buildTemporaryWorktreeBranchName } from "@t3tools/shared/git";
import * as Cause from "effect/Cause";
import { AsyncResult, Atom } from "effect/unstable/reactivity";
import { useCallback, useEffect, useRef, useState } from "react";

import { environmentCatalog } from "../connection/catalog";
import { scopedThreadKey } from "../lib/scopedEntities";
import { buildProjectThreadStartTurnInput } from "../lib/projectThreadStartTurn";
import { toUploadChatImageAttachments } from "../lib/composerImages";
import { randomHex } from "../lib/uuid";
import { appAtomRegistry } from "./atom-registry";
import { useProjects, useThreadShells } from "./entities";
import {
  confirmThreadOutboxMessageQueued,
  ensureThreadOutboxLoaded,
  markThreadOutboxMessageIndeterminateInMemory,
  markThreadOutboxMessageNeedsConfirmationInMemory,
  removeThreadOutboxMessage,
  updateThreadOutboxMessage,
} from "./thread-outbox";
import {
  isQueuedThreadCreationSendable,
  modelSelectionsEqual,
  queuedExternalResumeForWire,
  queuedMessageBlockedByCapabilities,
  queuedMessageRequiresExplicitDiscard,
  resolveQueuedExternalResumeDrainAction,
  resolveThreadOutboxDeliveryAction,
  resolveThreadOutboxDeliverySuccessAction,
  resolveCapabilityAllowedQueuedThreadSettings,
  resolveThreadOutboxFailureAction,
  threadOutboxRetryDelayMs,
  type QueuedThreadCreation,
  type QueuedThreadMessage,
  type ThreadOutboxCommandStage,
} from "./thread-outbox-model";
import { environmentShell } from "./shell";
import { environmentThreadShells, threadEnvironment } from "./threads";
import { useAtomCommand } from "./use-atom-command";
import {
  editingQueuedMessageIdsAtom,
  useThreadOutboxMessages,
  useThreadOutboxShellStatuses,
} from "./use-thread-outbox";
import { useRemoteConnectionStatus } from "./use-remote-environment-registry";

export const dispatchingQueuedMessageIdAtom = Atom.make<MessageId | null>(null).pipe(
  Atom.keepAlive,
  Atom.withLabel("mobile:thread-outbox:dispatching-message-id"),
);

function beginDispatchingQueuedMessage(queuedMessageId: MessageId): void {
  appAtomRegistry.set(dispatchingQueuedMessageIdAtom, queuedMessageId);
}

function finishDispatchingQueuedMessage(queuedMessageId: MessageId): void {
  const current = appAtomRegistry.get(dispatchingQueuedMessageIdAtom);
  appAtomRegistry.set(dispatchingQueuedMessageIdAtom, current === queuedMessageId ? null : current);
}

function findThread(
  threads: ReadonlyArray<EnvironmentThreadShell>,
  message: QueuedThreadMessage,
): EnvironmentThreadShell | undefined {
  return threads.find(
    (candidate) =>
      candidate.environmentId === message.environmentId && candidate.id === message.threadId,
  );
}

function findCreationProject(
  projects: ReadonlyArray<EnvironmentProject>,
  message: QueuedThreadMessage,
): EnvironmentProject | undefined {
  return projects.find(
    (candidate) =>
      candidate.environmentId === message.environmentId &&
      candidate.id === message.creation?.projectId,
  );
}

function settingsCommandId(message: QueuedThreadMessage, setting: string): CommandId {
  return CommandId.make(`${message.commandId}:${setting}`);
}

async function markQueuedMessageNeedsTakeoverConfirmation(
  message: QueuedThreadMessage,
): Promise<void> {
  const needsConfirmationMessage = {
    ...message,
    externalResume: "needsConfirmation" as const,
  };
  markThreadOutboxMessageNeedsConfirmationInMemory(message);
  try {
    await updateThreadOutboxMessage(needsConfirmationMessage);
  } catch (error) {
    console.warn("[thread-outbox] failed to persist takeover confirmation requirement", {
      environmentId: message.environmentId,
      threadId: message.threadId,
      messageId: message.messageId,
      error,
    });
  }
}

export function useThreadOutboxDrain(): void {
  const startTurn = useAtomCommand(threadEnvironment.startTurn, { reportFailure: false });
  const updateThreadMetadata = useAtomCommand(threadEnvironment.updateMetadata, {
    reportFailure: false,
  });
  const setThreadRuntimeMode = useAtomCommand(threadEnvironment.setRuntimeMode, {
    reportFailure: false,
  });
  const setThreadInteractionMode = useAtomCommand(threadEnvironment.setInteractionMode, {
    reportFailure: false,
  });
  const dispatchingQueuedMessageId = useAtomValue(dispatchingQueuedMessageIdAtom);
  const editingQueuedMessageIds = useAtomValue(editingQueuedMessageIdsAtom);
  const queuedMessagesByThreadKey = useThreadOutboxMessages();
  const shellStatuses = useThreadOutboxShellStatuses();
  const threads = useThreadShells();
  const projects = useProjects();
  const { connectedEnvironments } = useRemoteConnectionStatus();
  const [retryTick, setRetryTick] = useState(0);
  const retryAttemptRef = useRef(new Map<MessageId, number>());
  const retryNotBeforeRef = useRef(new Map<MessageId, number>());
  const retryTimersRef = useRef(new Map<MessageId, ReturnType<typeof setTimeout>>());

  useEffect(() => {
    ensureThreadOutboxLoaded();
    return () => {
      for (const timer of retryTimersRef.current.values()) {
        clearTimeout(timer);
      }
      retryTimersRef.current.clear();
    };
  }, []);

  const makeDeliveryHelpers = useCallback((queuedMessage: QueuedThreadMessage) => {
    const reportFailure = (
      commandResult: AtomCommandResult<unknown, unknown>,
      stage: ThreadOutboxCommandStage,
    ): ReturnType<typeof resolveThreadOutboxFailureAction> | null => {
      if (!AsyncResult.isFailure(commandResult)) {
        return null;
      }
      const action = resolveThreadOutboxFailureAction({
        stage,
        error: Cause.squash(commandResult.cause),
        interrupted: Cause.hasInterruptsOnly(commandResult.cause),
        externalResume: queuedMessage.externalResume,
      });
      console.warn("[thread-outbox] queued message delivery failed", {
        environmentId: queuedMessage.environmentId,
        threadId: queuedMessage.threadId,
        messageId: queuedMessage.messageId,
        stage,
        cause: commandResult.cause,
        action,
      });
      return action;
    };
    const completeFailureAction = async (
      action: ReturnType<typeof resolveThreadOutboxFailureAction>,
    ): Promise<boolean> => {
      if (action === "retry") {
        return false;
      }
      if (action === "needs-confirmation") {
        await markQueuedMessageNeedsTakeoverConfirmation(queuedMessage);
        return true;
      }
      try {
        await removeThreadOutboxMessage(queuedMessage);
        return true;
      } catch (error) {
        console.warn("[thread-outbox] failed to discard rejected queued message", {
          environmentId: queuedMessage.environmentId,
          threadId: queuedMessage.threadId,
          messageId: queuedMessage.messageId,
          error,
        });
        return false;
      }
    };
    const completeDelivery = async (
      deliveryResult: AtomCommandResult<DispatchResult, unknown>,
    ): Promise<boolean> => {
      const failureAction = reportFailure(deliveryResult, "start-turn");
      if (failureAction !== null) {
        return completeFailureAction(failureAction);
      }

      if (
        AsyncResult.isSuccess(deliveryResult) &&
        resolveThreadOutboxDeliverySuccessAction(deliveryResult.value.deliveryStatus) ===
          "mark-indeterminate"
      ) {
        const indeterminateMessage = {
          ...queuedMessage,
          deliveryStatus: "indeterminate" as const,
        };
        // Block this process before the durable rewrite: if storage fails, the
        // already-accepted command must still never be dispatched again.
        markThreadOutboxMessageIndeterminateInMemory(queuedMessage);
        try {
          await updateThreadOutboxMessage(indeterminateMessage);
        } catch (error) {
          console.warn("[thread-outbox] failed to persist indeterminate delivery", {
            environmentId: queuedMessage.environmentId,
            threadId: queuedMessage.threadId,
            messageId: queuedMessage.messageId,
            error,
          });
        }
        return true;
      }

      try {
        await removeThreadOutboxMessage(queuedMessage);
        return true;
      } catch (error) {
        console.warn("[thread-outbox] failed to remove delivered queued message", {
          environmentId: queuedMessage.environmentId,
          threadId: queuedMessage.threadId,
          messageId: queuedMessage.messageId,
          error,
        });
        return false;
      }
    };
    return { reportFailure, completeFailureAction, completeDelivery };
  }, []);

  const sendQueuedMessage = useCallback(
    async (queuedMessage: QueuedThreadMessage, thread: EnvironmentThreadShell) => {
      const settings = resolveCapabilityAllowedQueuedThreadSettings(queuedMessage, thread);
      const { reportFailure, completeFailureAction, completeDelivery } =
        makeDeliveryHelpers(queuedMessage);

      if (!modelSelectionsEqual(settings.modelSelection, thread.modelSelection)) {
        const updateResult = await updateThreadMetadata({
          environmentId: queuedMessage.environmentId,
          input: {
            commandId: settingsCommandId(queuedMessage, "model-selection"),
            threadId: queuedMessage.threadId,
            modelSelection: settings.modelSelection,
          },
        });
        if (AsyncResult.isFailure(updateResult)) {
          return completeFailureAction(reportFailure(updateResult, "settings-sync") ?? "retry");
        }
      }

      if (settings.runtimeMode !== thread.runtimeMode) {
        const runtimeResult = await setThreadRuntimeMode({
          environmentId: queuedMessage.environmentId,
          input: {
            commandId: settingsCommandId(queuedMessage, "runtime-mode"),
            threadId: queuedMessage.threadId,
            runtimeMode: settings.runtimeMode,
            createdAt: queuedMessage.createdAt,
          },
        });
        if (AsyncResult.isFailure(runtimeResult)) {
          return completeFailureAction(reportFailure(runtimeResult, "settings-sync") ?? "retry");
        }
      }

      if (settings.interactionMode !== thread.interactionMode) {
        const interactionResult = await setThreadInteractionMode({
          environmentId: queuedMessage.environmentId,
          input: {
            commandId: settingsCommandId(queuedMessage, "interaction-mode"),
            threadId: queuedMessage.threadId,
            interactionMode: settings.interactionMode,
            createdAt: queuedMessage.createdAt,
          },
        });
        if (AsyncResult.isFailure(interactionResult)) {
          return completeFailureAction(
            reportFailure(interactionResult, "settings-sync") ?? "retry",
          );
        }
      }

      const externalResume = queuedExternalResumeForWire(queuedMessage);
      const deliveryResult = await startTurn({
        environmentId: queuedMessage.environmentId,
        input: {
          commandId: queuedMessage.commandId,
          threadId: queuedMessage.threadId,
          message: {
            messageId: queuedMessage.messageId,
            role: "user",
            text: queuedMessage.text,
            attachments: toUploadChatImageAttachments(queuedMessage.attachments),
          },
          modelSelection: settings.modelSelection,
          runtimeMode: settings.runtimeMode,
          interactionMode: settings.interactionMode,
          ...(externalResume === undefined ? {} : { externalResume }),
          createdAt: queuedMessage.createdAt,
        },
      });
      return completeDelivery(deliveryResult);
    },
    [
      makeDeliveryHelpers,
      setThreadInteractionMode,
      setThreadRuntimeMode,
      startTurn,
      updateThreadMetadata,
    ],
  );

  const sendQueuedCreation = useCallback(
    async (
      queuedMessage: QueuedThreadMessage,
      creation: QueuedThreadCreation,
      projectCwd: string,
    ) => {
      const modelSelection = queuedMessage.modelSelection;
      if (modelSelection === undefined) {
        return false;
      }
      const { completeDelivery } = makeDeliveryHelpers(queuedMessage);
      const deliveryResult = await startTurn({
        environmentId: queuedMessage.environmentId,
        input: buildProjectThreadStartTurnInput({
          projectId: creation.projectId,
          projectCwd,
          threadId: queuedMessage.threadId,
          commandId: queuedMessage.commandId,
          messageId: queuedMessage.messageId,
          createdAt: queuedMessage.createdAt,
          text: queuedMessage.text.trim(),
          attachments: queuedMessage.attachments,
          modelSelection,
          runtimeMode: queuedMessage.runtimeMode ?? DEFAULT_RUNTIME_MODE,
          interactionMode: queuedMessage.interactionMode ?? DEFAULT_PROVIDER_INTERACTION_MODE,
          workspaceMode: creation.workspaceMode,
          branch: creation.branch,
          worktreePath: creation.worktreePath,
          startFromOrigin: creation.startFromOrigin ?? false,
          worktreeBranchName: buildTemporaryWorktreeBranchName(randomHex),
        }),
      });
      return completeDelivery(deliveryResult);
    },
    [makeDeliveryHelpers, startTurn],
  );

  useEffect(() => {
    if (dispatchingQueuedMessageId !== null) {
      return;
    }

    for (const [threadKey, queuedMessages] of Object.entries(queuedMessagesByThreadKey)) {
      const nextQueuedMessage = queuedMessages[0];
      if (!nextQueuedMessage) {
        continue;
      }
      if (queuedMessageRequiresExplicitDiscard(nextQueuedMessage)) {
        continue;
      }
      if (editingQueuedMessageIds[nextQueuedMessage.messageId]) {
        continue;
      }
      if ((retryNotBeforeRef.current.get(nextQueuedMessage.messageId) ?? 0) > Date.now()) {
        continue;
      }

      const thread = findThread(threads, nextQueuedMessage);
      if (thread && scopedThreadKey(thread.environmentId, thread.id) !== threadKey) {
        continue;
      }

      const creation = nextQueuedMessage.creation;
      if (
        creation === undefined &&
        thread !== undefined &&
        queuedMessageBlockedByCapabilities(nextQueuedMessage, thread)
      ) {
        continue;
      }
      if (creation === undefined && thread !== undefined) {
        const externalResumeAction = resolveQueuedExternalResumeDrainAction(
          nextQueuedMessage,
          thread.backing?.kind === "external" ? thread.backing.control : undefined,
        );
        if (externalResumeAction === "wait") {
          continue;
        }
        if (externalResumeAction === "mark-needs-confirmation") {
          void markQueuedMessageNeedsTakeoverConfirmation(nextQueuedMessage);
          continue;
        }
      }
      const environment = connectedEnvironments.find(
        (candidate) => candidate.environmentId === nextQueuedMessage.environmentId,
      );
      const shellStatus = shellStatuses.get(nextQueuedMessage.environmentId) ?? "empty";
      const deliveryAction = resolveThreadOutboxDeliveryAction({
        isCreation: creation !== undefined,
        threadExists: thread !== undefined,
        shellStatus,
        environmentConnected: environment?.connectionState === "connected",
        threadBusy: thread?.session?.status === "running" || thread?.session?.status === "starting",
        isExternalPiThread: nextQueuedMessage.threadId.startsWith("external:pi:"),
      });
      if (deliveryAction === "wait") {
        continue;
      }
      // The live project shell is preferred for the workspace path, with the
      // snapshot taken at enqueue time as the fallback so a task never dies
      // just because its project shell is not loaded.
      const creationProjectCwd =
        creation !== undefined
          ? (findCreationProject(projects, nextQueuedMessage)?.workspaceRoot ??
            creation.projectCwd ??
            null)
          : null;
      // An incomplete pending task (e.g. worktree mode without a branch) stays
      // queued until the user finishes it in the editor.
      if (deliveryAction === "send" && creation !== undefined) {
        if (!isQueuedThreadCreationSendable(nextQueuedMessage)) {
          continue;
        }
        if (creationProjectCwd === null && shellStatus !== "live") {
          continue;
        }
      }

      beginDispatchingQueuedMessage(nextQueuedMessage.messageId);
      const removeQueuedMessage = (warning: string) =>
        removeThreadOutboxMessage(nextQueuedMessage).then(
          () => true,
          (error) => {
            console.warn(warning, {
              environmentId: nextQueuedMessage.environmentId,
              threadId: nextQueuedMessage.threadId,
              messageId: nextQueuedMessage.messageId,
              error,
            });
            return false;
          },
        );
      // Enqueues publish optimistically before their durable write settles.
      // Confirm the write landed (and the message wasn't rolled back) before
      // sending, so a failed write can never chase an already-delivered turn.
      const delivery = confirmThreadOutboxMessageQueued(nextQueuedMessage).then((queued) => {
        if (!queued) {
          // Rolled back by a failed write; nothing to deliver or retry.
          return true;
        }
        // The guards evaluated before the confirmation await are stale by now:
        // the user may have opened this message in the editor. Re-read that
        // guard and defer to the next drain pass (returning true skips the
        // failure/backoff path) rather than sending a payload being edited.
        if (appAtomRegistry.get(editingQueuedMessageIdsAtom)[nextQueuedMessage.messageId]) {
          return true;
        }
        const currentThread = appAtomRegistry.get(
          environmentThreadShells.threadShellAtom({
            environmentId: nextQueuedMessage.environmentId,
            threadId: nextQueuedMessage.threadId,
          }),
        );
        const currentShellStatus = appAtomRegistry.get(
          environmentShell.stateValueAtom(nextQueuedMessage.environmentId),
        ).status;
        const currentConnection = appAtomRegistry.get(
          environmentCatalog.stateAtom(nextQueuedMessage.environmentId),
        );
        const currentDeliveryAction = resolveThreadOutboxDeliveryAction({
          isCreation: creation !== undefined,
          threadExists: currentThread !== null,
          shellStatus: currentShellStatus,
          environmentConnected:
            AsyncResult.isSuccess(currentConnection) &&
            currentConnection.value.phase === "connected",
          threadBusy:
            currentThread?.session?.status === "running" ||
            currentThread?.session?.status === "starting",
          isExternalPiThread: nextQueuedMessage.threadId.startsWith("external:pi:"),
        });
        if (currentDeliveryAction === "wait") {
          return true;
        }
        if (
          creation === undefined &&
          currentThread !== null &&
          queuedMessageBlockedByCapabilities(nextQueuedMessage, currentThread)
        ) {
          return true;
        }
        if (creation === undefined) {
          const currentExternalResumeAction = resolveQueuedExternalResumeDrainAction(
            nextQueuedMessage,
            currentThread?.backing?.kind === "external" ? currentThread.backing.control : undefined,
          );
          if (currentExternalResumeAction === "wait") {
            return true;
          }
          if (currentExternalResumeAction === "mark-needs-confirmation") {
            return markQueuedMessageNeedsTakeoverConfirmation(nextQueuedMessage).then(() => true);
          }
        }
        return currentDeliveryAction === "remove"
          ? removeQueuedMessage("[thread-outbox] failed to remove message for a missing thread")
          : creation !== undefined
            ? creationProjectCwd !== null
              ? sendQueuedCreation(nextQueuedMessage, creation, creationProjectCwd)
              : removeQueuedMessage("[thread-outbox] dropped pending task for a missing project")
            : currentThread !== null
              ? sendQueuedMessage(nextQueuedMessage, currentThread)
              : Promise.resolve(false);
      });
      void delivery
        .then((sent) => {
          if (sent) {
            retryAttemptRef.current.delete(nextQueuedMessage.messageId);
            retryNotBeforeRef.current.delete(nextQueuedMessage.messageId);
            const pendingTimer = retryTimersRef.current.get(nextQueuedMessage.messageId);
            if (pendingTimer !== undefined) {
              clearTimeout(pendingTimer);
              retryTimersRef.current.delete(nextQueuedMessage.messageId);
            }
            return;
          }

          const retryAttempt = (retryAttemptRef.current.get(nextQueuedMessage.messageId) ?? 0) + 1;
          retryAttemptRef.current.set(nextQueuedMessage.messageId, retryAttempt);
          const retryDelayMs = threadOutboxRetryDelayMs(retryAttempt);
          retryNotBeforeRef.current.set(nextQueuedMessage.messageId, Date.now() + retryDelayMs);
          const pendingTimer = retryTimersRef.current.get(nextQueuedMessage.messageId);
          if (pendingTimer !== undefined) {
            clearTimeout(pendingTimer);
          }
          const retryTimer = setTimeout(() => {
            retryTimersRef.current.delete(nextQueuedMessage.messageId);
            setRetryTick((current) => current + 1);
          }, retryDelayMs);
          retryTimersRef.current.set(nextQueuedMessage.messageId, retryTimer);
        })
        .finally(() => {
          finishDispatchingQueuedMessage(nextQueuedMessage.messageId);
        });
      return;
    }
  }, [
    connectedEnvironments,
    dispatchingQueuedMessageId,
    editingQueuedMessageIds,
    projects,
    queuedMessagesByThreadKey,
    retryTick,
    sendQueuedCreation,
    sendQueuedMessage,
    shellStatuses,
    threads,
  ]);
}
