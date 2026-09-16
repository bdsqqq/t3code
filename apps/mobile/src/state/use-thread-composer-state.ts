import type { ComposerTextPaste } from "../native/T3ComposerEditor.types";
import { useAtomValue } from "@effect/atom-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Alert } from "react-native";
import * as Cause from "effect/Cause";

import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  MessageId,
  PROVIDER_SEND_TURN_MAX_ATTACHMENTS,
  PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  type EnvironmentId,
  type ModelSelection,
  type ProviderInteractionMode,
  type RuntimeMode,
  type ThreadId,
} from "@t3tools/contracts";
import { safeErrorLogAttributes } from "@t3tools/client-runtime/errors";
import { isAtomCommandInterrupted } from "@t3tools/client-runtime/state/runtime";
import { clampFileAttachmentUploadBytes } from "@t3tools/client-runtime/state/attachments";
import { nextPastedTextFileName, pastedTextDisposition } from "@t3tools/client-runtime/text-paste";
import {
  parseCodexFeedbackCommand,
  submitCodexFeedback,
  threadAllows,
  type CodexFeedbackSubmission,
} from "@t3tools/client-runtime/state/threads";
import { deriveActiveWorkStartedAt } from "@t3tools/shared/orchestrationTiming";
import { upgradeLegacyContextMessage } from "@t3tools/shared/composerContextLegacy";
import { composerContextSendBlockReason, reidentifyComposerContext } from "../lib/composerContext";
import { uuidv4 } from "../lib/uuid";

import { makeQueuedMessageMetadata } from "../lib/commandMetadata";
import { isModelSelectionUnavailable } from "../lib/modelOptions";
import { resolveProviderInteractionMode } from "../features/threads/legacy-plan-mode";
import {
  convertPastedImagesToAttachments,
  createPastedTextComposerAttachment,
  pasteComposerClipboard,
  pickComposerFiles,
  pickComposerMedia,
  removePersistedComposerAttachmentFile,
} from "../lib/composerImages";
import type { DraftComposerImageAttachment } from "../lib/composerImages";
import { scopedThreadKey } from "../lib/scopedEntities";
import { copyTextWithHaptic } from "../lib/copyTextWithHaptic";
import { buildThreadFeed } from "../lib/threadActivity";
import { acknowledgedThreadMessagesAtom } from "./acknowledged-thread-messages";
import { appendPendingThreadMessages } from "../features/threads/pending-thread-feed";
import { appAtomRegistry } from "../state/atom-registry";
import { pendingThreadCreationMessage } from "./pending-thread-creation";
import {
  appendComposerDraftAttachments,
  captureComposerDraftInsertion,
  countComposerDraftAttachmentsAfterSelection,
  insertComposerDraftText,
  insertComposerDraftContext,
  clearComposerDraftContent,
  composerDraftsAtom,
  composerContextImportsAtom,
  ensureComposerDraftsLoaded,
  getComposerDraftSnapshot,
  mergeComposerDraftContent,
  removeComposerDraftAttachment,
  scheduleUnusedComposerAttachmentCleanup,
  setComposerDraftText,
  updateComposerDraftSettings,
  useComposerDraft,
} from "./use-composer-drafts";
import { setPendingConnectionError } from "../state/use-remote-environment-registry";
import { useSelectedThreadDetail } from "../state/use-thread-detail";
import { useThreadSelection } from "../state/use-thread-selection";
import { enqueueThreadOutboxMessage, updateThreadOutboxMessage } from "./thread-outbox";
import { removeThreadOutboxMessage } from "./thread-outbox-removal";
import { dispatchingQueuedMessageIdAtom, useThreadOutboxMessages } from "./use-thread-outbox";
import { renewQueuedExternalResumeTakeover, threadComposerQueueCount } from "./thread-outbox-model";
import { environmentThreadShells, threadEnvironment } from "./threads";
import { useAtomCommand } from "./use-atom-command";
import {
  composerAttachmentUploadBlockReason,
  composerAttachmentUploadsAtom,
} from "./composer-attachment-uploads";

function confirmExternalPiTakeover(): Promise<boolean> {
  return new Promise((resolve) => {
    Alert.alert(
      "Resume on this host?",
      "T3 starts a new Pi writer on this environment's host using this transcript copy. This session may still be running on another host or in another terminal. T3 cannot detect those writers. Continue only after checking that it is safe to resume here.",
      [
        { text: "Cancel", style: "cancel", onPress: () => resolve(false) },
        { text: "Resume on this host", onPress: () => resolve(true) },
      ],
      { cancelable: true, onDismiss: () => resolve(false) },
    );
  });
}

export function appendReviewCommentToDraft(input: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly text: string;
  readonly attachments?: ReadonlyArray<DraftComposerImageAttachment>;
}): void {
  const threadKey = scopedThreadKey(input.environmentId, input.threadId);
  const thread = appAtomRegistry.get(
    environmentThreadShells.threadShellAtom({
      environmentId: input.environmentId,
      threadId: input.threadId,
    }),
  );
  const upgraded = upgradeLegacyContextMessage(input.text);
  if (
    !insertComposerDraftContext(
      threadKey,
      reidentifyComposerContext(upgraded.text, upgraded.records, uuidv4),
    )
  ) {
    Alert.alert("Too many context items", "Remove some context from the draft and try again.");
    return;
  }
  if (
    thread &&
    threadAllows(thread, "attachments") &&
    input.attachments &&
    input.attachments.length > 0
  ) {
    // Capped: a review comment is new content, not a send-failure restore, so
    // it must not push the draft over the send limit. Overflow is released.
    const rejectedCount = appendComposerDraftAttachments(threadKey, input.attachments, {
      appendReference: true,
    });
    if (rejectedCount > 0) {
      setPendingConnectionError(
        `${rejectedCount} comment attachment${rejectedCount === 1 ? " was" : "s were"} not added. Messages can contain at most ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS} attachments.`,
      );
    }
  }
}

export function useThreadDraftForThread(input: {
  readonly environmentId?: EnvironmentId;
  readonly threadId?: ThreadId;
}) {
  const threadKey =
    input.environmentId && input.threadId
      ? scopedThreadKey(input.environmentId, input.threadId)
      : null;
  const draft = useComposerDraft(threadKey);

  return {
    draftMessage: draft.text,
    draftAttachments: draft.attachments,
  };
}

export function useThreadComposerState() {
  const {
    selectedThread: selectedThreadShell,
    selectedThreadCreation,
    selectedEnvironmentRuntime,
  } = useThreadSelection();
  const selectedThreadDetail = useSelectedThreadDetail();
  const composerDrafts = useAtomValue(composerDraftsAtom);
  const acknowledgedMessages = useAtomValue(acknowledgedThreadMessagesAtom);
  const queuedMessagesByThreadKey = useThreadOutboxMessages();
  const dispatchingQueuedMessageId = useAtomValue(dispatchingQueuedMessageIdAtom);
  const [feedbackSubmissionsByThreadKey, setFeedbackSubmissionsByThreadKey] = useState<
    Record<string, ReadonlyArray<CodexFeedbackSubmission>>
  >({});
  const uploadThreadFeedback = useAtomCommand(threadEnvironment.uploadFeedback, {
    reportFailure: false,
  });
  const pastedTextFileNamesRef = useRef<{ threadKey: string | null; names: Set<string> }>({
    threadKey: null,
    names: new Set(),
  });
  const reservePastedTextFileName = useCallback(
    (threadKey: string, existingNames: ReadonlyArray<string>) => {
      if (pastedTextFileNamesRef.current.threadKey !== threadKey) {
        pastedTextFileNamesRef.current = { threadKey, names: new Set() };
      }
      const names = pastedTextFileNamesRef.current.names;
      for (const name of existingNames) names.add(name);
      const nextName = nextPastedTextFileName([...names]);
      names.add(nextName);
      return nextName;
    },
    [],
  );

  useEffect(() => {
    ensureComposerDraftsLoaded();
  }, []);

  const selectedThreadKey = selectedThreadShell
    ? scopedThreadKey(selectedThreadShell.environmentId, selectedThreadShell.id)
    : null;
  // The creation entry is the thread itself (rendered as the first message),
  // not a follow-up waiting behind it.
  const selectedThreadQueuedMessages = useMemo(
    () =>
      selectedThreadKey
        ? (queuedMessagesByThreadKey[selectedThreadKey] ?? []).filter(
            (message) => message.creation === undefined,
          )
        : [],
    [queuedMessagesByThreadKey, selectedThreadKey],
  );
  const feedbackSubmissions = useMemo(
    () => (selectedThreadKey ? (feedbackSubmissionsByThreadKey[selectedThreadKey] ?? []) : []),
    [feedbackSubmissionsByThreadKey, selectedThreadKey],
  );
  const dismissFeedback = useCallback(
    (id: MessageId) => {
      if (!selectedThreadKey) return;
      setFeedbackSubmissionsByThreadKey((current) => ({
        ...current,
        [selectedThreadKey]: (current[selectedThreadKey] ?? []).filter((entry) => entry.id !== id),
      }));
    },
    [selectedThreadKey],
  );
  const selectedThreadMessages = selectedThreadDetail?.messages;
  const selectedThreadActivities = selectedThreadDetail?.activities;
  // A thread whose creation has not delivered its turn yet: the prompt only
  // exists in the outbox, so it is appended to whatever the server has. The
  // detail is usually present but empty during a worktree checkout, so this
  // cannot be an either/or with the loaded messages.
  const pendingCreationMessage = selectedThreadCreation?.message ?? null;
  const selectedThreadFeed = useMemo(() => {
    const loadedMessages = selectedThreadMessages ?? [];
    const feed =
      (selectedThreadMessages && selectedThreadActivities) || pendingCreationMessage !== null
        ? buildThreadFeed({
            messages:
              pendingCreationMessage !== null &&
              !loadedMessages.some((message) => message.id === pendingCreationMessage.messageId)
                ? [...loadedMessages, pendingThreadCreationMessage(pendingCreationMessage)]
                : loadedMessages,
            activities: selectedThreadActivities ?? [],
          })
        : [];
    const pendingAcknowledgments = acknowledgedMessages.filter(
      (message) =>
        scopedThreadKey(message.environmentId, message.threadId) === selectedThreadKey &&
        !selectedThreadQueuedMessages.some((queued) => queued.messageId === message.messageId),
    );
    if (pendingAcknowledgments.length === 0) return feed;
    return appendPendingThreadMessages(feed, feed, pendingAcknowledgments).map((entry) =>
      entry.pendingMessage ? { ...entry, acknowledged: true } : entry,
    );
  }, [
    selectedThreadActivities,
    selectedThreadMessages,
    pendingCreationMessage,
    selectedThreadKey,
    selectedThreadQueuedMessages,
    acknowledgedMessages,
  ]);
  useEffect(() => {
    const echoedIds = new Set(selectedThreadMessages?.map((message) => message.id));
    if (acknowledgedMessages.some((message) => echoedIds.has(message.messageId))) {
      appAtomRegistry.set(
        acknowledgedThreadMessagesAtom,
        appAtomRegistry
          .get(acknowledgedThreadMessagesAtom)
          .filter((message) => !echoedIds.has(message.messageId)),
      );
    }
  }, [acknowledgedMessages, selectedThreadMessages]);

  const selectedDraft = selectedThreadKey ? composerDrafts[selectedThreadKey] : null;
  const draftMessage = selectedDraft?.text ?? "";
  const draftAttachments = selectedDraft?.attachments ?? [];
  const selectedThread = selectedThreadDetail ?? selectedThreadShell;
  const allowsAttachments = selectedThread !== null && threadAllows(selectedThread, "attachments");
  const selectedThreadQueueCount = threadComposerQueueCount({
    localCount: selectedThreadQueuedMessages.length,
    hasDetail: selectedThreadDetail !== null,
    detailIntentCount: selectedThreadDetail?.pendingComposerIntents?.length,
    detailOmittedCount: selectedThreadDetail?.pendingComposerIntentOmittedCount,
    shellIntentCount: selectedThreadShell?.pendingComposerIntentCount,
  });
  const selectedThreadIndeterminateQueueCount = selectedThreadQueuedMessages.filter(
    (message) => message.deliveryStatus === "indeterminate",
  ).length;
  const selectedThreadTakeoverConfirmationQueueCount = selectedThreadQueuedMessages.filter(
    (message) =>
      message.externalResume === "needsConfirmation" && message.deliveryStatus !== "indeterminate",
  ).length;
  const modelSelection = selectedDraft?.modelSelection ?? selectedThread?.modelSelection ?? null;
  const runtimeMode = selectedDraft?.runtimeMode ?? selectedThread?.runtimeMode ?? null;
  const selectedProvider = selectedEnvironmentRuntime?.serverConfig?.providers.find(
    (provider) => provider.instanceId === modelSelection?.instanceId,
  );
  const interactionMode = selectedThread
    ? resolveProviderInteractionMode(
        selectedProvider,
        selectedDraft?.interactionMode ?? selectedThread.interactionMode,
      )
    : null;

  const selectedThreadSessionActivity = useMemo(() => {
    const selectedThread = selectedThreadDetail ?? selectedThreadShell;
    if (!selectedThread?.session) {
      return null;
    }

    return {
      orchestrationStatus: selectedThread.session.status,
      activeTurnId: selectedThread.session.activeTurnId ?? undefined,
    };
  }, [selectedThreadDetail, selectedThreadShell]);

  const isCompacting = useMemo(() => {
    const queuedMessage = selectedThreadQueuedMessages.findLast(
      (message) =>
        message.messageId === dispatchingQueuedMessageId &&
        message.text.trim().toLowerCase() === "/compact" &&
        message.attachments.length === 0,
    );
    const latestCompactMessage = selectedThreadDetail?.messages.findLast(
      (message) =>
        message.role === "user" &&
        message.text.trim().toLowerCase() === "/compact" &&
        !message.attachments?.length,
    );
    const compactRequestIsActive =
      latestCompactMessage !== undefined &&
      (latestCompactMessage.createdAt >
        (selectedThread?.latestTurn?.requestedAt ?? latestCompactMessage.createdAt) ||
        (selectedThread?.latestTurn?.state === "running" &&
          latestCompactMessage.createdAt === selectedThread.latestTurn.requestedAt));
    const compactionSettled = selectedThreadDetail?.activities.some((activity) => {
      if (!["context-compaction", "provider.turn.start.failed"].includes(activity.kind))
        return false;
      const payload =
        typeof activity.payload === "object" && activity.payload !== null
          ? (activity.payload as { readonly requestId?: unknown })
          : null;
      return payload?.requestId === latestCompactMessage?.id;
    });
    return (
      queuedMessage !== undefined ||
      ((selectedThread?.session?.status === "starting" ||
        selectedThread?.session?.status === "running") &&
        compactRequestIsActive &&
        !compactionSettled)
    );
  }, [
    dispatchingQueuedMessageId,
    selectedThread,
    selectedThreadDetail,
    selectedThreadQueuedMessages,
  ]);

  const activeWorkStartedAt = useMemo(() => {
    const selectedThread = selectedThreadDetail ?? selectedThreadShell;
    if (!selectedThread) {
      return null;
    }

    return deriveActiveWorkStartedAt(
      selectedThread.latestTurn,
      selectedThreadSessionActivity,
      null,
    );
  }, [selectedThreadDetail, selectedThreadSessionActivity, selectedThreadShell]);

  const onSendMessage = useCallback(
    async (requestedBehavior?: "steer" | "followUp") => {
      if (!selectedThreadShell) {
        return null;
      }
      // The server has not created this thread yet. A follow-up against its
      // provisional id would be orphaned if creation is rejected.
      if (selectedThreadCreation !== null) {
        return null;
      }

      const threadKey = scopedThreadKey(selectedThreadShell.environmentId, selectedThreadShell.id);
      const draft = getComposerDraftSnapshot(threadKey);
      if (appAtomRegistry.get(composerContextImportsAtom)[threadKey]) return null;
      const thread = selectedThreadDetail ?? selectedThreadShell;
      const text = draft.text.trim();
      const attachments = draft.attachments;
      if (!threadAllows(thread, "send")) {
        return null;
      }
      if (attachments.length > 0 && !threadAllows(thread, "attachments")) {
        return null;
      }
      if (
        composerAttachmentUploadBlockReason({
          environmentId: selectedThreadShell.environmentId,
          attachments,
          connected: selectedEnvironmentRuntime?.connectionState === "connected",
          serverConfig: selectedEnvironmentRuntime?.serverConfig ?? null,
          states: appAtomRegistry.get(composerAttachmentUploadsAtom),
        }) !== null
      )
        return null;
      if (text.length === 0 && attachments.length === 0) {
        return null;
      }
      // A send-failure restore appends with allowOverflow so it never drops the
      // user's files, which can leave the draft over the cap. Sending it anyway
      // would enqueue a message that outbox recovery rejects forever, so block
      // here until the user removes attachments.
      if (attachments.length > PROVIDER_SEND_TURN_MAX_ATTACHMENTS) {
        Alert.alert(
          "Too many attachments",
          `Remove attachments until there are at most ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS}.`,
        );
        return null;
      }

      const contextBlockReason = composerContextSendBlockReason(draft.context);
      if (contextBlockReason) {
        Alert.alert("Too much context", contextBlockReason);
        return null;
      }

      const modelSelection = draft.modelSelection ?? thread.modelSelection;
      const serverConfig = selectedEnvironmentRuntime?.serverConfig;
      if (
        selectedEnvironmentRuntime?.connectionState === "connected" &&
        isModelSelectionUnavailable(serverConfig, modelSelection)
      ) {
        Alert.alert(
          "Antigravity model unavailable",
          "Set up Antigravity on web or desktop, or choose another model.",
        );
        return null;
      }
      const provider = serverConfig?.providers.find(
        (entry) => entry.instanceId === modelSelection.instanceId,
      );
      const isResumableExternalThread =
        thread.backing?.kind === "external" && thread.backing.control === "resumable";
      if (isResumableExternalThread && !(await confirmExternalPiTakeover())) {
        return null;
      }
      const externalResume = isResumableExternalThread ? ("takeover" as const) : undefined;
      const streamingBehavior =
        thread.backing?.kind === "external" && thread.session?.status === "running"
          ? (requestedBehavior ?? "steer")
          : undefined;
      if (streamingBehavior !== undefined && !threadAllows(thread, streamingBehavior)) {
        return null;
      }
      const feedbackCommand =
        attachments.length === 0 &&
        (provider?.driver === "codex" || thread.session?.providerName === "codex")
          ? parseCodexFeedbackCommand(text)
          : null;
      if (feedbackCommand) {
        if (thread.session === null) {
          Alert.alert("Start a Codex thread first", "Send a message before you submit feedback.");
          return null;
        }
        const metadata = makeQueuedMessageMetadata();
        const result = await submitCodexFeedback({
          submission: {
            id: MessageId.make(metadata.messageId),
            command: text,
            createdAt: metadata.createdAt,
          },
          clearDraft: () => clearComposerDraftContent(threadKey),
          onUpdate: (submission) => {
            setFeedbackSubmissionsByThreadKey((current) => {
              const existing = current[threadKey] ?? [];
              const found = existing.some((entry) => entry.id === submission.id);
              return {
                ...current,
                [threadKey]: found
                  ? existing.map((entry) => (entry.id === submission.id ? submission : entry))
                  : [...existing, submission],
              };
            });
          },
          upload: () =>
            uploadThreadFeedback({
              environmentId: selectedThreadShell.environmentId,
              input: {
                threadId: selectedThreadShell.id,
                ...feedbackCommand,
              },
            }),
        });
        if (result._tag === "Failure") {
          if (isAtomCommandInterrupted(result)) {
            return null;
          }
          const error = Cause.squash(result.cause);
          Alert.alert(
            "Could not send feedback to OpenAI",
            error instanceof Error ? error.message : "An error occurred.",
          );
          return null;
        }
        const feedbackId = result.value.feedbackId;
        Alert.alert("Feedback sent to OpenAI", `Thread ID: ${feedbackId}`, [
          { text: "OK", style: "cancel" },
          {
            text: "Copy ID",
            onPress: () => copyTextWithHaptic(feedbackId, { target: "Codex feedback thread ID" }),
          },
        ]);
        return null;
      }

      const metadata = makeQueuedMessageMetadata();
      const messageId = MessageId.make(metadata.messageId);
      // Enqueue publishes the queued atom synchronously (the durable write
      // happens behind it), so clearing the draft here gives send feedback on
      // the tap frame instead of after file I/O. If the write fails the message
      // is rolled out of the queue and the content is merged back into the
      // draft, preserving anything typed since.
      const enqueuePromise = enqueueThreadOutboxMessage({
        environmentId: selectedThreadShell.environmentId,
        threadId: selectedThreadShell.id,
        messageId,
        commandId: CommandId.make(metadata.commandId),
        text,
        attachments,
        context: draft.context,
        modelSelection,
        runtimeMode: draft.runtimeMode ?? thread.runtimeMode,
        interactionMode: resolveProviderInteractionMode(
          provider,
          draft.interactionMode ?? thread.interactionMode,
        ),
        ...(streamingBehavior === undefined ? {} : { streamingBehavior }),
        ...(externalResume === undefined ? {} : { externalResume }),
        createdAt: metadata.createdAt,
      });
      clearComposerDraftContent(threadKey, { deferAttachmentCleanup: true });
      enqueuePromise.then(
        () => {
          // The queued message owns the files now; the sweep sees that and
          // spares them. Deferred to here so a failed write cannot roll the
          // message out of the queue mid-sweep and lose the bytes.
          scheduleUnusedComposerAttachmentCleanup(attachments);
        },
        (error: unknown) => {
          // Restore text via merge (idempotent) but attachments via the uncapped
          // append: the merge path slots existing attachments first and truncates
          // at the send limit, which would silently drop this message's images if
          // the user attached new ones while the write was in flight.
          void mergeComposerDraftContent(threadKey, {
            text,
            context: draft.context,
            attachments: [],
          });
          appendComposerDraftAttachments(threadKey, attachments, { allowOverflow: true });
          setPendingConnectionError(
            error instanceof Error ? error.message : "Failed to save the queued message.",
          );
        },
      );
      return messageId;
    },
    [
      selectedEnvironmentRuntime?.connectionState,
      selectedEnvironmentRuntime?.serverConfig,
      selectedThreadCreation,
      selectedThreadDetail,
      selectedThreadShell,
      uploadThreadFeedback,
    ],
  );

  const onChangeDraftMessage = useCallback(
    (value: string) => {
      if (!selectedThreadShell) {
        return;
      }

      const threadKey = scopedThreadKey(selectedThreadShell.environmentId, selectedThreadShell.id);
      setComposerDraftText(threadKey, value);
    },
    [selectedThreadShell],
  );

  const onPickDraftMedia = useCallback(async () => {
    if (!selectedThreadShell || !allowsAttachments) {
      return;
    }

    const threadKey = scopedThreadKey(selectedThreadShell.environmentId, selectedThreadShell.id);
    const insertion = captureComposerDraftInsertion(threadKey);
    const capabilities = selectedEnvironmentRuntime?.serverConfig?.environment.capabilities;
    const result = await pickComposerMedia({
      existingCount: countComposerDraftAttachmentsAfterSelection(threadKey, insertion),
      maxVideoBytes:
        capabilities?.attachmentUploads === true
          ? capabilities.fileAttachments?.maxUploadBytes
          : undefined,
    });
    const rejectedCount = appendComposerDraftAttachments(threadKey, result.attachments, {
      appendReference: true,
      insertion,
    });
    const problems = [
      ...(result.error ? [result.error] : []),
      ...(rejectedCount > 0
        ? [`You can attach up to ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS} attachments per message.`]
        : []),
    ];
    if (problems.length > 0) {
      Alert.alert("Could not attach photo or video", problems.join("\n\n"));
    }
  }, [
    allowsAttachments,
    composerDrafts,
    selectedEnvironmentRuntime?.serverConfig,
    selectedThreadShell,
  ]);

  const onPickDraftFiles = useCallback(async () => {
    if (!selectedThreadShell || !allowsAttachments) {
      return;
    }
    const maxBytes =
      selectedEnvironmentRuntime?.serverConfig?.environment.capabilities.fileAttachments
        ?.maxUploadBytes;
    if (maxBytes === undefined) {
      Alert.alert("Could not attach file", "This server does not support file attachments.");
      return;
    }

    const threadKey = scopedThreadKey(selectedThreadShell.environmentId, selectedThreadShell.id);
    const insertion = captureComposerDraftInsertion(threadKey);
    // pickComposerFiles clamps the advertised limit to the contract maximum.
    const result = await pickComposerFiles({
      existingCount: countComposerDraftAttachmentsAfterSelection(threadKey, insertion),
      maxBytes,
    });
    const rejectedCount = appendComposerDraftAttachments(threadKey, result.files, {
      appendReference: true,
      insertion,
    });
    // The picker error and the live-cap rejection can both happen in one
    // pick; report both in a single alert.
    const problems = [
      ...(result.error ? [result.error] : []),
      ...(rejectedCount > 0
        ? [`You can attach up to ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS} files per message.`]
        : []),
    ];
    if (problems.length > 0) {
      Alert.alert("Could not attach file", problems.join("\n\n"));
    }
  }, [
    allowsAttachments,
    composerDrafts,
    selectedEnvironmentRuntime?.serverConfig,
    selectedThreadShell,
  ]);

  const onPasteIntoDraft = useCallback(async () => {
    if (!selectedThreadShell) {
      return;
    }

    const threadKey = scopedThreadKey(selectedThreadShell.environmentId, selectedThreadShell.id);
    const insertion = captureComposerDraftInsertion(threadKey);
    const result = await pasteComposerClipboard({
      existingCount: countComposerDraftAttachmentsAfterSelection(threadKey, insertion),
    });
    const rejectedPasteCount = allowsAttachments
      ? appendComposerDraftAttachments(threadKey, result.images, {
          appendReference: true,
          insertion,
        })
      : 0;
    if (result.text) {
      const currentDraft = getComposerDraftSnapshot(threadKey);
      const currentAttachments = currentDraft.attachments;
      const capabilities = selectedEnvironmentRuntime?.serverConfig?.environment.capabilities;
      const advertisedMax =
        capabilities?.attachmentUploads === true
          ? capabilities.fileAttachments?.maxUploadBytes
          : undefined;
      const maxBytes =
        advertisedMax === undefined ? null : clampFileAttachmentUploadBytes(advertisedMax);
      const wouldExceedInputLimit =
        currentDraft.text.length -
          (currentDraft.text === insertion.text
            ? Math.max(0, insertion.end - insertion.start)
            : 0) +
          result.text.length >
        PROVIDER_SEND_TURN_MAX_INPUT_CHARS;
      const shouldFold =
        pastedTextDisposition({
          text: result.text,
          wouldExceedInputLimit,
          canAttach: true,
        }) === "attachment";
      const canAttach =
        allowsAttachments &&
        maxBytes !== null &&
        countComposerDraftAttachmentsAfterSelection(threadKey, insertion) <
          PROVIDER_SEND_TURN_MAX_ATTACHMENTS &&
        new TextEncoder().encode(result.text).byteLength <= maxBytes;
      if (shouldFold && canAttach && maxBytes !== null) {
        try {
          const attachment = await createPastedTextComposerAttachment({
            text: result.text,
            name: reservePastedTextFileName(
              threadKey,
              currentAttachments.map((item) => item.name),
            ),
            maxBytes,
          });
          // Same reference the pasted images above get: a folded paste is only visible
          // as its chip until the message is sent.
          if (
            appendComposerDraftAttachments(threadKey, [attachment], {
              appendReference: true,
              insertion,
            }) > 0
          ) {
            await removePersistedComposerAttachmentFile(attachment.fileUri);
            setPendingConnectionError(
              `You can attach up to ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS} files per message.`,
            );
          }
        } catch (error) {
          setPendingConnectionError(
            error instanceof Error ? error.message : "Could not attach pasted text.",
          );
        }
      } else if (shouldFold && !wouldExceedInputLimit) {
        insertComposerDraftText(threadKey, result.text, insertion);
      } else if (shouldFold) {
        setPendingConnectionError(
          wouldExceedInputLimit
            ? "Pasted text is too large for this message. Remove some text or an attachment, then paste again."
            : "Could not attach pasted text. Remove an attachment or use a smaller paste, then try again.",
        );
      } else {
        insertComposerDraftText(threadKey, result.text, insertion);
      }
    }
    if (result.error) {
      setPendingConnectionError(result.error);
    } else if (rejectedPasteCount > 0) {
      setPendingConnectionError(
        `You can attach up to ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS} files per message.`,
      );
    }
  }, [
    allowsAttachments,
    composerDrafts,
    reservePastedTextFileName,
    selectedEnvironmentRuntime?.serverConfig,
    selectedThreadShell,
  ]);

  const onNativePasteImages = useCallback(
    async (uris: ReadonlyArray<string>) => {
      if (!selectedThreadShell || !allowsAttachments || uris.length === 0) {
        return;
      }

      const threadKey = scopedThreadKey(selectedThreadShell.environmentId, selectedThreadShell.id);
      const insertion = captureComposerDraftInsertion(threadKey);
      try {
        const images = await convertPastedImagesToAttachments({
          uris,
          existingCount: countComposerDraftAttachmentsAfterSelection(threadKey, insertion),
        });
        if (images.length > 0) {
          appendComposerDraftAttachments(threadKey, images, { appendReference: true, insertion });
        }
      } catch (error) {
        console.error("[native paste] error converting images", {
          environmentId: selectedThreadShell.environmentId,
          threadId: selectedThreadShell.id,
          uriCount: uris.length,
          ...safeErrorLogAttributes(error),
        });
      }
    },
    [allowsAttachments, composerDrafts, selectedThreadShell],
  );

  const onNativePasteText = useCallback(
    async (paste: ComposerTextPaste) => {
      if (!selectedThreadShell || !allowsAttachments) return;
      const capabilities = selectedEnvironmentRuntime?.serverConfig?.environment.capabilities;
      const advertisedMax =
        capabilities?.attachmentUploads === true
          ? capabilities.fileAttachments?.maxUploadBytes
          : undefined;
      if (advertisedMax === undefined) return;

      const threadKey = scopedThreadKey(selectedThreadShell.environmentId, selectedThreadShell.id);
      const insertion = { text: paste.value, ...paste.selection };
      const currentAttachments = getComposerDraftSnapshot(threadKey).attachments;
      try {
        const attachment = await createPastedTextComposerAttachment({
          text: paste.text,
          name: reservePastedTextFileName(
            threadKey,
            currentAttachments.map((item) => item.name),
          ),
          maxBytes: clampFileAttachmentUploadBytes(advertisedMax),
        });
        // The chip is how a folded paste stays visible: without it the attachment is in the
        // draft but nothing in the composer says so until the message is sent. Web folds
        // through its ordinary attach path, which always writes a reference; match that.
        const rejectedCount = appendComposerDraftAttachments(threadKey, [attachment], {
          appendReference: true,
          insertion,
        });
        if (rejectedCount > 0) {
          await removePersistedComposerAttachmentFile(attachment.fileUri);
          setPendingConnectionError(
            `You can attach up to ${PROVIDER_SEND_TURN_MAX_ATTACHMENTS} files per message.`,
          );
        }
      } catch (error) {
        setPendingConnectionError(
          error instanceof Error ? error.message : "Could not attach pasted text.",
        );
      }
    },
    [
      allowsAttachments,
      composerDrafts,
      reservePastedTextFileName,
      selectedEnvironmentRuntime?.serverConfig,
      selectedThreadShell,
    ],
  );

  const onRemoveDraftImage = useCallback(
    (imageId: string) => {
      if (!selectedThreadShell) {
        return;
      }

      const threadKey = scopedThreadKey(selectedThreadShell.environmentId, selectedThreadShell.id);
      removeComposerDraftAttachment(threadKey, imageId);
    },
    [selectedThreadShell],
  );

  const onDiscardIndeterminateMessages = useCallback(async () => {
    await Promise.all(
      selectedThreadQueuedMessages
        .filter((message) => message.deliveryStatus === "indeterminate")
        .map((message) => removeThreadOutboxMessage(message)),
    );
  }, [selectedThreadQueuedMessages]);

  const onReviewQueuedExternalResumeMessages = useCallback(async () => {
    const messages = selectedThreadQueuedMessages.filter(
      (message) =>
        message.externalResume === "needsConfirmation" &&
        message.deliveryStatus !== "indeterminate",
    );
    if (messages.length === 0 || !(await confirmExternalPiTakeover())) {
      return;
    }
    try {
      await Promise.all(
        messages.map((message) =>
          updateThreadOutboxMessage(
            renewQueuedExternalResumeTakeover(message, CommandId.make(uuidv4())),
          ),
        ),
      );
    } catch (error) {
      setPendingConnectionError(
        error instanceof Error
          ? error.message
          : "Failed to save takeover confirmation for the queued message.",
      );
    }
  }, [selectedThreadQueuedMessages]);

  const onUpdateModelSelection = useCallback(
    (value: ModelSelection) => {
      if (!selectedThreadKey) {
        return;
      }
      const provider = selectedEnvironmentRuntime?.serverConfig?.providers.find(
        (candidate) => candidate.instanceId === value.instanceId,
      );
      updateComposerDraftSettings(selectedThreadKey, {
        modelSelection: value,
        ...(provider?.showInteractionModeToggle === false
          ? { interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE }
          : {}),
      });
    },
    [selectedEnvironmentRuntime?.serverConfig, selectedThreadKey],
  );

  const onUpdateRuntimeMode = useCallback(
    (value: RuntimeMode) => {
      if (!selectedThreadKey) {
        return;
      }
      updateComposerDraftSettings(selectedThreadKey, { runtimeMode: value });
    },
    [selectedThreadKey],
  );

  const onUpdateInteractionMode = useCallback(
    (value: ProviderInteractionMode) => {
      if (!selectedThreadKey) {
        return;
      }
      const modelSelection =
        getComposerDraftSnapshot(selectedThreadKey).modelSelection ??
        selectedThread?.modelSelection;
      const provider = selectedEnvironmentRuntime?.serverConfig?.providers.find(
        (candidate) => candidate.instanceId === modelSelection?.instanceId,
      );
      updateComposerDraftSettings(selectedThreadKey, {
        interactionMode: resolveProviderInteractionMode(provider, value),
      });
    },
    [selectedEnvironmentRuntime?.serverConfig, selectedThread?.modelSelection, selectedThreadKey],
  );

  return {
    selectedThread,
    feedbackSubmissions,
    dismissFeedback,
    selectedThreadFeed,
    selectedThreadQueueCount,
    selectedThreadIndeterminateQueueCount,
    selectedThreadTakeoverConfirmationQueueCount,
    selectedThreadQueuedMessages,
    dispatchingQueuedMessageId,
    activeWorkStartedAt,
    isCompacting,
    draftMessage,
    draftAttachments,
    modelSelection,
    runtimeMode,
    interactionMode,
    onChangeDraftMessage,
    onPickDraftMedia,
    onPickDraftFiles,
    onPasteIntoDraft,
    onNativePasteImages,
    onNativePasteText,
    onRemoveDraftImage,
    onDiscardIndeterminateMessages,
    onReviewQueuedExternalResumeMessages,
    onSendMessage,
    onUpdateModelSelection,
    onUpdateRuntimeMode,
    onUpdateInteractionMode,
  };
}
