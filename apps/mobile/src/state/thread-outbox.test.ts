import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { AtomRegistry } from "effect/unstable/reactivity";

import {
  decodeQueuedThreadMessage,
  encodeQueuedThreadMessage,
  groupQueuedThreadMessages,
  isQueuedThreadCreationSendable,
  modelSelectionsEqual,
  queuedExternalResumeForWire,
  queuedMessageRequiresExplicitDiscard,
  renewQueuedExternalResumeTakeover,
  resolveCapabilityAllowedQueuedThreadSettings,
  resolveQueuedExternalResumeDrainAction,
  resolveThreadOutboxDeliveryAction,
  resolveThreadOutboxDeliverySuccessAction,
  resolveThreadOutboxFailureAction,
  resolveQueuedThreadSettings,
  shouldRetryThreadOutboxDelivery,
  threadOutboxRetryDelayMs,
  waitsForQueuedThreadVisibility,
  newTaskTargetRequiresProvider,
  queuedMessageBlockedByCapabilities,
  threadComposerAllowsSend,
  threadComposerQueueCount,
  type QueuedThreadMessage,
} from "./thread-outbox-model";
import { createThreadOutboxManager, ThreadOutboxManagerError } from "./thread-outbox-manager";
import type { ThreadOutboxStorage } from "./thread-outbox-storage";

function queuedMessage(input: {
  readonly environmentId?: string;
  readonly threadId?: string;
  readonly messageId: string;
  readonly createdAt: string;
}): QueuedThreadMessage {
  return {
    environmentId: EnvironmentId.make(input.environmentId ?? "environment-1"),
    threadId: ThreadId.make(input.threadId ?? "thread-1"),
    messageId: MessageId.make(input.messageId),
    commandId: CommandId.make(`command-${input.messageId}`),
    text: input.messageId,
    attachments: [],
    createdAt: input.createdAt,
  };
}

describe("thread outbox", () => {
  it("includes omitted remote intents in the detail queue count", () => {
    expect(
      threadComposerQueueCount({
        localCount: 1,
        hasDetail: true,
        detailIntentCount: 2,
        detailOmittedCount: 3,
        shellIntentCount: 99,
      }),
    ).toBe(6);
  });

  it("retains a native first prompt until its new thread shell is visible", () => {
    expect(waitsForQueuedThreadVisibility({ awaitThreadVisibility: true }, false)).toBe(true);
    expect(waitsForQueuedThreadVisibility({ awaitThreadVisibility: true }, true)).toBe(false);
  });

  it("does not require an internal provider for the native Pi target", () => {
    expect(newTaskTargetRequiresProvider("pi")).toBe(false);
    expect(newTaskTargetRequiresProvider("t3")).toBe(true);
  });

  it("rejects editor submission for read-only external sessions", () => {
    const thread = {
      backing: {
        kind: "external",
        source: "pi",
        sourceKey: "opaque",
        control: "readOnly",
        capabilities: {
          send: false,
          attachments: false,
          streamingBehaviors: [],
          interrupt: false,
          stop: false,
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
      },
    } as never;
    expect(threadComposerAllowsSend(thread, true)).toBe(false);
    expect(queuedMessageBlockedByCapabilities({ attachments: [] }, thread)).toBe(true);
  });

  it("requires foreground confirmation only while an external Pi thread is resumable", () => {
    expect(resolveQueuedExternalResumeDrainAction({}, "resumable")).toBe("mark-needs-confirmation");
    expect(
      resolveQueuedExternalResumeDrainAction({ externalResume: "needsConfirmation" }, "resumable"),
    ).toBe("wait");
    expect(
      resolveQueuedExternalResumeDrainAction({ externalResume: "takeover" }, "resumable"),
    ).toBe("send");
    expect(
      resolveQueuedExternalResumeDrainAction({ externalResume: "needsConfirmation" }, "live"),
    ).toBe("send");
    expect(queuedExternalResumeForWire({ externalResume: "needsConfirmation" })).toBeUndefined();
    expect(queuedExternalResumeForWire({ externalResume: "takeover" })).toBe("takeover");
  });

  it("blocks indeterminate successes instead of removing or retrying them", () => {
    expect(resolveThreadOutboxDeliverySuccessAction("indeterminate")).toBe("mark-indeterminate");
    expect(queuedMessageRequiresExplicitDiscard({ deliveryStatus: "indeterminate" })).toBe(true);
    expect(resolveThreadOutboxDeliverySuccessAction("completed")).toBe("remove");
    expect(resolveThreadOutboxDeliverySuccessAction(undefined)).toBe("remove");
  });

  it("defers queued actions until current external capabilities allow them", () => {
    const thread = {
      session: { status: "idle" },
      backing: {
        kind: "external",
        source: "pi",
        sourceKey: "opaque",
        control: "live",
        capabilities: {
          send: true,
          attachments: false,
          streamingBehaviors: [],
          interrupt: false,
          stop: true,
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
      },
    };

    expect(
      queuedMessageBlockedByCapabilities(
        { attachments: [], streamingBehavior: "followUp" },
        thread as never,
      ),
    ).toBe(false);
    expect(
      queuedMessageBlockedByCapabilities({ attachments: [{} as never] }, thread as never),
    ).toBe(true);
    expect(
      queuedMessageBlockedByCapabilities({ attachments: [], streamingBehavior: "followUp" }, {
        ...thread,
        session: { status: "running" },
      } as never),
    ).toBe(true);
  });

  it("groups messages by scoped thread and preserves creation order", () => {
    const later = queuedMessage({
      messageId: "message-2",
      createdAt: "2026-06-08T10:00:02.000Z",
    });
    const earlier = queuedMessage({
      messageId: "message-1",
      createdAt: "2026-06-08T10:00:01.000Z",
    });

    expect(groupQueuedThreadMessages([later, earlier])).toEqual({
      "environment-1:thread-1": [earlier, later],
    });
  });

  it("decodes the persisted schema and rejects incomplete messages", () => {
    const message = queuedMessage({
      messageId: "message-1",
      createdAt: "2026-06-08T10:00:01.000Z",
    });

    expect(
      decodeQueuedThreadMessage({
        schemaVersion: 1,
        ...message,
      }),
    ).toEqual(message);
    expect(() =>
      decodeQueuedThreadMessage({
        schemaVersion: 1,
        environmentId: "environment-1",
      }),
    ).toThrow();
  });

  it("round-trips queued local state and decodes pre-state v7 messages", () => {
    const legacyMessage = queuedMessage({
      messageId: "message-1",
      createdAt: "2026-06-08T10:00:01.000Z",
    });
    const selectedMessage = {
      ...legacyMessage,
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.4",
        options: [{ id: "reasoningEffort", value: "xhigh" }],
      },
      runtimeMode: "approval-required",
      interactionMode: "plan",
      streamingBehavior: "followUp",
      externalResume: "takeover",
      deliveryStatus: "indeterminate",
    } satisfies QueuedThreadMessage;

    expect(encodeQueuedThreadMessage(selectedMessage)).toMatchObject({
      schemaVersion: 8,
      externalResume: "takeover",
    });
    expect(decodeQueuedThreadMessage(encodeQueuedThreadMessage(selectedMessage))).toEqual(
      selectedMessage,
    );
    expect(
      decodeQueuedThreadMessage({
        schemaVersion: 7,
        ...legacyMessage,
      }),
    ).toEqual(legacyMessage);
    expect(
      decodeQueuedThreadMessage(
        encodeQueuedThreadMessage({
          ...legacyMessage,
          externalResume: "needsConfirmation",
        }),
      ),
    ).toEqual({
      ...legacyMessage,
      externalResume: "needsConfirmation",
    });
    expect(
      resolveQueuedThreadSettings(legacyMessage, {
        modelSelection: selectedMessage.modelSelection,
        runtimeMode: selectedMessage.runtimeMode,
        interactionMode: selectedMessage.interactionMode,
      }),
    ).toEqual({
      modelSelection: selectedMessage.modelSelection,
      runtimeMode: selectedMessage.runtimeMode,
      interactionMode: selectedMessage.interactionMode,
    });
  });

  it("keeps current settings when external Pi capabilities disallow queued selectors", () => {
    const message = {
      ...queuedMessage({
        messageId: "message-1",
        createdAt: "2026-06-08T10:00:01.000Z",
      }),
      modelSelection: {
        instanceId: ProviderInstanceId.make("pi"),
        model: "queued-model",
      },
      runtimeMode: "approval-required",
      interactionMode: "plan",
    } satisfies QueuedThreadMessage;
    const thread = {
      modelSelection: {
        instanceId: ProviderInstanceId.make("pi"),
        model: "current-model",
      },
      runtimeMode: "full-access",
      interactionMode: "default",
      backing: {
        kind: "external",
        source: "pi",
        sourceKey: "opaque",
        control: "resumable",
        capabilities: {
          send: true,
          attachments: false,
          streamingBehaviors: [],
          interrupt: false,
          stop: false,
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
      },
    };

    expect(resolveCapabilityAllowedQueuedThreadSettings(message, thread as never)).toEqual({
      modelSelection: thread.modelSelection,
      runtimeMode: thread.runtimeMode,
      interactionMode: thread.interactionMode,
    });
  });

  it("compares model options as part of the queued settings change", () => {
    const base = {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.4",
      options: [{ id: "reasoningEffort", value: "medium" }],
    } as const;

    expect(modelSelectionsEqual(base, base)).toBe(true);
    expect(
      modelSelectionsEqual(base, {
        ...base,
        options: [{ id: "reasoningEffort", value: "xhigh" }],
      }),
    ).toBe(false);
  });

  it("backs off queued delivery retries and caps them at sixteen seconds", () => {
    expect([1, 2, 3, 4, 5, 6].map(threadOutboxRetryDelayMs)).toEqual([
      1_000, 2_000, 4_000, 8_000, 16_000, 16_000,
    ]);
  });

  it("turns a server-side takeover race into local confirmation state", () => {
    expect(
      resolveThreadOutboxFailureAction({
        stage: "start-turn",
        error: { code: "takeover_confirmation_required" },
        interrupted: false,
      }),
    ).toBe("needs-confirmation");
  });

  it("returns a rejected confirmed takeover to review without retrying other rejections", () => {
    expect(
      resolveThreadOutboxFailureAction({
        stage: "start-turn",
        error: { code: "command_rejected" },
        interrupted: false,
        externalResume: "takeover",
      }),
    ).toBe("needs-confirmation");
    expect(
      resolveThreadOutboxFailureAction({
        stage: "start-turn",
        error: { code: "command_rejected" },
        interrupted: false,
      }),
    ).toBe("discard");
    expect(
      resolveThreadOutboxFailureAction({
        stage: "settings-sync",
        error: { code: "command_rejected" },
        interrupted: false,
      }),
    ).toBe("retry");
  });

  it("renews only the command id when a queued takeover is reconfirmed", () => {
    const base = queuedMessage({
      messageId: "message-1",
      createdAt: "2026-06-08T10:00:01.000Z",
    });
    const message = { ...base, attachments: [{} as never] };
    const renewed = renewQueuedExternalResumeTakeover(
      { ...message, externalResume: "needsConfirmation" },
      CommandId.make("fresh-command"),
    );

    expect(renewed).toEqual({
      ...message,
      commandId: CommandId.make("fresh-command"),
      externalResume: "takeover",
    });
    expect(renewed.messageId).toBe(message.messageId);
    expect(renewed.text).toBe(message.text);
    expect(renewed.attachments).toBe(message.attachments);
  });

  it("returns external Pi read-only races to confirmation while preserving legacy retries", () => {
    expect(
      resolveThreadOutboxFailureAction({
        stage: "start-turn",
        error: { code: "read_only" },
        interrupted: false,
        externalResume: "takeover",
      }),
    ).toBe("needs-confirmation");
    expect(
      resolveThreadOutboxFailureAction({
        stage: "start-turn",
        error: { code: "read_only" },
        interrupted: false,
        externalResume: "needsConfirmation",
      }),
    ).toBe("needs-confirmation");
    expect(
      resolveThreadOutboxFailureAction({
        stage: "start-turn",
        error: { code: "read_only" },
        interrupted: false,
      }),
    ).toBe("retry");
  });

  it("retains a queued command while the Pi supervisor requires an upgrade", () => {
    expect(
      resolveThreadOutboxFailureAction({
        stage: "start-turn",
        error: { code: "supervisor_upgrade_required" },
        interrupted: false,
      }),
    ).toBe("retry");
  });

  it("retains prompts rejected by temporary external runtime state", () => {
    expect(
      resolveThreadOutboxFailureAction({
        stage: "start-turn",
        error: { code: "runtime_starting" },
        interrupted: false,
      }),
    ).toBe("retry");
    expect(
      resolveThreadOutboxFailureAction({
        stage: "start-turn",
        error: { code: "streaming_behavior_required" },
        interrupted: false,
      }),
    ).toBe("retry");
    expect(
      resolveThreadOutboxFailureAction({
        stage: "start-turn",
        error: { code: "supervisor" },
        interrupted: false,
      }),
    ).toBe("retry");
  });

  it("serializes mutations even when an earlier mutation is slower", async () => {
    const registry = AtomRegistry.make();
    const manager = createThreadOutboxManager({
      registry,
      storage: {
        load: async () => [],
        write: async () => undefined,
        remove: async () => undefined,
      },
    });
    const order: string[] = [];
    let releaseFirst!: () => void;
    const firstBlocked = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });

    const first = manager.serialize(async () => {
      order.push("first:start");
      await firstBlocked;
      order.push("first:end");
    });
    const second = manager.serialize(async () => {
      order.push("second");
    });

    await Promise.resolve();
    expect(order).toEqual(["first:start"]);
    releaseFirst();
    await Promise.all([first, second]);
    expect(order).toEqual(["first:start", "first:end", "second"]);
    registry.dispose();
  });

  it("holds the mutation queue while persisted messages are loading", async () => {
    const registry = AtomRegistry.make();
    const message = queuedMessage({
      messageId: "message-1",
      createdAt: "2026-06-08T10:00:01.000Z",
    });
    const stored = new Map([[message.messageId, message]]);
    let loadCalls = 0;
    let removeCalls = 0;
    let releaseInitialLoad!: () => void;
    const initialLoadBlocked = new Promise<void>((resolve) => {
      releaseInitialLoad = resolve;
    });
    const storage: ThreadOutboxStorage = {
      load: async () => {
        loadCalls += 1;
        if (loadCalls === 1) {
          await initialLoadBlocked;
        }
        return [...stored.values()];
      },
      write: async () => undefined,
      remove: async (candidate) => {
        removeCalls += 1;
        stored.delete(candidate.messageId);
      },
    };
    const manager = createThreadOutboxManager({ registry, storage });

    const loading = manager.load();
    await Promise.resolve();
    const clearing = manager.clearEnvironment(message.environmentId);
    await Promise.resolve();
    await Promise.resolve();

    expect(loadCalls).toBe(1);
    expect(removeCalls).toBe(0);

    releaseInitialLoad();
    await Promise.all([loading, clearing]);
    expect(registry.get(manager.queuedMessagesByThreadKeyAtom)).toEqual({});
    registry.dispose();
  });

  it("reports structured load failures and permits a retry", async () => {
    const registry = AtomRegistry.make();
    const loadCause = new Error("storage unavailable");
    const warnings: Array<{ message: string; error: unknown }> = [];
    let loadCalls = 0;
    const manager = createThreadOutboxManager({
      registry,
      storage: {
        load: async () => {
          loadCalls += 1;
          if (loadCalls === 1) throw loadCause;
          return [];
        },
        write: async () => undefined,
        remove: async () => undefined,
      },
      warn: (message, error) => warnings.push({ message, error }),
    });

    await manager.load();
    expect(warnings).toEqual([
      {
        message: "[thread-outbox] failed to load persisted messages",
        error: new ThreadOutboxManagerError({
          operation: "load",
          environmentId: null,
          threadId: null,
          messageId: null,
          cause: loadCause,
        }),
      },
    ]);

    await manager.load();
    expect(loadCalls).toBe(2);
    registry.dispose();
  });

  it("keeps atom state aligned with durable writes and removals", async () => {
    const registry = AtomRegistry.make();
    const stored = new Map<MessageId, QueuedThreadMessage>();
    const removalCause = new Error("remove failed");
    let failRemoval = true;
    const storage: ThreadOutboxStorage = {
      load: async () => [...stored.values()],
      write: async (message) => {
        stored.set(message.messageId, message);
      },
      remove: async (message) => {
        if (failRemoval) {
          throw removalCause;
        }
        stored.delete(message.messageId);
      },
    };
    const manager = createThreadOutboxManager({ registry, storage });
    const message = queuedMessage({
      messageId: "message-1",
      createdAt: "2026-06-08T10:00:01.000Z",
    });

    await manager.enqueue(message);
    expect(registry.get(manager.queuedMessagesByThreadKeyAtom)).toEqual({
      "environment-1:thread-1": [message],
    });

    await expect(manager.remove(message)).rejects.toEqual(
      new ThreadOutboxManagerError({
        operation: "remove",
        environmentId: message.environmentId,
        threadId: message.threadId,
        messageId: message.messageId,
        cause: removalCause,
      }),
    );
    expect(registry.get(manager.queuedMessagesByThreadKeyAtom)).toEqual({
      "environment-1:thread-1": [message],
    });

    failRemoval = false;
    await manager.remove(message);
    expect(registry.get(manager.queuedMessagesByThreadKeyAtom)).toEqual({});
    registry.dispose();
  });

  it("publishes an enqueued message before the durable write resolves", async () => {
    const registry = AtomRegistry.make();
    let releaseWrite!: () => void;
    const writeBlocked = new Promise<void>((resolve) => {
      releaseWrite = resolve;
    });
    const manager = createThreadOutboxManager({
      registry,
      storage: {
        load: async () => [],
        write: async () => writeBlocked,
        remove: async () => undefined,
      },
    });
    const message = queuedMessage({
      messageId: "message-1",
      createdAt: "2026-06-08T10:00:01.000Z",
    });

    const enqueueing = manager.enqueue(message);
    expect(registry.get(manager.queuedMessagesByThreadKeyAtom)).toEqual({
      "environment-1:thread-1": [message],
    });

    releaseWrite();
    await enqueueing;
    expect(registry.get(manager.queuedMessagesByThreadKeyAtom)).toEqual({
      "environment-1:thread-1": [message],
    });
    registry.dispose();
  });

  it("rolls an enqueued message back out when the durable write fails", async () => {
    const registry = AtomRegistry.make();
    const writeCause = new Error("disk full");
    const manager = createThreadOutboxManager({
      registry,
      storage: {
        load: async () => [],
        write: async () => {
          throw writeCause;
        },
        remove: async () => undefined,
      },
    });
    const message = queuedMessage({
      messageId: "message-1",
      createdAt: "2026-06-08T10:00:01.000Z",
    });

    await expect(manager.enqueue(message)).rejects.toEqual(
      new ThreadOutboxManagerError({
        operation: "enqueue",
        environmentId: message.environmentId,
        threadId: message.threadId,
        messageId: message.messageId,
        cause: writeCause,
      }),
    );
    expect(registry.get(manager.queuedMessagesByThreadKeyAtom)).toEqual({});
    registry.dispose();
  });

  it("keeps an immediate indeterminate block when its durable update fails", async () => {
    const registry = AtomRegistry.make();
    let failWrite = false;
    const manager = createThreadOutboxManager({
      registry,
      storage: {
        load: async () => [],
        write: async () => {
          if (failWrite) {
            throw new Error("disk full");
          }
        },
        remove: async () => undefined,
      },
    });
    const message = queuedMessage({
      messageId: "message-1",
      createdAt: "2026-06-08T10:00:01.000Z",
    });
    const indeterminateMessage = {
      ...message,
      deliveryStatus: "indeterminate" as const,
    };

    await manager.enqueue(message);
    registry.set(manager.queuedMessagesByThreadKeyAtom, {
      "environment-1:thread-1": [indeterminateMessage],
    });
    failWrite = true;

    await expect(manager.update(indeterminateMessage)).rejects.toBeInstanceOf(
      ThreadOutboxManagerError,
    );
    expect(registry.get(manager.queuedMessagesByThreadKeyAtom)).toEqual({
      "environment-1:thread-1": [indeterminateMessage],
    });
    expect(indeterminateMessage.commandId).toBe(message.commandId);
    expect(indeterminateMessage.text).toBe(message.text);
    registry.dispose();
  });

  it("keeps a same-id retry queued when the first attempt's write fails", async () => {
    const registry = AtomRegistry.make();
    let failNextWrite = true;
    let releaseFirstWrite!: () => void;
    const firstWriteBlocked = new Promise<void>((resolve) => {
      releaseFirstWrite = resolve;
    });
    const manager = createThreadOutboxManager({
      registry,
      storage: {
        load: async () => [],
        write: async () => {
          if (failNextWrite) {
            failNextWrite = false;
            await firstWriteBlocked;
            throw new Error("disk full");
          }
        },
        remove: async () => undefined,
      },
    });
    const message = queuedMessage({
      messageId: "message-1",
      createdAt: "2026-06-08T10:00:01.000Z",
    });
    const retried = { ...message, text: "retried" };

    const first = manager.enqueue(message);
    const second = manager.enqueue(retried);
    releaseFirstWrite();
    await expect(first).rejects.toBeInstanceOf(ThreadOutboxManagerError);
    await second;

    // The failed first attempt must not roll back the retry that replaced it.
    expect(registry.get(manager.queuedMessagesByThreadKeyAtom)).toEqual({
      "environment-1:thread-1": [retried],
    });
    await expect(manager.confirmQueued(retried)).resolves.toBe(true);
    await expect(manager.confirmQueued(message)).resolves.toBe(false);
    registry.dispose();
  });

  it("replaces an existing message when an enqueue retry uses the same id", async () => {
    const registry = AtomRegistry.make();
    const manager = createThreadOutboxManager({
      registry,
      storage: {
        load: async () => [],
        write: async () => undefined,
        remove: async () => undefined,
      },
    });
    const message = queuedMessage({
      messageId: "message-1",
      createdAt: "2026-06-08T10:00:01.000Z",
    });
    const retried = { ...message, text: "retried" };

    await manager.enqueue(message);
    await manager.enqueue(retried);

    expect(registry.get(manager.queuedMessagesByThreadKeyAtom)).toEqual({
      "environment-1:thread-1": [retried],
    });
    registry.dispose();
  });

  it("updates a queued message in place but never resurrects a removed one", async () => {
    const registry = AtomRegistry.make();
    const stored = new Map<MessageId, QueuedThreadMessage>();
    const storage: ThreadOutboxStorage = {
      load: async () => [...stored.values()],
      write: async (message) => {
        stored.set(message.messageId, message);
      },
      remove: async (message) => {
        stored.delete(message.messageId);
      },
    };
    const manager = createThreadOutboxManager({ registry, storage });
    const message = queuedMessage({
      messageId: "message-1",
      createdAt: "2026-06-08T10:00:01.000Z",
    });

    await manager.enqueue(message);
    const edited = { ...message, text: "edited" };
    await expect(manager.update(edited)).resolves.toBe(true);
    expect(registry.get(manager.queuedMessagesByThreadKeyAtom)).toEqual({
      "environment-1:thread-1": [edited],
    });
    expect(stored.get(message.messageId)).toEqual(edited);

    await manager.remove(edited);
    await expect(manager.update({ ...message, text: "stale flush" })).resolves.toBe(false);
    expect(registry.get(manager.queuedMessagesByThreadKeyAtom)).toEqual({});
    expect(stored.size).toBe(0);
    registry.dispose();
  });

  it("never automatically removes a missing external Pi message", () => {
    const base = {
      isCreation: false,
      threadExists: false,
      shellStatus: "live" as const,
      environmentConnected: true,
      threadBusy: false,
      isExternalPiThread: true,
    };

    expect(resolveThreadOutboxDeliveryAction(base)).toBe("wait");
    expect(
      resolveThreadOutboxDeliveryAction({
        ...base,
        shellStatus: "synchronizing",
      }),
    ).toBe("wait");
  });

  it("only removes a missing-thread message after shell synchronization is live", () => {
    expect(
      resolveThreadOutboxDeliveryAction({
        isCreation: false,
        threadExists: false,
        shellStatus: "synchronizing",
        environmentConnected: true,
        threadBusy: false,
      }),
    ).toBe("wait");
    expect(
      resolveThreadOutboxDeliveryAction({
        isCreation: false,
        threadExists: false,
        shellStatus: "live",
        environmentConnected: true,
        threadBusy: false,
      }),
    ).toBe("remove");
    expect(
      resolveThreadOutboxDeliveryAction({
        isCreation: false,
        threadExists: true,
        shellStatus: "live",
        environmentConnected: true,
        threadBusy: false,
      }),
    ).toBe("send");
  });

  it("sends existing-thread messages whenever connected so queued messages can steer", () => {
    expect(
      resolveThreadOutboxDeliveryAction({
        isCreation: false,
        threadExists: true,
        shellStatus: "live",
        environmentConnected: true,
        threadBusy: true,
      }),
    ).toBe("send");
    expect(
      resolveThreadOutboxDeliveryAction({
        isCreation: false,
        threadExists: true,
        shellStatus: "live",
        environmentConnected: false,
        threadBusy: true,
      }),
    ).toBe("wait");
  });

  it("sends queued creations once connected and live, removing already-created ones", () => {
    expect(
      resolveThreadOutboxDeliveryAction({
        isCreation: true,
        threadExists: false,
        shellStatus: "cached",
        environmentConnected: false,
        threadBusy: false,
      }),
    ).toBe("wait");
    // Connected but not yet synchronized: a previously delivered creation may
    // simply not be visible yet — sending now could duplicate the thread.
    expect(
      resolveThreadOutboxDeliveryAction({
        isCreation: true,
        threadExists: false,
        shellStatus: "synchronizing",
        environmentConnected: true,
        threadBusy: false,
      }),
    ).toBe("wait");
    expect(
      resolveThreadOutboxDeliveryAction({
        isCreation: true,
        threadExists: false,
        shellStatus: "live",
        environmentConnected: true,
        threadBusy: false,
      }),
    ).toBe("send");
    expect(
      resolveThreadOutboxDeliveryAction({
        isCreation: true,
        threadExists: true,
        shellStatus: "live",
        environmentConnected: true,
        threadBusy: true,
      }),
    ).toBe("remove");
  });

  it("round-trips queued creations and gates incomplete ones from sending", () => {
    const base = queuedMessage({
      messageId: "message-1",
      createdAt: "2026-06-08T10:00:01.000Z",
    });
    const creationMessage = {
      ...base,
      modelSelection: {
        instanceId: ProviderInstanceId.make("codex"),
        model: "gpt-5.4",
      },
      creation: {
        projectId: ProjectId.make("project-1"),
        workspaceMode: "worktree",
        branch: "main",
        worktreePath: null,
        startFromOrigin: true,
      },
    } satisfies QueuedThreadMessage;

    expect(decodeQueuedThreadMessage(encodeQueuedThreadMessage(creationMessage))).toEqual(
      creationMessage,
    );
    expect(isQueuedThreadCreationSendable(creationMessage)).toBe(true);
    expect(
      isQueuedThreadCreationSendable({
        ...creationMessage,
        creation: { ...creationMessage.creation, branch: null },
      }),
    ).toBe(false);
    expect(
      isQueuedThreadCreationSendable({
        ...creationMessage,
        creation: { ...creationMessage.creation, branch: "" },
      }),
    ).toBe(false);
    expect(isQueuedThreadCreationSendable({ ...creationMessage, modelSelection: undefined })).toBe(
      false,
    );
    expect(isQueuedThreadCreationSendable(base)).toBe(false);
  });

  it("retries transport failures but drops deterministic command failures", () => {
    expect(shouldRetryThreadOutboxDelivery(new Error("Socket is not connected"))).toBe(true);
    expect(
      shouldRetryThreadOutboxDelivery({
        _tag: "ConnectionTransientError",
        message: "temporarily unavailable",
      }),
    ).toBe(true);
    expect(shouldRetryThreadOutboxDelivery(new Error("Thread no longer exists"))).toBe(false);
  });

  it("retains queued messages when settings synchronization fails before startTurn", () => {
    const deterministicFailure = new Error("Thread no longer exists");

    expect(
      resolveThreadOutboxFailureAction({
        stage: "settings-sync",
        error: deterministicFailure,
        interrupted: false,
      }),
    ).toBe("retry");
    expect(
      resolveThreadOutboxFailureAction({
        stage: "start-turn",
        error: deterministicFailure,
        interrupted: false,
      }),
    ).toBe("discard");
  });
});
