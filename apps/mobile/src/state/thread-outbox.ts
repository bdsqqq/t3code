import type { EnvironmentId } from "@t3tools/contracts";

import { appAtomRegistry } from "./atom-registry";
import { createThreadOutboxManager } from "./thread-outbox-manager";
import type { QueuedThreadMessage } from "./thread-outbox-model";
import { expoThreadOutboxStorage } from "./thread-outbox-storage";

export * from "./thread-outbox-model";

export const threadOutboxManager = createThreadOutboxManager({
  registry: appAtomRegistry,
  storage: expoThreadOutboxStorage,
});

export function ensureThreadOutboxLoaded(): void {
  void threadOutboxManager.load();
}

export function enqueueThreadOutboxMessage(message: QueuedThreadMessage): Promise<void> {
  return threadOutboxManager.enqueue(message);
}

/** Rewrite a queued message; no-op (false) if it was removed in the meantime. */
export function updateThreadOutboxMessage(message: QueuedThreadMessage): Promise<boolean> {
  return threadOutboxManager.update(message);
}

export function markThreadOutboxMessageIndeterminateInMemory(message: QueuedThreadMessage): void {
  const queues = appAtomRegistry.get(threadOutboxManager.queuedMessagesByThreadKeyAtom);
  appAtomRegistry.set(
    threadOutboxManager.queuedMessagesByThreadKeyAtom,
    Object.fromEntries(
      Object.entries(queues).map(([key, entries]) => [
        key,
        entries.map((entry) =>
          entry.messageId === message.messageId
            ? { ...entry, deliveryStatus: "indeterminate" as const }
            : entry,
        ),
      ]),
    ),
  );
}

export function removeThreadOutboxMessage(message: QueuedThreadMessage): Promise<void> {
  return threadOutboxManager.remove(message);
}

export function clearThreadOutboxEnvironment(environmentId: EnvironmentId): Promise<void> {
  return threadOutboxManager.clearEnvironment(environmentId);
}
