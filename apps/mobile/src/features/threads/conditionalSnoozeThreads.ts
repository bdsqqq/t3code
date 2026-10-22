import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";

/** Dependencies stay environment-local even when the inbox spans multiple servers. */
export function conditionalSnoozeThreads(
  threads: readonly EnvironmentThreadShell[],
  source: Pick<EnvironmentThreadShell, "id" | "environmentId">,
  query: string,
) {
  const search = query.trim().toLocaleLowerCase();
  return threads.filter(
    (thread) =>
      thread.environmentId === source.environmentId &&
      thread.id !== source.id &&
      thread.archivedAt == null &&
      thread.deletedAt == null &&
      (!search || `${thread.title} ${thread.id}`.toLocaleLowerCase().includes(search)),
  );
}
