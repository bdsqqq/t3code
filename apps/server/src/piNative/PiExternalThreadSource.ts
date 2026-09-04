// @effect-diagnostics globalTimers:off
// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics preferSchemaOverJson:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import type {
  ClientOrchestrationCommand,
  DispatchResult,
  OrchestrationProjectShell,
  OrchestrationShellSnapshot,
  OrchestrationSubscribeThreadInput,
  OrchestrationThreadDetailSnapshot,
  OrchestrationThreadStreamItem,
  PiExternalCatalogSnapshot,
  PiExternalCatalogSubscribeInput,
  PiExternalCatalogStreamItem,
  PiExternalCreateSessionInput,
  PiExternalCreateSessionResult,
  ProjectId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import {
  CommandId as CommandIdSchema,
  DEFAULT_SERVER_SETTINGS,
  PiNativeError,
  PROVIDER_SEND_TURN_MAX_IMAGE_BYTES,
  ProjectId as ProjectIdSchema,
  TurnId as TurnIdSchema,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schedule from "effect/Schedule";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";

import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { resolveAutoSettlementAt } from "../orchestration/ThreadSettlementPolicy.ts";
import type { PiExternalLifecycleOverride } from "../persistence/Services/PiExternalLifecycleOverrides.ts";
import { PiExternalLifecycleOverrideRepository } from "../persistence/Services/PiExternalLifecycleOverrides.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import {
  defaultPiSessionsRoot,
  type PiSessionCatalogRecord,
  SessionCatalog,
} from "./SessionCatalog.ts";
import {
  isPiSubagentLiveEvent,
  piLiveToolCallId,
  projectPiExternalProject,
  projectPiLiveEvent,
  projectPiThread,
  projectPiThreadOverlay,
  projectPiThreadShell,
} from "./PiSessionProjection.ts";
import { SupervisorClient } from "./SupervisorClient.ts";
import type {
  SupervisorCommand,
  SupervisorCommandReceipt,
  SupervisorRuntimeState,
  SupervisorStreamEvent,
  SupervisorStreamItem,
} from "./SupervisorProtocol.ts";
import { supportsGuardedResume } from "./SupervisorProtocol.ts";

const EXTERNAL_THREAD_PREFIX = "external:pi:";
const CATALOG_MAX_THREADS = 5_000;
const CATALOG_MAX_SERIALIZED_BYTES = 8 * 1024 * 1024;
const CATALOG_ENVELOPE_RESERVE_BYTES = 1_024;
export function validExternalLifecycleOverride(
  record: PiSessionCatalogRecord,
  override: PiExternalLifecycleOverride | undefined,
) {
  const local =
    override?.observedFileSize === record.fileSize &&
    override.observedFileMtimeMs === record.fileMtimeMs
      ? {
          override: override.lifecycleOverride,
          updatedAt: override.updatedAt,
        }
      : undefined;
  const jsonl = record.jsonlLifecycle;
  if (local === undefined) return jsonl;
  if (jsonl === undefined) return local;
  return Date.parse(local.updatedAt) >= Date.parse(jsonl.updatedAt) ? local : jsonl;
}

/**
 * Imported Pi history has no orchestration aggregate to receive
 * `thread.auto-settle`. Derive only the automatic state here; explicit
 * settle/un-settle overrides still flow through the external lifecycle store.
 */
export function applyPiExternalAutoSettlement(input: {
  readonly snapshot: OrchestrationThreadDetailSnapshot;
  readonly now: string;
  readonly autoSettleAfterDays: number | null;
}): OrchestrationThreadDetailSnapshot {
  const settledAt = resolveAutoSettlementAt({
    thread: projectPiThreadShell(input.snapshot),
    pullRequest: null,
    now: input.now,
    autoSettleAfterDays: input.autoSettleAfterDays,
    autoSettleOnMerge: false,
  });
  if (settledAt === null) return input.snapshot;
  return {
    ...input.snapshot,
    thread: {
      ...input.snapshot.thread,
      settledOverride: "settled",
      settledAt,
    },
  };
}
export function shutdownCreatedRuntime(
  supervisor: Pick<SupervisorClient["Service"], "dispatch">,
  runtimeId: SupervisorRuntimeState["runtimeId"],
) {
  return supervisor
    .dispatch({
      type: "shutdown",
      commandId: CommandIdSchema.make(`pi-create-cleanup:${NodeCrypto.randomUUID()}`),
      runtimeId,
    })
    .pipe(Effect.ignore);
}
export class CatalogRuntimeAttachmentGate {
  #attached = false;
  attach(): void {
    this.#attached = true;
  }
  allowsCatalogUpdate(): boolean {
    return !this.#attached;
  }
}

export class PiThreadStreamSequenceGate {
  #projectedThrough = -1;

  allows(item: OrchestrationThreadStreamItem): boolean {
    if (item.kind === "synchronized") return true;
    const sequence =
      item.kind === "snapshot" ? item.snapshot.snapshotSequence : item.event.sequence;
    if (sequence < this.#projectedThrough) return false;
    this.#projectedThrough = Math.max(this.#projectedThrough, sequence);
    return true;
  }
}

export function catalogUpdateAfterRead<T>(
  gate: CatalogRuntimeAttachmentGate,
  snapshot: T,
): T | undefined {
  return gate.allowsCatalogUpdate() ? snapshot : undefined;
}
export function runtimeSnapshotAtSequence(
  current: SupervisorRuntimeState | undefined,
  sequence: number,
): SupervisorRuntimeState | undefined {
  return current === undefined ? undefined : { ...current, sequence };
}
export function runtimeSequenceStable(
  before: SupervisorRuntimeState | undefined,
  after: SupervisorRuntimeState | undefined,
): boolean {
  return before?.runtimeId === after?.runtimeId && before?.sequence === after?.sequence;
}
export const isRuntimeLifecycleEvent = (eventType: string | undefined): boolean =>
  eventType === "bridge_disconnected" ||
  eventType === "bridge_reconnected" ||
  eventType === "bridge_registered";
const supervisorEventType = (item: SupervisorStreamEvent): string | undefined => {
  if (typeof item.event !== "object" || item.event === null || Array.isArray(item.event)) {
    return undefined;
  }
  const payload = item.event as Readonly<Record<string, unknown>>;
  return payload.type === "event" && typeof payload.event === "string"
    ? payload.event
    : typeof payload.type === "string"
      ? payload.type
      : undefined;
};

export class PiSubagentStreamTracker {
  readonly #latestByToolCallId = new Map<string, SupervisorStreamEvent>();

  clear(): void {
    this.#latestByToolCallId.clear();
  }

  reset(events: ReadonlyArray<SupervisorStreamEvent>): void {
    this.clear();
    for (const event of events) {
      const toolCallId = piLiveToolCallId(event.event);
      if (!toolCallId) continue;
      if (
        isPiSubagentLiveEvent(event.event) &&
        supervisorEventType(event) !== "tool_execution_end"
      ) {
        this.#latestByToolCallId.set(toolCallId, event);
      }
      if (supervisorEventType(event) === "tool_execution_end") {
        this.#latestByToolCallId.delete(toolCallId);
      }
    }
  }

  observe(item: SupervisorStreamEvent): {
    readonly snapshot: boolean;
    readonly retainedEvents: ReadonlyArray<SupervisorStreamEvent>;
  } {
    const toolCallId = piLiveToolCallId(item.event);
    if (!toolCallId) return { snapshot: false, retainedEvents: [] };
    const retained = this.#latestByToolCallId.get(toolCallId);
    const recognized = isPiSubagentLiveEvent(item.event);
    if (
      supervisorEventType(item) === "tool_execution_end" &&
      (retained !== undefined || recognized)
    ) {
      this.#latestByToolCallId.delete(toolCallId);
      return {
        snapshot: true,
        retainedEvents: retained === undefined ? [item] : [retained, item],
      };
    }
    if (!recognized) return { snapshot: false, retainedEvents: [] };
    this.#latestByToolCallId.set(toolCallId, item);
    return {
      snapshot: retained === undefined,
      retainedEvents: retained === undefined ? [item] : [],
    };
  }
}

export function boundExternalCatalog(input: {
  readonly projects: ReadonlyArray<OrchestrationProjectShell>;
  readonly threads: ReadonlyArray<ReturnType<typeof projectPiThreadShell>>;
  readonly totalThreadCount: number;
  readonly maxThreads?: number;
  readonly maxSerializedBytes?: number;
}) {
  const maxThreads = input.maxThreads ?? CATALOG_MAX_THREADS;
  const maxSerializedBytes = input.maxSerializedBytes ?? CATALOG_MAX_SERIALIZED_BYTES;
  const select = (count: number) => {
    const threads = input.threads.slice(0, count);
    const referencedProjectIds = new Set(threads.map((thread) => thread.projectId));
    const projects = input.projects.filter((project) => referencedProjectIds.has(project.id));
    const serializedBytes =
      Buffer.byteLength(
        JSON.stringify({
          snapshotSequence: Number.MAX_SAFE_INTEGER,
          projects,
          threads,
          omittedProjectCount: input.projects.length - projects.length,
          omittedThreadCount: input.totalThreadCount - threads.length,
          updatedAt: "9999-12-31T23:59:59.999Z",
        }),
        "utf8",
      ) + CATALOG_ENVELOPE_RESERVE_BYTES;
    return { projects, serializedBytes, threads };
  };
  let low = 0;
  let high = Math.min(maxThreads, input.threads.length);
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    if (select(middle).serializedBytes <= maxSerializedBytes) low = middle;
    else high = middle - 1;
  }
  const { projects, threads } = select(low);
  return {
    projects,
    threads,
    omittedProjectCount: input.projects.length - projects.length,
    omittedThreadCount: input.totalThreadCount - threads.length,
  };
}
const projectIdFor = (cwd: string) =>
  ProjectIdSchema.make(
    `external:pi-project:${NodeCrypto.createHash("sha256").update(cwd).digest("hex")}`,
  );
const sourceError = (code: string, cause: unknown) =>
  new PiNativeError({
    code,
    message: cause instanceof Error ? cause.message : String(cause),
  });
const privateSourceError = (code: string, message: string) => new PiNativeError({ code, message });
export const receiptSessionFile = (receipt: SupervisorCommandReceipt): string | undefined => {
  if (
    typeof receipt.result !== "object" ||
    receipt.result === null ||
    !("sessionFile" in receipt.result)
  ) {
    return undefined;
  }
  return typeof receipt.result.sessionFile === "string" ? receipt.result.sessionFile : undefined;
};

export const isPiExternalThreadId = (threadId: ThreadId): boolean =>
  threadId.startsWith(EXTERNAL_THREAD_PREFIX);

const canonical = (value: string) =>
  NodeFS.promises.realpath(value).catch(() => NodePath.resolve(value));

interface Association {
  readonly projectIdByThread: ReadonlyMap<ThreadId, ProjectId>;
  readonly externalProjects: ReadonlyArray<OrchestrationProjectShell>;
}

async function associate(
  records: ReadonlyArray<PiSessionCatalogRecord>,
  internal: OrchestrationShellSnapshot,
): Promise<Association> {
  const projects = await Promise.all(
    internal.projects.map(async (project) => ({
      project,
      root: await canonical(project.workspaceRoot),
    })),
  );
  const worktrees = await Promise.all(
    internal.threads.flatMap((thread) =>
      thread.worktreePath === null
        ? []
        : [
            canonical(thread.worktreePath).then((root) => ({
              projectId: thread.projectId,
              root,
            })),
          ],
    ),
  );
  const projectIdByThread = new Map<ThreadId, ProjectId>();
  const unmatchedByCwd = new Map<string, PiSessionCatalogRecord[]>();
  for (const record of records) {
    const exactProject = projects.find(({ root }) => root === record.cwd);
    const exactWorktree = worktrees.find(({ root }) => root === record.cwd);
    const ancestor = projects
      .filter(({ root }) => record.cwd === root || record.cwd.startsWith(`${root}${NodePath.sep}`))
      .sort((a, b) => b.root.length - a.root.length)[0];
    const projectId =
      exactProject?.project.id ??
      exactWorktree?.projectId ??
      ancestor?.project.id ??
      projectIdFor(record.cwd);
    projectIdByThread.set(record.threadId, projectId);
    if (!exactProject && !exactWorktree && !ancestor) {
      const grouped = unmatchedByCwd.get(record.cwd);
      if (grouped) grouped.push(record);
      else unmatchedByCwd.set(record.cwd, [record]);
    }
  }
  return {
    projectIdByThread,
    externalProjects: [...unmatchedByCwd].map(([cwd, grouped]) =>
      projectPiExternalProject({
        projectId: projectIdFor(cwd),
        cwd,
        records: grouped,
      }),
    ),
  };
}

function runtimeFor(
  record: PiSessionCatalogRecord,
  runtimes: ReadonlyArray<SupervisorRuntimeState>,
) {
  return runtimes.find(
    (runtime) => runtime.sessionFile === record.canonicalFile && runtime.status !== "exited",
  );
}

type PiExternalTurnStartCommand = Extract<
  ClientOrchestrationCommand,
  { readonly type: "thread.turn.start" }
>;
type PiResumeAndSendCommand = Extract<SupervisorCommand, { readonly type: "resumeAndSend" }>;

export function planPiExternalTurnStart(input: {
  readonly command: PiExternalTurnStartCommand;
  readonly record: PiSessionCatalogRecord;
  readonly runtime: SupervisorRuntimeState | undefined;
  readonly guardedResumeSupported: boolean;
}):
  | { readonly type: "runtime"; readonly runtime: SupervisorRuntimeState }
  | { readonly type: "takeoverConfirmationRequired" }
  | { readonly type: "supervisorUpgradeRequired" }
  | { readonly type: "takeover"; readonly command: PiResumeAndSendCommand } {
  if (input.command.externalResume === "takeover") {
    if (!input.guardedResumeSupported) return { type: "supervisorUpgradeRequired" };
    return {
      type: "takeover",
      command: {
        type: "resumeAndSend",
        commandId: input.command.commandId,
        sessionKey: input.record.sourceKey,
        sessionFile: input.record.canonicalFile,
        cwd: input.record.cwd,
        message: input.command.message.text,
        streamingBehavior: input.command.streamingBehavior ?? "steer",
      },
    };
  }
  if (input.runtime !== undefined) return { type: "runtime", runtime: input.runtime };
  return { type: "takeoverConfirmationRequired" };
}

export function runtimeCatalogSignature(runtimes: ReadonlyArray<SupervisorRuntimeState>): string {
  return JSON.stringify(
    runtimes
      .filter((runtime) => runtime.status !== "exited" && runtime.sessionFile !== undefined)
      .map((runtime) => ({
        sessionFile: runtime.sessionFile,
        status: runtime.status,
        writerKind: runtime.writerKind,
      }))
      .sort((left, right) => (left.sessionFile ?? "").localeCompare(right.sessionFile ?? "")),
  );
}

function internalAssociationSignature(snapshot: OrchestrationShellSnapshot): string {
  return JSON.stringify({
    projects: snapshot.projects
      .map((project) => [project.id, project.workspaceRoot])
      .sort(([left], [right]) => left!.localeCompare(right!)),
    worktrees: snapshot.threads
      .filter((thread) => thread.worktreePath !== null)
      .map((thread) => [thread.projectId, thread.worktreePath])
      .sort(([left], [right]) => left!.localeCompare(right!)),
  });
}

async function* catalogTriggers(): AsyncGenerator<void> {
  const root = defaultPiSessionsRoot();
  let wake: (() => void) | undefined;
  let pending = true;
  let debounce: NodeJS.Timeout | undefined;
  const notify = () => {
    if (debounce) clearTimeout(debounce);
    debounce = setTimeout(() => {
      pending = true;
      wake?.();
    }, 150);
    debounce.unref();
  };
  const reconciliation = setInterval(() => {
    pending = true;
    wake?.();
  }, 30_000);
  reconciliation.unref();
  let watcher: NodeFS.FSWatcher | undefined;
  const attachWatcher = async () => {
    if (watcher) return;
    watcher = await NodeFS.promises
      .stat(root)
      .then(() => NodeFS.watch(root, { recursive: true }, notify))
      .catch(() => undefined);
  };
  try {
    while (true) {
      await attachWatcher();
      if (!pending) {
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
      }
      wake = undefined;
      pending = false;
      yield;
    }
  } finally {
    if (debounce) clearTimeout(debounce);
    clearInterval(reconciliation);
    watcher?.close();
  }
}

function validateInlineImages(
  command: Extract<ClientOrchestrationCommand, { readonly type: "thread.turn.start" }>,
) {
  for (const attachment of command.message.attachments) {
    // Uploaded attachments are rejected below with the same external-thread
    // capability error; only inline payloads need data-url validation here.
    if (!("dataUrl" in attachment)) {
      continue;
    }
    const comma = attachment.dataUrl.indexOf(",");
    if (comma < 0) throw new Error("invalid image data url");
    const data = attachment.dataUrl.slice(comma + 1);
    if (Buffer.byteLength(data, "base64") > PROVIDER_SEND_TURN_MAX_IMAGE_BYTES) {
      throw new Error("image exceeds the attachment byte ceiling");
    }
  }
}

export class PiExternalThreadSource extends Context.Service<
  PiExternalThreadSource,
  {
    readonly catalogSnapshot: () => Effect.Effect<PiExternalCatalogSnapshot, PiNativeError>;
    readonly subscribeCatalog: (
      input: PiExternalCatalogSubscribeInput,
    ) => Stream.Stream<PiExternalCatalogStreamItem, PiNativeError>;
    readonly resolve: (
      threadId: ThreadId,
    ) => Effect.Effect<PiSessionCatalogRecord | undefined, PiNativeError>;
    readonly threadSnapshot: (
      threadId: ThreadId,
    ) => Effect.Effect<OrchestrationThreadDetailSnapshot, PiNativeError>;
    readonly subscribeThread: (
      input: OrchestrationSubscribeThreadInput,
    ) => Stream.Stream<OrchestrationThreadStreamItem, PiNativeError>;
    readonly createSession: (
      input: PiExternalCreateSessionInput,
    ) => Effect.Effect<PiExternalCreateSessionResult, PiNativeError>;
    readonly dispatch: (
      command: ClientOrchestrationCommand,
    ) => Effect.Effect<DispatchResult, PiNativeError>;
  }
>()("t3/piNative/PiExternalThreadSource") {
  static readonly layer = Layer.effect(
    PiExternalThreadSource,
    Effect.gen(function* () {
      const catalog = yield* SessionCatalog;
      const supervisor = yield* SupervisorClient;
      const snapshots = yield* ProjectionSnapshotQuery;
      const lifecycleOverrides = yield* PiExternalLifecycleOverrideRepository;
      const settingsService = Option.getOrUndefined(
        yield* Effect.serviceOption(ServerSettingsService),
      );
      const catalogBuildSemaphore = yield* Semaphore.make(1);
      let catalogSequence = 0;
      let catalogSignature = "";
      let cachedRecords: ReadonlyArray<PiSessionCatalogRecord> | undefined;
      let cachedOmittedThreadCount = 0;
      let cachedInternal: OrchestrationShellSnapshot | undefined;
      let cachedAssociation: Association | undefined;
      let lastCatalogRuntimeSignature = "";
      let lastInternalAssociationSignature = "";
      let lastGuardedResumeSupported = false;

      const probeGuardedResume = (refresh = false) =>
        supervisor.probeCapabilities(refresh).pipe(Effect.map(supportsGuardedResume));
      const projectedGuardedResume = probeGuardedResume().pipe(Effect.orElseSucceed(() => false));

      const internalShell = snapshots
        .getShellSnapshot()
        .pipe(
          Effect.mapError(() =>
            privateSourceError("internal_shell", "Thread catalog access failed."),
          ),
        );

      const currentSettings = (
        settingsService?.getSettings ?? Effect.succeed(DEFAULT_SERVER_SETTINGS)
      ).pipe(
        Effect.mapError(() =>
          privateSourceError(
            "settings_unavailable",
            "Thread settlement settings could not be loaded.",
          ),
        ),
      );
      const autoSettlementInput = Effect.fn("PiExternalThreadSource.autoSettlementInput")(
        function* () {
          const [settings, now] = yield* Effect.all([
            currentSettings,
            DateTime.now.pipe(Effect.map(DateTime.formatIso)),
          ]);
          return {
            now,
            autoSettleAfterDays: settings.sidebarAutoSettleAfterDays,
          };
        },
      );

      const buildCatalogUnlocked = Effect.fn("PiExternalThreadSource.catalogSnapshot")(function* (
        refreshCatalog = true,
        runtimeOverride?: ReadonlyArray<SupervisorRuntimeState>,
        guardedResumeOverride?: boolean,
      ) {
        const runtimes =
          runtimeOverride ?? (yield* supervisor.list().pipe(Effect.orElseSucceed(() => [])));
        const catalogResumeSupported = guardedResumeOverride ?? (yield* projectedGuardedResume);
        if (refreshCatalog || cachedRecords === undefined || cachedInternal === undefined) {
          [cachedRecords, cachedInternal] = yield* Effect.all([
            catalog.list(
              runtimes.flatMap((runtime) =>
                runtime.sessionFile === undefined ? [] : [runtime.sessionFile],
              ),
            ),
            internalShell,
          ]);
          cachedOmittedThreadCount = yield* catalog.omittedCount();
          cachedAssociation = undefined;
        }
        const records = cachedRecords;
        const internal = cachedInternal;
        lastInternalAssociationSignature = internalAssociationSignature(internal);
        lastCatalogRuntimeSignature = runtimeCatalogSignature(runtimes);
        lastGuardedResumeSupported = catalogResumeSupported;
        if (cachedAssociation === undefined) {
          cachedAssociation = yield* Effect.tryPromise({
            try: () => associate(records, internal),
            catch: () =>
              privateSourceError("catalog_association", "Thread catalog association failed."),
          });
        }
        const association = cachedAssociation;
        const autoSettlement = yield* autoSettlementInput();
        const lifecycleBySourceKey = new Map(
          (yield* lifecycleOverrides
            .list()
            .pipe(
              Effect.mapError(() =>
                privateSourceError(
                  "lifecycle_store",
                  "External Pi lifecycle state could not be loaded.",
                ),
              ),
            )).map((value) => [value.sourceKey, value] as const),
        );
        const projectedThreads = records.slice(0, CATALOG_MAX_THREADS).map((record) => {
          const projectId = association.projectIdByThread.get(record.threadId)!;
          const lifecycle = validExternalLifecycleOverride(
            record,
            lifecycleBySourceKey.get(record.sourceKey),
          );
          const detail = applyPiExternalAutoSettlement({
            snapshot: projectPiThread({
              record,
              entries: [],
              projectId,
              catalogResumeSupported,
              ...(runtimeFor(record, runtimes) === undefined
                ? {}
                : { runtime: runtimeFor(record, runtimes)! }),
              ...(lifecycle === undefined ? {} : { lifecycle }),
            }),
            ...autoSettlement,
          });
          return projectPiThreadShell(detail);
        });
        const { projects, threads, omittedProjectCount, omittedThreadCount } = boundExternalCatalog(
          {
            projects: association.externalProjects,
            threads: projectedThreads,
            totalThreadCount: records.length + cachedOmittedThreadCount,
          },
        );
        const signature = JSON.stringify({
          projects,
          threads,
          omittedProjectCount,
          omittedThreadCount,
        });
        if (signature !== catalogSignature) {
          catalogSignature = signature;
          catalogSequence += 1;
        }
        return {
          snapshotSequence: catalogSequence,
          projects,
          threads,
          omittedProjectCount,
          omittedThreadCount,
          updatedAt: yield* DateTime.now.pipe(Effect.map(DateTime.formatIso)),
        } satisfies PiExternalCatalogSnapshot;
      });

      const buildCatalog = (
        refreshCatalog = true,
        runtimeOverride?: ReadonlyArray<SupervisorRuntimeState>,
      ) => catalogBuildSemaphore.withPermit(buildCatalogUnlocked(refreshCatalog, runtimeOverride));

      const buildCatalogForRuntimeChange = Effect.fn("PiExternalThreadSource.catalogRuntimeChange")(
        function* () {
          return yield* catalogBuildSemaphore.withPermit(
            Effect.gen(function* () {
              const [runtimes, internal, catalogResumeSupported] = yield* Effect.all([
                supervisor.list().pipe(Effect.orElseSucceed(() => [])),
                internalShell,
                projectedGuardedResume,
              ]);
              const runtimeSignature = runtimeCatalogSignature(runtimes);
              const internalSignature = internalAssociationSignature(internal);
              if (
                runtimeSignature === lastCatalogRuntimeSignature &&
                internalSignature === lastInternalAssociationSignature &&
                catalogResumeSupported === lastGuardedResumeSupported
              ) {
                return undefined;
              }
              if (internalSignature !== lastInternalAssociationSignature) {
                cachedInternal = internal;
                cachedAssociation = undefined;
              }
              return yield* buildCatalogUnlocked(false, runtimes, catalogResumeSupported);
            }),
          );
        },
      );

      const findRecord = (threadId: ThreadId) =>
        Effect.sync(() =>
          isPiExternalThreadId(threadId)
            ? cachedRecords?.find((record) => record.threadId === threadId)
            : undefined,
        );

      const readProjected = Effect.fn("PiExternalThreadSource.threadSnapshot")(function* (
        threadId: ThreadId,
        runtimeOverride?: SupervisorRuntimeState,
        entriesOverride?: ReadonlyArray<Readonly<Record<string, unknown>>>,
      ) {
        let result = yield* catalog.read(threadId);
        let runtime = runtimeOverride;
        if (entriesOverride === undefined) {
          const selectRuntime = (runtimes: ReadonlyArray<SupervisorRuntimeState>) =>
            runtimeOverride === undefined
              ? runtimeFor(result.record, runtimes)
              : runtimes.find((candidate) => candidate.runtimeId === runtimeOverride.runtimeId);
          for (let attempt = 0; attempt < 3; attempt += 1) {
            const before = selectRuntime(
              yield* supervisor.list().pipe(Effect.orElseSucceed(() => [])),
            );
            result = yield* catalog.read(threadId);
            const after = selectRuntime(
              yield* supervisor.list().pipe(Effect.orElseSucceed(() => [])),
            );
            runtime = after;
            if (runtimeSequenceStable(before, after)) break;
          }
          if (runtimeOverride !== undefined) {
            runtime = runtimeSnapshotAtSequence(runtime, runtimeOverride.sequence);
          }
        }
        const internal = yield* internalShell;
        const association = yield* Effect.tryPromise({
          try: () => associate([result.record], internal),
          catch: () =>
            privateSourceError("thread_association", "Thread project association failed."),
        });
        const lifecycle = validExternalLifecycleOverride(
          result.record,
          Option.getOrUndefined(
            yield* lifecycleOverrides
              .getBySourceKey(result.record.sourceKey)
              .pipe(
                Effect.mapError(() =>
                  privateSourceError(
                    "lifecycle_store",
                    "External Pi lifecycle state could not be loaded.",
                  ),
                ),
              ),
          ),
        );
        return applyPiExternalAutoSettlement({
          snapshot: projectPiThread({
            record: result.record,
            entries: entriesOverride ?? result.entries,
            projectId: association.projectIdByThread.get(threadId)!,
            catalogResumeSupported: yield* projectedGuardedResume,
            ...(runtime === undefined ? {} : { runtime }),
            ...(lifecycle === undefined ? {} : { lifecycle }),
          }),
          ...(yield* autoSettlementInput()),
        });
      });

      const projectSupervisorSnapshot = Effect.fn(
        "PiExternalThreadSource.projectSupervisorSnapshot",
      )(function* (
        record: PiSessionCatalogRecord,
        runtime: SupervisorRuntimeState,
        retainedSubagentEvents: ReadonlyArray<SupervisorStreamEvent> = [],
      ) {
        const first = yield* supervisor
          .subscribe(runtime.runtimeId)
          .pipe(Stream.take(1), Stream.runHead);
        if (Option.isNone(first) || first.value.type !== "snapshot") {
          return yield* readProjected(record.threadId, runtime);
        }
        const snapshot = yield* readProjected(
          record.threadId,
          first.value.runtime,
          first.value.entries,
        );
        const overlayEvents = [...first.value.events];
        for (const event of retainedSubagentEvents) {
          if (!overlayEvents.some((candidate) => candidate.eventId === event.eventId)) {
            overlayEvents.push(event);
          }
        }
        overlayEvents.sort((a, b) => a.sequence - b.sequence);
        return projectPiThreadOverlay(
          snapshot,
          record,
          overlayEvents,
          yield* DateTime.now.pipe(Effect.map(DateTime.formatIso)),
          first.value.omittedOverlayEventCount,
        );
      });

      const projectRuntimeItem = Effect.fn("PiExternalThreadSource.projectRuntimeItem")(function* (
        record: PiSessionCatalogRecord,
        runtime: SupervisorRuntimeState,
        item: SupervisorStreamItem,
        liveTurnId?: TurnId,
        liveUserEvent?: SupervisorStreamEvent,
        snapshotSubagentEvent = false,
        retainedSubagentEvents: ReadonlyArray<SupervisorStreamEvent> = [],
      ): Effect.fn.Return<OrchestrationThreadStreamItem | null, PiNativeError> {
        if (item.type === "synchronized") return { kind: "synchronized" };
        if (item.type === "snapshot") {
          const snapshot = yield* readProjected(record.threadId, item.runtime, item.entries);
          return {
            kind: "snapshot",
            snapshot: projectPiThreadOverlay(
              snapshot,
              record,
              item.events,
              yield* DateTime.now.pipe(Effect.map(DateTime.formatIso)),
              item.omittedOverlayEventCount,
            ),
          };
        }
        if (item.type === "entries" || item.type === "exited") {
          const currentRuntime =
            item.type === "exited"
              ? undefined
              : (yield* supervisor.list().pipe(Effect.orElseSucceed(() => []))).find(
                  (candidate) => candidate.runtimeId === runtime.runtimeId,
                );
          return {
            kind: "snapshot",
            snapshot: yield* readProjected(
              record.threadId,
              runtimeSnapshotAtSequence(currentRuntime, item.sequence),
            ),
          };
        }
        const eventType = supervisorEventType(item);
        if (isRuntimeLifecycleEvent(eventType)) {
          return {
            kind: "snapshot",
            snapshot: yield* projectSupervisorSnapshot(record, runtime),
          };
        }
        if (eventType === "agent_settled") {
          return {
            kind: "snapshot",
            snapshot: yield* readProjected(record.threadId, {
              ...runtime,
              sequence: item.sequence,
              status: "idle",
            }),
          };
        }
        if (eventType === "agent_start") {
          const currentRuntime = (yield* supervisor
            .list()
            .pipe(Effect.orElseSucceed(() => []))).find(
            (candidate) => candidate.runtimeId === runtime.runtimeId,
          );
          const snapshot = yield* readProjected(
            record.threadId,
            runtimeSnapshotAtSequence(currentRuntime, item.sequence),
          );
          return {
            kind: "snapshot",
            snapshot: projectPiThreadOverlay(
              snapshot,
              record,
              liveUserEvent ? [liveUserEvent, item] : [item],
            ),
          };
        }
        if (eventType === "queue_update") {
          return {
            kind: "snapshot",
            snapshot: yield* projectSupervisorSnapshot(record, runtime),
          };
        }
        if (snapshotSubagentEvent) {
          return {
            kind: "snapshot",
            snapshot: yield* projectSupervisorSnapshot(record, runtime, retainedSubagentEvents),
          };
        }
        const eventRuntime = {
          ...runtime,
          sequence: item.sequence,
        };
        const projected = projectPiLiveEvent({
          record,
          runtime: eventRuntime,
          item,
          activeTurnId: liveTurnId ?? null,
          occurredAt: yield* DateTime.now.pipe(Effect.map(DateTime.formatIso)),
        });
        return projected === null ? null : { kind: "event", event: projected };
      });

      const runtimeStream = (record: PiSessionCatalogRecord, runtime: SupervisorRuntimeState) => {
        let liveTurnId: TurnId | undefined;
        let liveUserEvent: SupervisorStreamEvent | undefined;
        let projectedThrough = -1;
        const subagentTracker = new PiSubagentStreamTracker();
        // Common thread cursors do not carry a runtime generation. A fresh
        // supervisor snapshot prevents a replacement runtime from interpreting
        // the prior runtime's numeric sequence as its own.
        return supervisor.subscribe(runtime.runtimeId).pipe(
          Stream.takeUntil((item) => item.type === "exited"),
          Stream.mapEffect((item) => {
            const sequence = item.type === "snapshot" ? item.runtime.sequence : item.sequence;
            if (item.type === "entries" || item.type === "exited") {
              liveUserEvent = undefined;
              liveTurnId = undefined;
              subagentTracker.clear();
            }
            if (
              item.type === "event" &&
              supervisorEventType(item) === "agent_start" &&
              liveUserEvent?.sequence !== item.sequence - 1
            ) {
              liveUserEvent = undefined;
              liveTurnId = undefined;
            }
            if (
              item.type !== "snapshot" &&
              item.type !== "synchronized" &&
              sequence <= projectedThrough
            ) {
              return Effect.succeed(null);
            }
            if (item.type === "snapshot") {
              subagentTracker.reset(item.events);
              liveUserEvent = item.events.findLast(
                (event) => supervisorEventType(event) === "message_start",
              );
              liveTurnId =
                liveUserEvent === undefined
                  ? undefined
                  : TurnIdSchema.make(`${record.sessionId}:live-user:${liveUserEvent.eventId}`);
            } else if (item.type === "event" && supervisorEventType(item) === "message_start") {
              liveUserEvent = item;
              liveTurnId = TurnIdSchema.make(`${record.sessionId}:live-user:${item.eventId}`);
            }
            let snapshotSubagentEvent = false;
            let retainedSubagentEvents: ReadonlyArray<SupervisorStreamEvent> = [];
            if (item.type === "event") {
              const decision = subagentTracker.observe(item);
              snapshotSubagentEvent = decision.snapshot;
              retainedSubagentEvents = decision.retainedEvents;
            }
            return projectRuntimeItem(
              record,
              runtime,
              item,
              liveTurnId,
              liveUserEvent,
              snapshotSubagentEvent,
              retainedSubagentEvents,
            ).pipe(
              Effect.tap((projected) =>
                Effect.sync(() => {
                  if (projected?.kind === "snapshot") {
                    projectedThrough = Math.max(
                      projectedThrough,
                      projected.snapshot.snapshotSequence,
                    );
                    if (liveUserEvent) {
                      liveTurnId = projected.snapshot.thread.latestTurn?.turnId ?? undefined;
                    }
                  } else if (projected?.kind === "event") {
                    projectedThrough = Math.max(projectedThrough, projected.event.sequence);
                  }
                }),
              ),
            );
          }),
          Stream.filter((item): item is OrchestrationThreadStreamItem => item !== null),
        );
      };

      const authoritativeThreadSnapshot = Effect.fn(
        "PiExternalThreadSource.authoritativeThreadSnapshot",
      )(function* (threadId: ThreadId) {
        if (!(yield* findRecord(threadId))) {
          return yield* privateSourceError("thread_not_found", "Native Pi thread was not found.");
        }
        const result = yield* catalog.read(threadId);
        const runtime = runtimeFor(
          result.record,
          yield* supervisor.list().pipe(Effect.orElseSucceed(() => [])),
        );
        if (!runtime) return yield* readProjected(threadId);
        return yield* projectSupervisorSnapshot(result.record, runtime);
      });

      const awaitRuntime = (record: PiSessionCatalogRecord) =>
        Stream.fromEffect(
          supervisor.list().pipe(
            Effect.map((runtimes) => runtimeFor(record, runtimes)),
            Effect.orElseSucceed(() => undefined),
          ),
        ).pipe(
          Stream.repeat(Schedule.spaced("500 millis")),
          Stream.filter((runtime): runtime is SupervisorRuntimeState => runtime !== undefined),
          Stream.take(1),
        );
      const dispatchSupervisor = Effect.fn("PiExternalThreadSource.dispatchSupervisor")(function* (
        command: SupervisorCommand,
      ) {
        const receipt = yield* supervisor.dispatch(command);
        if (receipt.status === "rejected") {
          return yield* privateSourceError("command_rejected", "Native Pi command rejected.");
        }
        return receipt;
      });

      const dispatch = Effect.fn("PiExternalThreadSource.dispatch")(function* (
        command: ClientOrchestrationCommand,
      ) {
        if (!("threadId" in command) || !isPiExternalThreadId(command.threadId)) {
          return yield* sourceError("not_external", "command is not for an external pi thread");
        }
        if (!(yield* findRecord(command.threadId))) {
          return yield* privateSourceError("thread_not_found", "Native Pi thread was not found.");
        }
        const result = yield* catalog.read(command.threadId);
        if (command.type === "thread.settle" || command.type === "thread.unsettle") {
          const lifecycleOverride = command.type === "thread.settle" ? "settled" : "active";
          const priorReceipt = yield* lifecycleOverrides
            .getByCommandId(command.commandId)
            .pipe(
              Effect.mapError(() =>
                privateSourceError(
                  "lifecycle_store",
                  "External Pi lifecycle receipt could not be loaded.",
                ),
              ),
            );
          if (Option.isSome(priorReceipt)) {
            if (
              priorReceipt.value.sourceKey !== result.record.sourceKey ||
              priorReceipt.value.lifecycleOverride !== lifecycleOverride
            ) {
              return yield* sourceError(
                "command_id_conflict",
                "The Pi lifecycle command id was already used for another operation.",
              );
            }
            const snapshot = yield* buildCatalog(true);
            yield* publishCatalogSnapshot(snapshot, true);
            yield* PubSub.publish(catalogInvalidations, undefined);
            yield* PubSub.publish(lifecycleInvalidations, command.threadId);
            return {
              sequence: snapshot.snapshotSequence,
              deliveryStatus: "completed",
            } satisfies DispatchResult;
          }
          const jsonlOperation = yield* catalog.findLifecycleOperation(
            command.threadId,
            command.commandId,
          );
          if (jsonlOperation !== undefined) {
            if (jsonlOperation.override !== lifecycleOverride) {
              return yield* sourceError(
                "command_id_conflict",
                "The Pi lifecycle command id was already used for another operation.",
              );
            }
            const receipt = yield* lifecycleOverrides
              .recordReceipt({
                sourceKey: result.record.sourceKey,
                commandId: command.commandId,
                lifecycleOverride,
                observedFileSize: result.record.fileSize,
                observedFileMtimeMs: result.record.fileMtimeMs,
                updatedAt: jsonlOperation.updatedAt,
              })
              .pipe(
                Effect.mapError(() =>
                  privateSourceError(
                    "lifecycle_store",
                    "External Pi lifecycle receipt could not be saved.",
                  ),
                ),
              );
            if (
              receipt.sourceKey !== result.record.sourceKey ||
              receipt.lifecycleOverride !== lifecycleOverride
            ) {
              return yield* sourceError(
                "command_id_conflict",
                "The Pi lifecycle command id was already used for another operation.",
              );
            }
            const snapshot = yield* buildCatalog(true);
            yield* publishCatalogSnapshot(snapshot, true);
            yield* PubSub.publish(catalogInvalidations, undefined);
            yield* PubSub.publish(lifecycleInvalidations, command.threadId);
            return {
              sequence: snapshot.snapshotSequence,
              deliveryStatus: "completed",
            } satisfies DispatchResult;
          }
          let lifecycleRuntime: SupervisorRuntimeState | undefined;
          if (command.type === "thread.settle") {
            lifecycleRuntime = runtimeFor(
              result.record,
              yield* supervisor
                .list()
                .pipe(
                  Effect.mapError(() =>
                    privateSourceError(
                      "runtime_state_unavailable",
                      "Pi runtime state could not be verified.",
                    ),
                  ),
                ),
            );
            if (
              lifecycleRuntime?.status === "starting" ||
              lifecycleRuntime?.status === "streaming"
            ) {
              return yield* sourceError(
                "active_session",
                "A running Pi session cannot be settled.",
              );
            }
          } else {
            lifecycleRuntime = runtimeFor(
              result.record,
              yield* supervisor.list().pipe(Effect.orElseSucceed(() => [])),
            );
          }
          if (
            lifecycleRuntime?.writerKind === "tuiBridge" &&
            lifecycleRuntime.status !== "starting"
          ) {
            yield* dispatchSupervisor({
              type: "setLifecycle",
              commandId: command.commandId,
              runtimeId: lifecycleRuntime.runtimeId,
              lifecycle: {
                version: 1,
                sessionId: result.record.sessionId,
                override: lifecycleOverride,
                operationId: command.commandId,
              },
            });
          }
          const updatedAt = yield* DateTime.now.pipe(Effect.map(DateTime.formatIso));
          const application = yield* lifecycleOverrides
            .apply({
              sourceKey: result.record.sourceKey,
              commandId: command.commandId,
              lifecycleOverride,
              observedFileSize: result.record.fileSize,
              observedFileMtimeMs: result.record.fileMtimeMs,
              updatedAt,
            })
            .pipe(
              Effect.mapError(() =>
                privateSourceError(
                  "lifecycle_store",
                  "External Pi lifecycle state could not be saved.",
                ),
              ),
            );
          if (
            application.value.sourceKey !== result.record.sourceKey ||
            application.value.lifecycleOverride !== lifecycleOverride
          ) {
            return yield* sourceError(
              "command_id_conflict",
              "The Pi lifecycle command id was already used for another operation.",
            );
          }
          // Reconcile on replays too: the first attempt may have committed its
          // receipt before a disconnect prevented publication to subscribers.
          const snapshot = yield* buildCatalog(true);
          yield* publishCatalogSnapshot(snapshot, true);
          yield* PubSub.publish(catalogInvalidations, undefined);
          yield* PubSub.publish(lifecycleInvalidations, command.threadId);
          return {
            sequence: snapshot.snapshotSequence,
            deliveryStatus: "completed",
          } satisfies DispatchResult;
        }
        const runtimes = yield* supervisor
          .list()
          .pipe(
            Effect.mapError(() =>
              privateSourceError(
                "runtime_state_unavailable",
                "Pi runtime state could not be verified.",
              ),
            ),
          );
        const runtime = runtimeFor(result.record, runtimes);
        let receipt: SupervisorCommandReceipt;
        if (command.type === "thread.turn.start") {
          yield* Effect.try({
            try: () => validateInlineImages(command),
            catch: (cause) => sourceError("invalid_attachment", cause),
          });
          if (command.message.attachments.length > 0) {
            return yield* sourceError(
              "attachments_unsupported",
              "Native Pi image attachments are unavailable.",
            );
          }
          const plan = planPiExternalTurnStart({
            command,
            record: result.record,
            runtime,
            guardedResumeSupported:
              command.externalResume === "takeover" ? yield* probeGuardedResume(true) : false,
          });
          if (plan.type === "supervisorUpgradeRequired") {
            return yield* sourceError(
              "supervisor_upgrade_required",
              "The running Pi supervisor must be restarted after live sessions finish before guarded takeover is available.",
            );
          }
          if (plan.type === "takeoverConfirmationRequired") {
            return yield* sourceError(
              "read_only",
              "Resuming this native Pi session requires takeover confirmation.",
            );
          }
          if (plan.type === "takeover") {
            receipt = yield* dispatchSupervisor(plan.command);
          } else {
            const liveRuntime = plan.runtime;
            if (liveRuntime.status === "starting") {
              return yield* sourceError("runtime_starting", "pi runtime is reconnecting");
            }
            const type = liveRuntime.status === "streaming" ? command.streamingBehavior : "send";
            if (type === undefined) {
              return yield* sourceError(
                "streaming_behavior_required",
                "steer or followUp is required while pi is streaming",
              );
            }
            receipt = yield* dispatchSupervisor({
              type,
              commandId: command.commandId,
              runtimeId: liveRuntime.runtimeId,
              message: command.message.text,
            });
          }
        } else if (command.type === "thread.turn.interrupt") {
          if (!runtime || runtime.status !== "streaming") {
            return yield* sourceError("interrupt_unsupported", "pi is not streaming");
          }
          receipt = yield* dispatchSupervisor({
            type: "abort",
            commandId: command.commandId,
            runtimeId: runtime.runtimeId,
          });
        } else if (command.type === "thread.session.stop") {
          if (!runtime || runtime.status === "starting") {
            return yield* sourceError("stop_unsupported", "pi has no managed writer");
          }
          receipt = yield* dispatchSupervisor({
            type: "shutdown",
            commandId: command.commandId,
            runtimeId: runtime.runtimeId,
          });
        } else {
          return yield* sourceError(
            "unsupported_external_mutation",
            `${command.type} is not supported for external pi threads`,
          );
        }
        const sequence =
          receipt.runtimeId === undefined
            ? 0
            : ((yield* supervisor.list().pipe(Effect.orElseSucceed(() => []))).find(
                (candidate) => candidate.runtimeId === receipt.runtimeId,
              )?.sequence ?? 0);
        return {
          sequence,
          deliveryStatus: receipt.status === "indeterminate" ? "indeterminate" : "completed",
        } satisfies DispatchResult;
      });

      // Subscribe before the initial read so a settings change cannot land
      // between the snapshot and the lazily started stream.
      const settingsChanges =
        settingsService === undefined ? Stream.empty : yield* settingsService.subscribeChanges;
      let lastAutoSettleAfterDays = (yield* currentSettings).sidebarAutoSettleAfterDays;
      const initialCatalogSnapshot: PiExternalCatalogSnapshot = yield* buildCatalog(true).pipe(
        Effect.catch(() =>
          DateTime.now.pipe(
            Effect.map(
              (now) =>
                ({
                  snapshotSequence: 0,
                  projects: [],
                  threads: [],
                  omittedProjectCount: 0,
                  omittedThreadCount: 0,
                  updatedAt: DateTime.formatIso(now),
                }) satisfies PiExternalCatalogSnapshot,
            ),
          ),
        ),
      );
      const catalogSnapshots = yield* SubscriptionRef.make(initialCatalogSnapshot);
      const catalogInvalidations = yield* PubSub.sliding<void>(1);
      const lifecycleInvalidations = yield* PubSub.sliding<ThreadId>(16);
      const catalogPublicationSemaphore = yield* Semaphore.make(1);
      yield* Effect.addFinalizer(() => PubSub.shutdown(catalogInvalidations));
      yield* Effect.addFinalizer(() => PubSub.shutdown(lifecycleInvalidations));
      const publishCatalogSnapshot = Effect.fn("PiExternalThreadSource.publishCatalogSnapshot")(
        function* (snapshot: PiExternalCatalogSnapshot, detailInvalidated = false) {
          yield* catalogPublicationSemaphore.withPermit(
            Effect.gen(function* () {
              const current = yield* SubscriptionRef.get(catalogSnapshots);
              if (snapshot.snapshotSequence <= current.snapshotSequence) return;
              yield* SubscriptionRef.set(catalogSnapshots, snapshot);
              if (!detailInvalidated) {
                yield* PubSub.publish(catalogInvalidations, undefined);
              }
            }),
          );
        },
      );
      const runtimeStreamWithLifecycle = (
        record: PiSessionCatalogRecord,
        runtime: SupervisorRuntimeState,
      ) => {
        const lifecycleUpdates = Stream.fromPubSub(lifecycleInvalidations).pipe(
          Stream.filter((threadId) => threadId === record.threadId),
          Stream.mapEffect(() => readProjected(record.threadId)),
          Stream.map((snapshot) => ({
            kind: "snapshot" as const,
            snapshot,
          })),
        );
        const catalogUpdates = Stream.fromPubSub(catalogInvalidations).pipe(
          // Preserve the supervisor's in-progress overlay. A plain catalog
          // projection would replace streamed text and tools with JSONL-only
          // history until the next authoritative runtime snapshot.
          Stream.mapEffect(() => projectSupervisorSnapshot(record, runtime)),
          Stream.map((snapshot) => ({
            kind: "snapshot" as const,
            snapshot,
          })),
        );
        const sequenceGate = new PiThreadStreamSequenceGate();
        return Stream.merge(
          runtimeStream(record, runtime),
          Stream.merge(lifecycleUpdates, catalogUpdates),
          { haltStrategy: "left" },
        ).pipe(Stream.filter((item) => sequenceGate.allows(item)));
      };
      const replacementRuntimeStreams = (record: PiSessionCatalogRecord) =>
        awaitRuntime(record).pipe(
          Stream.flatMap((runtime) => runtimeStreamWithLifecycle(record, runtime)),
          Stream.repeat(Schedule.spaced("100 millis")),
        );
      const filesystemUpdates = Stream.fromAsyncIterable(catalogTriggers(), () =>
        privateSourceError("catalog_watch", "Native Pi catalog watch failed."),
      ).pipe(
        Stream.tap(() => PubSub.publish(catalogInvalidations, undefined)),
        Stream.mapEffect(() => buildCatalog(true)),
        Stream.map((snapshot) => ({ snapshot, detailInvalidated: true as const })),
      );
      const runtimeUpdates = Stream.fromEffect(buildCatalogForRuntimeChange()).pipe(
        Stream.repeat(Schedule.spaced("1 second")),
        Stream.filter((snapshot) => snapshot !== undefined),
        Stream.map((snapshot) => ({ snapshot, detailInvalidated: false as const })),
      );
      const settingsUpdates = settingsChanges.pipe(
        Stream.filter((settings) => {
          if (settings.sidebarAutoSettleAfterDays === lastAutoSettleAfterDays) return false;
          lastAutoSettleAfterDays = settings.sidebarAutoSettleAfterDays;
          return true;
        }),
        Stream.mapEffect(() => buildCatalog(false)),
        Stream.map((snapshot) => ({ snapshot, detailInvalidated: false as const })),
      );
      // The ordinary settlement reactor sweeps every minute. External Pi
      // history is not in its sqlite snapshot, so re-evaluate the same policy
      // here without rereading JSONL.
      const inactivityUpdates = Stream.fromEffect(buildCatalog(false)).pipe(
        Stream.repeat(Schedule.spaced("1 minute")),
        Stream.drop(1),
        Stream.map((snapshot) => ({ snapshot, detailInvalidated: false as const })),
      );
      yield* Stream.merge(
        Stream.merge(filesystemUpdates, runtimeUpdates),
        Stream.merge(settingsUpdates, inactivityUpdates),
      ).pipe(
        Stream.retry(Schedule.spaced("1 second")),
        Stream.runForEach(({ snapshot, detailInvalidated }) => {
          return publishCatalogSnapshot(snapshot, detailInvalidated);
        }),
        Effect.forkScoped,
      );

      return PiExternalThreadSource.of({
        catalogSnapshot: () => SubscriptionRef.get(catalogSnapshots),
        subscribeCatalog: (input) => {
          let emittedSequence = -1;
          let synchronized = false;
          return SubscriptionRef.changes(catalogSnapshots).pipe(
            Stream.filter((snapshot) => {
              if (snapshot.snapshotSequence === emittedSequence) return false;
              emittedSequence = snapshot.snapshotSequence;
              return true;
            }),
            Stream.flatMap((snapshot) => {
              const snapshotItem = {
                kind: "snapshot",
                snapshot,
              } satisfies PiExternalCatalogStreamItem;
              if (input.requestCompletionMarker === true && !synchronized) {
                synchronized = true;
                return Stream.make(snapshotItem, {
                  kind: "synchronized",
                } satisfies PiExternalCatalogStreamItem);
              }
              return Stream.make(snapshotItem);
            }),
          );
        },
        resolve: findRecord,
        threadSnapshot: authoritativeThreadSnapshot,
        subscribeThread: (input) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const record = yield* findRecord(input.threadId);
              if (!record) {
                return Stream.fail(
                  sourceError("thread_not_found", `Thread ${input.threadId} was not found`),
                );
              }
              const runtime = runtimeFor(
                record,
                yield* supervisor.list().pipe(Effect.orElseSucceed(() => [])),
              );
              if (runtime) {
                return Stream.concat(
                  runtimeStreamWithLifecycle(record, runtime),
                  replacementRuntimeStreams(record),
                );
              }
              const initial = yield* readProjected(input.threadId);
              const attachmentGate = new CatalogRuntimeAttachmentGate();
              const catalogUpdates = Stream.fromPubSub(catalogInvalidations).pipe(
                Stream.takeWhile(() => attachmentGate.allowsCatalogUpdate()),
                Stream.mapEffect(() => readProjected(input.threadId)),
                Stream.map((snapshot) => catalogUpdateAfterRead(attachmentGate, snapshot)),
                Stream.takeWhile((snapshot) => snapshot !== undefined),
                Stream.filter(
                  (snapshot): snapshot is OrchestrationThreadDetailSnapshot =>
                    snapshot !== undefined,
                ),
                Stream.map((snapshot) => ({
                  kind: "snapshot" as const,
                  snapshot,
                })),
              );
              return Stream.concat(
                Stream.make(
                  { kind: "snapshot" as const, snapshot: initial },
                  { kind: "synchronized" as const },
                ),
                Stream.merge(
                  catalogUpdates,
                  replacementRuntimeStreams(record).pipe(
                    Stream.tap(() =>
                      Effect.sync(() => {
                        attachmentGate.attach();
                      }),
                    ),
                  ),
                ),
              );
            }),
          ),
        createSession: (input) =>
          Effect.gen(function* () {
            const receipt = yield* dispatchSupervisor({
              type: "start",
              commandId: input.commandId,
              cwd: input.cwd,
            });
            if (!receipt.runtimeId) {
              return yield* sourceError("create_failed", "pi did not return a runtime");
            }
            const runtime = (yield* supervisor.list()).find(
              (candidate) => candidate.runtimeId === receipt.runtimeId,
            );
            const sessionFile = runtime?.sessionFile ?? receiptSessionFile(receipt);
            if (!sessionFile) {
              yield* shutdownCreatedRuntime(supervisor, receipt.runtimeId);
              return yield* sourceError("create_failed", "pi did not create a session file");
            }
            const record = (yield* catalog.list()).find(
              (candidate) => candidate.canonicalFile === sessionFile,
            );
            if (!record) {
              yield* shutdownCreatedRuntime(supervisor, receipt.runtimeId);
              return yield* sourceError("create_failed", "pi session was not cataloged");
            }
            if (!runtime || runtime.status === "exited") {
              const recovery = yield* dispatchSupervisor({
                type: "start",
                commandId: CommandIdSchema.make(`pi-create-recovery:${input.commandId}`),
                cwd: input.cwd,
                sessionFile,
              });
              const recoveredRuntime =
                recovery.runtimeId === undefined
                  ? undefined
                  : (yield* supervisor.list()).find(
                      (candidate) =>
                        candidate.runtimeId === recovery.runtimeId && candidate.status !== "exited",
                    );
              if (!recoveredRuntime) {
                if (recovery.runtimeId !== undefined) {
                  yield* shutdownCreatedRuntime(supervisor, recovery.runtimeId);
                }
                return yield* sourceError(
                  "create_failed",
                  "pi session did not retain a managed runtime",
                );
              }
            }
            const snapshot = yield* buildCatalog(true);
            yield* publishCatalogSnapshot(snapshot, true);
            yield* PubSub.publish(catalogInvalidations, undefined);
            return { threadId: record.threadId };
          }),
        dispatch,
      });
    }),
  );
}
