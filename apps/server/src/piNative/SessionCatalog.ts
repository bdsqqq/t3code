// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import type {
  ExternalThreadHistoryTruncation,
  PiNativeJsonlEntry,
  PiNativeSessionKey,
  PiThreadLifecycleOverride,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import {
  PiNativeError,
  PiThreadLifecycleCustomEntry,
  ThreadId as ThreadIdSchema,
} from "@t3tools/contracts";
import { defaultPiSessionsRoot } from "./PiSessionsRoot.ts";
export { defaultPiSessionsRoot, resolvePiSessionsRoot } from "./PiSessionsRoot.ts";

export interface SessionCatalogOptions {
  readonly root?: string;
}
const SESSION_ENTRY_LIMIT = 1_000;
const SESSION_HEAD_BYTES = 256 * 1024;
const SESSION_TAIL_BYTES = 16 * 1024 * 1024;
const SESSION_CATALOG_FILE_LIMIT = 5_000;
const SESSION_LIST_ENTRY_LIMIT = 50;
const SESSION_LIST_HEAD_BYTES = 64 * 1024;
const SESSION_LIST_TAIL_BYTES = 64 * 1024;
const SESSION_TITLE_MAX_CHARS = 512;
const SESSION_METADATA_LINE_MAX_CHARS = 1024 * 1024;
const keyFor = (file: string) =>
  NodeCrypto.createHash("sha256").update(file).digest("hex") as PiNativeSessionKey;
const threadIdFor = (canonicalFile: string) =>
  ThreadIdSchema.make(`external:pi:path:${keyFor(canonicalFile)}`);
const record = Schema.is(Schema.Record(Schema.String, Schema.Unknown));
const lifecycleEntry = Schema.is(PiThreadLifecycleCustomEntry);
const isUserMessageEntry = (entry: PiNativeJsonlEntry): boolean =>
  entry.type === "message" &&
  (entry.role === "user" || (record(entry.message) && entry.message.role === "user"));

export interface PiSessionCatalogRecord {
  readonly sourceKey: PiNativeSessionKey;
  readonly threadId: ThreadId;
  readonly canonicalFile: string;
  readonly sessionId: string;
  readonly model?: string;
  readonly cwd: string;
  readonly title: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly fileSize: number;
  readonly fileMtimeMs: number;
  readonly lastActivityAt?: string;
  readonly jsonlLifecycle?: {
    readonly override: PiThreadLifecycleOverride;
    readonly operationId: string;
    readonly updatedAt: string;
  };
  readonly historyTruncation: ExternalThreadHistoryTruncation;
}
type PiSessionCatalogMetadata = Omit<PiSessionCatalogRecord, "threadId">;
interface CachedCatalogMetadata {
  readonly size: number;
  readonly mtimeMs: number;
  readonly row: PiSessionCatalogMetadata;
}

function textFrom(value: unknown): string | undefined {
  if (typeof value === "string" && value.trim()) return value.trim();
  if (Array.isArray(value))
    for (const item of value) {
      const found = textFrom(item);
      if (found) return found;
    }
  if (record(value))
    for (const key of ["name", "text", "content", "message"]) {
      const found = textFrom(value[key]);
      if (found) return found;
    }
}

async function walk(root: string): Promise<{
  readonly files: ReadonlyArray<string>;
  readonly omittedCount: number;
}> {
  const newest: string[] = [];
  let total = 0;
  const key = (file: string) => `${NodePath.basename(file)}\0${file}`;
  const bubbleDown = () => {
    let index = 0;
    while (true) {
      const left = index * 2 + 1;
      const right = left + 1;
      let oldest = index;
      if (left < newest.length && key(newest[left]!) < key(newest[oldest]!)) oldest = left;
      if (right < newest.length && key(newest[right]!) < key(newest[oldest]!)) oldest = right;
      if (oldest === index) return;
      [newest[index], newest[oldest]] = [newest[oldest]!, newest[index]!];
      index = oldest;
    }
  };
  const retain = (file: string) => {
    total += 1;
    if (newest.length < SESSION_CATALOG_FILE_LIMIT) {
      newest.push(file);
      let index = newest.length - 1;
      while (index > 0) {
        const parent = Math.floor((index - 1) / 2);
        if (key(newest[parent]!) <= key(newest[index]!)) break;
        [newest[parent], newest[index]] = [newest[index]!, newest[parent]!];
        index = parent;
      }
      return;
    }
    if (key(file) <= key(newest[0]!)) return;
    newest[0] = file;
    bubbleDown();
  };
  const visit = async (directory: string): Promise<void> => {
    let entries: NodeFS.Dirent[];
    try {
      entries = await NodeFS.promises.readdir(directory, { withFileTypes: true });
    } catch (cause) {
      const code = (cause as NodeJS.ErrnoException).code;
      if (code === "ENOENT" || code === "ENOTDIR") return;
      throw cause;
    }
    entries.sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      const candidate = NodePath.join(directory, entry.name);
      if (entry.isSymbolicLink()) continue;
      if (entry.isDirectory()) await visit(candidate);
      else if (entry.isFile() && entry.name.endsWith(".jsonl")) retain(candidate);
    }
  };
  await visit(root);
  return {
    files: newest.sort((left, right) => key(right).localeCompare(key(left))),
    omittedCount: Math.max(0, total - newest.length),
  };
}

function parseEntry(line: string): PiNativeJsonlEntry | undefined {
  try {
    const value: unknown = JSON.parse(line);
    return record(value) ? value : undefined;
  } catch {
    return undefined;
  }
}

function modelFromEntry(entry: PiNativeJsonlEntry): string | undefined {
  if (entry.type === "model_change" && typeof entry.modelId === "string") {
    return typeof entry.provider === "string"
      ? `${entry.provider}/${entry.modelId}`
      : entry.modelId;
  }
  if (
    entry.type === "message" &&
    record(entry.message) &&
    entry.message.role === "assistant" &&
    typeof entry.message.model === "string"
  ) {
    return typeof entry.message.provider === "string"
      ? `${entry.message.provider}/${entry.message.model}`
      : entry.message.model;
  }
}

function modelFromEntries(entries: ReadonlyArray<PiNativeJsonlEntry>): string | undefined {
  return entries.reduce<string | undefined>(
    (model, entry) => modelFromEntry(entry) ?? model,
    undefined,
  );
}

function parseEntries(text: string): PiNativeJsonlEntry[] {
  return text
    .split(/\r?\n/)
    .filter(Boolean)
    .flatMap((line) => {
      const entry = parseEntry(line);
      return entry === undefined ? [] : [entry];
    });
}

async function readLatestSessionMetadata(
  file: string,
  size: number,
  sessionId: string,
  targetOperationId?: string,
  fallbackModel?: string,
): Promise<{
  readonly model?: string;
  readonly jsonlLifecycle?: NonNullable<PiSessionCatalogRecord["jsonlLifecycle"]>;
  readonly lastActivityAt?: string;
  readonly lifecycleOperation?: {
    readonly override: PiThreadLifecycleOverride;
    readonly updatedAt: string;
    readonly supersededByUser: boolean;
  };
}> {
  const handle = await NodeFS.promises.open(file, "r");
  try {
    const chunkSize = 64 * 1024;
    let position = size;
    let leadingFragment = "";
    let discardingOversizedLine = false;
    let lifecycleResolved = false;
    let model: string | undefined;
    let jsonlLifecycle: PiSessionCatalogRecord["jsonlLifecycle"];
    let lastActivityAt: string | undefined;
    let laterUserSeen = false;
    let lifecycleOperation:
      | {
          readonly override: PiThreadLifecycleOverride;
          readonly updatedAt: string;
          readonly supersededByUser: boolean;
        }
      | undefined;
    const inspect = (line: string) => {
      const entry = parseEntry(line);
      if (entry === undefined) return;
      model ??= modelFromEntry(entry);
      if (
        lastActivityAt === undefined &&
        entry.type === "message" &&
        typeof entry.timestamp === "string" &&
        !Number.isNaN(Date.parse(entry.timestamp))
      ) {
        lastActivityAt = entry.timestamp;
      }
      if (isUserMessageEntry(entry)) {
        laterUserSeen = true;
        if (!lifecycleResolved) lifecycleResolved = true;
        return;
      }
      if (!lifecycleEntry(entry) || entry.data.sessionId !== sessionId) return;
      if (targetOperationId !== undefined && entry.data.operationId === targetOperationId) {
        lifecycleOperation = {
          override: entry.data.override,
          updatedAt: entry.timestamp,
          supersededByUser: laterUserSeen,
        };
      }
      if (lifecycleResolved) return;
      lifecycleResolved = true;
      jsonlLifecycle = {
        override: entry.data.override,
        operationId: entry.data.operationId,
        updatedAt: entry.timestamp,
      };
    };
    const inspectOversizedPrefix = (prefix: string) => {
      if (!/"type"\s*:\s*"message"/.test(prefix)) return;
      const timestamp = prefix.match(/"timestamp"\s*:\s*"([^"]+)"/)?.[1];
      if (
        lastActivityAt === undefined &&
        timestamp !== undefined &&
        !Number.isNaN(Date.parse(timestamp))
      ) {
        lastActivityAt = timestamp;
      }
      // The discarded body can place `role` after arbitrarily large content,
      // so its value is not safely recoverable from a bounded prefix. Treat
      // any oversized message as activity that clears settlement: a false
      // reset is preferable to hiding potentially new user work.
      laterUserSeen = true;
      if (!lifecycleResolved) lifecycleResolved = true;
    };

    while (
      position > 0 &&
      (!lifecycleResolved ||
        (model === undefined && fallbackModel !== undefined) ||
        lastActivityAt === undefined ||
        (targetOperationId !== undefined && lifecycleOperation === undefined))
    ) {
      const start = Math.max(0, position - chunkSize);
      const buffer = Buffer.alloc(position - start);
      const { bytesRead } = await handle.read(buffer, 0, buffer.length, start);
      let chunk = buffer.subarray(0, bytesRead).toString("utf8");
      if (discardingOversizedLine) {
        const newline = chunk.lastIndexOf("\n");
        if (newline < 0) {
          position = start;
          continue;
        }
        inspectOversizedPrefix(chunk.slice(newline + 1));
        chunk = chunk.slice(0, newline + 1);
        discardingOversizedLine = false;
      }
      const lines = `${chunk}${leadingFragment}`.split(/\r?\n/);
      leadingFragment = start === 0 ? "" : (lines.shift() ?? "");
      if (leadingFragment.length > SESSION_METADATA_LINE_MAX_CHARS) {
        leadingFragment = "";
        discardingOversizedLine = true;
      }
      if (start === 0 && lines[0] === "") lines.shift();
      for (let index = lines.length - 1; index >= 0; index -= 1) {
        const line = lines[index];
        if (line) inspect(line);
      }
      position = start;
    }
    const resolvedModel = model ?? fallbackModel;
    return {
      ...(resolvedModel === undefined ? {} : { model: resolvedModel }),
      ...(jsonlLifecycle === undefined ? {} : { jsonlLifecycle }),
      ...(lastActivityAt === undefined ? {} : { lastActivityAt }),
      ...(lifecycleOperation === undefined ? {} : { lifecycleOperation }),
    };
  } finally {
    await handle.close();
  }
}

async function readBoundedEntries(
  file: string,
  size: number,
  options: { readonly entryLimit: number; readonly headBytes?: number; readonly tailBytes: number },
) {
  const handle = await NodeFS.promises.open(file, "r");
  try {
    const readRange = async (start: number, length: number) => {
      const buffer = Buffer.alloc(length);
      const { bytesRead } = await handle.read(buffer, 0, length, start);
      return buffer.subarray(0, bytesRead).toString("utf8");
    };
    const headLength = Math.min(size, options.headBytes ?? SESSION_HEAD_BYTES);
    const tailStart = Math.max(0, size - options.tailBytes);
    const [rawHead, rawTail] = await Promise.all([
      readRange(0, headLength),
      readRange(tailStart, size - tailStart),
    ]);
    const head =
      headLength < size && !rawHead.endsWith("\n")
        ? rawHead.slice(0, rawHead.lastIndexOf("\n") + 1)
        : rawHead;
    const tail =
      tailStart > 0 && !rawTail.startsWith("\n")
        ? rawTail.slice(rawTail.indexOf("\n") + 1)
        : rawTail;
    const headEntries = parseEntries(head);
    const tailEntries = parseEntries(tail);
    const header = headEntries.find((entry) => entry.type === "session");
    return {
      metadataEntries: tailStart === 0 ? tailEntries : [...headEntries, ...tailEntries],
      entries: header
        ? [
            header,
            ...tailEntries
              .filter((entry) => entry.type !== "session" || entry.id !== header.id)
              .slice(-(options.entryLimit - 1)),
          ]
        : tailEntries.slice(-options.entryLimit),
      truncated: tailStart > 0 || tailEntries.length > options.entryLimit,
    };
  } finally {
    await handle.close();
  }
}
function titleFrom(entries: ReadonlyArray<PiNativeJsonlEntry>): string {
  for (let index = entries.length - 1; index >= 0; index--)
    if (entries[index]?.type === "session_info") {
      const title = textFrom(entries[index]);
      if (title) return title.slice(0, SESSION_TITLE_MAX_CHARS);
    }
  for (const entry of entries)
    if (
      entry.type === "message" &&
      (entry.role === "user" || (record(entry.message) && entry.message.role === "user"))
    ) {
      const title = textFrom(entry);
      if (title) return title.slice(0, SESSION_TITLE_MAX_CHARS);
    }
  return "Untitled pi session";
}

export function piThreadLifecycleFromEntries(
  entries: ReadonlyArray<PiNativeJsonlEntry>,
  sessionId: string,
): PiSessionCatalogRecord["jsonlLifecycle"] {
  let lifecycle: PiSessionCatalogRecord["jsonlLifecycle"];
  for (const entry of entries) {
    if (isUserMessageEntry(entry)) {
      lifecycle = undefined;
      continue;
    }
    if (!lifecycleEntry(entry) || entry.data.sessionId !== sessionId) continue;
    lifecycle = {
      override: entry.data.override,
      operationId: entry.data.operationId,
      updatedAt: entry.timestamp,
    };
  }
  return lifecycle;
}

export class SessionCatalog extends Context.Service<
  SessionCatalog,
  {
    readonly list: (
      priorityFiles?: ReadonlyArray<string>,
    ) => Effect.Effect<ReadonlyArray<PiSessionCatalogRecord>, PiNativeError>;
    readonly omittedCount: () => Effect.Effect<number>;
    readonly read: (threadId: ThreadId) => Effect.Effect<
      {
        readonly record: PiSessionCatalogRecord;
        readonly entries: ReadonlyArray<PiNativeJsonlEntry>;
      },
      PiNativeError
    >;
    readonly findLifecycleOperation: (
      threadId: ThreadId,
      operationId: string,
    ) => Effect.Effect<
      | {
          readonly override: PiThreadLifecycleOverride;
          readonly updatedAt: string;
          readonly supersededByUser: boolean;
        }
      | undefined,
      PiNativeError
    >;
  }
>()("t3/piNative/SessionCatalog") {
  static layer = (options: SessionCatalogOptions = {}) =>
    Layer.succeed(SessionCatalog, makeSessionCatalog(options));
}

export function makeSessionCatalog(options: SessionCatalogOptions = {}): SessionCatalog["Service"] {
  const configuredRoot = NodePath.resolve(options.root ?? defaultPiSessionsRoot());
  let metadataByFile = new Map<string, CachedCatalogMetadata>();
  let omittedFileCount = 0;
  const scan = async (priorityFiles: ReadonlyArray<string> = []) => {
    let root: string;
    try {
      root = await NodeFS.promises.realpath(configuredRoot);
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code === "ENOENT") return [];
      throw cause;
    }
    const discoveredPaths = await walk(root);
    omittedFileCount = discoveredPaths.omittedCount;
    const selectedCandidates = [...discoveredPaths.files];
    const selectedCandidateSet = new Set(selectedCandidates);
    for (const priorityFile of priorityFiles.slice(0, SESSION_CATALOG_FILE_LIMIT)) {
      const accepted = await (async () => {
        const canonical = await NodeFS.promises.realpath(priorityFile);
        if (!canonical.startsWith(`${root}${NodePath.sep}`)) return undefined;
        const stat = await NodeFS.promises.stat(canonical);
        return stat.isFile() ? canonical : undefined;
      })().catch(() => undefined);
      if (!accepted || selectedCandidateSet.has(accepted)) continue;
      if (selectedCandidates.length >= SESSION_CATALOG_FILE_LIMIT) {
        const removed = selectedCandidates.pop();
        if (removed) selectedCandidateSet.delete(removed);
      }
      selectedCandidates.push(accepted);
      selectedCandidateSet.add(accepted);
    }
    const discovered: Array<{
      readonly canonical: string;
      readonly stat: NodeFS.Stats;
    }> = [];
    for (let offset = 0; offset < selectedCandidates.length; offset += 32) {
      const batch = await Promise.all(
        selectedCandidates.slice(offset, offset + 32).map((candidate) =>
          (async () => {
            const canonical = await NodeFS.promises.realpath(candidate);
            if (!canonical.startsWith(`${root}${NodePath.sep}`)) return undefined;
            const stat = await NodeFS.promises.stat(canonical);
            return stat.isFile() ? { canonical, stat } : undefined;
          })().catch(() => undefined),
        ),
      );
      for (const found of batch) if (found) discovered.push(found);
    }
    const files = discovered.sort(
      (left, right) =>
        right.stat.mtimeMs - left.stat.mtimeMs || right.canonical.localeCompare(left.canonical),
    );
    const rows: PiSessionCatalogMetadata[] = [];
    const nextMetadataByFile = new Map<string, CachedCatalogMetadata>();
    for (const { canonical, stat } of files) {
      const row = await (async () => {
        const cached = metadataByFile.get(canonical);
        if (cached && cached.size === stat.size && cached.mtimeMs === stat.mtimeMs) {
          nextMetadataByFile.set(canonical, cached);
          return cached.row;
        }
        const bounded = await readBoundedEntries(canonical, stat.size, {
          entryLimit: SESSION_LIST_ENTRY_LIMIT,
          headBytes: SESSION_LIST_HEAD_BYTES,
          tailBytes: SESSION_LIST_TAIL_BYTES,
        });
        const header = bounded.metadataEntries[0];
        if (
          !header ||
          header.type !== "session" ||
          typeof header.id !== "string" ||
          typeof header.cwd !== "string"
        )
          return undefined;
        const created =
          typeof header.timestamp === "string" ? header.timestamp : stat.birthtime.toISOString();
        const headerCwd = header.cwd;
        const latestMetadata = await readLatestSessionMetadata(
          canonical,
          stat.size,
          header.id,
          undefined,
          modelFromEntries(bounded.metadataEntries),
        );
        const metadata = {
          sourceKey: keyFor(canonical),
          canonicalFile: canonical,
          sessionId: header.id,
          cwd: await NodeFS.promises.realpath(headerCwd).catch(() => NodePath.resolve(headerCwd)),
          title: titleFrom(bounded.metadataEntries),
          createdAt: created,
          updatedAt: stat.mtime.toISOString(),
          fileSize: stat.size,
          fileMtimeMs: stat.mtimeMs,
          ...latestMetadata,
          historyTruncation: { truncated: bounded.truncated },
        } satisfies PiSessionCatalogMetadata;
        nextMetadataByFile.set(canonical, {
          size: stat.size,
          mtimeMs: stat.mtimeMs,
          row: metadata,
        });
        return metadata;
      })().catch(() => undefined);
      if (row) rows.push(row);
    }
    metadataByFile = nextMetadataByFile;
    return rows
      .map((row) => ({
        ...row,
        threadId: threadIdFor(row.canonicalFile),
      }))
      .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
  };
  const failure = () =>
    new PiNativeError({ code: "catalog_io", message: "Native Pi catalog access failed." });
  let recordsByThreadId = new Map<ThreadId, PiSessionCatalogRecord>();
  const refresh = async (priorityFiles?: ReadonlyArray<string>) => {
    try {
      const records = await scan(priorityFiles);
      recordsByThreadId = new Map(records.map((record) => [record.threadId, record]));
      return records;
    } catch (cause) {
      if (recordsByThreadId.size > 0) return [...recordsByThreadId.values()];
      throw cause;
    }
  };
  return SessionCatalog.of({
    list: (priorityFiles) =>
      Effect.tryPromise({ try: () => refresh(priorityFiles), catch: failure }),
    omittedCount: () => Effect.succeed(omittedFileCount),
    read: (threadId) =>
      Effect.tryPromise({
        try: async () => {
          const catalogRecord =
            recordsByThreadId.get(threadId) ??
            (await refresh()).find((candidate) => candidate.threadId === threadId);
          if (!catalogRecord) throw new Error("unknown external thread");
          const root = await NodeFS.promises.realpath(configuredRoot);
          const sessionFile = await NodeFS.promises.realpath(catalogRecord.canonicalFile);
          if (!sessionFile.startsWith(`${root}${NodePath.sep}`))
            throw new Error("session escaped catalog root");
          const stat = await NodeFS.promises.stat(sessionFile);
          const bounded = await readBoundedEntries(sessionFile, stat.size, {
            entryLimit: SESSION_ENTRY_LIMIT,
            tailBytes: SESSION_TAIL_BYTES,
          });
          const header = bounded.metadataEntries[0];
          if (
            !header ||
            header.type !== "session" ||
            typeof header.id !== "string" ||
            typeof header.cwd !== "string"
          )
            throw new Error("invalid session header");
          const latestMetadata = await readLatestSessionMetadata(
            sessionFile,
            stat.size,
            header.id,
            undefined,
            modelFromEntries(bounded.metadataEntries),
          );
          const {
            jsonlLifecycle: _priorLifecycle,
            lastActivityAt: _priorActivity,
            model: _priorModel,
            ...freshCatalogRecord
          } = catalogRecord;
          return {
            record: {
              ...freshCatalogRecord,
              title: titleFrom(bounded.metadataEntries),
              updatedAt: stat.mtime.toISOString(),
              fileSize: stat.size,
              fileMtimeMs: stat.mtimeMs,
              ...latestMetadata,
              historyTruncation: { truncated: bounded.truncated },
            },
            entries: bounded.entries,
          };
        },
        catch: failure,
      }),
    findLifecycleOperation: (threadId, operationId) =>
      Effect.tryPromise({
        try: async () => {
          const catalogRecord =
            recordsByThreadId.get(threadId) ??
            (await refresh()).find((candidate) => candidate.threadId === threadId);
          if (!catalogRecord) throw new Error("unknown external thread");
          const root = await NodeFS.promises.realpath(configuredRoot);
          const sessionFile = await NodeFS.promises.realpath(catalogRecord.canonicalFile);
          if (!sessionFile.startsWith(`${root}${NodePath.sep}`)) {
            throw new Error("session escaped catalog root");
          }
          const stat = await NodeFS.promises.stat(sessionFile);
          return (
            await readLatestSessionMetadata(
              sessionFile,
              stat.size,
              catalogRecord.sessionId,
              operationId,
            )
          ).lifecycleOperation;
        },
        catch: failure,
      }),
  });
}
