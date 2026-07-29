// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import type { PiNativeJsonlEntry, PiNativeSession, PiNativeSessionKey } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { PiNativeError } from "@t3tools/contracts";

export interface SessionCatalogOptions {
  readonly root?: string;
}
const SESSION_ENTRY_LIMIT = 1_000;
const SESSION_HEAD_BYTES = 256 * 1024;
const SESSION_TAIL_BYTES = 16 * 1024 * 1024;
const SESSION_LIST_ENTRY_LIMIT = 50;
const SESSION_LIST_TAIL_BYTES = 1024 * 1024;
const keyFor = (file: string) =>
  NodeCrypto.createHash("sha256").update(file).digest("hex") as PiNativeSessionKey;
const record = Schema.is(Schema.Record(Schema.String, Schema.Unknown));

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

async function walk(root: string): Promise<string[]> {
  const out: string[] = [];
  for (const entry of await NodeFS.promises.readdir(root, { withFileTypes: true })) {
    const candidate = NodePath.join(root, entry.name);
    if (entry.isSymbolicLink()) continue;
    if (entry.isDirectory()) out.push(...(await walk(candidate)));
    else if (entry.isFile() && entry.name.endsWith(".jsonl")) out.push(candidate);
  }
  return out;
}

function parseEntries(text: string): PiNativeJsonlEntry[] {
  return text
    .split(/\r?\n/)
    .filter(Boolean)
    .flatMap((line) => {
      try {
        const value: unknown = JSON.parse(line);
        return record(value) ? [value] : [];
      } catch {
        return [];
      }
    });
}

async function readBoundedEntries(
  file: string,
  size: number,
  options: { readonly entryLimit: number; readonly tailBytes: number },
) {
  const handle = await NodeFS.promises.open(file, "r");
  try {
    const readRange = async (start: number, length: number) => {
      const buffer = Buffer.alloc(length);
      const { bytesRead } = await handle.read(buffer, 0, length, start);
      return buffer.subarray(0, bytesRead).toString("utf8");
    };
    const headLength = Math.min(size, SESSION_HEAD_BYTES);
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
    };
  } finally {
    await handle.close();
  }
}

export class SessionCatalog extends Context.Service<
  SessionCatalog,
  {
    readonly list: () => Effect.Effect<ReadonlyArray<PiNativeSession>, PiNativeError>;
    readonly read: (
      key: PiNativeSessionKey,
    ) => Effect.Effect<
      { readonly session: PiNativeSession; readonly entries: ReadonlyArray<PiNativeJsonlEntry> },
      PiNativeError
    >;
  }
>()("t3/piNative/SessionCatalog") {
  static layer = (options: SessionCatalogOptions = {}) =>
    Layer.succeed(SessionCatalog, makeSessionCatalog(options));
}

export function makeSessionCatalog(options: SessionCatalogOptions = {}): SessionCatalog["Service"] {
  const configuredRoot = NodePath.resolve(
    options.root ?? NodePath.join(NodeOS.homedir(), ".pi", "agent", "sessions"),
  );
  const scan = async () => {
    let root: string;
    try {
      root = await NodeFS.promises.realpath(configuredRoot);
    } catch (cause) {
      if ((cause as NodeJS.ErrnoException).code === "ENOENT") return [];
      throw cause;
    }
    const files = await walk(root);
    const rows = [];
    for (const candidate of files) {
      const row = await (async () => {
        const canonical = await NodeFS.promises.realpath(candidate);
        if (!canonical.startsWith(`${root}${NodePath.sep}`)) return undefined;
        const stat = await NodeFS.promises.stat(canonical);
        if (!stat.isFile()) return undefined;
        const bounded = await readBoundedEntries(canonical, stat.size, {
          entryLimit: SESSION_LIST_ENTRY_LIMIT,
          tailBytes: SESSION_LIST_TAIL_BYTES,
        });
        const entries = bounded.entries;
        const header = bounded.metadataEntries[0];
        if (
          !header ||
          header.type !== "session" ||
          typeof header.id !== "string" ||
          typeof header.cwd !== "string"
        )
          return undefined;
        let title: string | undefined;
        for (let i = bounded.metadataEntries.length - 1; i >= 0; i--)
          if (bounded.metadataEntries[i]?.type === "session_info") {
            title = textFrom(bounded.metadataEntries[i]);
            if (title) break;
          }
        if (!title)
          for (const entry of bounded.metadataEntries)
            if (
              entry.type === "message" &&
              (entry.role === "user" || (record(entry.message) && entry.message.role === "user"))
            ) {
              title = textFrom(entry);
              if (title) break;
            }
        const created =
          typeof header.timestamp === "string" ? header.timestamp : stat.birthtime.toISOString();
        return {
          session: {
            sessionKey: keyFor(canonical),
            sessionFile: canonical,
            sessionId: header.id,
            cwd: header.cwd,
            title: title ?? "Untitled pi session",
            createdAt: created,
            updatedAt: stat.mtime.toISOString(),
            liveness: "historical" as const,
          },
          entries,
        };
      })();
      if (row) rows.push(row);
    }
    return rows;
  };
  const failure = (cause: unknown) =>
    new PiNativeError({ code: "catalog_io", message: String(cause) });
  return SessionCatalog.of({
    list: () =>
      Effect.tryPromise({ try: scan, catch: failure }).pipe(
        Effect.map((rows) =>
          rows.map((row) => row.session).sort((a, b) => b.updatedAt.localeCompare(a.updatedAt)),
        ),
      ),
    read: (key) =>
      Effect.tryPromise({
        try: async () => {
          const found = (await scan()).find((row) => row.session.sessionKey === key);
          if (!found) throw new Error("unknown sessionKey");
          const stat = await NodeFS.promises.stat(found.session.sessionFile);
          const bounded = await readBoundedEntries(found.session.sessionFile, stat.size, {
            entryLimit: SESSION_ENTRY_LIMIT,
            tailBytes: SESSION_TAIL_BYTES,
          });
          return { ...found, entries: bounded.entries };
        },
        catch: failure,
      }),
  });
}
