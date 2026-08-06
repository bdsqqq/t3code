// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics preferSchemaOverJson:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { makeSessionCatalog } from "./SessionCatalog.ts";
const digest = async (file: string) =>
  NodeCrypto.createHash("sha256")
    .update(await NodeFS.promises.readFile(file))
    .digest("hex");
describe("SessionCatalog", () => {
  it.effect("reads without mutation", () =>
    Effect.acquireUseRelease(
      Effect.tryPromise(() => NodeFS.promises.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-pi-"))),
      (root) =>
        Effect.gen(function* () {
          const file = NodePath.join(root, "nested", "session.jsonl");
          yield* Effect.tryPromise(() =>
            NodeFS.promises.mkdir(NodePath.dirname(file), { recursive: true }),
          );
          yield* Effect.tryPromise(() =>
            NodeFS.promises.writeFile(
              file,
              JSON.stringify({ type: "session", id: "s1", cwd: root }) +
                "\n" +
                JSON.stringify({ type: "message", role: "user", content: "hello" }) +
                "\n",
            ),
          );
          const before = yield* Effect.tryPromise(async () => ({
            hash: await digest(file),
            stat: await NodeFS.promises.stat(file),
          }));
          const catalog = makeSessionCatalog({ root });
          const listed = yield* catalog.list();
          expect(listed[0]?.title).toBe("hello");
          expect((yield* catalog.read(listed[0]!.threadId)).entries).toHaveLength(2);
          const originalThreadId = listed[0]!.threadId;
          yield* Effect.tryPromise(() =>
            NodeFS.promises.copyFile(file, NodePath.join(root, "nested", "copy.jsonl")),
          );
          const relisted = yield* catalog.list();
          const canonicalFile = yield* Effect.tryPromise(() => NodeFS.promises.realpath(file));
          expect(relisted.find((record) => record.canonicalFile === canonicalFile)?.threadId).toBe(
            originalThreadId,
          );
          const after = yield* Effect.tryPromise(async () => ({
            hash: await digest(file),
            stat: await NodeFS.promises.stat(file),
          }));
          expect(after.hash).toBe(before.hash);
          expect(after.stat.mtimeMs).toBe(before.stat.mtimeMs);
          expect(after.stat.size).toBe(before.stat.size);
        }),
      (root) => Effect.promise(() => NodeFS.promises.rm(root, { recursive: true, force: true })),
    ),
  );
  it.effect("ignores symlink escapes", () =>
    Effect.acquireUseRelease(
      Effect.tryPromise(async () => ({
        root: await NodeFS.promises.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-root-")),
        outside: await NodeFS.promises.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-out-")),
      })),
      ({ root, outside }) =>
        Effect.gen(function* () {
          yield* Effect.tryPromise(() =>
            NodeFS.promises.writeFile(
              NodePath.join(outside, "escaped.jsonl"),
              JSON.stringify({ type: "session", id: "escape", cwd: outside }) + "\n",
            ),
          );
          yield* Effect.tryPromise(() =>
            NodeFS.promises.symlink(outside, NodePath.join(root, "linked")),
          );
          expect(yield* makeSessionCatalog({ root }).list()).toEqual([]);
        }),
      ({ root, outside }) =>
        Effect.promise(() =>
          Promise.all([
            NodeFS.promises.rm(root, { recursive: true, force: true }),
            NodeFS.promises.rm(outside, { recursive: true, force: true }),
          ]).then(() => undefined),
        ),
    ),
  );
  it.effect("resolves Pi parent session paths to parent thread ids", () =>
    Effect.acquireUseRelease(
      Effect.tryPromise(() => NodeFS.promises.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-pi-"))),
      (root) =>
        Effect.gen(function* () {
          const parentFile = NodePath.join(root, "parent.jsonl");
          const childFile = NodePath.join(root, "child.jsonl");
          yield* Effect.tryPromise(() =>
            Promise.all([
              NodeFS.promises.writeFile(
                parentFile,
                JSON.stringify({ type: "session", id: "parent", cwd: root }) + "\n",
              ),
              NodeFS.promises.writeFile(
                childFile,
                JSON.stringify({
                  type: "session",
                  id: "child",
                  cwd: root,
                  parentSession: parentFile,
                }) + "\n",
              ),
            ]),
          );

          const listed = yield* makeSessionCatalog({ root }).list();
          const parent = listed.find((record) => record.sessionId === "parent");
          const child = listed.find((record) => record.sessionId === "child");
          expect(child?.parentThreadId).toBe(parent?.threadId);
          expect(child?.parentSessionFile).toBe(parent?.canonicalFile);
        }),
      (root) => Effect.promise(() => NodeFS.promises.rm(root, { recursive: true, force: true })),
    ),
  );
});
