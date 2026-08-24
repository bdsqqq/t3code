// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics preferSchemaOverJson:off
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { makeSessionCatalog, piThreadLifecycleFromEntries } from "./SessionCatalog.ts";
const digest = async (file: string) =>
  NodeCrypto.createHash("sha256")
    .update(await NodeFS.promises.readFile(file))
    .digest("hex");
describe("SessionCatalog", () => {
  it("projects the latest valid lifecycle marker and resets it on later activity", () => {
    const marker = {
      type: "custom",
      id: "lifecycle-1",
      parentId: "message-1",
      timestamp: "2026-08-08T10:00:00.000Z",
      customType: "t3.thread-lifecycle.v1",
      data: {
        version: 1,
        sessionId: "s1",
        override: "settled",
        operationId: "operation-1",
      },
    } as const;

    expect(piThreadLifecycleFromEntries([marker], "s1")).toMatchObject({
      override: "settled",
      operationId: "operation-1",
    });
    expect(
      piThreadLifecycleFromEntries(
        [
          marker,
          {
            type: "message",
            id: "message-2",
            parentId: "lifecycle-1",
            timestamp: "2026-08-08T10:01:00.000Z",
            message: { role: "user", content: "new work" },
          },
        ],
        "s1",
      ),
    ).toBeUndefined();
    expect(
      piThreadLifecycleFromEntries(
        [
          {
            ...marker,
            data: { ...marker.data, sessionId: "another-session" },
          },
        ],
        "s1",
      ),
    ).toBeUndefined();
  });

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
                JSON.stringify({
                  type: "model_change",
                  provider: "openai-codex",
                  modelId: "gpt-5.6-sol",
                }) +
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
          expect(listed[0]?.model).toBe("openai-codex/gpt-5.6-sol");
          expect((yield* catalog.read(listed[0]!.threadId)).entries).toHaveLength(3);
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
  it.effect("finds lifecycle state beyond the bounded catalog tail", () =>
    Effect.acquireUseRelease(
      Effect.tryPromise(() =>
        NodeFS.promises.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-pi-lifecycle-")),
      ),
      (root) =>
        Effect.gen(function* () {
          const file = NodePath.join(root, "session.jsonl");
          const entries = [
            { type: "session", id: "s1", cwd: root },
            {
              type: "message",
              id: "user-1",
              parentId: null,
              timestamp: "2026-08-08T09:00:00.000Z",
              message: { role: "user", content: "work" },
            },
            {
              type: "custom",
              id: "lifecycle-1",
              parentId: "user-1",
              timestamp: "2026-08-08T10:00:00.000Z",
              customType: "t3.thread-lifecycle.v1",
              data: {
                version: 1,
                sessionId: "s1",
                override: "settled",
                operationId: "operation-1",
              },
            },
            ...Array.from({ length: 200 }, (_, index) => ({
              type: "custom",
              id: `noise-${String(index)}`,
              parentId: "lifecycle-1",
              timestamp: "2026-08-08T10:01:00.000Z",
              customType: "test.noise",
              data: { text: "x".repeat(1_024) },
            })),
          ];
          yield* Effect.tryPromise(() =>
            NodeFS.promises.writeFile(
              file,
              `${entries.map((entry) => JSON.stringify(entry)).join("\n")}\n`,
            ),
          );

          const catalog = makeSessionCatalog({ root });
          const listed = yield* catalog.list();
          expect(listed[0]?.jsonlLifecycle?.override).toBe("settled");
          expect(
            yield* catalog.findLifecycleOperation(listed[0]!.threadId, "operation-1"),
          ).toMatchObject({ override: "settled", supersededByUser: false });
          yield* Effect.tryPromise(() =>
            NodeFS.promises.appendFile(
              file,
              `${JSON.stringify({
                type: "message",
                id: "user-2",
                parentId: "lifecycle-1",
                timestamp: "2026-08-08T11:00:00.000Z",
                message: {
                  content: "new work".repeat(150_000),
                  role: "user",
                },
              })}\n`,
            ),
          );
          expect((yield* catalog.list())[0]?.jsonlLifecycle).toBeUndefined();
          expect(
            yield* catalog.findLifecycleOperation(listed[0]!.threadId, "operation-1"),
          ).toMatchObject({ override: "settled", supersededByUser: true });
        }),
      (root) => Effect.promise(() => NodeFS.promises.rm(root, { recursive: true, force: true })),
    ),
  );
  it.effect("finds the latest model beyond the bounded catalog head and tail", () =>
    Effect.acquireUseRelease(
      Effect.tryPromise(() => NodeFS.promises.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-pi-"))),
      (root) =>
        Effect.gen(function* () {
          const file = NodePath.join(root, "session.jsonl");
          const noise = (prefix: string) =>
            Array.from({ length: 80 }, (_, index) => ({
              type: "custom",
              customType: "test.noise",
              data: { text: `${prefix}-${String(index)}-${"x".repeat(1_024)}` },
            }));
          const entries = [
            { type: "session", id: "s1", cwd: root },
            { type: "model_change", provider: "openai", modelId: "old-model" },
            ...noise("before"),
            { type: "model_change", provider: "openai", modelId: "new-model" },
            { type: "message", role: "user", content: "latest turn" },
            ...noise("after"),
          ];
          yield* Effect.tryPromise(() =>
            NodeFS.promises.writeFile(
              file,
              `${entries.map((entry) => JSON.stringify(entry)).join("\n")}\n`,
            ),
          );

          const listed = yield* makeSessionCatalog({ root }).list();

          expect(listed[0]?.model).toBe("openai/new-model");
        }),
      (root) => Effect.promise(() => NodeFS.promises.rm(root, { recursive: true, force: true })),
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
