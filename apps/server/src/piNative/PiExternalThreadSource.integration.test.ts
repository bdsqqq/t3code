// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - isolated process and JSONL fixtures.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { CommandId, type OrchestrationThreadDetailSnapshot } from "@t3tools/contracts";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { afterAll, expect, vi } from "vite-plus/test";

const sandbox = vi.hoisted(() => {
  // Unix socket paths must stay short, including on macOS.
  const fs = require("node:fs") as typeof NodeFS;
  const root = fs.realpathSync(fs.mkdtempSync("/tmp/t3-pi-refresh-"));
  vi.stubEnv("T3_PI_SUPERVISOR_ROOT", `${root}/supervisor`);
  vi.stubEnv("T3_PI_SESSIONS_ROOT", `${root}/sessions`);
  return root;
});

import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { PiExternalLifecycleOverrideRepository } from "../persistence/Services/PiExternalLifecycleOverrides.ts";
import { PiExternalThreadSource } from "./PiExternalThreadSource.ts";
import { SessionCatalog } from "./SessionCatalog.ts";
import { SupervisorClient } from "./SupervisorClient.ts";

afterAll(() => {
  vi.unstubAllEnvs();
  NodeFS.rmSync(sandbox, { recursive: true, force: true });
});

it.live("refreshes an attached idle thread after a custom JSONL append without another turn", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const sessions = NodePath.join(sandbox, "sessions");
      const supervisorRoot = NodePath.join(sandbox, "supervisor");
      NodeFS.mkdirSync(sessions);
      NodeFS.mkdirSync(supervisorRoot);
      const sessionFile = NodePath.join(sessions, "session.jsonl");
      const createdAt = "2026-09-15T00:00:00.000Z";
      NodeFS.writeFileSync(
        sessionFile,
        [
          { type: "session", version: 3, id: "idle-session", cwd: sandbox, timestamp: createdAt },
          {
            type: "message",
            id: "user-1",
            parentId: null,
            timestamp: createdAt,
            message: { role: "user", content: "inspect the fixture" },
          },
          {
            type: "message",
            id: "assistant-1",
            parentId: "user-1",
            timestamp: createdAt,
            message: {
              role: "assistant",
              content: [{ type: "text", text: "done" }],
              stopReason: "stop",
            },
          },
        ]
          .map((entry) => JSON.stringify(entry))
          .join("\n") + "\n",
      );
      const executable = NodePath.join(sandbox, "pi-fixture");
      // A quiescent RPC peer: no agent events, prompts, model calls, or live Pi state.
      NodeFS.writeFileSync(
        executable,
        `#!${process.execPath}\n
process.stdin.setEncoding("utf8");
let buffer = "";
process.stdin.on("data", chunk => {
  buffer += chunk;
  let newline;
  while ((newline = buffer.indexOf("\\n")) >= 0) {
    const command = JSON.parse(buffer.slice(0, newline));
    buffer = buffer.slice(newline + 1);
    process.stdout.write(JSON.stringify({
      type: "response", id: command.id, command: command.type,
      success: command.type === "get_state",
      data: { sessionFile: ${JSON.stringify(sessionFile)}, isStreaming: false, pendingMessageCount: 0 },
    }) + "\\n");
  }
});
process.stdin.on("end", () => process.exit(0));
`,
        { mode: 0o700 },
      );

      const listening = Promise.withResolvers<void>();
      const watcher = NodeFS.watch(supervisorRoot, (_event, filename) => {
        if (filename === "supervisor.sock") listening.resolve();
      });
      yield* Effect.addFinalizer(() => Effect.sync(() => watcher.close()));
      const daemonUrl = new URL("./SupervisorDaemon.ts", import.meta.url);
      const daemon = NodeChildProcess.spawn(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          `import { runSupervisorDaemon } from ${JSON.stringify(daemonUrl.href)}; await runSupervisorDaemon();`,
        ],
        {
          cwd: sandbox,
          env: { ...process.env, T3_PI_EXECUTABLE: executable },
          stdio: ["ignore", "ignore", "pipe"],
        },
      );
      let errors = "";
      daemon.stderr.setEncoding("utf8").on("data", (chunk: string) => {
        errors += chunk;
      });
      const exited = new Promise<void>((resolve) => daemon.once("exit", () => resolve()));
      yield* Effect.addFinalizer(() =>
        Effect.promise(async () => {
          daemon.kill("SIGTERM");
          await exited;
        }),
      );
      yield* Effect.promise(() => listening.promise).pipe(Effect.timeout("5 seconds"));
      expect(errors).toBe("");

      const dependencies = Layer.mergeAll(
        SessionCatalog.layer({ root: sessions }),
        SupervisorClient.layer,
        Layer.mock(ProjectionSnapshotQuery)({
          getShellSnapshot: () =>
            Effect.succeed({
              snapshotSequence: 0,
              projects: [],
              threads: [],
              updatedAt: createdAt,
            }),
        }),
        Layer.mock(PiExternalLifecycleOverrideRepository)({
          list: () => Effect.succeed([]),
          getBySourceKey: () => Effect.succeed(Option.none()),
        }),
      );
      yield* Effect.gen(function* () {
        const supervisor = yield* SupervisorClient;
        const receipt = yield* supervisor.dispatch({
          type: "start",
          commandId: CommandId.make("idle-refresh-start"),
          cwd: sandbox,
          sessionFile,
        });
        expect(receipt.status).toBe("completed");
        const runtimeId = receipt.runtimeId!;
        yield* Effect.gen(function* () {
          const runtimeBefore = (yield* supervisor.list())[0]!;
          expect(runtimeBefore.status).toBe("idle");
          const source = yield* PiExternalThreadSource;
          const catalog = yield* SessionCatalog;
          const record = (yield* catalog.list())[0]!;
          const snapshots = yield* Queue.unbounded<OrchestrationThreadDetailSnapshot>();
          yield* source.subscribeThread({ threadId: record.threadId }).pipe(
            Stream.runForEach((item) =>
              item.kind === "snapshot" ? Queue.offer(snapshots, item.snapshot) : Effect.void,
            ),
            Effect.forkScoped,
          );
          const before = yield* Queue.take(snapshots).pipe(Effect.timeout("5 seconds"));
          expect(before.thread.messages).toHaveLength(2);
          const customEntry = {
            type: "custom",
            id: "recap-1",
            parentId: "assistant-1",
            timestamp: "2026-09-15T00:02:00.000Z",
            customType: "@bds_pi/session-recap",
            data: {
              version: 1,
              throughLeafId: "assistant-1",
              text: "isolated refresh proof",
              idleMs: 120_000,
            },
          };
          NodeFS.appendFileSync(sessionFile, JSON.stringify(customEntry) + "\n");
          const after = yield* Queue.take(snapshots).pipe(
            Effect.repeat({
              until: (snapshot) =>
                snapshot.thread.activities.some((activity) => activity.kind === "session.recap"),
            }),
            Effect.timeout("5 seconds"),
          );
          expect(after.thread.activities).toMatchObject([
            {
              id: "idle-session:recap-1",
              kind: "session.recap",
              turnId: null,
              payload: { detail: customEntry.data.text },
            },
          ]);
          expect(after.snapshotSequence).toBe(before.snapshotSequence);
          expect(after.thread.messages).toEqual(before.thread.messages);
          expect((yield* supervisor.list())[0]).toEqual(runtimeBefore);
        }).pipe(
          Effect.ensuring(
            supervisor
              .dispatch({
                type: "shutdown",
                commandId: CommandId.make("idle-refresh-stop"),
                runtimeId,
              })
              .pipe(
                Effect.orDie,
                // Wake the catalog's pending async-iterator read during scope teardown.
                Effect.ensuring(Effect.sync(() => NodeFS.rmSync(sessionFile, { force: true }))),
              ),
          ),
        );
      }).pipe(
        Effect.provide(PiExternalThreadSource.layer.pipe(Layer.provideMerge(dependencies))),
        Effect.timeout("15 seconds"),
      );
    }),
  ),
);
