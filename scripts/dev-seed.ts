#!/usr/bin/env node

// @effect-diagnostics nodeBuiltinImport:off - node:os resolves the shared T3 home guard.
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeOS from "node:os";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { Command, Flag } from "effect/unstable/cli";

import { DevSeedError, seedDevDatabase } from "./lib/dev-seed.ts";

const DEFAULT_THREAD_LIMIT = 25;
const DEFAULT_ACTIVITY_LIMIT = 200;

class DevSeedTargetError extends Schema.TaggedErrorClass<DevSeedTargetError>()(
  "DevSeedTargetError",
  {
    reason: Schema.Literals(["shared-home", "missing-target", "not-a-worktree"]),
    detail: Schema.String,
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "shared-home":
        return [
          `Refusing to seed ${this.detail}: that is the shared T3 Code home.`,
          "This command overwrites projection tables. Run it from a worktree, or pass --to <base-dir>.",
        ].join("\n");
      case "missing-target":
        return [
          `No database at ${this.detail}.`,
          "Start the dev server once so migrations run (`bun run dev`), then seed.",
        ].join("\n");
      case "not-a-worktree":
        return [
          "Not inside a git worktree, so there is no worktree-local data directory to seed.",
          "Pass --to <base-dir> to choose one explicitly.",
        ].join("\n");
    }
  }
}

const stateDbPath = (path: Path.Path, baseDir: string) =>
  path.join(baseDir, "userdata", "state.sqlite");

/** The worktree's own `.t3`, matching what the dev runner uses. */
const resolveDefaultTarget = Effect.fn("devSeed.resolveDefaultTarget")(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const cwd = process.cwd();
  const gitInfo = yield* fileSystem.stat(path.join(cwd, ".git")).pipe(Effect.option);
  if (Option.isNone(gitInfo) || gitInfo.value.type !== "File") {
    return undefined;
  }
  return path.join(cwd, ".t3");
});

const devSeedCli = Command.make("dev-seed", {
  from: Flag.string("from").pipe(
    Flag.withDescription(
      "Base directory to copy from (default: the shared T3 Code home, ~/.t3). Read-only.",
    ),
    Flag.optional,
    Flag.map(Option.getOrUndefined),
  ),
  to: Flag.string("to").pipe(
    Flag.withDescription(
      "Base directory to seed (default: this worktree's .t3). Its projection tables are replaced.",
    ),
    Flag.optional,
    Flag.map(Option.getOrUndefined),
  ),
  threads: Flag.integer("threads").pipe(
    Flag.withDescription(
      `How many recent threads to copy (default ${String(DEFAULT_THREAD_LIMIT)}).`,
    ),
    Flag.withDefault(DEFAULT_THREAD_LIMIT),
  ),
  activities: Flag.integer("activities").pipe(
    Flag.withDescription(
      `Newest activities kept per thread (default ${String(DEFAULT_ACTIVITY_LIMIT)}).`,
    ),
    Flag.withDefault(DEFAULT_ACTIVITY_LIMIT),
  ),
}).pipe(
  Command.withDescription(
    "Copy recent projects and threads from a T3 Code data directory into an isolated dev one.",
  ),
  Command.withHandler((input) =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;

      const sharedHome = path.join(NodeOS.homedir(), ".t3");
      const sourceBaseDir = input.from ? path.resolve(input.from) : sharedHome;

      const defaultTarget = yield* resolveDefaultTarget();
      const targetBaseDir = input.to ? path.resolve(input.to) : defaultTarget;
      if (targetBaseDir === undefined) {
        return yield* new DevSeedTargetError({ reason: "not-a-worktree", detail: process.cwd() });
      }

      // The whole point is to keep dev data off the real home; overwriting it
      // here would be the exact accident this command exists to avoid.
      const [canonicalTarget, canonicalShared] = yield* Effect.all([
        fileSystem.realPath(targetBaseDir).pipe(Effect.orElseSucceed(() => targetBaseDir)),
        fileSystem.realPath(sharedHome).pipe(Effect.orElseSucceed(() => sharedHome)),
      ]);
      if (canonicalTarget === canonicalShared) {
        return yield* new DevSeedTargetError({ reason: "shared-home", detail: targetBaseDir });
      }

      const targetDbPath = stateDbPath(path, targetBaseDir);
      if (!(yield* fileSystem.exists(targetDbPath))) {
        return yield* new DevSeedTargetError({ reason: "missing-target", detail: targetDbPath });
      }

      const seededAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
      const summary = yield* Effect.try({
        try: () =>
          seedDevDatabase({
            sourceDbPath: stateDbPath(path, sourceBaseDir),
            targetDbPath,
            threadLimit: input.threads,
            activityLimit: input.activities,
            seededAt,
          }),
        catch: (cause) => cause as DevSeedError,
      });

      yield* Console.log(
        [
          `Seeded ${targetDbPath}`,
          `  from     ${stateDbPath(path, sourceBaseDir)}`,
          `  projects ${String(summary.projects)}`,
          `  threads  ${String(summary.threads)}`,
          `  messages ${String(summary.messages)}`,
          `  activity ${String(summary.activities)}`,
          `  turns    ${String(summary.turns)}  sessions ${String(summary.sessions)}`,
          ...(summary.skippedColumns.length > 0
            ? [
                `  note: skipped ${String(summary.skippedColumns.length)} column(s) absent from the target schema`,
                `        (${summary.skippedColumns.join(", ")})`,
              ]
            : []),
          "",
          "Restart the dev server to pick it up.",
        ].join("\n"),
      );
    }).pipe(
      Effect.tapError((error) =>
        Effect.logError(
          error instanceof DevSeedError
            ? `${error.message}${error.hint ? `\n${error.hint}` : ""}`
            : error.message,
        ),
      ),
    ),
  ),
);

if (import.meta.main) {
  Command.run(devSeedCli, { version: "0.0.0" }).pipe(
    Effect.provide(Layer.mergeAll(Logger.layer([Logger.consolePretty()]), NodeServices.layer)),
    NodeRuntime.runMain,
  );
}
