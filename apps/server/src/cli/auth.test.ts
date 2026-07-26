import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import { persistServerRuntimeState } from "../serverRuntimeState.ts";
import { findLiveServerRuntimeState } from "./auth.ts";

/** A pid that cannot belong to a live process (above the usual pid_max). */
const DEAD_PID = 0x7ffffffe;

const writeRuntimeState = Effect.fn(function* (input: {
  readonly baseDir: string;
  readonly stateDir: "dev" | "userdata";
  readonly pid: number;
  readonly port: number;
  readonly devUrl?: string;
}) {
  const path = yield* Path.Path;
  yield* persistServerRuntimeState({
    path: path.join(input.baseDir, input.stateDir, "server-runtime.json"),
    state: {
      version: 1,
      pid: input.pid,
      port: input.port,
      origin: `http://127.0.0.1:${String(input.port)}`,
      ...(input.devUrl ? { devUrl: input.devUrl } : {}),
      startedAt: "2026-07-20T00:00:00.000Z",
    },
  });
});

const makeBaseDir = Effect.fn(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const baseDir = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-auth-cli-test-" });
  return {
    baseDir,
    // What the flag heuristic resolves to without --dev-url: the userdata dir.
    serverRuntimeStatePath: path.join(baseDir, "userdata", "server-runtime.json"),
  };
});

describe("findLiveServerRuntimeState", () => {
  it.effect("prefers the dev server when both state directories are live", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const config = yield* makeBaseDir();
      // The configured path points at userdata, but this command is about dev.
      yield* writeRuntimeState({
        baseDir: config.baseDir,
        stateDir: "userdata",
        pid: process.pid,
        port: 16_601,
      });
      yield* writeRuntimeState({
        baseDir: config.baseDir,
        stateDir: "dev",
        pid: process.pid,
        port: 16_602,
        devUrl: "http://localhost:8888/",
      });

      const found = yield* findLiveServerRuntimeState(config);

      const live = Option.getOrThrow(found);
      assert.equal(live.stateDir, path.join(config.baseDir, "dev"));
      assert.equal(live.state.devUrl, "http://localhost:8888/");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("falls back to a live non-dev server when no dev server is running", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const config = yield* makeBaseDir();
      yield* writeRuntimeState({
        baseDir: config.baseDir,
        stateDir: "userdata",
        pid: process.pid,
        port: 16_601,
      });

      const found = yield* findLiveServerRuntimeState(config);

      const live = Option.getOrThrow(found);
      assert.equal(live.stateDir, path.join(config.baseDir, "userdata"));
      assert.equal(live.state.port, 16_601);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  // A server killed with SIGKILL never clears its state file.
  it.effect("ignores state files whose process is gone", () =>
    Effect.gen(function* () {
      const config = yield* makeBaseDir();
      yield* writeRuntimeState({
        baseDir: config.baseDir,
        stateDir: "dev",
        pid: DEAD_PID,
        port: 16_602,
        devUrl: "http://localhost:8888/",
      });

      assert.isTrue(Option.isNone(yield* findLiveServerRuntimeState(config)));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("skips a stale dev server in favor of a live one elsewhere", () =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const config = yield* makeBaseDir();
      yield* writeRuntimeState({
        baseDir: config.baseDir,
        stateDir: "dev",
        pid: DEAD_PID,
        port: 16_602,
        devUrl: "http://localhost:8888/",
      });
      yield* writeRuntimeState({
        baseDir: config.baseDir,
        stateDir: "userdata",
        pid: process.pid,
        port: 16_601,
      });

      const live = Option.getOrThrow(yield* findLiveServerRuntimeState(config));
      assert.equal(live.stateDir, path.join(config.baseDir, "userdata"));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reports nothing when no state files exist", () =>
    Effect.gen(function* () {
      const config = yield* makeBaseDir();
      assert.isTrue(Option.isNone(yield* findLiveServerRuntimeState(config)));
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
