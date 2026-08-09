import { CommandId, PiNativeSessionKey } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { PiExternalLifecycleOverrideRepository } from "../Services/PiExternalLifecycleOverrides.ts";
import { PiExternalLifecycleOverrideRepositoryLive } from "./PiExternalLifecycleOverrides.ts";
import { SqlitePersistenceMemory } from "./Sqlite.ts";

const layer = it.layer(
  PiExternalLifecycleOverrideRepositoryLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
);

layer("PiExternalLifecycleOverrideRepository", (it) => {
  it.effect("persists the latest idempotent override for a session file", () =>
    Effect.gen(function* () {
      const repository = yield* PiExternalLifecycleOverrideRepository;
      const sourceKey = PiNativeSessionKey.make("source-1");

      yield* repository.apply({
        sourceKey,
        commandId: CommandId.make("settle-1"),
        lifecycleOverride: "settled",
        observedFileSize: 10,
        observedFileMtimeMs: 20,
        updatedAt: "2026-08-07T20:00:00.000Z",
      });
      yield* repository.apply({
        sourceKey,
        commandId: CommandId.make("unsettle-1"),
        lifecycleOverride: "active",
        observedFileSize: 10,
        observedFileMtimeMs: 20,
        updatedAt: "2026-08-07T20:01:00.000Z",
      });

      const found = yield* repository.getBySourceKey(sourceKey);
      assert.ok(Option.isSome(found));
      assert.equal(found.value.lifecycleOverride, "active");
      assert.equal((yield* repository.list()).length, 1);
      assert.ok(Option.isSome(yield* repository.getByCommandId(CommandId.make("settle-1"))));
    }),
  );

  it.effect("does not reapply an old command after the file fingerprint changes", () =>
    Effect.gen(function* () {
      const repository = yield* PiExternalLifecycleOverrideRepository;
      const sourceKey = PiNativeSessionKey.make("source-retry");
      const commandId = CommandId.make("settle-retry");
      const first = {
        sourceKey,
        commandId,
        lifecycleOverride: "settled" as const,
        observedFileSize: 10,
        observedFileMtimeMs: 20,
        updatedAt: "2026-08-07T20:00:00.000Z",
      };

      assert.equal((yield* repository.apply(first)).applied, true);
      assert.equal(
        (yield* repository.apply({
          ...first,
          observedFileSize: 99,
          observedFileMtimeMs: 100,
        })).applied,
        false,
      );
      const found = yield* repository.getBySourceKey(sourceKey);
      assert.ok(Option.isSome(found));
      assert.equal(found.value.observedFileSize, 10);
      assert.equal(found.value.observedFileMtimeMs, 20);
    }),
  );

  it.effect("records a Pi-written operation without changing effective state", () =>
    Effect.gen(function* () {
      const repository = yield* PiExternalLifecycleOverrideRepository;
      const sourceKey = PiNativeSessionKey.make("source-pi-receipt");
      yield* repository.apply({
        sourceKey,
        commandId: CommandId.make("active-current"),
        lifecycleOverride: "active",
        observedFileSize: 20,
        observedFileMtimeMs: 30,
        updatedAt: "2026-08-07T20:00:00.000Z",
      });

      yield* repository.recordReceipt({
        sourceKey,
        commandId: CommandId.make("settled-in-jsonl"),
        lifecycleOverride: "settled",
        observedFileSize: 10,
        observedFileMtimeMs: 20,
        updatedAt: "2026-08-07T19:00:00.000Z",
      });

      const found = yield* repository.getBySourceKey(sourceKey);
      assert.ok(Option.isSome(found));
      assert.equal(found.value.lifecycleOverride, "active");
    }),
  );
});
