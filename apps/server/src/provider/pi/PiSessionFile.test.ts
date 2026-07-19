import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  allocateFreshPiSessionFile,
  cleanupFreshPiSessionFile,
  cleanupResumedPiSessionFile,
  piInstanceStateRoot,
  piStateMatchesCursor,
  validatePiResumeSessionFile,
} from "./PiSessionFile.ts";

describe("PiSessionFile", () => {
  it.layer(NodeServices.layer)("allocates private, exclusive per-instance session files", (it) => {
    it.effect("allocates and only cleans fresh files", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pi-session-" });
          const root = yield* piInstanceStateRoot({ stateDir, instanceId: "pi_local" });
          const cursor = yield* allocateFreshPiSessionFile({
            stateRoot: root,
            sessionId: "session/1",
          });
          expect(cursor.sessionFile).toBe(path.join(yield* fs.realPath(root), "session%2F1.jsonl"));
          expect((yield* fs.stat(root)).mode & 0o777).toBe(0o700);
          expect((yield* fs.stat(cursor.sessionFile)).mode & 0o777).toBe(0o600);
          expect(piStateMatchesCursor(cursor, cursor)).toBe(true);
          expect(
            yield* Effect.result(
              allocateFreshPiSessionFile({ stateRoot: root, sessionId: "session/1" }),
            ),
          ).toMatchObject({ _tag: "Failure" });
          yield* cleanupResumedPiSessionFile(cursor);
          expect(yield* fs.exists(cursor.sessionFile)).toBe(true);
          yield* cleanupFreshPiSessionFile(cursor);
          expect(yield* fs.exists(cursor.sessionFile)).toBe(false);
        }),
      ),
    );
  });

  it.layer(NodeServices.layer)("validates resume files", (it) => {
    it.effect("accepts an exact header and rejects traversal and symlinks", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pi-resume-" });
          const cwd = path.join(stateDir, "workspace");
          const root = yield* piInstanceStateRoot({ stateDir, instanceId: "pi_local" });
          yield* fs.makeDirectory(root, { recursive: true });
          const sessionFile = path.join(root, "resume.jsonl");
          yield* fs.writeFileString(
            sessionFile,
            `{"type":"session","id":"session-1","cwd":"${cwd}"}\n`,
          );
          const canonicalSessionFile = yield* fs.realPath(sessionFile);
          const cursor = {
            schemaVersion: 1 as const,
            sessionFile: canonicalSessionFile,
            sessionId: "session-1",
          };
          expect(yield* validatePiResumeSessionFile({ stateRoot: root, cursor, cwd })).toEqual(
            cursor,
          );
          const outside = path.join(stateDir, "outside.jsonl");
          yield* fs.symlink(sessionFile, outside);
          expect(
            yield* Effect.result(
              validatePiResumeSessionFile({
                stateRoot: root,
                cursor: { ...cursor, sessionFile: outside },
                cwd,
              }),
            ),
          ).toMatchObject({ _tag: "Failure" });
          const link = path.join(root, "link.jsonl");
          yield* fs.symlink(sessionFile, link);
          expect(
            yield* Effect.result(
              validatePiResumeSessionFile({
                stateRoot: root,
                cursor: { ...cursor, sessionFile: link },
                cwd,
              }),
            ),
          ).toMatchObject({ _tag: "Failure" });
        }),
      ),
    );
  });
});
