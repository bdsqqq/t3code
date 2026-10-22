import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { makeThreadShellFixture } from "../../test-fixtures";
import { conditionalSnoozeThreads } from "./conditionalSnoozeThreads";

describe("conditional snooze thread selection", () => {
  const source = makeThreadShellFixture({
    id: ThreadId.make("source"),
    environmentId: EnvironmentId.make("local"),
  });
  const available = makeThreadShellFixture({
    id: ThreadId.make("target"),
    environmentId: source.environmentId,
    title: "Release readiness",
  });

  it("excludes self, other environments, archived and deleted threads", () => {
    const threads = [
      source,
      available,
      { ...available, id: ThreadId.make("remote"), environmentId: EnvironmentId.make("remote") },
      { ...available, id: ThreadId.make("archived"), archivedAt: "2026-10-14T00:00:00.000Z" },
      { ...available, id: ThreadId.make("deleted"), deletedAt: "2026-10-14T00:00:00.000Z" },
    ];
    expect(conditionalSnoozeThreads(threads, source, "")).toEqual([available]);
  });

  it("searches titles and IDs without case or surrounding whitespace sensitivity", () => {
    expect(conditionalSnoozeThreads([available], source, " RELEASE ")).toEqual([available]);
    expect(conditionalSnoozeThreads([available], source, "TARGET")).toEqual([available]);
    expect(conditionalSnoozeThreads([available], source, "missing")).toEqual([]);
  });
});
