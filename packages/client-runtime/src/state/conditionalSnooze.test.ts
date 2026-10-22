import { ThreadId, type ThreadSnoozeCondition } from "@t3tools/contracts";
import { describe, expect, expectTypeOf, it } from "vite-plus/test";
import type { SnoozeThreadInput } from "../operations/commands.ts";
import {
  effectiveSnoozed,
  resolveConditionalSnooze,
  snoozeConditionLabel,
  threadWokeAt,
  type SnoozeChoice,
} from "./threadSettled.ts";

const condition = { type: "thread-settled", threadId: ThreadId.make("dependency") } as const;
const now = "2026-10-14T12:00:00.000Z";
const snoozedAt = "2026-10-14T10:00:00.000Z";
const shell = {
  snoozeCondition: condition,
  snoozedUntil: null,
  snoozedAt,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
};

describe("conditional snooze", () => {
  it("requires exactly one timer or condition in choices and command inputs", () => {
    type Timer = { snoozedUntil: string };
    type Conditional = { condition: ThreadSnoozeCondition };
    expectTypeOf<Timer>().toMatchTypeOf<SnoozeChoice>();
    expectTypeOf<Conditional>().toMatchTypeOf<SnoozeChoice>();
    expectTypeOf<{}>().not.toMatchTypeOf<SnoozeChoice>();
    expectTypeOf<Timer & Conditional>().not.toMatchTypeOf<SnoozeChoice>();
    expectTypeOf<Timer & { threadId: ThreadId }>().toMatchTypeOf<SnoozeThreadInput>();
    expectTypeOf<Conditional & { threadId: ThreadId }>().toMatchTypeOf<SnoozeThreadInput>();
    expectTypeOf<{ threadId: ThreadId }>().not.toMatchTypeOf<SnoozeThreadInput>();
    expectTypeOf<
      Timer & Conditional & { threadId: ThreadId }
    >().not.toMatchTypeOf<SnoozeThreadInput>();
  });

  it("stays snoozed without a timer until the server clears the dependency", () => {
    expect(effectiveSnoozed(shell, { now })).toBe(true);
    expect(threadWokeAt(shell, { now })).toBeNull();
    const awake = { ...shell, snoozeCondition: null, lastSnoozeWakeAt: now };
    expect(effectiveSnoozed(awake, { now })).toBe(false);
    expect(threadWokeAt(awake, { now })).toBe(now);
  });

  it("retains early wake for pending input and errors", () => {
    expect(effectiveSnoozed({ ...shell, hasPendingUserInput: true }, { now })).toBe(false);
    expect(
      effectiveSnoozed(
        {
          ...shell,
          runtime: { status: "failed", updatedAt: now },
        },
        { now },
      ),
    ).toBe(false);
  });

  it.each([
    condition,
    { type: "pull-request-merged", host: "github.com", repository: "owner/repo", number: 12 },
  ] as const)("keeps a $type snooze across a completed run", (snoozeCondition) => {
    const completed = {
      ...shell,
      snoozeCondition,
      latestRun: { status: "completed", completedAt: now },
    };
    expect(effectiveSnoozed(completed, { now })).toBe(true);
    expect(threadWokeAt(completed, { now })).toBeNull();
  });

  it("does not mistake recovery metadata for a fresh failure", () => {
    const failed = {
      ...shell,
      runtime: { status: "failed", updatedAt: now },
      latestRun: { status: "failed", completedAt: "2026-10-14T09:00:00.000Z" },
    };
    expect(effectiveSnoozed(failed, { now })).toBe(true);
    expect(threadWokeAt(failed, { now })).toBeNull();
  });

  it("timestamps a new failure wake at the run failure, not a later metadata update", () => {
    const failed = {
      ...shell,
      runtime: { status: "failed", updatedAt: "2026-10-14T12:30:00.000Z" },
      latestRun: { status: "failed", completedAt: now },
    };
    expect(effectiveSnoozed(failed, { now })).toBe(false);
    expect(threadWokeAt(failed, { now })).toBe(now);
  });

  it("resolves a selected thread and rejects an empty selection", () => {
    expect(resolveConditionalSnooze({ mode: "thread", threadId: " dependency " })).toEqual(
      condition,
    );
    expect(resolveConditionalSnooze({ mode: "thread", threadId: " " })).toBeNull();
  });

  it.each([
    ["https://github.com/owner/repo/pull/12", "github.com", "owner/repo", 12],
    [
      "https://gitlab.example/team/sub/repo/-/merge_requests/4",
      "gitlab.example",
      "team/sub/repo",
      4,
    ],
    ["https://bitbucket.org/team/repo/pull-requests/3", "bitbucket.org", "team/repo", 3],
    ["https://code.example/team/repo/pulls/2", "code.example", "team/repo", 2],
    [
      "https://dev.azure.com/org/project/_git/repo/pullrequest/7",
      "dev.azure.com",
      "org/project/_git/repo",
      7,
    ],
    ["https://git.cafe/team/repo/pulls/9", "git.cafe", "team/repo", 9],
  ])("resolves supported PR URL %s", (url, host, repository, number) => {
    expect(resolveConditionalSnooze({ mode: "pull-request", url })).toEqual({
      type: "pull-request-merged",
      host,
      repository,
      number,
    });
  });

  it.each(["", "#12", "https://github.com/owner/repo/issues/12", "javascript:alert(1)"])(
    "does not install a dependency from %s",
    (url) => expect(resolveConditionalSnooze({ mode: "pull-request", url })).toBeNull(),
  );

  it("describes dependency wakes instead of inventing a return time", () => {
    expect(snoozeConditionLabel(condition)).toBe("Until thread settles");
    expect(
      snoozeConditionLabel({
        type: "pull-request-merged",
        host: "github.com",
        repository: "owner/repo",
        number: 12,
      }),
    ).toBe("Until owner/repo#12 merges");
  });
});
