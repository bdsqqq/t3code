import { ThreadId, type ThreadSnoozeCondition } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { makeThreadShellFixture } from "../../test-fixtures";
import {
  buildThreadListV2Items,
  buildThreadListV2ListItems,
  getThreadListV2OrderedSection,
} from "./threadListV2";

const now = "2026-10-14T00:00:00.000Z";

describe("conditional snoozes in mobile lists", () => {
  it("uses only the dependency title from the source environment", () => {
    const thread = makeThreadShellFixture({
      snoozeCondition: { type: "thread-settled", threadId: ThreadId.make("dependency") },
    });
    const items = [
      {
        thread,
        variant: "slim" as const,
        snoozed: true,
        pinned: false,
        isLast: true,
      },
    ];
    const threadTitlesByKey = new Map([["other:dependency", "Wrong environment"]]);
    expect(
      buildThreadListV2ListItems({
        items,
        pendingTasks: [],
        threadTitlesByKey,
      })[0],
    ).toMatchObject({ snoozeWakeLabelText: "Until thread settles" });
    threadTitlesByKey.set(`${thread.environmentId}:dependency`, "Release readiness");
    expect(
      buildThreadListV2ListItems({
        items,
        pendingTasks: [],
        threadTitlesByKey,
      })[0],
    ).toMatchObject({ snoozeWakeLabelText: "Until Release readiness settles" });
  });

  it.each([
    [{ type: "thread-settled", threadId: ThreadId.make("dependency") }, "Until thread settles"],
    [
      {
        type: "pull-request-merged",
        host: "github.com",
        repository: "pingdotgg/t3code",
        number: 42,
      },
      "Until pingdotgg/t3code#42 merges",
    ],
  ] satisfies [ThreadSnoozeCondition, string][])(
    "parks %j without a timer and displays the dependency",
    (condition, label) => {
      const thread = makeThreadShellFixture({
        snoozeCondition: condition,
        snoozedUntil: null,
        snoozedAt: "2026-10-13T00:00:00.000Z",
      });
      const layout = buildThreadListV2Items({
        threads: [thread],
        environmentId: null,
        searchQuery: "",
        now,
        snoozedShelfExpanded: true,
      });
      expect(layout.snoozedCount).toBe(1);
      expect(layout.nextSnoozeWakeAt).toBeNull();
      expect(layout.items[0]?.snoozed).toBe(true);
      const row = buildThreadListV2ListItems({ items: layout.items, pendingTasks: [] })[0];
      expect(row).toMatchObject({ snoozeWakeLabelText: label, timeLabel: "" });
      expect(getThreadListV2OrderedSection({ threads: [thread], section: "active", now })).toEqual(
        [],
      );
      expect(
        buildThreadListV2Items({
          threads: [{ ...thread, snoozeCondition: null }],
          environmentId: null,
          searchQuery: "",
          now,
        }).items[0]?.snoozed,
      ).toBe(false);
    },
  );

  it("retains timed wake boundaries alongside conditional snoozes", () => {
    const layout = buildThreadListV2Items({
      threads: [
        makeThreadShellFixture({
          id: ThreadId.make("condition"),
          snoozeCondition: { type: "thread-settled", threadId: ThreadId.make("dependency") },
        }),
        makeThreadShellFixture({
          id: ThreadId.make("timer"),
          snoozedUntil: "2026-10-14T01:00:00.000Z",
        }),
      ],
      environmentId: null,
      searchQuery: "",
      now,
    });
    expect(layout.snoozedCount).toBe(2);
    expect(layout.nextSnoozeWakeAt).toBe("2026-10-14T01:00:00.000Z");
  });
});
