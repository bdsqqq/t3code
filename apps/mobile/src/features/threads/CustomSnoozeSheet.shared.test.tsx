import { act, type ReactNode } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { makeThreadShellFixture } from "../../test-fixtures";

const platform = vi.hoisted(() => ({ OS: "android" }));

vi.mock("react-native", () => {
  const Container = ({ children }: { children?: ReactNode }) => <div>{children}</div>;
  return {
    Platform: platform,
    Modal: Container,
    KeyboardAvoidingView: Container,
    ScrollView: Container,
    View: Container,
    TextInput: () => null,
    Pressable: (props: {
      children?: ReactNode;
      accessibilityLabel?: string;
      onPress: () => void;
    }) => (
      <button aria-label={props.accessibilityLabel} onClick={props.onPress}>
        {props.children}
      </button>
    ),
  };
});
vi.mock("../../components/ui/Button", () => ({
  Button: (props: { label: string; onPress: () => void }) => (
    <button onClick={props.onPress}>{props.label}</button>
  ),
}));
vi.mock("../../components/ui/AppText", () => ({
  AppText: ({ children }: { children?: ReactNode }) => <span>{children}</span>,
}));
vi.mock("../../components/ui/SegmentedControl", () => ({
  SegmentedControl: (props: {
    options: ReadonlyArray<{ value: string; label: string }>;
    onSelect: (value: string) => void;
  }) => (
    <div>
      {props.options.map((option) => (
        <button key={option.value} onClick={() => props.onSelect(option.value)}>
          {option.label}
        </button>
      ))}
    </div>
  ),
}));
vi.mock("@effect/atom-react", () => ({ useAtomValue: () => [] }));
vi.mock("../../state/threads", () => ({
  environmentThreadShells: { threadShellsAtom: null },
}));
vi.mock("@expo/ui/community/datetime-picker", () => ({
  // Expo's Android dialog emits confirmation without unmounting itself. The
  // caller owns dismissal; iOS's inline spinner must instead remain editable.
  DateTimePicker: (props: {
    mode: "date" | "time";
    onValueChange: (
      event: { nativeEvent: { timestamp: number; utcOffset: number } },
      date: Date,
    ) => void;
  }) => (
    <div role="dialog" aria-label={`${props.mode} picker`}>
      <button
        onClick={() => {
          const selected =
            props.mode === "date" ? new Date(2026, 9, 20) : new Date(2026, 9, 14, 8, 15);
          props.onValueChange(
            { nativeEvent: { timestamp: selected.getTime(), utcOffset: 0 } },
            selected,
          );
        }}
      >
        Confirm {props.mode}
      </button>
    </div>
  ),
}));

import { CustomSnoozeSheet } from "./CustomSnoozeSheet.shared";

let root: Root | null = null;
let container: HTMLDivElement;

function press(label: string) {
  const button = [...container.querySelectorAll("button")].find(
    (element) => element.getAttribute("aria-label") === label || element.textContent === label,
  );
  if (!button) throw new Error(`Missing button: ${label}`);
  act(() => button.click());
}

beforeEach(() => {
  (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
  vi.useFakeTimers();
  vi.setSystemTime(new Date(2026, 9, 14, 12));
  container = document.createElement("div");
  root = createRoot(container);
});

afterEach(() => {
  act(() => root?.unmount());
  root = null;
  vi.useRealTimers();
});

describe("switching a conditional snooze to a date", () => {
  it.each([
    ["android", "thread"],
    ["android", "pull-request"],
    ["ios", "thread"],
    ["ios", "pull-request"],
  ] as const)("keeps date/time editing usable on %s from %s", (os, initialMode) => {
    platform.OS = os;
    const onSnooze = vi.fn();
    const onClose = vi.fn();
    act(() =>
      root!.render(
        <CustomSnoozeSheet
          thread={makeThreadShellFixture()}
          conditionalSnoozeSupported
          initialMode={initialMode}
          onSnooze={onSnooze}
          onClose={onClose}
        />,
      ),
    );
    press("Date");
    press("Choose date");
    press("Confirm date");
    expect(container.querySelector('[aria-label="date picker"]') !== null).toBe(os === "ios");
    expect(onSnooze).not.toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();

    press("Choose time");
    press("Confirm time");
    expect(container.querySelector('[aria-label="time picker"]') !== null).toBe(os === "ios");
    press("Snooze");
    expect(onSnooze).toHaveBeenCalledExactlyOnceWith({
      snoozedUntil: new Date(2026, 9, 20, 8, 15).toISOString(),
    });
    expect(onClose).toHaveBeenCalledOnce();
  });
});
