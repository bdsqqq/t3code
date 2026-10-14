import { DateTimePicker } from "@expo/ui/community/datetime-picker";
import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import {
  localSnoozeDate,
  localSnoozeTime,
  resolveCustomSnooze,
  resolveConditionalSnooze,
  type CustomSnoozeInput,
  type SnoozeChoice,
} from "@t3tools/client-runtime/state/thread-settled";
import { useMemo, useState } from "react";
import {
  KeyboardAvoidingView,
  Modal,
  Platform,
  Pressable,
  ScrollView,
  TextInput,
  View,
} from "react-native";
import { AppText } from "../../components/AppText";
import { SegmentedControl } from "../../components/SegmentedControl";
import { environmentThreadShells } from "../../state/threads";
import { conditionalSnoozeThreads } from "./conditionalSnoozeThreads";

export function CustomSnoozeSheet(props: {
  readonly thread: EnvironmentThreadShell;
  readonly conditionalSnoozeSupported: boolean;
  readonly initialMode?: "thread" | "pull-request";
  readonly onClose: () => void;
  readonly onSnooze: (choice: SnoozeChoice) => void;
}) {
  const [mode, setMode] = useState<CustomSnoozeInput["mode"] | "thread" | "pull-request">(
    props.initialMode ?? "date",
  );
  const [date, setDate] = useState(() => new Date(Date.now() + 3_600_000));
  const [picker, setPicker] = useState<"date" | "time" | null>(null);
  const [amount, setAmount] = useState("2");
  const [unit, setUnit] = useState<"minutes" | "hours" | "days">("hours");
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [selectedThreadId, setSelectedThreadId] = useState<string | null>(null);
  const [url, setUrl] = useState("");
  const [visibleThreadCount, setVisibleThreadCount] = useState(50);
  // Subscribe only while the sheet is mounted, not on every recycled inbox row.
  const threads = useAtomValue(environmentThreadShells.threadShellsAtom);
  const candidates = useMemo(
    () => conditionalSnoozeThreads(threads, props.thread, query),
    [threads, props.thread, query],
  );
  return (
    <Modal visible transparent animationType="fade" onRequestClose={props.onClose}>
      <KeyboardAvoidingView
        className="flex-1 items-center justify-center bg-backdrop px-6"
        behavior={Platform.OS === "ios" ? "padding" : undefined}
      >
        <ScrollView
          className="max-h-[80%] w-full max-w-md grow-0 rounded-3xl bg-screen"
          keyboardShouldPersistTaps="handled"
          contentInsetAdjustmentBehavior="automatic"
          contentContainerStyle={{ padding: 24, paddingBottom: 24, gap: 20 }}
        >
          <AppText accessibilityRole="header" className="text-xl font-t3-semibold">
            Custom snooze
          </AppText>
          <AppText className="text-base text-foreground-secondary">
            Choose when snoozed threads return to your inbox.
          </AppText>
          <SegmentedControl
            options={
              [
                {
                  value: "date",
                  label: props.conditionalSnoozeSupported ? "Date" : "Date and time",
                },
                { value: "duration", label: "Duration" },
                ...(props.conditionalSnoozeSupported
                  ? ([
                      { value: "thread", label: "Thread" },
                      { value: "pull-request", label: "PR" },
                    ] as const)
                  : []),
              ] as const
            }
            selected={mode}
            onSelect={(value) => {
              setMode(value);
              setPicker(null);
              setError(null);
            }}
            role="tab"
          />
          {mode === "date" ? (
            <View className="gap-3">
              {(["date", "time"] as const).map((value) => (
                <Pressable
                  key={value}
                  accessibilityRole="button"
                  accessibilityLabel={value === "date" ? "Choose date" : "Choose time"}
                  className="min-h-12 flex-row items-center justify-between rounded-xl bg-subtle px-3"
                  onPress={() => setPicker(value)}
                >
                  <AppText>{value === "date" ? "Date" : "Time"}</AppText>
                  <AppText>
                    {value === "date"
                      ? date.toLocaleDateString()
                      : date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}
                  </AppText>
                </Pressable>
              ))}
              {picker && (
                <DateTimePicker
                  value={date}
                  mode={picker}
                  display={Platform.OS === "ios" ? "spinner" : "default"}
                  onDismiss={() => setPicker(null)}
                  onValueChange={(_, selected) => {
                    const next = new Date(date);
                    if (picker === "date")
                      next.setFullYear(
                        selected.getFullYear(),
                        selected.getMonth(),
                        selected.getDate(),
                      );
                    else next.setHours(selected.getHours(), selected.getMinutes(), 0, 0);
                    setDate(next);
                    setError(null);
                  }}
                />
              )}
            </View>
          ) : mode === "duration" ? (
            <View className="gap-3">
              <AppText>Snooze for</AppText>
              <TextInput
                accessibilityLabel="Duration"
                className="min-h-12 rounded-xl bg-subtle px-3 text-base text-foreground"
                keyboardType="decimal-pad"
                value={amount}
                onChangeText={(value) => {
                  setAmount(value);
                  setError(null);
                }}
              />
              <View className="flex-row gap-2">
                {(["minutes", "hours", "days"] as const).map((value) => (
                  <Pressable
                    key={value}
                    accessibilityRole="radio"
                    accessibilityState={{ checked: unit === value }}
                    className={
                      unit === value
                        ? "min-h-12 flex-1 items-center justify-center rounded-xl bg-subtle"
                        : "min-h-12 flex-1 items-center justify-center rounded-xl"
                    }
                    onPress={() => {
                      setUnit(value);
                      setError(null);
                    }}
                  >
                    <AppText>
                      {value === "minutes" ? "Minutes" : value === "hours" ? "Hours" : "Days"}
                    </AppText>
                  </Pressable>
                ))}
              </View>
            </View>
          ) : mode === "thread" ? (
            <View className="gap-3">
              <AppText>Wake when another thread in this environment settles.</AppText>
              <TextInput
                accessibilityLabel="Search threads"
                placeholder="Search threads"
                className="min-h-12 rounded-xl bg-subtle px-3 text-base text-foreground"
                value={query}
                onChangeText={(value) => {
                  setQuery(value);
                  setVisibleThreadCount(50);
                }}
              />
              <ScrollView
                className="max-h-56"
                nestedScrollEnabled
                keyboardShouldPersistTaps="handled"
              >
                {candidates.slice(0, visibleThreadCount).map((thread) => (
                  <Pressable
                    key={thread.id}
                    accessibilityRole="radio"
                    accessibilityState={{ checked: selectedThreadId === thread.id }}
                    className={
                      selectedThreadId === thread.id
                        ? "min-h-12 justify-center rounded-xl bg-subtle px-3 py-2"
                        : "min-h-12 justify-center rounded-xl px-3 py-2"
                    }
                    onPress={() => {
                      setSelectedThreadId(thread.id);
                      setError(null);
                    }}
                  >
                    <AppText numberOfLines={2}>{thread.title || "Untitled thread"}</AppText>
                  </Pressable>
                ))}
                {candidates.length === 0 && <AppText>No matching threads.</AppText>}
                {candidates.length > visibleThreadCount && (
                  <Pressable
                    accessibilityRole="button"
                    className="min-h-12 justify-center px-3"
                    onPress={() => setVisibleThreadCount((count) => count + 50)}
                  >
                    <AppText>Show more threads</AppText>
                  </Pressable>
                )}
              </ScrollView>
            </View>
          ) : (
            <View className="gap-3">
              <AppText>Wake when this pull request merges.</AppText>
              <TextInput
                accessibilityLabel="Pull request URL"
                placeholder="Full pull request URL"
                autoCapitalize="none"
                autoCorrect={false}
                keyboardType="url"
                className="min-h-12 rounded-xl bg-subtle px-3 text-base text-foreground"
                value={url}
                onChangeText={(value) => {
                  setUrl(value);
                  setError(null);
                }}
              />
            </View>
          )}
          {error && (
            <AppText accessibilityRole="alert" className="text-danger-foreground">
              {error}
            </AppText>
          )}
          <View className="flex-row justify-end gap-3">
            <Pressable
              accessibilityRole="button"
              className="min-h-12 justify-center px-3"
              onPress={props.onClose}
            >
              <AppText>Cancel</AppText>
            </Pressable>
            <Pressable
              accessibilityRole="button"
              className="min-h-12 justify-center rounded-xl bg-subtle px-3"
              onPress={() => {
                if (mode === "thread" || mode === "pull-request") {
                  const selected =
                    selectedThreadId !== null
                      ? conditionalSnoozeThreads(threads, props.thread, "").find(
                          (thread) => thread.id === selectedThreadId,
                        )
                      : undefined;
                  const condition = resolveConditionalSnooze(
                    mode === "thread" ? { mode, threadId: selected?.id ?? "" } : { mode, url },
                  );
                  if (!props.conditionalSnoozeSupported || !condition) {
                    setError(
                      mode === "thread"
                        ? "Choose an available thread in this environment."
                        : "Enter a full pull request URL from a supported host.",
                    );
                    return;
                  }
                  props.onSnooze({ condition });
                  props.onClose();
                  return;
                }
                const input: CustomSnoozeInput =
                  mode === "date"
                    ? { mode, date: localSnoozeDate(date), time: localSnoozeTime(date) }
                    : { mode, amount: amount.replace(",", "."), unit };
                const snoozedUntil = resolveCustomSnooze(input, new Date());
                if (!snoozedUntil) {
                  setError(
                    mode === "date"
                      ? "Choose a date and time in the future."
                      : "Enter a positive duration.",
                  );
                  return;
                }
                props.onSnooze({ snoozedUntil });
                props.onClose();
              }}
            >
              <AppText className="text-foreground">Snooze</AppText>
            </Pressable>
          </View>
        </ScrollView>
      </KeyboardAvoidingView>
    </Modal>
  );
}
