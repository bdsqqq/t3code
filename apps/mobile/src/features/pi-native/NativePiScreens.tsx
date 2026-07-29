import { useAtomValue } from "@effect/atom-react";
import { presentPiNativeUnknown } from "@t3tools/client-runtime/state/pi-native-presentation";
import { LegendList } from "@legendapp/list/react-native";
import type { StaticScreenProps } from "@react-navigation/native";
import { useNavigation } from "@react-navigation/native";
import {
  CommandId,
  EnvironmentId,
  PiNativeRuntimeId,
  PiNativeSessionKey,
  type PiNativeJsonlEntry,
  type PiNativeSession,
} from "@t3tools/contracts";
import { AsyncResult } from "effect/unstable/reactivity";
import { memo, useState } from "react";
import { Pressable, TextInput, View } from "react-native";

import { AppText } from "../../components/AppText";
import { useAtomCommand } from "../../state/use-atom-command";
import { piNativeEnvironment } from "../../state/pi-native";

function ActionButton({
  label,
  onPress,
  disabled = false,
}: {
  label: string;
  onPress: () => void;
  disabled?: boolean;
}) {
  return (
    <Pressable
      accessibilityLabel={label}
      accessibilityRole="button"
      disabled={disabled}
      onPress={onPress}
      className="rounded-lg bg-primary px-4 py-3 disabled:opacity-50"
    >
      <AppText className="font-t3-bold text-primary-foreground">{label}</AppText>
    </Pressable>
  );
}

const SessionRow = memo(function SessionRow({
  session,
  onPress,
}: {
  session: PiNativeSession;
  onPress: () => void;
}) {
  return (
    <Pressable
      accessibilityLabel={`Open ${session.title || "Native Pi session"}`}
      onPress={onPress}
      className="border-b border-border px-5 py-4"
    >
      <View className="flex-row justify-between gap-3">
        <AppText className="flex-1 font-t3-bold text-foreground" numberOfLines={1}>
          {session.title || "Untitled session"}
        </AppText>
        <AppText className="text-xs text-foreground-muted">
          {session.runtime?.status ?? session.liveness}
        </AppText>
      </View>
      <AppText className="mt-1 font-mono text-xs text-foreground-muted" numberOfLines={1}>
        {session.cwd}
      </AppText>
      <AppText className="mt-1 text-xs text-foreground-muted">
        {new Date(session.updatedAt).toLocaleString()}
      </AppText>
    </Pressable>
  );
});

export function NativePiListScreen({ route }: StaticScreenProps<{ environmentId: string }>) {
  const environmentId = EnvironmentId.make(route.params.environmentId);
  const navigation = useNavigation();
  const result = useAtomValue(piNativeEnvironment.list({ environmentId, input: {} }));
  const sessions = AsyncResult.isSuccess(result) ? result.value.sessions : [];
  const runtimes = AsyncResult.isSuccess(result) ? result.value.runtimes : [];
  const start = useAtomCommand(piNativeEnvironment.start, { reportFailure: false });
  const createCommandId = useAtomCommand(piNativeEnvironment.commandId, { reportFailure: false });
  const [cwd, setCwd] = useState("");
  const [starting, setStarting] = useState(false);
  const [startFeedback, setStartFeedback] = useState<string | null>(null);
  const [startRetry, setStartRetry] = useState<{
    readonly commandId: CommandId;
    readonly cwd: string;
  } | null>(null);
  const handleStart = async (
    submission: { readonly commandId: CommandId; readonly cwd: string } | null,
  ) => {
    const value = submission?.cwd ?? cwd.trim();
    if (!value || starting) return;
    setStarting(true);
    setStartFeedback(null);
    const generated =
      submission === null ? await createCommandId({ environmentId, input: undefined }) : null;
    if (generated?._tag === "Failure") {
      setStartFeedback("Could not prepare the start command.");
      setStarting(false);
      return;
    }
    const next = submission ?? { commandId: generated!.value, cwd: value };
    setStartRetry(next);
    const receiptResult = await start({ environmentId, input: next });
    if (receiptResult._tag === "Failure") {
      setStartFeedback("Could not confirm startup. Retry uses the same command ID.");
      setStarting(false);
      return;
    }
    const receipt = receiptResult.value;
    if ((receipt.status !== "started" && receipt.status !== "completed") || !receipt.runtimeId) {
      setStartRetry(null);
      setStartFeedback(receipt.error ?? `Start ${receipt.status}.`);
      setStarting(false);
      return;
    }
    setStartRetry(null);
    setStarting(false);
    navigation.navigate("NativePiRuntime", { environmentId, runtimeId: receipt.runtimeId });
  };
  return (
    <View className="flex-1 bg-screen">
      <View className="flex-row gap-2 border-b border-border p-4">
        <TextInput
          accessibilityLabel="Working directory"
          autoCapitalize="none"
          autoCorrect={false}
          className="flex-1 rounded-lg border border-input-border bg-input px-3 py-2 text-foreground"
          placeholder="/path/to/project"
          placeholderTextColor="#888"
          value={cwd}
          onChangeText={setCwd}
        />
        <ActionButton
          label={starting ? "Starting…" : "Start"}
          disabled={!cwd.trim() || starting}
          onPress={() => void handleStart(null)}
        />
      </View>
      <View className="flex-row items-center gap-2 px-5 py-2">
        <AppText accessibilityLiveRegion="polite" className="flex-1 text-foreground-muted">
          {startFeedback}
        </AppText>
        {startRetry && !starting ? (
          <ActionButton label="Retry" onPress={() => void handleStart(startRetry)} />
        ) : null}
      </View>
      {AsyncResult.isFailure(result) ? (
        <AppText className="p-5 text-danger-foreground">Could not load Native Pi sessions.</AppText>
      ) : (
        <LegendList
          data={[
            ...runtimes.map((runtime) => ({ type: "runtime" as const, runtime })),
            ...sessions.map((session) => ({ type: "session" as const, session })),
          ]}
          keyExtractor={(item) =>
            item.type === "runtime" ? item.runtime.runtimeId : item.session.sessionKey
          }
          renderItem={({ item }) =>
            item.type === "runtime" ? (
              <Pressable
                accessibilityLabel="Open Native Pi runtime"
                onPress={() =>
                  navigation.navigate("NativePiRuntime", {
                    environmentId,
                    runtimeId: item.runtime.runtimeId,
                  })
                }
                className="border-b border-border px-5 py-4"
              >
                <View className="flex-row justify-between gap-3">
                  <AppText className="font-t3-bold text-foreground">Native Pi runtime</AppText>
                  <AppText className="text-xs text-foreground-muted">{item.runtime.status}</AppText>
                </View>
                <AppText className="mt-1 font-mono text-xs text-foreground-muted">
                  {item.runtime.cwd ?? "Starting…"}
                </AppText>
              </Pressable>
            ) : (
              <SessionRow
                session={item.session}
                onPress={() =>
                  navigation.navigate("NativePiDetail", {
                    environmentId,
                    sessionKey: item.session.sessionKey,
                  })
                }
              />
            )
          }
        />
      )}
    </View>
  );
}

const EntryRow = memo(function EntryRow({ entry }: { entry: PiNativeJsonlEntry }) {
  const direct = presentPiNativeUnknown(entry);
  const { label, text } =
    direct.text === null && entry.event !== undefined
      ? presentPiNativeUnknown(entry.event)
      : direct;
  return (
    <View className="mx-4 my-1 rounded-lg border border-border bg-sheet p-3">
      <AppText className="mb-2 text-xs font-t3-bold text-foreground-muted">{label}</AppText>
      <AppText selectable className="text-foreground">
        {typeof text === "string" ? text : JSON.stringify(entry, null, 2)}
      </AppText>
    </View>
  );
});

export function NativePiDetailScreen({
  route,
}: StaticScreenProps<{ environmentId: string; sessionKey: string }>) {
  const environmentId = EnvironmentId.make(route.params.environmentId);
  const sessionKey = PiNativeSessionKey.make(route.params.sessionKey);
  const result = useAtomValue(piNativeEnvironment.read({ environmentId, input: { sessionKey } }));
  const read = AsyncResult.isSuccess(result) ? result.value : null;
  const [runtimeId, setRuntimeId] = useState<PiNativeRuntimeId | null>(null);
  if (AsyncResult.isFailure(result))
    return (
      <View className="flex-1 items-center justify-center bg-screen">
        <AppText className="text-danger-foreground">Could not read this Native Pi session.</AppText>
      </View>
    );
  if (read === null)
    return (
      <View className="flex-1 items-center justify-center bg-screen">
        <AppText>Loading session…</AppText>
      </View>
    );
  const activeRuntimeId = runtimeId ?? read.session.runtime?.runtimeId ?? null;
  return activeRuntimeId === null ? (
    <HistoricalDetail environmentId={environmentId} read={read} onRuntime={setRuntimeId} />
  ) : (
    <LiveDetail environmentId={environmentId} read={read} runtimeId={activeRuntimeId} />
  );
}

export function NativePiRuntimeScreen({
  route,
}: StaticScreenProps<{ environmentId: string; runtimeId: string }>) {
  return (
    <LiveDetail
      environmentId={EnvironmentId.make(route.params.environmentId)}
      read={null}
      runtimeId={PiNativeRuntimeId.make(route.params.runtimeId)}
    />
  );
}

function HistoryList({ entries }: { entries: ReadonlyArray<PiNativeJsonlEntry> }) {
  return (
    <LegendList
      className="flex-1"
      data={entries}
      keyExtractor={(_, index) => String(index)}
      renderItem={({ item }) => <EntryRow entry={item} />}
    />
  );
}

function HistoricalDetail({
  environmentId,
  read,
  onRuntime,
}: {
  environmentId: EnvironmentId;
  read: { session: PiNativeSession; entries: ReadonlyArray<PiNativeJsonlEntry> };
  onRuntime: (id: PiNativeRuntimeId) => void;
}) {
  const resume = useAtomCommand(piNativeEnvironment.resume, { reportFailure: false });
  return (
    <View className="flex-1 bg-screen">
      <HistoryList entries={read.entries} />
      <View className="border-t border-border p-4">
        <ActionButton
          label="Resume"
          onPress={() =>
            void resume({ environmentId, input: { sessionKey: read.session.sessionKey } }).then(
              (result) => {
                if (result._tag === "Success" && result.value.runtimeId)
                  onRuntime(result.value.runtimeId);
              },
            )
          }
        />
      </View>
    </View>
  );
}

function LiveDetail({
  environmentId,
  read,
  runtimeId,
}: {
  environmentId: EnvironmentId;
  read: {
    session: PiNativeSession;
    entries: ReadonlyArray<PiNativeJsonlEntry>;
  } | null;
  runtimeId: PiNativeRuntimeId;
}) {
  const stream = useAtomValue(piNativeEnvironment.runtime({ environmentId, input: { runtimeId } }));
  const live = AsyncResult.isSuccess(stream) ? stream.value : null;
  const message = useAtomCommand(piNativeEnvironment.message, { reportFailure: false });
  const createCommandId = useAtomCommand(piNativeEnvironment.commandId, { reportFailure: false });
  const control = useAtomCommand(piNativeEnvironment.control, { reportFailure: false });
  const [text, setText] = useState("");
  const [mode, setMode] = useState<"steer" | "followUp">("steer");
  const [pending, setPending] = useState(false);
  const [feedback, setFeedback] = useState<string | null>(null);
  const [retry, setRetry] = useState<{
    readonly commandId: CommandId;
    readonly type: "send" | "steer" | "followUp";
    readonly message: string;
  } | null>(null);
  const entries: ReadonlyArray<PiNativeJsonlEntry> = [
    ...(live?.entries ?? read?.entries ?? []),
    ...(live?.events.map((item) => ({ type: "live", event: item.event })) ?? []),
  ];
  const streaming = live?.runtime?.status === "streaming";
  const submitMessage = async (submission = retry) => {
    if (pending) return;
    const value = submission?.message ?? text.trim();
    if (!value) return;
    setPending(true);
    const generated =
      submission === null ? await createCommandId({ environmentId, input: undefined }) : null;
    if (generated?._tag === "Failure") {
      setPending(false);
      setFeedback("Could not prepare the message command.");
      return;
    }
    const next = submission ?? {
      commandId: generated!.value,
      type: streaming ? mode : "send",
      message: value,
    };
    setFeedback(null);
    setRetry(next);
    const result = await message({ environmentId, input: { ...next, runtimeId } });
    setPending(false);
    if (result._tag === "Failure") {
      setFeedback("Message could not be delivered. Retry uses the same command ID.");
      return;
    }
    const receipt = result.value;
    if (receipt.status !== "completed") {
      setRetry(null);
      setFeedback(
        receipt.status === "indeterminate"
          ? "Delivery outcome is unknown. Inspect session history before sending a new command."
          : (receipt.error ?? `Message ${receipt.status}.`),
      );
      return;
    }
    setText("");
    setRetry(null);
    setFeedback("Message delivered.");
  };
  return (
    <View className="flex-1 bg-screen">
      <HistoryList entries={entries} />
      <View className="gap-2 border-t border-border bg-screen p-4">
        {streaming ? (
          <View className="flex-row gap-2">
            <ActionButton
              label={mode === "steer" ? "Steer ✓" : "Steer"}
              onPress={() => setMode("steer")}
            />
            <ActionButton
              label={mode === "followUp" ? "Follow up ✓" : "Follow up"}
              onPress={() => setMode("followUp")}
            />
          </View>
        ) : null}
        <View className="flex-row gap-2">
          <TextInput
            accessibilityLabel="Message"
            className="flex-1 rounded-lg border border-input-border bg-input px-3 py-2 text-foreground"
            value={text}
            onChangeText={setText}
            placeholder="Message Pi"
            placeholderTextColor="#888"
          />
          <ActionButton
            disabled={pending || !text.trim()}
            label={pending ? "Sending…" : "Send"}
            onPress={() => void submitMessage(null)}
          />
        </View>
        <View accessibilityLiveRegion="polite" className="flex-row items-center gap-2">
          <AppText className="flex-1 text-sm text-foreground-muted">{feedback}</AppText>
          {retry && !pending ? (
            <ActionButton label="Retry" onPress={() => void submitMessage(retry)} />
          ) : null}
        </View>
        <View className="flex-row gap-2">
          <ActionButton
            label="Abort"
            onPress={() => void control({ environmentId, input: { type: "abort", runtimeId } })}
          />
          <ActionButton
            label="Shutdown"
            onPress={() => void control({ environmentId, input: { type: "shutdown", runtimeId } })}
          />
        </View>
      </View>
    </View>
  );
}
