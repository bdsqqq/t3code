import { useAtomValue } from "@effect/atom-react";
import { presentPiNativeUnknown } from "@t3tools/client-runtime/state/pi-native-presentation";
import type { PiNativeJsonlEntry, PiNativeSession } from "@t3tools/contracts";
import {
  CommandId,
  EnvironmentId,
  PiNativeRuntimeId,
  PiNativeSessionKey,
} from "@t3tools/contracts";
import { Link, useNavigate } from "@tanstack/react-router";
import { AsyncResult } from "effect/unstable/reactivity";
import { memo, useMemo, useState } from "react";

import { formatRelativeTimeLabel } from "../timestampFormat";
import { piNativeEnvironment } from "../state/piNative";
import { useAtomCommand } from "../state/use-atom-command";
import ChatMarkdown from "./ChatMarkdown";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { SidebarInset } from "./ui/sidebar";

function entryPresentation(entry: PiNativeJsonlEntry) {
  const direct = presentPiNativeUnknown(entry);
  return direct.text === null && entry.event !== undefined
    ? presentPiNativeUnknown(entry.event)
    : direct;
}

function historyEntryKey(entry: PiNativeJsonlEntry): string {
  if (typeof entry.id === "string") return entry.id;
  if (typeof entry.timestamp === "string")
    return `${String(entry.type ?? "entry")}:${entry.timestamp}`;
  return JSON.stringify(entry);
}

const HistoryRow = memo(function HistoryRow({
  entry,
  cwd,
}: {
  entry: PiNativeJsonlEntry;
  cwd: string;
}) {
  const { label, text } = entryPresentation(entry);
  return (
    <article className="rounded-lg border border-border bg-card p-3">
      <div className="mb-2 text-xs font-medium text-muted-foreground">{label}</div>
      {text === null ? (
        <pre className="overflow-x-auto whitespace-pre-wrap text-xs">
          {JSON.stringify(entry, null, 2)}
        </pre>
      ) : (
        <ChatMarkdown text={text} cwd={cwd} />
      )}
    </article>
  );
});

function Page({ children }: { children: React.ReactNode }) {
  return (
    <SidebarInset className="min-h-0 overflow-auto bg-background text-foreground">
      <main className="mx-auto w-full max-w-4xl p-4 sm:p-8">{children}</main>
    </SidebarInset>
  );
}

export function NativePiList({ environmentId: rawEnvironmentId }: { environmentId: string }) {
  const environmentId = EnvironmentId.make(rawEnvironmentId);
  const result = useAtomValue(piNativeEnvironment.list({ environmentId, input: {} }));
  const start = useAtomCommand(piNativeEnvironment.start, { reportFailure: false });
  const createCommandId = useAtomCommand(piNativeEnvironment.commandId, { reportFailure: false });
  const [cwd, setCwd] = useState("");
  const [starting, setStarting] = useState(false);
  const [startFeedback, setStartFeedback] = useState<string | null>(null);
  const [startRetry, setStartRetry] = useState<{
    readonly commandId: CommandId;
    readonly cwd: string;
  } | null>(null);
  const navigate = useNavigate();
  const sessions = AsyncResult.isSuccess(result) ? result.value.sessions : [];
  const runtimes = AsyncResult.isSuccess(result) ? result.value.runtimes : [];
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
    await navigate({
      to: "/pi/$environmentId/runtime/$runtimeId",
      params: { environmentId, runtimeId: receipt.runtimeId },
    });
  };
  return (
    <Page>
      <div className="mb-6 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold">Native Pi</h1>
          <p className="text-sm text-muted-foreground">Direct Pi sessions in this environment.</p>
        </div>
        <form
          className="flex gap-2"
          onSubmit={(event) => {
            event.preventDefault();
            void handleStart(null);
          }}
        >
          <Input
            aria-label="Working directory"
            placeholder="/path/to/project"
            value={cwd}
            onChange={(event) => setCwd(event.target.value)}
          />
          <Button type="submit" disabled={!cwd.trim() || starting}>
            {starting ? "Starting…" : "Start"}
          </Button>
        </form>
      </div>
      <p aria-live="polite" className="mb-3 text-sm text-muted-foreground">
        {startFeedback}
        {startRetry && !starting ? (
          <Button
            type="button"
            size="sm"
            variant="ghost"
            onClick={() => void handleStart(startRetry)}
          >
            Retry
          </Button>
        ) : null}
      </p>
      {AsyncResult.isFailure(result) ? (
        <p role="alert">Could not load Native Pi sessions.</p>
      ) : null}
      <div className="grid gap-2">
        {runtimes.map((runtime) => (
          <Link
            key={runtime.runtimeId}
            to="/pi/$environmentId/runtime/$runtimeId"
            params={{ environmentId, runtimeId: runtime.runtimeId }}
            className="rounded-lg border border-border bg-card p-3 hover:bg-muted/50 focus-visible:outline-2"
          >
            <div className="flex justify-between gap-3">
              <strong className="truncate">Native Pi runtime</strong>
              <span className="text-xs text-muted-foreground">{runtime.status}</span>
            </div>
            <div className="mt-1 truncate font-mono text-xs text-muted-foreground">
              {runtime.cwd ?? "Starting…"}
            </div>
          </Link>
        ))}
        {sessions.map((session) => (
          <SessionRow key={session.sessionKey} environmentId={rawEnvironmentId} session={session} />
        ))}
      </div>
    </Page>
  );
}

const SessionRow = memo(function SessionRow({
  environmentId,
  session,
}: {
  environmentId: string;
  session: PiNativeSession;
}) {
  const status = session.runtime?.status ?? session.liveness;
  return (
    <Link
      to="/pi/$environmentId/$sessionKey"
      params={{ environmentId, sessionKey: session.sessionKey }}
      className="rounded-lg border border-border bg-card p-3 hover:bg-muted/50 focus-visible:outline-2"
    >
      <div className="flex justify-between gap-3">
        <strong className="truncate">{session.title || "Untitled session"}</strong>
        <span className="text-xs text-muted-foreground">{status}</span>
      </div>
      <div className="mt-1 flex justify-between gap-3 text-xs text-muted-foreground">
        <span className="truncate font-mono">{session.cwd}</span>
        <span>{formatRelativeTimeLabel(session.updatedAt)}</span>
      </div>
    </Link>
  );
});

export function NativePiDetail({
  environmentId: rawEnvironmentId,
  sessionKey: rawSessionKey,
}: {
  environmentId: string;
  sessionKey: string;
}) {
  const environmentId = EnvironmentId.make(rawEnvironmentId);
  const sessionKey = PiNativeSessionKey.make(rawSessionKey);
  const readResult = useAtomValue(
    piNativeEnvironment.read({ environmentId, input: { sessionKey } }),
  );
  const read = AsyncResult.isSuccess(readResult) ? readResult.value : null;
  const [resumedRuntimeId, setResumedRuntimeId] = useState<PiNativeRuntimeId | null>(null);
  const runtimeId = resumedRuntimeId ?? read?.session.runtime?.runtimeId ?? null;
  if (AsyncResult.isFailure(readResult))
    return (
      <Page>
        <p role="alert">Could not read this Native Pi session.</p>
      </Page>
    );
  return (
    <NativePiDetailContent
      environmentId={environmentId}
      read={read}
      runtimeId={runtimeId}
      onRuntime={setResumedRuntimeId}
    />
  );
}

export function NativePiRuntimeDetail({
  environmentId: rawEnvironmentId,
  runtimeId: rawRuntimeId,
}: {
  environmentId: string;
  runtimeId: string;
}) {
  return (
    <LiveDetail
      environmentId={EnvironmentId.make(rawEnvironmentId)}
      read={null}
      runtimeId={PiNativeRuntimeId.make(rawRuntimeId)}
    />
  );
}

function NativePiDetailContent({
  environmentId,
  read,
  runtimeId,
  onRuntime,
}: {
  environmentId: EnvironmentId;
  read: { session: PiNativeSession; entries: ReadonlyArray<PiNativeJsonlEntry> } | null;
  runtimeId: PiNativeRuntimeId | null;
  onRuntime: (id: PiNativeRuntimeId) => void;
}) {
  if (read === null)
    return (
      <Page>
        <p>Loading session…</p>
      </Page>
    );
  return runtimeId === null ? (
    <HistoricalDetail environmentId={environmentId} read={read} onRuntime={onRuntime} />
  ) : (
    <LiveDetail environmentId={environmentId} read={read} runtimeId={runtimeId} />
  );
}

function SessionHeader({
  environmentId,
  session,
  status,
}: {
  environmentId: EnvironmentId;
  session: PiNativeSession;
  status: string;
}) {
  return (
    <header className="mb-5">
      <Link
        to="/pi/$environmentId"
        params={{ environmentId }}
        className="text-sm text-muted-foreground"
      >
        ← Native Pi
      </Link>
      <div className="mt-3 flex items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold">{session.title || "Untitled session"}</h1>
          <p className="font-mono text-xs text-muted-foreground">{session.cwd}</p>
        </div>
        <span className="text-sm">{status}</span>
      </div>
    </header>
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
    <Page>
      <SessionHeader
        environmentId={environmentId}
        session={read.session}
        status={read.session.liveness}
      />
      <div className="grid gap-3">
        {read.entries.map((entry) => (
          <HistoryRow key={historyEntryKey(entry)} entry={entry} cwd={read.session.cwd} />
        ))}
      </div>
      <div className="sticky bottom-0 mt-5 rounded-xl border border-border bg-background/95 p-3">
        <Button
          onClick={() =>
            void resume({ environmentId, input: { sessionKey: read.session.sessionKey } }).then(
              (result) => {
                if (result._tag === "Success" && result.value.runtimeId)
                  onRuntime(result.value.runtimeId);
              },
            )
          }
        >
          Resume
        </Button>
      </div>
    </Page>
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
  const runtimeResult = useAtomValue(
    piNativeEnvironment.runtime({ environmentId, input: { runtimeId } }),
  );
  const live = AsyncResult.isSuccess(runtimeResult) ? runtimeResult.value : null;
  const message = useAtomCommand(piNativeEnvironment.message, { reportFailure: false });
  const createCommandId = useAtomCommand(piNativeEnvironment.commandId, { reportFailure: false });
  const control = useAtomCommand(piNativeEnvironment.control, { reportFailure: false });
  const [text, setText] = useState("");
  const [mode, setMode] = useState<"send" | "steer" | "followUp">("send");
  const [pending, setPending] = useState(false);
  const [feedback, setFeedback] = useState<string | null>(null);
  const [retry, setRetry] = useState<{
    readonly commandId: CommandId;
    readonly type: "send" | "steer" | "followUp";
    readonly message: string;
  } | null>(null);
  const entries = useMemo(
    () => live?.entries ?? read?.entries ?? [],
    [live?.entries, read?.entries],
  );
  const cwd = read?.session.cwd ?? live?.runtime?.cwd ?? "";
  const status = live?.runtime?.status ?? read?.session.runtime?.status ?? "connecting";
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
      type: live?.runtime?.status === "streaming" ? mode : "send",
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
    <Page>
      {read ? (
        <SessionHeader environmentId={environmentId} session={read.session} status={status} />
      ) : (
        <header className="mb-5">
          <Link
            to="/pi/$environmentId"
            params={{ environmentId }}
            className="text-sm text-muted-foreground"
          >
            ← Native Pi
          </Link>
          <div className="mt-3 flex items-start justify-between gap-3">
            <div>
              <h1 className="text-xl font-semibold">Native Pi runtime</h1>
              <p className="font-mono text-xs text-muted-foreground">{cwd || "Starting…"}</p>
            </div>
            <span className="text-sm">{status}</span>
          </div>
        </header>
      )}
      <div className="grid gap-3">
        {entries.map((entry) => (
          <HistoryRow key={historyEntryKey(entry)} entry={entry} cwd={cwd} />
        ))}
        {live?.events.map((item) => (
          <HistoryRow key={item.eventId} entry={{ type: "live", event: item.event }} cwd={cwd} />
        ))}
      </div>
      <div className="sticky bottom-0 mt-5 rounded-xl border border-border bg-background/95 p-3">
        <form
          className="flex gap-2"
          onSubmit={(event) => {
            event.preventDefault();
            void submitMessage(null);
          }}
        >
          {live?.runtime?.status === "streaming" ? (
            <select
              aria-label="Message mode"
              value={mode}
              onChange={(e) => setMode(e.target.value as typeof mode)}
              className="rounded-md border bg-background px-2"
            >
              <option value="steer">Steer</option>
              <option value="followUp">Follow up</option>
            </select>
          ) : null}
          <Input
            aria-label="Message"
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder="Message Pi"
          />
          <Button type="submit" disabled={pending || !text.trim()}>
            {pending ? "Sending…" : "Send"}
          </Button>
        </form>
        <div aria-live="polite" className="mt-2 text-sm text-muted-foreground">
          {feedback}
          {retry && !pending ? (
            <Button
              type="button"
              size="sm"
              variant="ghost"
              onClick={() => void submitMessage(retry)}
            >
              Retry
            </Button>
          ) : null}
        </div>
        <div className="mt-2 flex gap-2">
          <Button
            variant="outline"
            onClick={() => void control({ environmentId, input: { type: "abort", runtimeId } })}
          >
            Abort
          </Button>
          <Button
            variant="outline"
            onClick={() => void control({ environmentId, input: { type: "shutdown", runtimeId } })}
          >
            Shutdown
          </Button>
        </div>
      </div>
    </Page>
  );
}
