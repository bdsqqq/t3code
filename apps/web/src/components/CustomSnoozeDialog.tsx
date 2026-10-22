import { useEffect, useId, useState } from "react";
import { create } from "zustand";
import {
  localSnoozeDate,
  localSnoozeTime,
  resolveCustomSnooze,
  resolveConditionalSnooze,
  type CustomSnoozeInput,
  type SnoozeChoice,
} from "@t3tools/client-runtime/state/thread-settled";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { readThreadShells, readEnvironmentSupportsConditionalSnooze } from "../state/entities";
import {
  Combobox,
  ComboboxInput,
  ComboboxPopup,
  ComboboxList,
  ComboboxItem,
  ComboboxEmpty,
} from "./ui/combobox";
import { Button } from "./ui/button";
import { CalendarIcon } from "lucide-react";
import { Calendar } from "./ui/calendar";
import { weekStartsOn } from "../timestampFormat";
import { Popover, PopoverTrigger, PopoverPopup } from "./ui/popover";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { Select, SelectTrigger, SelectValue, SelectPopup, SelectItem } from "./ui/select";
import {
  NumberField,
  NumberFieldGroup,
  NumberFieldInput,
  NumberFieldDecrement,
  NumberFieldIncrement,
} from "./ui/number-field";
import {
  Dialog,
  DialogPopup,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogPanel,
  DialogFooter,
} from "./ui/dialog";

type ThreadOption = { readonly id: string; readonly title: string };
type Request = {
  readonly id: number;
  readonly resolve: (choice: SnoozeChoice | null) => void;
  readonly conditional: boolean;
  readonly threads: ReadonlyArray<ThreadOption>;
};
const useRequest = create<{ request: Request | null }>(() => ({ request: null }));
let nextRequestId = 0;

export function requestCustomSnooze(
  targets: ReadonlyArray<ScopedThreadRef> = [],
): Promise<SnoozeChoice | null> {
  useRequest.getState().request?.resolve(null);
  const environmentId = targets[0]?.environmentId;
  const conditional =
    environmentId !== undefined &&
    targets.every((target) => target.environmentId === environmentId) &&
    readEnvironmentSupportsConditionalSnooze(environmentId);
  const excluded = new Set(targets.map((target) => target.threadId));
  const threads = conditional
    ? readThreadShells()
        .filter(
          (thread) =>
            thread.environmentId === environmentId &&
            thread.archivedAt === null &&
            thread.deletedAt === null &&
            !excluded.has(thread.id),
        )
        .map((thread) => ({ id: thread.id, title: thread.title || thread.branch || thread.id }))
    : [];
  return new Promise((resolve) =>
    useRequest.setState({ request: { id: ++nextRequestId, resolve, conditional, threads } }),
  );
}

function finish(choice: SnoozeChoice | null) {
  const request = useRequest.getState().request;
  useRequest.setState({ request: null });
  request?.resolve(choice);
}

export function CustomSnoozeDialogHost() {
  const request = useRequest((state) => state.request);
  useEffect(() => () => finish(null), []);
  return request ? <CustomSnoozeDialog key={request.id} request={request} /> : null;
}

function CustomSnoozeDialog({ request }: { readonly request: Request }) {
  const id = useId();
  const [initial] = useState(() => new Date(Date.now() + 3_600_000));
  const [mode, setMode] = useState<CustomSnoozeInput["mode"] | "thread" | "pull-request">("date");
  const [thread, setThread] = useState<ThreadOption | null>(null);
  const [url, setUrl] = useState("");
  const [date, setDate] = useState(initial);
  const [calendarOpen, setCalendarOpen] = useState(false);
  const [time, setTime] = useState(localSnoozeTime(initial));
  const [amount, setAmount] = useState("2");
  const [unit, setUnit] = useState<"minutes" | "hours" | "days">("hours");
  const [error, setError] = useState<string | null>(null);
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) finish(null);
      }}
    >
      <DialogPopup className="sm:max-w-sm">
        <form
          className="flex min-h-0 flex-col"
          onSubmit={(event) => {
            event.preventDefault();
            if (mode === "thread" || mode === "pull-request") {
              const condition = resolveConditionalSnooze(
                mode === "thread" ? { mode, threadId: thread?.id ?? "" } : { mode, url },
              );
              if (!condition) {
                setError(
                  mode === "thread"
                    ? "Choose another thread."
                    : "Enter a supported pull request URL.",
                );
                return;
              }
              finish({ condition });
              return;
            }
            const input: CustomSnoozeInput =
              mode === "date"
                ? { mode, date: localSnoozeDate(date), time }
                : { mode, amount, unit };
            const snoozedUntil = resolveCustomSnooze(input, new Date());
            if (!snoozedUntil) {
              setError(
                mode === "date"
                  ? "Choose a valid date and time in the future."
                  : "Enter a positive duration.",
              );
              return;
            }
            finish({ snoozedUntil });
          }}
        >
          <DialogHeader>
            <DialogTitle>Custom snooze</DialogTitle>
            <DialogDescription>Choose when snoozed threads return to your inbox.</DialogDescription>
          </DialogHeader>
          <DialogPanel>
            <div className="flex flex-col gap-4">
              <div className="flex flex-col gap-1.5">
                <Label htmlFor={`${id}-mode`}>Wake when</Label>
                <Select
                  value={mode}
                  items={{
                    date: "Date and time",
                    duration: "Duration",
                    thread: "Another thread settles",
                    "pull-request": "A pull request merges",
                  }}
                  onValueChange={(value) => {
                    if (
                      value === "date" ||
                      value === "duration" ||
                      value === "thread" ||
                      value === "pull-request"
                    )
                      setMode(value);
                    setError(null);
                  }}
                >
                  <SelectTrigger id={`${id}-mode`}>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectPopup>
                    <SelectItem value="date">Date and time</SelectItem>
                    <SelectItem value="duration">Duration</SelectItem>
                    {request.conditional && (
                      <SelectItem value="thread">Another thread settles</SelectItem>
                    )}
                    {request.conditional && (
                      <SelectItem value="pull-request">A pull request merges</SelectItem>
                    )}
                  </SelectPopup>
                </Select>
              </div>
              <div className="flex flex-col gap-4">
                {mode === "date" ? (
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                    <div className="flex min-w-0 flex-col gap-1.5">
                      <Label htmlFor={`${id}-date`}>Date</Label>
                      <Popover open={calendarOpen} onOpenChange={setCalendarOpen}>
                        <PopoverTrigger
                          render={
                            <Button
                              id={`${id}-date`}
                              variant="outline"
                              className="w-full justify-between"
                            />
                          }
                        >
                          {date.toLocaleDateString(undefined, {
                            month: "short",
                            day: "numeric",
                            year: "numeric",
                          })}
                          <CalendarIcon className="size-4 text-muted-foreground" />
                        </PopoverTrigger>
                        <PopoverPopup align="start" aria-label="Choose snooze date">
                          <Calendar
                            mode="single"
                            required
                            selected={date}
                            defaultMonth={date}
                            {...(weekStartsOn === undefined ? {} : { weekStartsOn })}
                            disabled={{ before: new Date(new Date().setHours(0, 0, 0, 0)) }}
                            onSelect={(selected) => {
                              setDate(selected);
                              setCalendarOpen(false);
                              setError(null);
                            }}
                          />
                        </PopoverPopup>
                      </Popover>
                    </div>
                    <Label className="flex min-w-0 flex-col items-stretch" htmlFor={`${id}-time`}>
                      Time
                      <Input
                        nativeInput
                        id={`${id}-time`}
                        className="h-9 sm:h-8"
                        type="time"
                        required
                        value={time}
                        onChange={(event) => {
                          setTime(event.target.value);
                          setError(null);
                        }}
                      />
                    </Label>
                  </div>
                ) : mode === "duration" ? (
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                    <NumberField
                      id={`${id}-amount`}
                      min={0}
                      step="any"
                      value={amount === "" ? null : Number(amount)}
                      onValueChange={(value) => {
                        setAmount(value === null ? "" : String(value));
                        setError(null);
                      }}
                    >
                      <Label htmlFor={`${id}-amount`}>Snooze for</Label>
                      <NumberFieldGroup>
                        <NumberFieldDecrement aria-label="Decrease duration" />
                        <NumberFieldInput required />
                        <NumberFieldIncrement aria-label="Increase duration" />
                      </NumberFieldGroup>
                    </NumberField>
                    <Label className="flex min-w-0 flex-col items-stretch" htmlFor={`${id}-unit`}>
                      Unit
                      <Select
                        value={unit}
                        items={{ minutes: "Minutes", hours: "Hours", days: "Days" }}
                        onValueChange={(value) => {
                          if (value === "minutes" || value === "hours" || value === "days")
                            setUnit(value);
                          setError(null);
                        }}
                      >
                        <SelectTrigger id={`${id}-unit`} className="min-w-0">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectPopup>
                          <SelectItem value="minutes">Minutes</SelectItem>
                          <SelectItem value="hours">Hours</SelectItem>
                          <SelectItem value="days">Days</SelectItem>
                        </SelectPopup>
                      </Select>
                    </Label>
                  </div>
                ) : mode === "thread" ? (
                  <div className="flex flex-col gap-1.5">
                    <Label htmlFor={`${id}-thread`}>Wait for thread</Label>
                    <Combobox
                      items={request.threads}
                      value={thread}
                      itemToStringLabel={(item) => item.title}
                      isItemEqualToValue={(item, value) => item.id === value.id}
                      onValueChange={(value) => {
                        setThread(value);
                        setError(null);
                      }}
                    >
                      <ComboboxInput id={`${id}-thread`} placeholder="Search threads…" />
                      <ComboboxPopup>
                        <ComboboxEmpty>No matching threads.</ComboboxEmpty>
                        <ComboboxList>
                          {(item: ThreadOption) => (
                            <ComboboxItem key={item.id} value={item}>
                              {item.title}
                            </ComboboxItem>
                          )}
                        </ComboboxList>
                      </ComboboxPopup>
                    </Combobox>
                    <p className="text-xs text-muted-foreground">
                      Returns when this thread is settled, not just when its agent stops.
                    </p>
                  </div>
                ) : (
                  <div className="flex flex-col gap-1.5">
                    <Label htmlFor={`${id}-pr`}>Pull request URL</Label>
                    <Input
                      id={`${id}-pr`}
                      type="url"
                      required
                      value={url}
                      placeholder="https://github.com/owner/repo/pull/123"
                      onChange={(event) => {
                        setUrl(event.target.value);
                        setError(null);
                      }}
                    />
                    <p className="text-xs text-muted-foreground">
                      The server checks for a merge while you’re away. This does not start the
                      agent.
                    </p>
                  </div>
                )}
              </div>
            </div>
            {error && (
              <p role="alert" className="text-destructive">
                {error}
              </p>
            )}
          </DialogPanel>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => finish(null)}>
              Cancel
            </Button>
            <Button type="submit">Snooze</Button>
          </DialogFooter>
        </form>
      </DialogPopup>
    </Dialog>
  );
}
