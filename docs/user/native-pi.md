# control native pi sessions

t3 code can list and control sessions stored by pi under `~/.pi/agent/sessions`.
the jsonl file remains the conversation's source of truth; opening a native
session does not import it into t3's thread database.

open **native pi** from the web sidebar or the mobile home header. choose an
environment first when more than one is connected.

## session states

- **live** — a supervisor-owned rpc process or registered tui bridge currently
  owns the session.
- **historical** — the jsonl file is readable, but no writer is registered.
- **unmanaged** — reserved for a writer whose history cannot be safely
  cataloged. current bridge registration rejects this state instead of exposing
  an unusable session.

starting a session uses pi's normal session directory. resuming opens the
selected jsonl in its recorded working directory. a live session accepts normal
messages, steering messages, queued follow-ups, abort, and shutdown.

## reconnect behavior

the host-local supervisor owns rpc processes independently of the t3 server.
restarting t3 or reconnecting a phone attaches to the same runtime. finalized
history is reread from jsonl; a bounded event overlay restores an in-progress
response.

the control view renders the session header and latest 999 appended entries.
the jsonl remains complete on disk; this display bound prevents a long-running
session from repeatedly crossing the remote connection.

command ids are persisted before delivery. retrying an identical command id
returns its existing receipt. reusing an id with different content is rejected.
if the supervisor itself crashes after recording a command but before recording
its result, the receipt is `indeterminate` and t3 does not resend it.

supervised rpc sessions cannot render an extension's terminal dialog on a
remote client. select, confirm, input, and editor requests are cancelled
automatically instead of blocking the agent. notification-style extension UI
events remain visible in the live event stream.

## terminal sessions

ordinary terminal pi sessions need the optional bridge because a jsonl file
contains history, not a control channel. see
[control a native pi tui from t3 code](./pi-tui-bridge.md).

unbridged terminal sessions continue to work normally, but t3 cannot determine
whether they are live. pi needs a core writer-lease API to make that distinction
authoritative without the bridge.
