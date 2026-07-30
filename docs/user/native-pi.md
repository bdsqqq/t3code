# control native pi sessions

t3 code can list and control sessions stored by pi under `~/.pi/agent/sessions`.
the jsonl file remains the conversation's source of truth; opening a native
session does not import it into t3's thread database.

native sessions appear beside ordinary threads under the project that matches
their working directory. selecting one opens the normal thread route, timeline,
and composer.

on web, choose **new native pi session** from the command palette while a
project is active. on mobile, choose **native pi** as the run target in the
normal new-task composer. both flows return to the ordinary thread screen.

## session states

- **live** — a supervisor-owned rpc process or registered tui bridge currently
  owns the session.
- **historical** — the jsonl file is readable, but no writer is registered; t3
  keeps it read-only because an unbridged tui cannot be ruled out.
- **unmanaged** — reserved for a writer whose history cannot be safely
  cataloged. current bridge registration rejects this state instead of exposing
  an unusable session.

starting a managed session uses pi's normal session directory. while pi streams,
send defaults to steering; the composer menu also offers a queued follow-up.
interrupt and supervisor shutdown use the same thread controls.

rename, archive, delete, model changes, runtime-mode changes, interaction-mode
changes, and checkpoints are unavailable because pi, rather than t3, owns this
history.

image attachments are unavailable in v1 because native jsonl images do not yet
have an authenticated t3 asset URL.

## reconnect behavior

the host-local supervisor owns rpc processes independently of the t3 server.
restarting t3 or reconnecting a phone attaches to the same runtime. finalized
history is reread from jsonl; a bounded event overlay restores an in-progress
response.

the normal timeline renders a bounded active branch. explicit truncation
metadata records omitted history; the jsonl remains complete on disk. socket
frames, replay rings, live overlays, pending queues, headers, and jsonl reads
also have byte ceilings so remote reconnects remain bounded.

current ceilings are 1,000 projected entries, a 256 kib header read, a 16 mib
jsonl tail, an 8 mib live overlay, a 16 mib replay ring, a 4 mib pending queue,
a 16 mib outbound socket queue, and a 112 mib inbound protocol frame. an
individual streamed item is capped at 32 mib. catalog snapshots are capped at
5,000 threads and 8 mib; omission counts are included in the snapshot.

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
