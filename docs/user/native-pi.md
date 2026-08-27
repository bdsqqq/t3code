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
- **resumable** — the jsonl file is readable, but no writer is registered. text
  send is available through a guarded takeover; attachments, interrupt, and stop
  remain unavailable until t3 owns the resumed runtime.
- **unmanaged** — reserved for a writer whose history cannot be safely
  cataloged. current bridge registration rejects this state instead of exposing
  an unusable session.

guarded takeover requires a current host supervisor. if t3 was upgraded while an
older detached supervisor kept running, catalog-only sessions remain read-only.
finish its live sessions, then restart the old supervisor or the t3 host. t3
does not automatically kill it or start a competing supervisor.

starting a managed session uses pi's normal session directory. sending to a
resumable session first asks you to confirm takeover. confirmation is required
because an unbridged terminal pi may still be writing the same jsonl; t3 cannot
detect that process, so takeover can still create a second writer. after
confirmation, t3 validates the session path and header before starting pi.

while pi streams, send defaults to steering; the composer menu also offers a
queued follow-up. interrupt and supervisor shutdown use the same thread
controls.

rename, archive, delete, model changes, runtime-mode changes, interaction-mode
changes, and checkpoints are unavailable because pi, rather than t3, owns this
history.

settle and un-settle are available for every native pi session. t3 stores that
inbox state in the current environment and clears the local override when the
jsonl file changes, so new pi activity cannot remain hidden behind stale
settlement. historical sessions without an explicit override use the same
configured inactivity window as ordinary t3 threads.

t3 also reads `t3.thread-lifecycle.v1` custom entries written through pi's
extension API. a connected tui bridge writes those entries when settlement is
changed from t3, allowing pi-side extensions and t3 to share the state for that
session. native rpc and t3-managed pi runtimes do not yet expose pi's
`appendEntry` API; their settlement remains environment-local until pi adds
that rpc capability.

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

command ids are persisted before delivery. guarded takeover resumes the session
and sends the text as one supervisor command under the original send command id.
if the runtime appears before a retry, the same command safely targets that
writer instead of changing command shape. retrying an identical command id
returns its existing receipt. reusing an id with different content is rejected.
if the supervisor itself crashes after recording a command
but before recording its result, the receipt is `indeterminate` and t3 does not
resend it.

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
