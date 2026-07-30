# Control a native Pi TUI from T3 Code

T3 Code includes an optional Pi extension source at
`apps/server/src/provider/pi/assets/t3-control-v2.ts`. It lets a Pi process started in an ordinary
terminal register with T3's native-Pi supervisor. The extension is not loaded by this repository.

## Install

Copy the extension into Pi's global extension directory:

```sh
mkdir -p ~/.pi/agent/extensions
cp apps/server/src/provider/pi/assets/t3-control-v2.ts \
  ~/.pi/agent/extensions/t3-control-v2.ts
```

Start Pi normally. For a TUI that was already running, enter `/reload`; Pi tears down the old extension
runtime and starts the bridge for the current canonical session. Removing the copied file and running
`/reload` disables it.

The extension activates only when `ctx.mode === "tui"`. It connects to
`~/.pi/agent/t3-control-v2/supervisor.sock` and retries with backoff capped at five seconds while the
session remains active. A missing supervisor does not prevent local TUI use.

## Supervisor protocol

The Unix socket carries one compact JSON object per LF-terminated line. Core agent integration should
use this shape rather than infer state from Pi's session file:

- registration: `{"type":"register","protocol":"t3-control-v2","sessionId":"…","sessionFile":"…","cwd":"…","pid":123,"isStreaming":false}`
- live event: `{"type":"event","protocol":"t3-control-v2","sessionId":"…","eventId":1,"event":"agent_start","data":{}}`
- command: `{"type":"command","commandId":"stable-id","command":"send","text":"…"}`
- receipt: `{"type":"receipt","protocol":"t3-control-v2","commandId":"stable-id","status":"submitted"}`
- shutdown: `{"type":"unregister","protocol":"t3-control-v2","sessionId":"…"}`

Commands are `send`, `steer`, `followUp`, `abort`, and `shutdown`. `steer` and `followUp` map to Pi's
matching `deliverAs` modes. Every command needs a stable `commandId`; the extension rejects duplicate
IDs within the TUI process. Event IDs increase monotonically within one loaded extension runtime.

Pi's extension API does not acknowledge message preflight. Message, steering,
and follow-up receipts are therefore `submitted`, which T3 reports as
`indeterminate` rather than claiming delivery. Abort and shutdown can return
`accepted`.

The bridge emits live agent, message, tool-execution, and queue signals. It does not read or copy
conversation history, add session entries, or write session markers.

## Limitations

Only TUI sessions with this extension installed and connected appear live. Pi RPC, JSON, and print-mode
sessions are intentionally ignored. Delivery is not exactly once if the TUI process crashes after
accepting a command but before its receipt reaches the supervisor; the process-local dedupe set is lost
with that process. Events emitted while disconnected are dropped rather than replayed as history.
