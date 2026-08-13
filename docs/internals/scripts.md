# Scripts

> For maintainers. Using T3 Code? See [docs/user](../user/).

## First checkout

T3 Code uses [Vite+](https://viteplus.dev/guide/). Install the global `vp` command, install
dependencies, then start the dev stack:

```bash
curl -fsSL https://vite.plus | bash   # Windows: irm https://vite.plus/ps1 | iex
vp i
vp run dev
```

Node 24 is required. Bun is not: the server picks Bun adapters when it detects Bun and falls back to
Node otherwise, and nothing in contributor setup needs it.

`vp run dev` prints a one-time pairing URL. Open it so the first browser navigation is
authenticated.

## Dev

- `vp run dev`: Starts contracts, server, and web in watch mode.
- `vp run dev --share`: Also publishes the web port over HTTPS on this machine's tailnet. The
  startup pairing URL is built against the shared origin, and the mapping is removed on exit.
- `vp run dev --browser`: Auto-opens a browser. Off by default. The dev runner writes
  `T3CODE_NO_BROWSER` itself from this flag, so setting `T3CODE_NO_BROWSER=0` in your environment has
  no effect; use `--browser`.
- `vp run dev:server`: Starts just the server. It runs on Node (`node --watch src/bin.ts`), so
  without Bun present it selects `NodePtyAdapter` and `NodeHttpServer`.
- `vp run dev:web`: Starts just the Vite dev server for the web app.
- `vp run dev:desktop`: Starts the Electron shell against the dev server.
- `vp run dev:marketing`: Starts the Astro marketing site.
- Pass dev-runner flags directly after the root task name, for example:
  `vp run dev --home-dir /tmp/t3code-dev`

### Dev state directories

- Dev commands run from a linked **git worktree** default to that worktree's gitignored `.t3`, even
  when `T3CODE_HOME` is set, storing state in `<worktree>/.t3/userdata`. Pass `--home-dir <path>` to
  choose another isolated directory explicitly. Submodules are not worktrees and keep the normal
  precedence.
- From the **main checkout**, dev commands implicitly use `~/.t3/dev`, keeping development state
  separate from `~/.t3/userdata`. An explicit `--home-dir <path>` stores state under
  `<path>/userdata`; the base directory remains available for caches, worktrees, and other shared
  data.

## Build, check, test

- `vp run build`: Fans out over `apps/*`, `packages/*`, `oxlint-plugin-t3code`, and `scripts`.
  Workspaces that define a build task run one: desktop, marketing, server (which depends on web), and
  web. Shared packages are consumed and bundled transitively rather than built separately.
- `vp run build:desktop`: Builds the desktop pipeline (desktop plus server).
- `vp run start`: Runs the production server (serves the built web app as static files).
- `vp check`: Vite+ format, lint, and type checks. This repo sets `typeCheck: false` in its lint
  options, so workspace type checking runs separately.
- `vp run typecheck`: Strict TypeScript checks for all packages.
- `vp run test`: Runs workspace tests.
- `vp run lint:mobile`: Mobile native static analysis (`scripts/mobile-native-static-check.ts`).
- `node apps/server/scripts/t3-sqlite-state.ts <query|exec> --base-dir <path> ...`: Inspects or seeds
  an isolated T3 SQLite database; writes create a private backup first.
- `node apps/server/scripts/t3-sqlite-growth.ts --database <path>`: Analyzes T3 SQLite payload
  growth without modifying the supplied database. Add `--json` for the complete machine-readable
  report.

### SQLite growth analysis

The growth analyzer requires an explicit path and opens it with SQLite's strict read-only mode:

```bash
node apps/server/scripts/t3-sqlite-growth.ts \
  --database ~/.t3/userdata/state.sqlite

node apps/server/scripts/t3-sqlite-growth.ts \
  --database /path/to/state.sqlite \
  --json > growth-report.json
```

It reports database/page/WAL metadata and payload row counts plus UTF-8 byte totals by thread, kind,
and age. `projection_thread_activities` is labeled as **derived projection data**;
`orchestration_events` is labeled separately as the **canonical source of truth**. The report is
evidence for planning only and does not recommend deleting canonical events.

Payload size is measured inside grouped SQL aggregates with
`length(CAST(payload_json AS BLOB))`. The analyzer does not select or parse payload JSON, start a
transaction spanning the report, checkpoint the WAL, migrate, vacuum, or write any pragma. Each
grouping query still scans the relevant table and can take time on a large database. A running
server may grow its WAL while any individual scan holds a read snapshot. If WAL pinning is an
operational concern, analyze a consistent copy made with SQLite's online backup API instead. Do not
copy only a live `state.sqlite` file while omitting its WAL.

### Historical derived activity compaction

Migration 041 adds semantic retention metadata for derived thread activities. Inspect an explicit
database in strict read-only mode first:

```bash
node apps/server/scripts/t3-sqlite-activity-compact.ts --database /path/to/state.sqlite
node apps/server/scripts/t3-sqlite-activity-compact.ts --database /path/to/state.sqlite --json
```

Policy `semantic-tool-updates-v1` operates independently per `(thread_id, turn_id)`. It preserves
all rows except superseded, identified, non-error `tool.updated` snapshots. For those snapshots it
keeps the newest row for every explicit `itemId`/`toolCallId`, plus a contiguous newest-first UX
tail capped at 100 rows and 4,194,304 payload UTF-8 bytes. The newest row is retained alone when it
exceeds the byte cap. Unknown identities, invalid JSON, lifecycle/turn/checkpoint rows,
approvals/user input, errors, and task-title recovery rows are never candidates.

To apply the report, stop or quiesce the server and provide the policy name exactly:

```bash
node apps/server/scripts/t3-sqlite-activity-compact.ts --database /path/to/state.sqlite \
  --apply --confirm semantic-tool-updates-v1
```

The tool only updates/deletes `projection_thread_activities` and advances
`projection_thread_activity_history`; it never changes canonical `orchestration_events`, runs
migrations, checkpoints WAL, or vacuums. It preserves semantic, unknown-identity, and invalid-JSON
rows conservatively, and reports exact row and UTF-8 payload-byte totals by thread, activity kind,
and category at one captured `projection.thread-activities` sequence. Apply transactions are
byte/row bounded and resumable; compare-and-swap mutations defer any concurrently changed row to
the next run.

Concurrent projection writes are safe but conservative: run a final quiesced dry-run and apply for
exact convergence. Deletion creates SQLite freelist pages; shrinking the file (and any WAL
checkpoint) is a separate operator action outside this tool.

#### Disk-space budget

Let `D` be the current `state.sqlite` file size, rounded up; do not subtract expected compactor
candidates before a replacement file has actually been produced. A conservative same-filesystem
workflow may hold these additional files at once:

- rollback backup: `1 × D`;
- clean replay or compaction working copy: `1 × D`;
- compacted replacement: `1 × D`;
- SQLite WAL and temporary headroom: `1 × D`.

That is `4 × D` additional space. Reserve `5 × D` free before starting to leave one more database
size for growth and operator error. A copy-only dry run needs approximately `1 × D`, but retaining
at least `2 × D` free avoids turning source-WAL growth or SQLite temporary work into a disk-full
incident. Existing `state.sqlite-wal` bytes count separately when checking the source filesystem.

For the measured 8,136,081,408-byte database (`8.14 GB` / `7.58 GiB`), the artifacts above total
`32.54 GB` / `30.31 GiB`, and the recommended `5 × D` reserve is `40.68 GB` / `37.89 GiB`. A macOS
filesystem reporting `90 GB` free therefore has about `49.32 GB` beyond this conservative reserve.
Re-check free space and the database/WAL sizes immediately before the maintenance window; stop if
free space is below the reserve. The replacement may be smaller after compaction, but that saving
is not part of the preflight budget.

## Desktop artifacts

- `vp run dist:desktop:artifact --platform <mac|linux|win> --target <target> --arch <arch>`: Builds a desktop artifact for a specific platform/target/arch.
- `vp run dist:desktop:dmg`: Builds a shareable macOS `.dmg` into `./release`. Architecture defaults
  to the host, so this produces an arm64 DMG on Apple Silicon. Use `dist:desktop:dmg:arm64` or
  `dist:desktop:dmg:x64`, or pass `--arch <arm64|x64|universal>`, to force one.
- `vp run dist:desktop:linux`: Builds a Linux AppImage into `./release`.
- `vp run dist:desktop:win`: Builds a Windows NSIS installer into `./release`. `:arm64` and `:x64`
  variants exist.

### Desktop `.dmg` packaging notes

- Default build is unsigned/not notarized for local sharing.
- The DMG build uses `assets/prod/black-macos-1024.png` as the production app icon source.
- Desktop production windows load the bundled UI from the `t3code://app/` root URL (not a
  `127.0.0.1` document URL, and not an explicit `index.html` path).
- Desktop packaging includes `apps/server/dist` (the `t3` backend) and starts it on loopback with an
  auth token for WebSocket/API traffic.
- Your tester can still open it on macOS by right-clicking the app and choosing **Open** on first
  launch.
- To keep staging files for debugging package contents, run: `vp run dist:desktop:dmg --keep-stage`
- To allow code-signing/notarization when configured in CI/secrets, add: `--signed`.
- Signed macOS builds also require `T3CODE_APPLE_TEAM_ID` and
  `T3CODE_MACOS_PROVISIONING_PROFILE`. The passkey RP domain is derived from
  `T3CODE_CLERK_PUBLISHABLE_KEY` unless `T3CODE_CLERK_PASSKEY_RP_DOMAINS` overrides it.
- Windows `--signed` uses Azure Trusted Signing and expects:
  `AZURE_TRUSTED_SIGNING_ENDPOINT`, `AZURE_TRUSTED_SIGNING_ACCOUNT_NAME`,
  `AZURE_TRUSTED_SIGNING_CERTIFICATE_PROFILE_NAME`, and `AZURE_TRUSTED_SIGNING_PUBLISHER_NAME`.
- Azure authentication env vars are also required (for example service principal with secret):
  `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`.

## Browser development

`dev` and `dev:web` leave `VITE_HTTP_URL` and `VITE_WS_URL` unset so the browser resolves the backend
from `window.location.origin`. Vite proxies `/api`, `/ws`, `/oauth`, and `/.well-known` to the
server, allowing the same bundle to work from localhost or a tailnet hostname.

## Running multiple dev instances

Worktrees derive a preferred port offset from their path.

- Default ports: server `13773`, web `5733`
- Shifted ports: `base + offset`
- Example: `T3CODE_DEV_INSTANCE=branch-a vp run dev:desktop`

Offset resolution, in order:

1. `T3CODE_PORT_OFFSET`, which must be a non-negative integer. Negative values are rejected.
2. `T3CODE_DEV_INSTANCE`. An all-digit value is used directly as the offset; any other non-empty
   value is hashed into one.
3. The worktree path hash.

Collision scanning depends on the mode. `dev:web` scans only the web port and shifts only the web
offset. `dev:server` scans only the server port. `dev` and `dev:desktop` scan both and shift them
together as one shared offset. Explicit server or dev-URL overrides remove the corresponding port
from the availability check. Treat the `[dev-runner]` output as authoritative.
