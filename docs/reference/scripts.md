# Scripts

- `vp run dev` — Starts contracts, server, and web in watch mode.
- `vp run dev --share` (or `vp run dev:share`) — Same, plus publishes the web port on this machine's tailnet over HTTPS via `tailscale serve`. Prints the shareable URL, and the pairing URL is built against it so it can be opened from a phone or another laptop as-is. The mapping is removed on exit; if the tailnet is unavailable, the dev server still starts locally and logs a warning.
- `vp run dev:pair` — Prints a fresh pairing URL for the dev server already running against this data directory, resolving its port and web origin from `server-runtime.json`. Searches both the `dev` and `userdata` state directories and requires the recorded process to still be alive, so a crashed server's leftover state file is not mistaken for a running one. Add `--base-dir <path>` only when the server was started with `--home-dir`.
- `vp run dev:seed` — Copies recent projects and threads from the shared `~/.t3` into this worktree's isolated dev database, so the UI opens on realistic data instead of an empty sidebar. Defaults to the 25 newest threads with 200 activities each; tune with `--threads` / `--activities`, override either side with `--from` / `--to`. It refuses to write to the shared home. Stop the dev server first, and restart it afterwards. See [Seeding dev data](#seeding-dev-data).
- `vp run dev:server` — Starts just the WebSocket server. The server process runs on Bun (`@effect/platform-bun` + `BunPtyAdapter`), but task running uses `vp run`.
- `vp run dev:web` — Starts just the Vite dev server for the web app.
- Dev commands run from a **git worktree** default to that worktree's own gitignored `.t3`, so feature work never writes to the data directory the installed app uses. This deliberately outranks an ambient `T3CODE_HOME`; pass `--home-dir <path>` to choose somewhere else.
- From the **main checkout**, dev commands implicitly use `~/.t3/dev`, keeping development state separate from `~/.t3/userdata`. An explicit `--home-dir <path>` stores state under `<path>/userdata`; the base directory remains available for caches, worktrees, and other shared data.
- Web dev commands do not auto-open a browser. Open the one-time pairing URL printed by the server so the first browser navigation is authenticated. Set `T3CODE_NO_BROWSER=0` only when interactive auto-open is intentional.
- Pass dev-runner flags directly after the root task name, for example:
  `vp run dev --home-dir /tmp/t3code-dev`
- `vp run start` — Runs the production server (serves built web app as static files).
- `vp run build` — Builds contracts, web app, and server.
- `vp run typecheck` — Strict TypeScript checks for all packages.
- `vp run test` — Runs workspace tests.
- `node apps/server/scripts/t3-sqlite-state.ts <query|exec> --base-dir <path> ...` — Inspects or seeds an isolated T3 SQLite database; writes create a private backup first.
- `vp run dist:desktop:artifact -- --platform <mac|linux|win> --target <target> --arch <arch>` — Builds a desktop artifact for a specific platform/target/arch.
- `vp run dist:desktop:dmg` — Builds a shareable macOS `.dmg` into `./release`.
- `vp run dist:desktop:dmg:x64` — Builds an Intel macOS `.dmg`.
- `vp run dist:desktop:linux` — Builds a Linux AppImage into `./release`.
- `vp run dist:desktop:win` — Builds a Windows NSIS installer into `./release`.

## Desktop `.dmg` packaging notes

- Default build is unsigned/not notarized for local sharing.
- The DMG build uses `assets/prod/black-macos-1024.png` as the production app icon source.
- Desktop production windows load the bundled UI from `t3code://app/index.html` (not a `127.0.0.1` document URL).
- Desktop packaging includes `apps/server/dist` (the `t3` backend) and starts it on loopback with an auth token for WebSocket/API traffic.
- Your tester can still open it on macOS by right-clicking the app and choosing **Open** on first launch.
- To keep staging files for debugging package contents, run: `vp run dist:desktop:dmg -- --keep-stage`
- To allow code-signing/notarization when configured in CI/secrets, add: `--signed`.
- Signed macOS builds also require `T3CODE_APPLE_TEAM_ID` and
  `T3CODE_MACOS_PROVISIONING_PROFILE`. The passkey RP domain is derived from
  `T3CODE_CLERK_PUBLISHABLE_KEY` unless `T3CODE_CLERK_PASSKEY_RP_DOMAINS` overrides it.
- Windows `--signed` uses Azure Trusted Signing and expects:
  `AZURE_TRUSTED_SIGNING_ENDPOINT`, `AZURE_TRUSTED_SIGNING_ACCOUNT_NAME`,
  `AZURE_TRUSTED_SIGNING_CERTIFICATE_PROFILE_NAME`, and `AZURE_TRUSTED_SIGNING_PUBLISHER_NAME`.
- Azure authentication env vars are also required (for example service principal with secret):
  `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`.

## Running multiple dev instances

Ports resolve in this order, first match winning:

1. `T3CODE_PORT_OFFSET=<n>` — exact numeric offset, full control.
2. `T3CODE_DEV_INSTANCE=<value>` — numeric offset, or a hashed one for non-numeric values. Example: `T3CODE_DEV_INSTANCE=branch-a vp run dev:desktop`
3. **Git worktree** — the offset is hashed from the worktree path, so a worktree gets the same preferred pair every time instead of everyone starting at the default and racing for it.
4. Otherwise the defaults: server `13773`, web `5733`.

Whatever the source, this only picks a _preferred_ offset. Both ports are then checked on loopback and shifted together if either is taken, so two worktrees whose hashes collide — or one whose ports something else already holds — still start, just not on the offset they asked for. Ports are stable across restarts in practice, not guaranteed.

Which means: read the resolved values from the `[dev-runner] …` line rather than assuming them, and re-read them after a restart. `--dry-run` prints them without starting anything (it resolves only — it will not touch a `--share` mapping).

## Seeding dev data

An isolated dev database starts empty, which makes anything list- or
thread-shaped awkward to look at. `vp run dev:seed` copies the newest threads
and their projects out of the shared home:

```bash
vp run dev              # once, so migrations create the database
# stop it, then:
vp run dev:seed --threads 40
vp run dev --share
```

What it does, and why:

- **Projections only.** `orchestration_events` is never copied. The projector
  cursor is exclusive (`WHERE sequence > cursor`), so an empty event log means
  bootstrap streams nothing and leaves the copied rows alone. Copying a partial
  event range is the actual hazard — the projector would replay a tail whose
  creating events are missing.
- **Writes all nine `projection_state` rows.** Without them
  `computeSnapshotSequence` returns 0 and every shell snapshot advertises
  sequence 0.
- **Neutralizes live state.** Sessions are forced to `stopped` with no active
  turn (a copied `running` session has no agent behind it and would spin
  forever, and the session reaper skips anything with an active turn), and
  pending approval/input counts are zeroed since approvals are not copied.
- **Copies the intersection of columns.** The two databases are often on
  different migrations; a column only one side has is skipped and reported
  rather than failing the copy.
- **Refuses to write to `~/.t3`.** It replaces projection tables wholesale, so
  the shared home is rejected outright.

The copy contains real message bodies, tool payloads, and absolute host paths
from the source machine. That is the point — it is a local-to-local convenience
— but it is why the target must stay gitignored, and why there is no flag to
aim it at anything but a dev directory.

## Browser dev is single-origin

`dev` and `dev:web` deliberately leave `VITE_HTTP_URL` and `VITE_WS_URL` unset so
the client resolves its backend from `window.location.origin`, with Vite proxying
`/api`, `/ws`, `/oauth`, and `/.well-known` to the server. That is what lets a dev
server work unchanged from a tailnet name, a LAN IP, or a phone.

Setting those variables for web dev compiles absolute `localhost` URLs into the
bundle, and any browser that isn't on this machine will then try to reach its own
localhost. `dev:desktop` still sets them, because the Electron renderer talks to
the backend directly.

Non-`.ts.net` hostnames need `T3CODE_DEV_ALLOWED_HOSTS` (comma-separated) to pass
Vite's host check; `T3CODE_DEV_ALLOWED_ORIGINS` does the same for the server's
CORS allowlist.
