/**
 * Shares a running dev server on the local tailnet via `tailscale serve`, so it
 * can be opened from a phone, another laptop, or by whoever is reviewing the
 * work.
 *
 * Because browser dev is single-origin (Vite proxies the backend — see
 * `resolveDevProxyTarget` in apps/web/vite.config.ts), one proxy rule covering
 * the web port is enough; the backend needs no mapping of its own.
 */

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

export class DevShareError extends Schema.TaggedErrorClass<DevShareError>()("DevShareError", {
  reason: Schema.Literals([
    "tailscale-missing",
    "status-failed",
    "status-unreadable",
    "no-tailnet-name",
    "serve-failed",
  ]),
  detail: Schema.optional(Schema.String),
}) {
  override get message(): string {
    const base = {
      "tailscale-missing": "tailscale is not installed or not on PATH",
      "status-failed": "could not read tailscale status",
      "status-unreadable": "could not parse tailscale status output",
      "no-tailnet-name": "this machine has no tailnet DNS name",
      "serve-failed": "tailscale serve failed",
    }[this.reason];
    return this.detail ? `${base}: ${this.detail}` : base;
  }

  /** What the user can actually do about it. */
  get hint(): string | undefined {
    switch (this.reason) {
      case "tailscale-missing":
        return "Install Tailscale, or drop --share and open the printed localhost URL.";
      case "status-failed":
        return "Is tailscaled running? Try `tailscale status`.";
      case "no-tailnet-name":
        return "Run `tailscale up` and make sure MagicDNS is enabled.";
      default:
        return undefined;
    }
  }
}

/** The one field we need out of `tailscale status --json`. */
const TailscaleStatus = Schema.fromJsonString(
  Schema.Struct({
    Self: Schema.Struct({
      DNSName: Schema.String,
    }),
  }),
);
const decodeTailscaleStatus = Schema.decodeUnknownEffect(TailscaleStatus);

const collectStreamAsString = <E>(stream: Stream.Stream<Uint8Array, E>): Effect.Effect<string, E> =>
  stream.pipe(
    Stream.decodeText(),
    Stream.runFold(
      () => "",
      (accumulated, chunk) => accumulated + chunk,
    ),
  );

interface TailscaleResult {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
}

const runTailscale = Effect.fn("devShare.runTailscale")(function* (
  args: ReadonlyArray<string>,
  spawnFailureReason: DevShareError["reason"],
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const child = yield* spawner
    .spawn(ChildProcess.make("tailscale", args))
    .pipe(Effect.mapError(() => new DevShareError({ reason: "tailscale-missing" })));

  const [stdout, stderr, exitCode] = yield* Effect.all(
    [
      collectStreamAsString(child.stdout),
      collectStreamAsString(child.stderr),
      child.exitCode.pipe(Effect.map(Number)),
    ],
    { concurrency: "unbounded" },
  ).pipe(
    Effect.mapError(
      (cause) => new DevShareError({ reason: spawnFailureReason, detail: String(cause) }),
    ),
  );

  return { exitCode, stdout, stderr } satisfies TailscaleResult;
});

/** The tailnet DNS name of this machine, e.g. `bb-1.example.ts.net`. */
export const resolveTailnetHost = Effect.fn("devShare.resolveTailnetHost")(function* () {
  const status = yield* runTailscale(["status", "--json"], "status-failed");
  if (status.exitCode !== 0) {
    return yield* new DevShareError({
      reason: "status-failed",
      ...(status.stderr.trim() ? { detail: status.stderr.trim() } : {}),
    });
  }

  const decoded = yield* decodeTailscaleStatus(status.stdout).pipe(
    Effect.mapError(() => new DevShareError({ reason: "status-unreadable" })),
  );

  // MagicDNS names come back fully qualified, with the trailing dot.
  const host = decoded.Self.DNSName.replace(/\.$/, "");
  if (!host) {
    return yield* new DevShareError({ reason: "no-tailnet-name" });
  }
  return host;
});

export interface DevShareResult {
  readonly url: string;
  readonly host: string;
}

/**
 * `tailscale serve … off` exits nonzero with this when the port had no mapping,
 * which is the normal case for a first-time share — not a failure.
 */
const NO_EXISTING_HANDLER_PATTERN = /handler does not exist/i;

/**
 * Removes any mapping for `webPort`, reporting whether the port is now clear.
 *
 * Runs uninterruptibly with its own scope: this is called from a finalizer on
 * the way out of an interrupted program, and spawning the cleanup subprocess
 * under the dying scope would cancel it before `tailscale` ever ran — leaving
 * exactly the stale mapping it exists to remove.
 */
export const unshareDevServer = (
  webPort: number,
): Effect.Effect<
  { readonly cleared: boolean; readonly detail?: string | undefined },
  never,
  ChildProcessSpawner.ChildProcessSpawner
> =>
  runTailscale(["serve", `--https=${String(webPort)}`, "off"], "serve-failed").pipe(
    Effect.map((result) => {
      if (result.exitCode === 0) {
        return { cleared: true } as const;
      }
      const stderr = result.stderr.trim();
      // Nothing was mapped, so the port is clear either way.
      if (NO_EXISTING_HANDLER_PATTERN.test(stderr)) {
        return { cleared: true } as const;
      }
      return { cleared: false, detail: stderr || `exit code ${String(result.exitCode)}` } as const;
    }),
    // A spawn failure (no tailscale on PATH) also means we cannot vouch for the
    // port being clear.
    Effect.catch((error: DevShareError) =>
      Effect.succeed({ cleared: false, detail: error.message } as const),
    ),
    Effect.scoped,
    Effect.uninterruptible,
  );

/**
 * Publishes `webPort` on the tailnet at the same port number and returns the
 * resulting HTTPS URL. Idempotent: re-running replaces any existing mapping.
 */
export const shareDevServer = Effect.fn("devShare.shareDevServer")(function* (input: {
  readonly webPort: number;
}) {
  const host = yield* resolveTailnetHost();
  const port = String(input.webPort);

  // Clear any mapping left behind by a run that was killed before its finalizer
  // could fire. Serve config survives both the process and a reboot, and a
  // stale entry may carry path routes we no longer want — older versions mapped
  // /ws, /api and friends to a separate backend port, and serving "/" alone
  // would leave those pointing at a port nothing is listening on.
  const cleared = yield* unshareDevServer(input.webPort);
  if (!cleared.cleared) {
    // Serving over routes we failed to remove would hand out a URL that is
    // broken in a way the user cannot see: the page loads while /ws and /api
    // silently resolve to a dead backend. Better to refuse and say why.
    return yield* new DevShareError({
      reason: "serve-failed",
      detail: `could not clear the existing mapping for port ${port}${
        cleared.detail ? `: ${cleared.detail}` : ""
      }. Run \`tailscale serve --https=${port} off\` and retry.`,
    });
  }

  const serve = yield* runTailscale(
    ["serve", "--bg", `--https=${port}`, `http://127.0.0.1:${port}`],
    "serve-failed",
  );

  if (serve.exitCode !== 0) {
    // The clear above already happened, so say so: on a re-share this port is
    // now serving nothing, and an operator who only saw "serve failed" would
    // reasonably assume the previous mapping survived.
    const cause = serve.stderr.trim() || `exit code ${String(serve.exitCode)}`;
    return yield* new DevShareError({
      reason: "serve-failed",
      detail: `${cause} (port ${port} is no longer served; any previous mapping for it was cleared before this attempt)`,
    });
  }

  return { url: `https://${host}:${port}/`, host } satisfies DevShareResult;
});
