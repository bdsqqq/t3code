import {
  AuthAdministrativeScopes,
  AuthSessionId,
  AuthStandardClientScopes,
} from "@t3tools/contracts";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as References from "effect/References";
import * as Schema from "effect/Schema";
import { Argument, Command, Flag, GlobalFlag } from "effect/unstable/cli";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";

import {
  formatIssuedPairingCredential,
  formatIssuedSession,
  formatPairingCredentialList,
  formatSessionList,
} from "../cliAuthFormat.ts";
import * as ServerConfig from "../config.ts";
import {
  isPersistedServerRuntimeStateLive,
  type PersistedServerRuntimeState,
  readPersistedServerRuntimeState,
} from "../serverRuntimeState.ts";
import {
  authLocationFlags,
  type CliAuthLocationFlags,
  DurationFromString,
  resolveCliAuthConfig,
} from "./config.ts";

class NoRunningServerError extends Schema.TaggedErrorClass<NoRunningServerError>()(
  "NoRunningServerError",
  { baseDir: Schema.String },
) {
  override get message(): string {
    return [
      `No running T3 Code server was found under ${this.baseDir}.`,
      "Start one with `bun run dev`, or point at another directory with --base-dir.",
    ].join("\n");
  }
}

const runWithEnvironmentAuth = <A, E>(
  flags: CliAuthLocationFlags,
  run: (environmentAuth: EnvironmentAuth.EnvironmentAuth["Service"]) => Effect.Effect<A, E>,
  options?: {
    readonly quietLogs?: boolean;
  },
) =>
  Effect.gen(function* () {
    const logLevel = yield* GlobalFlag.LogLevel;
    const config = yield* resolveCliAuthConfig(flags, logLevel);
    const minimumLogLevel = options?.quietLogs ? "Error" : config.logLevel;
    return yield* Effect.gen(function* () {
      const environmentAuth = yield* EnvironmentAuth.EnvironmentAuth;
      return yield* run(environmentAuth);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(EnvironmentAuth.runtimeLayer).pipe(
          Layer.provide(ServerConfig.layer(config)),
          Layer.provide(Layer.succeed(References.MinimumLogLevel, minimumLogLevel)),
        ),
      ),
    );
  });

const ttlFlag = Flag.string("ttl").pipe(
  Flag.withSchema(DurationFromString),
  Flag.withDescription("TTL, for example `5m`, `1h`, `30d`, or `15 minutes`."),
  Flag.optional,
);

const jsonFlag = Flag.boolean("json").pipe(
  Flag.withDescription("Emit JSON instead of human-readable output."),
  Flag.withDefault(false),
);

const labelFlag = Flag.string("label").pipe(
  Flag.withDescription("Optional human-readable label."),
  Flag.optional,
);

const subjectFlag = Flag.string("subject").pipe(
  Flag.withDescription("Optional session subject."),
  Flag.optional,
);

const baseUrlFlag = Flag.string("base-url").pipe(
  Flag.withDescription("Optional public base URL used to print a ready `/pair#token=...` link."),
  Flag.optional,
);

const tokenOnlyFlag = Flag.boolean("token-only").pipe(
  Flag.withDescription("Print only the issued bearer token."),
  Flag.withDefault(false),
);

const pairingCreateCommand = Command.make("create", {
  ...authLocationFlags,
  ttl: ttlFlag,
  label: labelFlag,
  baseUrl: baseUrlFlag,
  json: jsonFlag,
}).pipe(
  Command.withDescription("Issue a new client pairing token."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(
      flags,
      (environmentAuth) =>
        Effect.gen(function* () {
          const issued = yield* environmentAuth.createPairingLink({
            scopes: AuthStandardClientScopes,
            subject: "one-time-token",
            ...(Option.isSome(flags.ttl) ? { ttl: flags.ttl.value } : {}),
            ...(Option.isSome(flags.label) ? { label: flags.label.value } : {}),
          });
          const output = formatIssuedPairingCredential(issued, {
            json: flags.json,
            ...(Option.isSome(flags.baseUrl) ? { baseUrl: flags.baseUrl.value } : {}),
          });
          yield* Console.log(output);
        }),
      {
        quietLogs: flags.json,
      },
    ),
  ),
);

/**
 * A git worktree's own `.t3`, or undefined outside one. Git marks a linked
 * worktree by making `.git` a file (`gitdir: …`) rather than a directory —
 * mirrors `resolveWorktreePath` in scripts/dev-runner.ts, which is what puts
 * dev state there in the first place.
 */
export const resolveWorktreeBaseDir = Effect.fn("auth.resolveWorktreeBaseDir")(function* (
  cwd: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const info = yield* fileSystem.stat(path.join(cwd, ".git")).pipe(Effect.option);
  if (Option.isNone(info) || info.value.type !== "File") {
    return undefined;
  }
  const baseDir = path.join(cwd, ".t3");
  // Only claim it when the dev runner has actually created it; otherwise let
  // the normal resolution report "no running server" against the real home.
  const exists = yield* fileSystem.exists(baseDir).pipe(Effect.orElseSucceed(() => false));
  return exists ? baseDir : undefined;
});

/**
 * The state directory is `<base>/dev` for an implicit dev home and
 * `<base>/userdata` otherwise — a split that depends on flags the caller of
 * this command should not have to reason about, and which silently mints
 * tokens into a database the running server never reads when guessed wrong.
 * So check both, and let the live server decide which one is right.
 */
export const findLiveServerRuntimeState = Effect.fn("auth.findLiveServerRuntimeState")(function* (
  config: Pick<ServerConfig.ServerConfig["Service"], "baseDir" | "serverRuntimeStatePath">,
) {
  const path = yield* Path.Path;
  const candidatePaths = [
    config.serverRuntimeStatePath,
    ...(["dev", "userdata"] as const).map((stateDir) =>
      path.join(config.baseDir, stateDir, "server-runtime.json"),
    ),
  ].filter((candidate, index, all) => all.indexOf(candidate) === index);

  const live: Array<{
    readonly stateDir: string;
    readonly state: PersistedServerRuntimeState;
  }> = [];

  for (const statePath of candidatePaths) {
    const state = yield* readPersistedServerRuntimeState(statePath);
    if (Option.isNone(state)) {
      continue;
    }
    // A file left behind by a killed or crashed server describes a port
    // nothing is listening on. Minting against it produces a token the live
    // server rejects with `invalid_credential`.
    if (yield* isPersistedServerRuntimeStateLive(state.value)) {
      live.push({ stateDir: path.dirname(statePath), state: state.value });
    }
  }

  // `devUrl` is only recorded by a server fronted by a web dev server, so it
  // distinguishes the dev instance from a production-style one when both are
  // running under the same base directory. This command is about dev, so a dev
  // server wins regardless of which state directory the flags happened to
  // resolve to.
  return Option.fromNullishOr(
    live.find((candidate) => candidate.state.devUrl !== undefined) ?? live[0],
  );
});

/**
 * `t3 auth pairing url` — print a ready-to-open pairing link for a dev server
 * that is already running, without having to know its port, its state
 * directory, or which of those two the `--base-dir`/`--dev-url` combination
 * happens to select. The running server records all of it in
 * `server-runtime.json`; this reads that back.
 *
 * The startup link printed in the server's own log is usually enough. This is
 * for when it has been consumed, scrolled away, or the log isn't at hand.
 */
const pairingUrlCommand = Command.make("url", {
  ...authLocationFlags,
  ttl: ttlFlag,
  json: jsonFlag,
}).pipe(
  Command.withDescription(
    "Print a pairing URL for the dev server running against this data directory.",
  ),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      const logLevel = yield* GlobalFlag.LogLevel;
      // Run from a worktree, this command means "the dev server I just started
      // here" — which the dev runner puts in the worktree's own `.t3`. Falling
      // through to the shared home would mint a credential into the database
      // the user's installed T3 Code is running against.
      const worktreeBaseDir = yield* resolveWorktreeBaseDir(process.cwd());
      const resolvedFlags =
        Option.isSome(flags.baseDir) || worktreeBaseDir === undefined
          ? flags
          : { ...flags, baseDir: Option.some(worktreeBaseDir) };
      const config = yield* resolveCliAuthConfig(resolvedFlags, logLevel);
      const live = yield* findLiveServerRuntimeState(config);

      if (Option.isNone(live)) {
        return yield* new NoRunningServerError({ baseDir: config.baseDir });
      }

      // Issue against the same state directory the server is using, whichever
      // of the two it turned out to be.
      const derivedPaths = yield* ServerConfig.deriveServerPaths(config.baseDir, config.devUrl, {
        stateDir: live.value.stateDir,
      });

      // Prefer the web origin the user actually opens; a server with no dev URL
      // serves the app itself, so its own origin is the right target.
      const baseUrl = live.value.state.devUrl ?? live.value.state.origin;

      return yield* Effect.gen(function* () {
        const environmentAuth = yield* EnvironmentAuth.EnvironmentAuth;
        const issued = yield* environmentAuth.createPairingLink({
          scopes: AuthStandardClientScopes,
          subject: "one-time-token",
          ...(Option.isSome(flags.ttl) ? { ttl: flags.ttl.value } : {}),
          label: "cli-issued pairing url",
        });
        yield* Console.log(formatIssuedPairingCredential(issued, { json: flags.json, baseUrl }));
      }).pipe(
        Effect.provide(
          Layer.mergeAll(EnvironmentAuth.runtimeLayer).pipe(
            Layer.provide(ServerConfig.layer({ ...config, ...derivedPaths })),
            Layer.provide(
              Layer.succeed(References.MinimumLogLevel, flags.json ? "Error" : config.logLevel),
            ),
          ),
        ),
      );
    }),
  ),
);

const pairingListCommand = Command.make("list", {
  ...authLocationFlags,
  json: jsonFlag,
}).pipe(
  Command.withDescription("List active client pairing tokens without revealing their secrets."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(
      flags,
      (environmentAuth) =>
        Effect.gen(function* () {
          const pairingLinks = yield* environmentAuth.listPairingLinks({
            excludeSubjects: [EnvironmentAuth.INTERNAL_ADMINISTRATIVE_BOOTSTRAP_SUBJECT],
          });
          yield* Console.log(formatPairingCredentialList(pairingLinks, { json: flags.json }));
        }),
      {
        quietLogs: flags.json,
      },
    ),
  ),
);

const pairingRevokeCommand = Command.make("revoke", {
  ...authLocationFlags,
  id: Argument.string("id").pipe(Argument.withDescription("Pairing credential id to revoke.")),
}).pipe(
  Command.withDescription("Revoke an active client pairing token."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(flags, (environmentAuth) =>
      Effect.gen(function* () {
        const revoked = yield* environmentAuth.revokePairingLink(flags.id);
        yield* Console.log(
          revoked
            ? `Revoked pairing credential ${flags.id}.\n`
            : `No active pairing credential found for ${flags.id}.\n`,
        );
      }),
    ),
  ),
);

const pairingCommand = Command.make("pairing").pipe(
  Command.withDescription("Manage one-time client pairing tokens."),
  Command.withSubcommands([
    pairingCreateCommand,
    pairingUrlCommand,
    pairingListCommand,
    pairingRevokeCommand,
  ]),
);

const sessionIssueCommand = Command.make("issue", {
  ...authLocationFlags,
  ttl: ttlFlag,
  label: labelFlag,
  subject: subjectFlag,
  tokenOnly: tokenOnlyFlag,
  json: jsonFlag,
}).pipe(
  Command.withDescription("Issue a scoped bearer access token for headless or remote clients."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(
      flags,
      (environmentAuth) =>
        Effect.gen(function* () {
          const issued = yield* environmentAuth.issueSession({
            scopes: AuthAdministrativeScopes,
            ...(Option.isSome(flags.ttl) ? { ttl: flags.ttl.value } : {}),
            ...(Option.isSome(flags.label) ? { label: flags.label.value } : {}),
            ...(Option.isSome(flags.subject) ? { subject: flags.subject.value } : {}),
          });
          yield* Console.log(
            formatIssuedSession(issued, {
              json: flags.json,
              tokenOnly: flags.tokenOnly,
            }),
          );
        }),
      {
        quietLogs: flags.json || flags.tokenOnly,
      },
    ),
  ),
);

const sessionListCommand = Command.make("list", {
  ...authLocationFlags,
  json: jsonFlag,
}).pipe(
  Command.withDescription("List active sessions without revealing bearer tokens."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(
      flags,
      (environmentAuth) =>
        Effect.gen(function* () {
          const sessions = yield* environmentAuth.listSessions();
          yield* Console.log(formatSessionList(sessions, { json: flags.json }));
        }),
      {
        quietLogs: flags.json,
      },
    ),
  ),
);

const sessionRevokeCommand = Command.make("revoke", {
  ...authLocationFlags,
  sessionId: Argument.string("session-id").pipe(
    Argument.withDescription("Session id to revoke."),
    Argument.withSchema(AuthSessionId),
  ),
}).pipe(
  Command.withDescription("Revoke an active session."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(flags, (environmentAuth) =>
      Effect.gen(function* () {
        const revoked = yield* environmentAuth.revokeSession(flags.sessionId);
        yield* Console.log(
          revoked
            ? `Revoked session ${flags.sessionId}.\n`
            : `No active session found for ${flags.sessionId}.\n`,
        );
      }),
    ),
  ),
);

const sessionCommand = Command.make("session").pipe(
  Command.withDescription("Manage bearer sessions."),
  Command.withSubcommands([sessionIssueCommand, sessionListCommand, sessionRevokeCommand]),
);

export const authCommand = Command.make("auth").pipe(
  Command.withDescription("Manage the local auth control plane for headless deployments."),
  Command.withSubcommands([pairingCommand, sessionCommand]),
);
