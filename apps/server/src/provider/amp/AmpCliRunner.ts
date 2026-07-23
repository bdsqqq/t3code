import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

const Content = Schema.Union([
  Schema.Struct({ type: Schema.Literal("text"), text: Schema.String }),
  Schema.Struct({ type: Schema.Literal("thinking"), thinking: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("redacted_thinking"),
    data: Schema.optional(Schema.Unknown),
  }),
  Schema.Struct({
    type: Schema.Literal("tool_use"),
    id: Schema.String,
    name: Schema.String,
    input: Schema.Unknown,
  }),
  Schema.Struct({
    type: Schema.Literal("tool_result"),
    tool_use_id: Schema.String,
    content: Schema.Unknown,
    is_error: Schema.optional(Schema.Boolean),
  }),
]);
export const AmpCliMessage = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("system"),
    subtype: Schema.Literal("init"),
    session_id: Schema.String,
    cwd: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    type: Schema.Literal("assistant"),
    session_id: Schema.String,
    message: Schema.Struct({ content: Schema.Array(Content) }),
  }),
  Schema.Struct({
    type: Schema.Literal("user"),
    session_id: Schema.String,
    message: Schema.Struct({ content: Schema.Array(Content) }),
  }),
  Schema.Struct({
    type: Schema.Literal("result"),
    subtype: Schema.optional(Schema.String),
    session_id: Schema.String,
    is_error: Schema.Boolean,
    error: Schema.optional(Schema.String),
    result: Schema.optional(Schema.Unknown),
  }),
]);
export type AmpCliMessage = typeof AmpCliMessage.Type;

export class AmpCliTransportError extends Schema.TaggedErrorClass<AmpCliTransportError>()(
  "AmpCliTransportError",
  {
    detail: Schema.String,
    stderr: Schema.optional(Schema.String),
    exitCode: Schema.optional(Schema.Number),
    cause: Schema.optional(Schema.Defect()),
  },
) {}
const decodeJson = Schema.decodeUnknownEffect(Schema.UnknownFromJsonString);
const decodeAmpCliMessage = Schema.decodeUnknownEffect(AmpCliMessage);
const isAmpCliTransportError = Schema.is(AmpCliTransportError);

export interface AmpCliRunOptions {
  readonly binaryPath: string;
  readonly cwd: string;
  readonly env?: Readonly<Record<string, string | undefined>>;
  readonly prompt: string;
  readonly mode: string;
  readonly continueThreadId?: string;
}
export interface AmpCliRunner {
  readonly run: (options: AmpCliRunOptions) => Stream.Stream<AmpCliMessage, AmpCliTransportError>;
}
export type AmpCliRunnerFactory = () => Effect.Effect<
  AmpCliRunner,
  never,
  ChildProcessSpawner.ChildProcessSpawner
>;

export function buildAmpCliArgs(options: AmpCliRunOptions): ReadonlyArray<string> {
  return [
    ...(options.continueThreadId ? ["threads", "continue", options.continueThreadId] : []),
    "--no-ide",
    "--no-notifications",
    "--execute",
    "--stream-json-thinking",
    "--no-archive-after-execute",
    "--mode",
    options.mode,
  ];
}

export const makeAmpCliRunner: AmpCliRunnerFactory = Effect.fn("AmpCliRunner.make")(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  return {
    run: (options) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const args = buildAmpCliArgs(options);
          const resolved = yield* resolveSpawnCommand(
            options.binaryPath,
            args,
            options.env ? { env: options.env } : {},
          ).pipe(
            Effect.mapError(
              (cause) =>
                new AmpCliTransportError({
                  detail: "failed to resolve Amp CLI command",
                  cause: () => cause,
                }),
            ),
          );
          const child = yield* spawner
            .spawn(
              ChildProcess.make(resolved.command, resolved.args, {
                cwd: options.cwd,
                env: options.env,
                extendEnv: true,
                shell: resolved.shell,
              }),
            )
            .pipe(
              Effect.mapError(
                (cause) =>
                  new AmpCliTransportError({
                    detail: "failed to spawn Amp CLI",
                    cause: () => cause,
                  }),
              ),
            );
          yield* Stream.fromIterable([new TextEncoder().encode(options.prompt)]).pipe(
            Stream.run(child.stdin),
            Effect.mapError(
              (cause) =>
                new AmpCliTransportError({
                  detail: "failed to write Amp CLI stdin",
                  cause: () => cause,
                }),
            ),
          );
          const stderrFiber = yield* child.stderr.pipe(
            Stream.decodeText(),
            Stream.runFold(
              () => "",
              (a, b) => a + b,
            ),
            Effect.forkScoped,
          );
          let result: AmpCliMessage | undefined;
          const messages = child.stdout.pipe(
            Stream.decodeText(),
            Stream.splitLines,
            Stream.filter((line) => line.trim().length > 0),
            Stream.mapEffect((line) =>
              decodeJson(line).pipe(
                Effect.flatMap(decodeAmpCliMessage),
                Effect.mapError(
                  (cause) =>
                    new AmpCliTransportError({
                      detail: `invalid Amp stream-json line: ${line}`,
                      cause: () => cause,
                    }),
                ),
              ),
            ),
            Stream.mapEffect((message) => {
              if (message.type !== "result")
                return Effect.succeed(Option.some<AmpCliMessage>(message));
              if (result)
                return Effect.fail(
                  new AmpCliTransportError({ detail: "Amp CLI emitted multiple results" }),
                );
              result = message;
              return Effect.succeed(Option.none<AmpCliMessage>());
            }),
            Stream.filter(Option.isSome),
            Stream.map((message) => message.value),
          );
          const checked = messages.pipe(
            Stream.concat(
              Stream.fromEffect(
                Effect.gen(function* () {
                  const code = yield* child.exitCode;
                  const stderr = yield* Fiber.join(stderrFiber);
                  if (Number(code) !== 0)
                    return yield* new AmpCliTransportError({
                      detail: "Amp CLI exited unsuccessfully",
                      exitCode: Number(code),
                      stderr,
                    });
                  if (!result)
                    return yield* new AmpCliTransportError({
                      detail: "Amp CLI exited without a result",
                      stderr,
                    });
                  return result;
                }),
              ),
            ),
          );
          return checked;
        }),
      ).pipe(
        Stream.mapError((cause) =>
          isAmpCliTransportError(cause)
            ? cause
            : new AmpCliTransportError({ detail: String(cause), cause: () => cause }),
        ),
      ),
  };
});
