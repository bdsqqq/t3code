import { PiSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import { ChildProcessSpawner } from "effect/unstable/process";

import { mapPiDiscoveredModels } from "../pi/PiModel.ts";
import {
  makePiRpcClient,
  type PiRpcClient,
  type PiRpcError,
  type PiRpcSpawnOptions,
} from "../pi/PiRpcClient.ts";
import {
  buildServerProvider,
  isCommandMissingCause,
  providerModelsFromSettings,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const PROVIDER = ProviderDriverKind.make("pi");
const PRESENTATION = {
  displayName: "Pi",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: false,
} as const;
const DETERMINISTIC_ARGS = [
  "--no-session",
  "--offline",
  "--no-context-files",
  "--no-extensions",
  "--no-skills",
  "--no-prompt-templates",
] as const;

type PiRpcClientFactory = (
  options: PiRpcSpawnOptions,
) => Effect.Effect<PiRpcClient, PiRpcError, ChildProcessSpawner.ChildProcessSpawner | Scope.Scope>;
const models = (settings: PiSettings, discovered = mapPiDiscoveredModels([])) =>
  providerModelsFromSettings(discovered, PROVIDER, settings.customModels, {
    optionDescriptors: [],
  });

export const makePendingPiProvider = (settings: PiSettings): Effect.Effect<ServerProviderDraft> =>
  DateTime.now.pipe(
    Effect.map(DateTime.formatIso),
    Effect.map((checkedAt) =>
      buildServerProvider({
        presentation: PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models: models(settings),
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: settings.enabled
            ? "Pi provider status has not been checked in this session yet."
            : "Pi is disabled in T3 Code settings.",
        },
      }),
    ),
  );

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  settings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
  makeRpcClient: PiRpcClientFactory = makePiRpcClient,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  if (!settings.enabled) return yield* makePendingPiProvider(settings);
  const inventory = yield* Effect.scoped(
    Effect.gen(function* () {
      const client = yield* makeRpcClient({
        command: settings.binaryPath,
        args: DETERMINISTIC_ARGS,
        env: environment,
      });
      return yield* client.getAvailableModels();
    }),
  ).pipe(Effect.exit);
  if (inventory._tag === "Failure") {
    const error = Cause.squash(inventory.cause);
    return buildServerProvider({
      presentation: PRESENTATION,
      enabled: true,
      checkedAt,
      models: models(settings),
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: `Pi model discovery failed: ${String(error)}`,
      },
    });
  }
  const discovered = mapPiDiscoveredModels(
    inventory.value.models.map((model) => ({
      provider: model.provider,
      id: model.id,
      name: model.name?.trim() || model.id,
      ...(model.reasoning === undefined ? {} : { reasoning: model.reasoning }),
    })),
  );
  return buildServerProvider({
    presentation: PRESENTATION,
    enabled: true,
    checkedAt,
    models: models(settings, discovered),
    probe: {
      installed: true,
      version: null,
      status: discovered.length > 0 ? "ready" : "warning",
      auth: { status: discovered.length > 0 ? "authenticated" : "unknown", type: "pi" },
      message:
        discovered.length > 0
          ? `Pi reported ${discovered.length} available model${discovered.length === 1 ? "" : "s"}.`
          : "Pi is available, but it did not report any models.",
    },
  });
});
