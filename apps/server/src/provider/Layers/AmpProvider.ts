import { AmpSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { ChildProcess } from "effect/unstable/process";

import { resolveSpawnCommand } from "@t3tools/shared/shell";
import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const PROVIDER = ProviderDriverKind.make("amp");
const EFFORT = buildSelectOptionDescriptor({
  id: "effort",
  label: "Effort",
  options: ["none", "minimal", "low", "medium", "high", "xhigh", "max"].map((value) => ({
    value,
    label: value,
    isDefault: value === "medium",
  })),
});
export const ampModelsFromSettings = (settings: AmpSettings) =>
  providerModelsFromSettings(
    ["low", "medium", "high", "ultra"].map((mode) => ({
      slug: mode,
      name: mode[0]!.toUpperCase() + mode.slice(1),
      isCustom: false,
      capabilities: { optionDescriptors: [EFFORT] },
    })),
    PROVIDER,
    settings.customModels,
    { optionDescriptors: [EFFORT] },
  );
const presentation = {
  displayName: "Amp",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: false,
} as const;

export const makePendingAmpProvider = (settings: AmpSettings): Effect.Effect<ServerProviderDraft> =>
  DateTime.now.pipe(
    Effect.map(DateTime.formatIso),
    Effect.map((checkedAt) =>
      buildServerProvider({
        presentation,
        enabled: settings.enabled,
        checkedAt,
        models: ampModelsFromSettings(settings),
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: settings.enabled ? "Amp status has not been checked yet." : "Amp is disabled.",
        },
      }),
    ),
  );

export const checkAmpProviderStatus = Effect.fn("checkAmpProviderStatus")(function* (
  settings: AmpSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  if (!settings.enabled) return yield* makePendingAmpProvider(settings);
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const result = yield* Effect.gen(function* () {
    const command = yield* resolveSpawnCommand(settings.binaryPath, ["--version"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      settings.binaryPath,
      ChildProcess.make(command.command, command.args, { env: environment, shell: command.shell }),
    );
  }).pipe(Effect.exit);
  const failed = result._tag === "Failure";
  const output = failed ? "" : `${result.value.stdout}\n${result.value.stderr}`;
  const ok = !failed && result.value.code === 0;
  return buildServerProvider({
    presentation,
    enabled: true,
    checkedAt,
    models: ampModelsFromSettings(settings),
    probe: {
      installed: failed ? !isCommandMissingCause(result.cause) : true,
      version: ok ? parseGenericCliVersion(output) : null,
      status: ok ? "ready" : "error",
      auth: { status: "unknown", type: "amp" },
      message: ok
        ? "Amp CLI is available; authentication is checked on first use."
        : "Amp CLI probe failed.",
    },
  });
});
