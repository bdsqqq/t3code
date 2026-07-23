import { AmpSettings, ProviderDriverKind, type ServerProvider } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ServerSettingsService } from "../../serverSettings.ts";
import { makeAmpTextGeneration } from "../../textGeneration/AmpTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeAmpAdapter } from "../Layers/AmpAdapter.ts";
import { checkAmpProviderStatus, makePendingAmpProvider } from "../Layers/AmpProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const KIND = ProviderDriverKind.make("amp");
export type AmpDriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | ServerSettingsService;
export const AmpDriver: ProviderDriver<AmpSettings, AmpDriverEnv> = {
  driverKind: KIND,
  metadata: { displayName: "Amp", supportsMultipleInstances: true },
  configSchema: AmpSettings,
  defaultConfig: () => Schema.decodeSync(AmpSettings)({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const settingsService = yield* ServerSettingsService;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const effective = { ...config, enabled } satisfies AmpSettings;
      const env = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: KIND,
        instanceId,
      });
      const stamp = (snapshot: ServerProviderDraft): ServerProvider => ({
        ...snapshot,
        instanceId,
        driver: KIND,
        ...(displayName ? { displayName } : {}),
        ...(accentColor ? { accentColor } : {}),
        continuation: { groupKey: continuationIdentity.continuationKey },
      });
      const adapter = yield* makeAmpAdapter({
        binaryPath: effective.binaryPath,
        providerInstanceId: instanceId,
        environment: env,
        allowedModes: new Set(["low", "medium", "high", "ultra", ...effective.customModels]),
      });
      const maintenanceCapabilities = makeManualOnlyProviderMaintenanceCapabilities({
        provider: KIND,
        packageName: null,
      });
      const source = makeProviderSnapshotSettingsSource(effective, settingsService);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<AmpSettings>>({
        maintenanceCapabilities,
        getSettings: source.getSettings,
        streamSettings: source.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          makePendingAmpProvider(settings.provider).pipe(Effect.map(stamp)),
        checkProvider: checkAmpProviderStatus(effective, env).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.map(stamp),
        ),
        refreshInterval: Duration.minutes(5),
      });
      return {
        instanceId,
        driverKind: KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration: makeAmpTextGeneration(),
      } satisfies ProviderInstance;
    }).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderDriverError({ driver: KIND, instanceId, detail: String(cause), cause }),
      ),
    ),
};
