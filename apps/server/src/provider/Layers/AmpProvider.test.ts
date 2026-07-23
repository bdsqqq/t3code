import assert from "node:assert/strict";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { AmpSettings } from "@t3tools/contracts";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  ampModelsFromSettings,
  checkAmpProviderStatus,
  makePendingAmpProvider,
} from "./AmpProvider.ts";

const settings = Schema.decodeSync(AmpSettings)({ customModels: ["architect"] });

it.effect("publishes Amp modes, effort options, and custom plugin modes", () =>
  Effect.gen(function* () {
    const snapshot = yield* makePendingAmpProvider(settings);
    assert.deepEqual(
      snapshot.models.map((model) => model.slug),
      ["low", "medium", "high", "ultra", "architect"],
    );
    const effort = snapshot.models[1]?.capabilities?.optionDescriptors?.[0];
    assert.equal(effort?.id, "effort");
    assert.equal(effort?.currentValue, "medium");
    assert.deepEqual(
      ampModelsFromSettings(settings).map((model) => model.name),
      ["Low", "Medium", "High", "Ultra", "architect"],
    );
  }),
);

it.effect("does not probe the CLI while Amp is disabled", () =>
  Effect.gen(function* () {
    const snapshot = yield* checkAmpProviderStatus({ ...settings, enabled: false });
    assert.equal(snapshot.enabled, false);
    assert.equal(snapshot.status, "disabled");
  }).pipe(Effect.provide(NodeServices.layer)),
);
