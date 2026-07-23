import assert from "node:assert/strict";

import { it } from "@effect/vitest";

import { BUILT_IN_DRIVERS } from "../builtInDrivers.ts";
import { AmpDriver } from "./AmpDriver.ts";

it("registers Amp as a built-in driver", () => {
  assert.equal(
    BUILT_IN_DRIVERS.find((driver) => driver.driverKind === "amp"),
    AmpDriver,
  );
});
