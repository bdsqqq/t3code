import { describe, expect, it } from "vite-plus/test";

import { composerProviderUnavailable } from "./ChatComposer";

describe("external thread provider availability", () => {
  it("uses external backing capabilities without an internal provider", () => {
    expect(
      composerProviderUnavailable({
        externalBacking: true,
        configuredProviderAvailable: false,
      }),
    ).toBe(false);
    expect(
      composerProviderUnavailable({
        externalBacking: false,
        configuredProviderAvailable: false,
      }),
    ).toBe(true);
  });
});
