import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { USAGE_CONTRACT_VERSION, type UsageProviderKind, UsageSummary } from "./usage.ts";

const decodeUsageSummary = Schema.decodeUnknownSync(UsageSummary);

const summary = (
  provider: UsageProviderKind,
  sourcePath?: string,
  contractVersion: number = USAGE_CONTRACT_VERSION,
) => ({
  contractVersion,
  readAt: "2026-08-24T22:00:00.000Z",
  timeZone: "UTC",
  sinceDay: "2026-08-24",
  untilDay: "2026-08-24",
  buckets: [
    {
      ...(sourcePath === undefined ? {} : { sourcePath }),
      day: "2026-08-24",
      provider,
      model: "gpt-5.6-sol",
      totals: {
        uncachedInputTokens: 100,
        cachedInputTokens: 200,
        cacheCreationTokens: 0,
        outputTokens: 20,
        reasoningTokens: 5,
      },
      costUsd: 0.01,
      cacheSavingsUsd: 0.02,
      costSource: "providerReported",
      records: 1,
      unpricedRecords: 0,
      sessions: 1,
    },
  ],
  sources: [
    {
      fingerprint: {
        hostId: "test-host",
        provider,
        resolvedHomePath: sourcePath ?? "/home/test/.codex/sessions",
        volumeId: "1:2",
      },
      status: "ok",
      scannedFiles: 1,
      skippedFiles: 0,
      malformedRecords: 0,
      distinctSessions: 1,
      message: null,
    },
  ],
  pricing: {
    status: "fresh",
    source: "litellm",
    fetchedAt: "2026-08-24T22:00:00.000Z",
    knownModels: 1,
  },
  scanDurationMs: 1,
});

describe("UsageSummary", () => {
  it("decodes a legacy v4 bucket before the client rejects its contract version", () => {
    expect(
      decodeUsageSummary(summary("codex", undefined, 4)).buckets[0]?.sourcePath,
    ).toBeUndefined();
  });

  it("decodes Pi buckets with source attribution", () => {
    const decoded = decodeUsageSummary(summary("pi", "/home/test/.pi/agent/sessions"));

    expect(decoded.buckets[0]).toMatchObject({
      provider: "pi",
      sourcePath: "/home/test/.pi/agent/sessions",
    });
  });

  it("decodes Grok buckets in the combined contract", () => {
    const decoded = decodeUsageSummary(summary("grok", "/home/test/.grok/sessions"));

    expect(decoded.buckets[0]).toMatchObject({
      provider: "grok",
      sourcePath: "/home/test/.grok/sessions",
    });
  });
});
