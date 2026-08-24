import { describe, expect, it } from "@effect/vitest";

import {
  initialCodexScanState,
  initialPiScanState,
  parseClaudeLine,
  parseCodexLine,
  parsePiLine,
  totalTokens,
} from "./usageTranscripts.ts";

/** Shaped after a real Claude Code assistant record. */
function claudeLine(overrides: {
  messageId: string;
  contentType: string;
  model?: string;
  outputTokens?: number;
}): string {
  return JSON.stringify({
    type: "assistant",
    timestamp: "2026-08-07T04:05:13.944Z",
    sessionId: "5a128faa-8253-489e-b935-6c08e8e670c0",
    cwd: "/home/theo/project",
    message: {
      id: overrides.messageId,
      role: "assistant",
      model: overrides.model ?? "claude-fable-5",
      content: [{ type: overrides.contentType }],
      usage: {
        input_tokens: 2,
        cache_creation_input_tokens: 66818,
        cache_read_input_tokens: 1000,
        output_tokens: overrides.outputTokens ?? 286,
      },
    },
  });
}

describe("parseClaudeLine", () => {
  it("extracts token totals and a dedupe key", () => {
    const record = parseClaudeLine(claudeLine({ messageId: "msg_1", contentType: "text" }));

    expect(record).not.toBeNull();
    expect(record?.provider).toBe("claude");
    expect(record?.model).toBe("claude-fable-5");
    expect(record?.totals).toEqual({
      uncachedInputTokens: 2,
      cachedInputTokens: 1000,
      cacheCreationTokens: 66818,
      outputTokens: 286,
      reasoningTokens: 0,
    });
    expect(record?.dedupeKey).toBe("msg_1:");
  });

  it("gives every content block of one message the same dedupe key", () => {
    // T3 Code writes one record per content block, each repeating the parent
    // message's full usage. Summing them would overcount ~2.4x on real data.
    const text = parseClaudeLine(claudeLine({ messageId: "msg_2", contentType: "text" }));
    const toolUse = parseClaudeLine(claudeLine({ messageId: "msg_2", contentType: "tool_use" }));

    expect(text?.dedupeKey).toBe(toolUse?.dedupeKey);
    expect(text?.totals).toEqual(toolUse?.totals);
  });

  it("ignores records that are not assistant messages", () => {
    expect(parseClaudeLine(JSON.stringify({ type: "user", message: {} }))).toBeNull();
    expect(parseClaudeLine("not json")).toBeNull();
  });
});

describe("parseCodexLine", () => {
  const sessionMeta = JSON.stringify({
    type: "session_meta",
    timestamp: "2026-08-01T05:17:41.289Z",
    payload: { type: "session_meta", id: "019fbbc1-b12c-7360-a685-28c181f0025f" },
  });
  const turnContext = JSON.stringify({
    type: "turn_context",
    timestamp: "2026-08-01T05:17:42.694Z",
    payload: { type: "turn_context", model: "gpt-5.6-sol" },
  });
  const tokenCount = (inputTokens: number, cached: number, output: number, reasoning: number) =>
    JSON.stringify({
      type: "event_msg",
      timestamp: "2026-08-01T05:17:49.919Z",
      payload: {
        type: "token_count",
        info: {
          last_token_usage: {
            input_tokens: inputTokens,
            cached_input_tokens: cached,
            cache_write_input_tokens: 0,
            output_tokens: output,
            reasoning_output_tokens: reasoning,
          },
        },
      },
    });

  it("attributes usage to the model from the preceding turn context", () => {
    const state = initialCodexScanState();
    parseCodexLine(sessionMeta, state);
    parseCodexLine(turnContext, state);
    const record = parseCodexLine(tokenCount(19239, 11008, 299, 116), state);

    expect(record?.provider).toBe("codex");
    expect(record?.model).toBe("gpt-5.6-sol");
    expect(record?.sessionId).toBe("019fbbc1-b12c-7360-a685-28c181f0025f");
    // Codex reports input_tokens inclusive of the cached portion.
    expect(record?.totals.uncachedInputTokens).toBe(19239 - 11008);
    expect(record?.totals.cachedInputTokens).toBe(11008);
    expect(record?.totals.reasoningTokens).toBe(116);
  });

  it("skips a repeated token_count so deltas are not double counted", () => {
    const state = initialCodexScanState();
    parseCodexLine(turnContext, state);
    const first = parseCodexLine(tokenCount(100, 0, 10, 0), state);
    const repeat = parseCodexLine(tokenCount(100, 0, 10, 0), state);

    expect(first).not.toBeNull();
    expect(repeat).toBeNull();
  });

  it("drops usage that arrives before any model is known", () => {
    const state = initialCodexScanState();
    expect(parseCodexLine(tokenCount(100, 0, 10, 0), state)).toBeNull();
  });

  it("does not let a pre-model event poison the duplicate signature", () => {
    // A token_count before its turn_context is dropped; the identical event
    // re-emitted once the model is known must still be counted.
    const state = initialCodexScanState();
    expect(parseCodexLine(tokenCount(100, 0, 10, 0), state)).toBeNull();
    parseCodexLine(turnContext, state);
    expect(parseCodexLine(tokenCount(100, 0, 10, 0), state)).not.toBeNull();
  });

  // A forked/subagent rollout opens with the parent's history copied in and
  // every line re-stamped to the fork instant, then the ancestors' session
  // metas. Counting those again multiplied usage ~1.85x on real data (#5758).
  describe("forked rollouts", () => {
    const meta = (overrides: {
      id: string;
      timestamp: string;
      forkedFromId?: string;
      spawnParentId?: string;
    }) =>
      JSON.stringify({
        type: "session_meta",
        timestamp: overrides.timestamp,
        payload: {
          type: "session_meta",
          id: overrides.id,
          ...(overrides.forkedFromId === undefined
            ? {}
            : { forked_from_id: overrides.forkedFromId }),
          ...(overrides.spawnParentId === undefined
            ? {}
            : {
                source: {
                  subagent: { thread_spawn: { parent_thread_id: overrides.spawnParentId } },
                },
              }),
        },
      });
    const stamped = (timestamp: string, line: string) => {
      const parsed = JSON.parse(line) as { timestamp: string };
      parsed.timestamp = timestamp;
      return JSON.stringify(parsed);
    };

    it("keeps the child session id over copied ancestor metas", () => {
      const state = initialCodexScanState();
      parseCodexLine(meta({ id: "child", timestamp: "2026-08-01T05:00:00.000Z" }), state);
      parseCodexLine(meta({ id: "parent", timestamp: "2026-08-01T05:00:00.000Z" }), state);
      parseCodexLine(turnContext, state);
      const record = parseCodexLine(tokenCount(100, 0, 10, 0), state);

      expect(record?.sessionId).toBe("child");
    });

    it("drops the re-stamped copied burst and keeps the first real event", () => {
      const state = initialCodexScanState();
      const forkInstant = "2026-08-01T05:00:00.000Z";
      parseCodexLine(meta({ id: "child", timestamp: forkInstant, forkedFromId: "parent" }), state);
      parseCodexLine(meta({ id: "parent", timestamp: forkInstant }), state);
      parseCodexLine(stamped(forkInstant, turnContext), state);

      // Copied history: written in one burst at the fork instant.
      expect(
        parseCodexLine(stamped("2026-08-01T05:00:00.001Z", tokenCount(100, 0, 10, 0)), state),
      ).toBeNull();
      expect(
        parseCodexLine(stamped("2026-08-01T05:00:00.002Z", tokenCount(200, 0, 20, 0)), state),
      ).toBeNull();

      // The child's first genuine turn lands seconds later and must count.
      const real = parseCodexLine(
        stamped("2026-08-01T05:00:06.000Z", tokenCount(300, 0, 30, 0)),
        state,
      );
      expect(real).not.toBeNull();
      expect(real?.totals.outputTokens).toBe(30);

      // Suppression never restarts, even for closely spaced later events.
      const next = parseCodexLine(
        stamped("2026-08-01T05:00:06.100Z", tokenCount(400, 0, 40, 0)),
        state,
      );
      expect(next).not.toBeNull();
    });

    it("recognizes subagent spawns without forked_from_id", () => {
      const state = initialCodexScanState();
      const spawnInstant = "2026-08-01T05:00:00.000Z";
      parseCodexLine(
        meta({ id: "child", timestamp: spawnInstant, spawnParentId: "parent" }),
        state,
      );
      parseCodexLine(stamped(spawnInstant, turnContext), state);
      expect(
        parseCodexLine(stamped("2026-08-01T05:00:00.001Z", tokenCount(100, 0, 10, 0)), state),
      ).toBeNull();
    });

    it("does not suppress anything in a rollout that is not a fork", () => {
      const state = initialCodexScanState();
      parseCodexLine(meta({ id: "root", timestamp: "2026-08-01T05:00:00.000Z" }), state);
      parseCodexLine(stamped("2026-08-01T05:00:00.100Z", turnContext), state);
      const record = parseCodexLine(
        stamped("2026-08-01T05:00:00.200Z", tokenCount(100, 0, 10, 0)),
        state,
      );
      expect(record).not.toBeNull();
    });
  });
});

describe("parsePiLine", () => {
  const session = JSON.stringify({
    type: "session",
    id: "01a035bd-7418-7504-b1c6-e52ee3d517b3",
    timestamp: "2026-08-24T21:46:43.609Z",
    cwd: "/home/theo/project",
  });
  const assistant = (overrides: {
    id?: string;
    timestamp?: string;
    usage?: Record<string, unknown>;
    provider?: string;
    model?: string;
    responseModel?: string;
  }) =>
    JSON.stringify({
      type: "message",
      id: overrides.id ?? "c753f88a",
      timestamp: overrides.timestamp ?? "2026-08-24T21:46:56.846Z",
      message: {
        role: "assistant",
        provider: overrides.provider ?? "openai-codex",
        model: overrides.model ?? "gpt-5.6-sol",
        ...(overrides.responseModel === undefined
          ? {}
          : { responseModel: overrides.responseModel }),
        usage: overrides.usage ?? {
          input: 8219,
          output: 388,
          cacheRead: 82432,
          cacheWrite: 20,
          reasoning: 44,
          totalTokens: 91059,
          cost: { total: 0.093951 },
        },
      },
    });

  it("extracts Pi's disjoint token counts, reported cost, and session identity", () => {
    const state = initialPiScanState();
    parsePiLine(session, state);
    const record = parsePiLine(assistant({}), state);

    expect(record).toMatchObject({
      provider: "pi",
      model: "openai-codex/gpt-5.6-sol",
      sessionId: "01a035bd-7418-7504-b1c6-e52ee3d517b3",
      reportedCostUsd: 0.093951,
      dedupeKey: `pi:c753f88a:${Date.parse("2026-08-24T21:46:56.846Z")}`,
      totals: {
        uncachedInputTokens: 8219,
        cachedInputTokens: 82432,
        cacheCreationTokens: 20,
        outputTokens: 388,
        reasoningTokens: 44,
      },
    });
  });

  it("uses the message id to deduplicate history copied into another session file", () => {
    const original = initialPiScanState();
    const fork = initialPiScanState();
    parsePiLine(session, original);
    parsePiLine(
      JSON.stringify({ type: "session", id: "fork-session", timestamp: "2026-08-24T22:00:00Z" }),
      fork,
    );

    expect(parsePiLine(assistant({ id: "copied-message" }), original)?.dedupeKey).toBe(
      parsePiLine(assistant({ id: "copied-message" }), fork)?.dedupeKey,
    );
  });

  it("keeps unrelated session-local ids apart", () => {
    const first = parsePiLine(
      assistant({ id: "same-id", timestamp: "2026-08-24T21:46:56.846Z" }),
      initialPiScanState(),
    );
    const second = parsePiLine(
      assistant({ id: "same-id", timestamp: "2026-08-24T22:46:56.846Z" }),
      initialPiScanState(),
    );

    expect(first?.dedupeKey).not.toBe(second?.dedupeKey);
  });

  it("attributes routed responses to the concrete response model", () => {
    const record = parsePiLine(
      assistant({
        provider: "opencode",
        model: "big-pickle",
        responseModel: "deepseek-v4-flash",
      }),
      initialPiScanState(),
    );

    expect(record?.model).toBe("opencode/deepseek-v4-flash");
  });

  it("counts delegated usage unless a persisted child transcript owns it", () => {
    const toolResult = (details: Record<string, unknown>) =>
      JSON.stringify({
        type: "message",
        id: "delegated-result",
        timestamp: "2026-08-24T22:03:00.000Z",
        message: {
          role: "toolResult",
          toolName: "finder",
          usage: {
            input: 1000,
            output: 100,
            cacheRead: 200,
            cacheWrite: 0,
            reasoning: 20,
            cost: { total: 0.04 },
          },
          details,
        },
      });

    expect(
      parsePiLine(
        toolResult({ agent: "finder", model: "openai-codex/gpt-5.6-luna", messages: [] }),
        initialPiScanState(),
      ),
    ).toMatchObject({
      provider: "pi",
      model: "openai-codex/gpt-5.6-luna",
      reportedCostUsd: 0.04,
      totals: {
        uncachedInputTokens: 1000,
        cachedInputTokens: 200,
        outputTokens: 100,
        reasoningTokens: 20,
      },
    });
    expect(
      parsePiLine(
        toolResult({
          agent: "finder",
          messages: [
            {
              role: "assistant",
              provider: "openai-codex",
              model: "gpt-5.6-luna",
            },
          ],
        }),
        initialPiScanState(),
      )?.model,
    ).toBe("openai-codex/gpt-5.6-luna");
    expect(
      parsePiLine(
        toolResult({
          agent: "finder",
          model: "openai-codex/gpt-5.6-luna",
          sessionFile: "/sessions/child.jsonl",
        }),
        initialPiScanState(),
      ),
    ).toBeNull();
  });

  it("counts model-generated compaction and branch summaries", () => {
    const state = initialPiScanState();
    parsePiLine(session, state);
    parsePiLine(assistant({ provider: "openrouter", model: "google/gemini-3.1-pro" }), state);
    const compaction = parsePiLine(
      JSON.stringify({
        type: "compaction",
        id: "compaction-a",
        timestamp: "2026-08-24T22:00:00.000Z",
        usage: {
          input: 1200,
          output: 200,
          cacheRead: 300,
          cacheWrite: 40,
          reasoning: 10,
          cost: { total: 0.05 },
        },
      }),
      state,
    );
    parsePiLine(
      JSON.stringify({
        type: "model_change",
        id: "model-change-a",
        timestamp: "2026-08-24T22:01:00.000Z",
        provider: "openai-codex",
        modelId: "gpt-5.6-sol",
      }),
      state,
    );
    const branchSummary = parsePiLine(
      JSON.stringify({
        type: "branch_summary",
        id: "branch-summary-a",
        timestamp: "2026-08-24T22:02:00.000Z",
        usage: {
          input: 100,
          output: 20,
          cacheRead: 0,
          cacheWrite: 0,
          reasoning: 5,
          cost: { total: 0.01 },
        },
      }),
      state,
    );

    expect(compaction).toMatchObject({
      model: "openrouter/google/gemini-3.1-pro",
      reportedCostUsd: 0.05,
      totals: {
        uncachedInputTokens: 1200,
        cachedInputTokens: 300,
        cacheCreationTokens: 40,
        outputTokens: 200,
        reasoningTokens: 10,
      },
    });
    expect(branchSummary?.model).toBe("openai-codex/gpt-5.6-sol");
  });

  it("drops assistant messages without billable activity", () => {
    const state = initialPiScanState();
    parsePiLine(session, state);

    expect(
      parsePiLine(
        assistant({
          usage: {
            input: 0,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            reasoning: 0,
            totalTokens: 0,
            cost: { total: 0 },
          },
        }),
        state,
      ),
    ).toBeNull();
  });
});

describe("totalTokens", () => {
  it("does not add reasoning on top of output", () => {
    expect(
      totalTokens({
        uncachedInputTokens: 10,
        cachedInputTokens: 20,
        cacheCreationTokens: 30,
        outputTokens: 40,
        reasoningTokens: 25,
      }),
    ).toBe(100);
  });
});
