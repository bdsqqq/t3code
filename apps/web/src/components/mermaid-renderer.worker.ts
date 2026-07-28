/// <reference lib="webworker" />

import { isMermaidRenderRequest, type MermaidRenderResponse } from "./mermaid-renderer-protocol";
import { renderSafeMermaidSvg } from "./mermaid-renderer";

const worker = self;

worker.addEventListener("message", (event: MessageEvent<unknown>) => {
  if (!isMermaidRenderRequest(event.data)) return;

  const startedAt = performance.now();
  let response: MermaidRenderResponse;
  try {
    response = {
      type: "render-result",
      requestId: event.data.requestId,
      ok: true,
      svg: renderSafeMermaidSvg(event.data.code, event.data.namespace),
      renderDurationMs: performance.now() - startedAt,
    };
  } catch (cause) {
    response = {
      type: "render-result",
      requestId: event.data.requestId,
      ok: false,
      error: cause instanceof Error ? cause.message.slice(0, 300) : "Mermaid rendering failed",
      renderDurationMs: performance.now() - startedAt,
    };
  }

  // Dedicated workers do not accept a targetOrigin argument.
  // oxlint-disable-next-line unicorn/require-post-message-target-origin
  worker.postMessage(response);
});
