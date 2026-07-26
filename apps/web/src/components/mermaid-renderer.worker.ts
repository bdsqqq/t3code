/// <reference lib="webworker" />

import { renderSafeMermaidSvg } from "./mermaid-renderer";

interface MermaidWorkerRequest {
  readonly code: string;
}

interface MermaidWorkerResponse {
  readonly svg?: string;
  readonly error?: string;
}

self.addEventListener("message", (event: MessageEvent<MermaidWorkerRequest>) => {
  let response: MermaidWorkerResponse;
  try {
    response = { svg: renderSafeMermaidSvg(event.data.code) };
  } catch (error) {
    response = { error: error instanceof Error ? error.message : String(error) };
  }
  // Dedicated workers do not accept a targetOrigin argument.
  // oxlint-disable-next-line unicorn/require-post-message-target-origin
  self.postMessage(response);
});
