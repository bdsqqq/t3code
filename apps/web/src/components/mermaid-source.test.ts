import { describe, expect, it } from "vite-plus/test";

import { prepareMermaidSource } from "./mermaid-source";

describe("prepareMermaidSource", () => {
  it("normalizes semicolon statements without splitting comments or labels", () => {
    expect(prepareMermaidSource("graph LR; A --> B; %% hidden; C --> D")).toBe(
      "graph LR\n A --> B\n %% hidden; C --> D",
    );
    expect(prepareMermaidSource("graph LR; A[100%% coverage]; B --> C")).toBe(
      "graph LR\n A[100%% coverage]\n B --> C",
    );
  });

  it("rejects oversized chart datasets", () => {
    const points = Array.from({ length: 201 }, (_, index) => index).join(",");

    expect(() => prepareMermaidSource(`xychart-beta\nbar [${points}]`)).toThrow(
      "too many data points",
    );
  });
});
