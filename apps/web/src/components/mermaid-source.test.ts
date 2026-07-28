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
    expect(prepareMermaidSource("graph LR; A &amp; B --> C")).toBe("graph LR\n A &amp; B --> C");
    expect(prepareMermaidSource("graph LR; A &#000000000000000000000038; B --> C")).toBe(
      "graph LR\n A &#000000000000000000000038; B --> C",
    );
  });

  it("rejects oversized source before normalization", () => {
    expect(() => prepareMermaidSource(";".repeat(20_001))).toThrow("source is too large");
  });

  it("rejects oversized chart datasets", () => {
    const points = Array.from({ length: 201 }, (_, index) => index).join(",");

    expect(() => prepareMermaidSource(`xychart-beta\nbar [${points}]`)).toThrow(
      "too many data points",
    );
  });
});
