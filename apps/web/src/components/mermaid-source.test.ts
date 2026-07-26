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

  it("rejects expensive Cartesian edge expansion", () => {
    const left = Array.from({ length: 11 }, (_, index) => `A${index}`).join(" & ");
    const right = Array.from({ length: 11 }, (_, index) => `B${index}`).join(" & ");

    expect(() => prepareMermaidSource(`graph LR\n${left} --> ${right}`)).toThrow("too many edges");
  });

  it("rejects oversized chart datasets", () => {
    const points = Array.from({ length: 201 }, (_, index) => index).join(",");

    expect(() => prepareMermaidSource(`xychart-beta\nbar [${points}]`)).toThrow(
      "too many data points",
    );
  });
});
