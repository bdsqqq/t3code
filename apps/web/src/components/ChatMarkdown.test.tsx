import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import ChatMarkdown, { MermaidDiagram } from "./ChatMarkdown";

describe("Mermaid markdown routing", () => {
  it("shows Mermaid source while rendering is pending", () => {
    const html = renderToStaticMarkup(<MermaidDiagram code={"graph LR\n A --> B"} />);

    expect(html).toContain("<pre>");
    expect(html).toContain('class="language-mermaid"');
    expect(html).not.toContain("<svg");
  });

  it("routes Mermaid fences through the chat markdown renderer", () => {
    const html = renderToStaticMarkup(
      <ChatMarkdown text={"```mermaid\ngraph LR\n A --> B\n```"} cwd={undefined} />,
    );

    expect(html).toContain('<code class="language-mermaid">');
    expect(html).toContain("graph LR");
  });
});
