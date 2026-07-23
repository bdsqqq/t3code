import { TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as TextGeneration from "./TextGeneration.ts";

export const makeAmpTextGeneration = (): TextGeneration.TextGeneration["Service"] => {
  const unsupported = (operation: string) =>
    Effect.fail(
      new TextGenerationError({
        operation,
        detail: "Amp text generation is not available in this release.",
      }),
    );
  return {
    generateCommitMessage: () => unsupported("generateCommitMessage"),
    generatePrContent: () => unsupported("generatePrContent"),
    generateBranchName: () => unsupported("generateBranchName"),
    generateThreadTitle: () => unsupported("generateThreadTitle"),
  };
};
