import * as Effect from "effect/Effect";

import PullRequestFilesViewed from "./053_PullRequestFilesViewed.ts";
import AutoSettleDisabledAt from "./054_ProjectionThreadsAutoSettleDisabledAt.ts";

// Both histories retain their recorded ids. These idempotent additions run above
// the fork's published 57 fence, including when upstream already applied them.
export default Effect.gen(function* () {
  yield* PullRequestFilesViewed;
  yield* AutoSettleDisabledAt;
});
