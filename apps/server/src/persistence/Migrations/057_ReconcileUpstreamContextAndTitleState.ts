import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import Migration0051 from "./051_ProjectionThreadMessageContext.ts";
import { createPiLifecycleTables } from "./052_PiExternalLifecycleOverrides.ts";

export default Effect.gen(function* () {
  // Upstream and fork releases assigned different schemas to ids 51–52.
  // Replay behind the fork's latest fence so either history gains both schemas.
  yield* createPiLifecycleTables;
  yield* Migration0051;
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`PRAGMA table_info(projection_threads)`;
  if (!columns.some((column) => column.name === "title_state_json")) {
    yield* sql`ALTER TABLE projection_threads ADD COLUMN title_state_json TEXT`;
  }
});
