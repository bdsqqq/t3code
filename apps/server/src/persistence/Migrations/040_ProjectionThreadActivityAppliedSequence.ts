import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Existing activities predate every cursor issued after this migration.
  yield* sql`
    ALTER TABLE projection_thread_activities
    ADD COLUMN applied_sequence INTEGER NOT NULL DEFAULT 0
  `;
});
