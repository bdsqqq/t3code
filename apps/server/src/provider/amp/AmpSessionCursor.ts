import * as Schema from "effect/Schema";

export const AmpSessionCursor = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  threadId: Schema.String.check(Schema.isPattern(/^T-[A-Za-z0-9_-]+$/)),
});
export type AmpSessionCursor = typeof AmpSessionCursor.Type;
