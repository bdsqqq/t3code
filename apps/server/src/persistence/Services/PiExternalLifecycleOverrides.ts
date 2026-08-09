import { CommandId, IsoDateTime, NonNegativeInt, PiNativeSessionKey } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { PersistenceSqlError } from "../Errors.ts";

export const PiExternalLifecycleOverride = Schema.Struct({
  sourceKey: PiNativeSessionKey,
  commandId: CommandId,
  lifecycleOverride: Schema.Literals(["settled", "active"]),
  observedFileSize: NonNegativeInt,
  observedFileMtimeMs: Schema.Number,
  updatedAt: IsoDateTime,
});
export type PiExternalLifecycleOverride = typeof PiExternalLifecycleOverride.Type;

export interface PiExternalLifecycleOverrideRepositoryShape {
  readonly apply: (value: PiExternalLifecycleOverride) => Effect.Effect<
    {
      readonly applied: boolean;
      readonly value: PiExternalLifecycleOverride;
    },
    PersistenceSqlError
  >;
  readonly recordReceipt: (
    value: PiExternalLifecycleOverride,
  ) => Effect.Effect<PiExternalLifecycleOverride, PersistenceSqlError>;
  readonly list: () => Effect.Effect<
    ReadonlyArray<PiExternalLifecycleOverride>,
    PersistenceSqlError
  >;
  readonly getBySourceKey: (
    sourceKey: PiExternalLifecycleOverride["sourceKey"],
  ) => Effect.Effect<Option.Option<PiExternalLifecycleOverride>, PersistenceSqlError>;
  readonly getByCommandId: (
    commandId: PiExternalLifecycleOverride["commandId"],
  ) => Effect.Effect<Option.Option<PiExternalLifecycleOverride>, PersistenceSqlError>;
}

export class PiExternalLifecycleOverrideRepository extends Context.Service<
  PiExternalLifecycleOverrideRepository,
  PiExternalLifecycleOverrideRepositoryShape
>()("t3/persistence/Services/PiExternalLifecycleOverrides/PiExternalLifecycleOverrideRepository") {}
