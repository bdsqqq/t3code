import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlSchema from "effect/unstable/sql/SqlSchema";

import { toPersistenceSqlError } from "../Errors.ts";
import {
  PiExternalLifecycleOverride,
  PiExternalLifecycleOverrideRepository,
  type PiExternalLifecycleOverrideRepositoryShape,
} from "../Services/PiExternalLifecycleOverrides.ts";

const makePiExternalLifecycleOverrideRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const upsertRow = SqlSchema.void({
    Request: PiExternalLifecycleOverride,
    execute: (value) => sql`
      INSERT INTO pi_external_lifecycle_overrides (
        source_key,
        command_id,
        lifecycle_override,
        observed_file_size,
        observed_file_mtime_ms,
        updated_at
      )
      VALUES (
        ${value.sourceKey},
        ${value.commandId},
        ${value.lifecycleOverride},
        ${value.observedFileSize},
        ${value.observedFileMtimeMs},
        ${value.updatedAt}
      )
      ON CONFLICT (source_key)
      DO UPDATE SET
        command_id = excluded.command_id,
        lifecycle_override = excluded.lifecycle_override,
        observed_file_size = excluded.observed_file_size,
        observed_file_mtime_ms = excluded.observed_file_mtime_ms,
        updated_at = excluded.updated_at
    `,
  });
  const insertCommandReceipt = SqlSchema.findOneOption({
    Request: PiExternalLifecycleOverride,
    Result: PiExternalLifecycleOverride,
    execute: (value) => sql`
      INSERT INTO pi_external_lifecycle_command_receipts (
        command_id,
        source_key,
        lifecycle_override,
        observed_file_size,
        observed_file_mtime_ms,
        updated_at
      )
      VALUES (
        ${value.commandId},
        ${value.sourceKey},
        ${value.lifecycleOverride},
        ${value.observedFileSize},
        ${value.observedFileMtimeMs},
        ${value.updatedAt}
      )
      ON CONFLICT (command_id) DO NOTHING
      RETURNING
        source_key AS "sourceKey",
        command_id AS "commandId",
        lifecycle_override AS "lifecycleOverride",
        observed_file_size AS "observedFileSize",
        observed_file_mtime_ms AS "observedFileMtimeMs",
        updated_at AS "updatedAt"
    `,
  });
  const getCommandReceipt = SqlSchema.findOne({
    Request: PiExternalLifecycleOverride.fields.commandId,
    Result: PiExternalLifecycleOverride,
    execute: (commandId) => sql`
      SELECT
        source_key AS "sourceKey",
        command_id AS "commandId",
        lifecycle_override AS "lifecycleOverride",
        observed_file_size AS "observedFileSize",
        observed_file_mtime_ms AS "observedFileMtimeMs",
        updated_at AS "updatedAt"
      FROM pi_external_lifecycle_command_receipts
      WHERE command_id = ${commandId}
    `,
  });
  const findCommandReceipt = SqlSchema.findOneOption({
    Request: PiExternalLifecycleOverride.fields.commandId,
    Result: PiExternalLifecycleOverride,
    execute: (commandId) => sql`
      SELECT
        source_key AS "sourceKey",
        command_id AS "commandId",
        lifecycle_override AS "lifecycleOverride",
        observed_file_size AS "observedFileSize",
        observed_file_mtime_ms AS "observedFileMtimeMs",
        updated_at AS "updatedAt"
      FROM pi_external_lifecycle_command_receipts
      WHERE command_id = ${commandId}
    `,
  });

  const listRows = SqlSchema.findAll({
    Request: Schema.Void,
    Result: PiExternalLifecycleOverride,
    execute: () => sql`
      SELECT
        source_key AS "sourceKey",
        command_id AS "commandId",
        lifecycle_override AS "lifecycleOverride",
        observed_file_size AS "observedFileSize",
        observed_file_mtime_ms AS "observedFileMtimeMs",
        updated_at AS "updatedAt"
      FROM pi_external_lifecycle_overrides
    `,
  });
  const getRow = SqlSchema.findOneOption({
    Request: PiExternalLifecycleOverride.fields.sourceKey,
    Result: PiExternalLifecycleOverride,
    execute: (sourceKey) => sql`
      SELECT
        source_key AS "sourceKey",
        command_id AS "commandId",
        lifecycle_override AS "lifecycleOverride",
        observed_file_size AS "observedFileSize",
        observed_file_mtime_ms AS "observedFileMtimeMs",
        updated_at AS "updatedAt"
      FROM pi_external_lifecycle_overrides
      WHERE source_key = ${sourceKey}
    `,
  });
  const persistReceipt = (value: PiExternalLifecycleOverride) =>
    insertCommandReceipt(value).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => getCommandReceipt(value.commandId),
          onSome: (fresh) => Effect.succeed(fresh),
        }),
      ),
    );

  const apply: PiExternalLifecycleOverrideRepositoryShape["apply"] = (value) =>
    sql
      .withTransaction(
        insertCommandReceipt(value).pipe(
          Effect.flatMap((inserted) =>
            Option.match(inserted, {
              onNone: () =>
                getCommandReceipt(value.commandId).pipe(
                  Effect.map((existing) => ({
                    applied: false,
                    value: existing,
                  })),
                ),
              onSome: (fresh) =>
                upsertRow(fresh).pipe(
                  Effect.as({
                    applied: true,
                    value: fresh,
                  }),
                ),
            }),
          ),
        ),
      )
      .pipe(
        Effect.mapError(toPersistenceSqlError("PiExternalLifecycleOverrideRepository.apply:query")),
      );
  const recordReceipt: PiExternalLifecycleOverrideRepositoryShape["recordReceipt"] = (value) =>
    sql
      .withTransaction(persistReceipt(value))
      .pipe(
        Effect.mapError(
          toPersistenceSqlError("PiExternalLifecycleOverrideRepository.recordReceipt:query"),
        ),
      );
  const list: PiExternalLifecycleOverrideRepositoryShape["list"] = () =>
    listRows().pipe(
      Effect.mapError(toPersistenceSqlError("PiExternalLifecycleOverrideRepository.list:query")),
    );
  const getBySourceKey: PiExternalLifecycleOverrideRepositoryShape["getBySourceKey"] = (
    sourceKey,
  ) =>
    getRow(sourceKey).pipe(
      Effect.mapError(
        toPersistenceSqlError("PiExternalLifecycleOverrideRepository.getBySourceKey:query"),
      ),
    );
  const getByCommandId: PiExternalLifecycleOverrideRepositoryShape["getByCommandId"] = (
    commandId,
  ) =>
    findCommandReceipt(commandId).pipe(
      Effect.mapError(
        toPersistenceSqlError("PiExternalLifecycleOverrideRepository.getByCommandId:query"),
      ),
    );

  return {
    apply,
    recordReceipt,
    list,
    getBySourceKey,
    getByCommandId,
  } satisfies PiExternalLifecycleOverrideRepositoryShape;
});

export const PiExternalLifecycleOverrideRepositoryLive = Layer.effect(
  PiExternalLifecycleOverrideRepository,
  makePiExternalLifecycleOverrideRepository,
);
