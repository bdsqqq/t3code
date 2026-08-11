#!/usr/bin/env node

import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { Command, Flag } from "effect/unstable/cli";

import * as NodeSqliteClient from "../src/persistence/NodeSqliteClient.ts";

const HUMAN_GROUP_LIMIT = 15;

const synchronousModeNames: Readonly<Record<number, string>> = {
  0: "OFF",
  1: "NORMAL",
  2: "FULL",
  3: "EXTRA",
};

const ageBucketLabels: Readonly<Record<string, string>> = {
  future: "future timestamps",
  under_7_days: "under 7 days",
  seven_to_30_days: "7 to 30 days",
  thirty_to_90_days: "30 to 90 days",
  ninety_to_180_days: "90 to 180 days",
  one_eighty_to_365_days: "180 to 365 days",
  over_365_days: "over 365 days",
  unknown: "invalid/unknown timestamps",
};

export class SqliteGrowthDatabaseMissingError extends Schema.TaggedErrorClass<SqliteGrowthDatabaseMissingError>()(
  "SqliteGrowthDatabaseMissingError",
  {
    databasePath: Schema.String,
  },
) {
  override get message(): string {
    return `Database does not exist at '${this.databasePath}'.`;
  }
}

export class SqliteGrowthAnalysisError extends Schema.TaggedErrorClass<SqliteGrowthAnalysisError>()(
  "SqliteGrowthAnalysisError",
  {
    databasePath: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to analyze T3 SQLite database at '${this.databasePath}'.`;
  }
}

interface AggregateFields {
  readonly rows: number;
  readonly payloadUtf8Bytes: number;
  readonly oldestAt: string | null;
  readonly newestAt: string | null;
}

export interface KindGrowth extends AggregateFields {
  readonly kind: string;
}

export interface ThreadGrowth extends AggregateFields {
  readonly threadId: string;
}

export interface AggregateKindGrowth extends AggregateFields {
  readonly aggregateKind: string;
}

export interface AgeGrowth extends AggregateFields {
  readonly bucket: string;
}

export interface SqliteGrowthReport {
  readonly analyzedAt: string;
  readonly database: {
    readonly path: string;
    readonly fileBytes: number;
    readonly walBytes: number | null;
    readonly sqliteRuntimeVersion: string;
    readonly openMode: "read-only";
    readonly journalMode: string;
    readonly synchronousMode: {
      readonly value: number;
      readonly name: string;
    };
    readonly pageCount: number;
    readonly pageSize: number;
    readonly allocatedBytes: number;
    readonly freelistCount: number;
    readonly freelistBytes: number;
  };
  readonly measurement: {
    readonly payloadBytesSql: "length(CAST(payload_json AS BLOB))";
    readonly parsesPayloadJson: false;
  };
  readonly derivedProjection: {
    readonly classification: "derived projection data";
    readonly table: "projection_thread_activities";
    readonly cleanupNote: "rebuildable from canonical orchestration events";
    readonly totals: AggregateFields;
    readonly byThread: ReadonlyArray<ThreadGrowth>;
    readonly byActivityKind: ReadonlyArray<KindGrowth>;
    readonly byAge: ReadonlyArray<AgeGrowth>;
  };
  readonly canonicalEvents: {
    readonly classification: "canonical orchestration events";
    readonly table: "orchestration_events";
    readonly cleanupNote: "source of truth; event deletion is not recommended by this report";
    readonly totals: AggregateFields;
    readonly byThread: ReadonlyArray<ThreadGrowth>;
    readonly byEventKind: ReadonlyArray<KindGrowth>;
    readonly byAggregateKind: ReadonlyArray<AggregateKindGrowth>;
    readonly byAge: ReadonlyArray<AgeGrowth>;
  };
}

interface RunSqliteGrowthInput {
  readonly database: string;
  readonly analyzedAt?: string | undefined;
}

interface SqliteVersionRow {
  readonly sqliteRuntimeVersion: string;
}

interface JournalModeRow {
  readonly journal_mode: string;
}

interface SynchronousModeRow {
  readonly synchronous: number;
}

interface PageCountRow {
  readonly page_count: number;
}

interface PageSizeRow {
  readonly page_size: number;
}

interface FreelistCountRow {
  readonly freelist_count: number;
}

function makeAgeGrowthQuery(table: string, timestampColumn: string): string {
  return `
    WITH bucketed AS (
      SELECT
        CASE
          WHEN julianday(${timestampColumn}) IS NULL THEN 'unknown'
          WHEN input.as_of_jd - julianday(${timestampColumn}) < 0 THEN 'future'
          WHEN input.as_of_jd - julianday(${timestampColumn}) < 7 THEN 'under_7_days'
          WHEN input.as_of_jd - julianday(${timestampColumn}) < 30 THEN 'seven_to_30_days'
          WHEN input.as_of_jd - julianday(${timestampColumn}) < 90 THEN 'thirty_to_90_days'
          WHEN input.as_of_jd - julianday(${timestampColumn}) < 180 THEN 'ninety_to_180_days'
          WHEN input.as_of_jd - julianday(${timestampColumn}) < 365 THEN 'one_eighty_to_365_days'
          ELSE 'over_365_days'
        END AS age_bucket,
        ${timestampColumn} AS timestamp,
        length(CAST(payload_json AS BLOB)) AS payload_bytes
      FROM ${table}
      CROSS JOIN (SELECT julianday(?) AS as_of_jd) AS input
    )
    SELECT
      age_bucket AS bucket,
      COUNT(*) AS rows,
      COALESCE(SUM(payload_bytes), 0) AS payloadUtf8Bytes,
      MIN(timestamp) AS oldestAt,
      MAX(timestamp) AS newestAt
    FROM bucketed
    GROUP BY age_bucket
    ORDER BY CASE age_bucket
      WHEN 'future' THEN 0
      WHEN 'under_7_days' THEN 1
      WHEN 'seven_to_30_days' THEN 2
      WHEN 'thirty_to_90_days' THEN 3
      WHEN 'ninety_to_180_days' THEN 4
      WHEN 'one_eighty_to_365_days' THEN 5
      WHEN 'over_365_days' THEN 6
      ELSE 7
    END
  `;
}

function summarize(groups: ReadonlyArray<AggregateFields>): AggregateFields {
  let rows = 0;
  let payloadUtf8Bytes = 0;
  let oldestAt: string | null = null;
  let newestAt: string | null = null;

  for (const group of groups) {
    rows += group.rows;
    payloadUtf8Bytes += group.payloadUtf8Bytes;
    if (group.oldestAt !== null && (oldestAt === null || group.oldestAt < oldestAt)) {
      oldestAt = group.oldestAt;
    }
    if (group.newestAt !== null && (newestAt === null || group.newestAt > newestAt)) {
      newestAt = group.newestAt;
    }
  }

  return { rows, payloadUtf8Bytes, oldestAt, newestAt };
}

export const runSqliteGrowthAnalysis = Effect.fn("runSqliteGrowthAnalysis")(function* (
  input: RunSqliteGrowthInput,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const databasePath = path.resolve(input.database);
  const analyzedAt = input.analyzedAt ?? DateTime.formatIso(yield* DateTime.now);

  if (!(yield* fs.exists(databasePath))) {
    return yield* new SqliteGrowthDatabaseMissingError({ databasePath });
  }

  return yield* Effect.gen(function* () {
    const databaseInfo = yield* fs.stat(databasePath);
    const walBytes = yield* fs.stat(`${databasePath}-wal`).pipe(
      Effect.map((info) => Number(info.size)),
      Effect.orElseSucceed(() => null),
    );
    const sql = yield* SqlClient.SqlClient;

    const sqliteRuntimeVersion = (yield* sql<SqliteVersionRow>`
        SELECT sqlite_version() AS sqliteRuntimeVersion
      `)[0]!.sqliteRuntimeVersion;
    const journalMode = (yield* sql.unsafe<JournalModeRow>("PRAGMA journal_mode").unprepared)[0]!
      .journal_mode;
    const synchronousMode = (yield* sql.unsafe<SynchronousModeRow>("PRAGMA synchronous")
      .unprepared)[0]!.synchronous;
    const pageCount = (yield* sql.unsafe<PageCountRow>("PRAGMA page_count").unprepared)[0]!
      .page_count;
    const pageSize = (yield* sql.unsafe<PageSizeRow>("PRAGMA page_size").unprepared)[0]!.page_size;
    const freelistCount = (yield* sql.unsafe<FreelistCountRow>("PRAGMA freelist_count")
      .unprepared)[0]!.freelist_count;

    const projectionByThread = yield* sql<ThreadGrowth>`
      SELECT
        thread_id AS threadId,
        COUNT(*) AS rows,
        COALESCE(SUM(length(CAST(payload_json AS BLOB))), 0) AS payloadUtf8Bytes,
        MIN(created_at) AS oldestAt,
        MAX(created_at) AS newestAt
      FROM projection_thread_activities
      GROUP BY thread_id
      ORDER BY payloadUtf8Bytes DESC, rows DESC, threadId ASC
    `;
    const projectionByActivityKind = yield* sql<KindGrowth>`
      SELECT
        kind,
        COUNT(*) AS rows,
        COALESCE(SUM(length(CAST(payload_json AS BLOB))), 0) AS payloadUtf8Bytes,
        MIN(created_at) AS oldestAt,
        MAX(created_at) AS newestAt
      FROM projection_thread_activities
      GROUP BY kind
      ORDER BY payloadUtf8Bytes DESC, rows DESC, kind ASC
    `;
    const projectionByAge = yield* sql.unsafe<AgeGrowth>(
      makeAgeGrowthQuery("projection_thread_activities", "created_at"),
      [analyzedAt],
    ).unprepared;

    const canonicalByThread = yield* sql<ThreadGrowth>`
      SELECT
        stream_id AS threadId,
        COUNT(*) AS rows,
        COALESCE(SUM(length(CAST(payload_json AS BLOB))), 0) AS payloadUtf8Bytes,
        MIN(occurred_at) AS oldestAt,
        MAX(occurred_at) AS newestAt
      FROM orchestration_events
      WHERE aggregate_kind = 'thread'
      GROUP BY stream_id
      ORDER BY payloadUtf8Bytes DESC, rows DESC, threadId ASC
    `;
    const canonicalByEventKind = yield* sql<KindGrowth>`
      SELECT
        event_type AS kind,
        COUNT(*) AS rows,
        COALESCE(SUM(length(CAST(payload_json AS BLOB))), 0) AS payloadUtf8Bytes,
        MIN(occurred_at) AS oldestAt,
        MAX(occurred_at) AS newestAt
      FROM orchestration_events
      GROUP BY event_type
      ORDER BY payloadUtf8Bytes DESC, rows DESC, kind ASC
    `;
    const canonicalByAggregateKind = yield* sql<AggregateKindGrowth>`
      SELECT
        aggregate_kind AS aggregateKind,
        COUNT(*) AS rows,
        COALESCE(SUM(length(CAST(payload_json AS BLOB))), 0) AS payloadUtf8Bytes,
        MIN(occurred_at) AS oldestAt,
        MAX(occurred_at) AS newestAt
      FROM orchestration_events
      GROUP BY aggregate_kind
      ORDER BY payloadUtf8Bytes DESC, rows DESC, aggregateKind ASC
    `;
    const canonicalByAge = yield* sql.unsafe<AgeGrowth>(
      makeAgeGrowthQuery("orchestration_events", "occurred_at"),
      [analyzedAt],
    ).unprepared;

    return {
      analyzedAt,
      database: {
        path: databasePath,
        fileBytes: Number(databaseInfo.size),
        walBytes,
        sqliteRuntimeVersion,
        openMode: "read-only",
        journalMode,
        synchronousMode: {
          value: synchronousMode,
          name: synchronousModeNames[synchronousMode] ?? `UNKNOWN(${synchronousMode})`,
        },
        pageCount,
        pageSize,
        allocatedBytes: pageCount * pageSize,
        freelistCount,
        freelistBytes: freelistCount * pageSize,
      },
      measurement: {
        payloadBytesSql: "length(CAST(payload_json AS BLOB))",
        parsesPayloadJson: false,
      },
      derivedProjection: {
        classification: "derived projection data",
        table: "projection_thread_activities",
        cleanupNote: "rebuildable from canonical orchestration events",
        totals: summarize(projectionByActivityKind),
        byThread: projectionByThread,
        byActivityKind: projectionByActivityKind,
        byAge: projectionByAge,
      },
      canonicalEvents: {
        classification: "canonical orchestration events",
        table: "orchestration_events",
        cleanupNote: "source of truth; event deletion is not recommended by this report",
        totals: summarize(canonicalByEventKind),
        byThread: canonicalByThread,
        byEventKind: canonicalByEventKind,
        byAggregateKind: canonicalByAggregateKind,
        byAge: canonicalByAge,
      },
    } satisfies SqliteGrowthReport;
  }).pipe(
    Effect.provide(NodeSqliteClient.layer({ filename: databasePath, readonly: true })),
    Effect.mapError((cause) => new SqliteGrowthAnalysisError({ databasePath, cause })),
  );
});

function formatBytes(bytes: number | null): string {
  if (bytes === null) {
    return "not present";
  }
  if (bytes < 1024) {
    return `${bytes.toLocaleString("en-US")} B`;
  }

  const units = ["KiB", "MiB", "GiB", "TiB"];
  let value = bytes;
  let unit = "B";
  for (const candidate of units) {
    value /= 1024;
    unit = candidate;
    if (value < 1024) {
      break;
    }
  }
  return `${value.toFixed(value >= 10 ? 1 : 2)} ${unit}`;
}

function formatAggregate(label: string, value: AggregateFields): string {
  return `  ${label}: ${value.rows.toLocaleString("en-US")} rows, ${formatBytes(value.payloadUtf8Bytes)}`;
}

function formatLargestGroups<A extends AggregateFields>(
  heading: string,
  groups: ReadonlyArray<A>,
  label: (group: A) => string,
): ReadonlyArray<string> {
  const visible = groups.slice(0, HUMAN_GROUP_LIMIT);
  const lines = [heading, ...visible.map((group) => formatAggregate(label(group), group))];
  if (groups.length > visible.length) {
    lines.push(`  … ${groups.length - visible.length} more; use --json for all groups`);
  }
  return lines;
}

export function formatSqliteGrowthReport(report: SqliteGrowthReport): string {
  const projection = report.derivedProjection;
  const canonical = report.canonicalEvents;

  return [
    "T3 SQLite growth analysis (read-only)",
    `Database: ${report.database.path}`,
    `File: ${formatBytes(report.database.fileBytes)}; WAL: ${formatBytes(report.database.walBytes)}`,
    `SQLite: ${report.database.sqliteRuntimeVersion}; journal: ${report.database.journalMode}; synchronous: ${report.database.synchronousMode.name} (${report.database.synchronousMode.value})`,
    `Pages: ${report.database.pageCount.toLocaleString("en-US")} × ${formatBytes(report.database.pageSize)} = ${formatBytes(report.database.allocatedBytes)}; freelist: ${report.database.freelistCount.toLocaleString("en-US")} pages (${formatBytes(report.database.freelistBytes)})`,
    "Payload bytes: UTF-8 length(CAST(payload_json AS BLOB)); payload JSON was not parsed.",
    "",
    `DERIVED PROJECTION DATA — ${projection.table}`,
    `  ${projection.cleanupNote}.`,
    formatAggregate("total", projection.totals),
    ...formatLargestGroups(
      `Largest activity kinds (top ${HUMAN_GROUP_LIMIT}):`,
      projection.byActivityKind,
      (group) => group.kind,
    ),
    ...formatLargestGroups(
      `Largest threads (top ${HUMAN_GROUP_LIMIT}):`,
      projection.byThread,
      (group) => group.threadId,
    ),
    "Age buckets:",
    ...projection.byAge.map((group) =>
      formatAggregate(ageBucketLabels[group.bucket] ?? group.bucket, group),
    ),
    "",
    `CANONICAL ORCHESTRATION EVENTS — ${canonical.table}`,
    `  ${canonical.cleanupNote}.`,
    formatAggregate("total", canonical.totals),
    ...formatLargestGroups(
      `Largest event kinds (top ${HUMAN_GROUP_LIMIT}):`,
      canonical.byEventKind,
      (group) => group.kind,
    ),
    ...formatLargestGroups(
      `Largest thread streams (top ${HUMAN_GROUP_LIMIT}; excludes non-thread aggregates):`,
      canonical.byThread,
      (group) => group.threadId,
    ),
    "Aggregate kinds:",
    ...canonical.byAggregateKind.map((group) => formatAggregate(group.aggregateKind, group)),
    "Age buckets:",
    ...canonical.byAge.map((group) =>
      formatAggregate(ageBucketLabels[group.bucket] ?? group.bucket, group),
    ),
  ].join("\n");
}

export function formatSqliteGrowthReportJson(report: SqliteGrowthReport): string {
  return JSON.stringify(report, null, 2);
}

export const t3SqliteGrowthCommand = Command.make(
  "t3-sqlite-growth",
  {
    database: Flag.string("database").pipe(
      Flag.withDescription("Explicit path to a T3 state.sqlite database."),
    ),
    json: Flag.boolean("json").pipe(
      Flag.withDescription("Emit the complete machine-readable report as JSON."),
      Flag.withDefault(false),
    ),
  },
  ({ database, json }) =>
    runSqliteGrowthAnalysis({ database }).pipe(
      Effect.flatMap((report) =>
        Console.log(json ? formatSqliteGrowthReportJson(report) : formatSqliteGrowthReport(report)),
      ),
    ),
).pipe(
  Command.withDescription(
    "Analyze T3 SQLite payload growth using grouped queries on a strictly read-only connection.",
  ),
);

if (import.meta.main) {
  Command.run(t3SqliteGrowthCommand, { version: "0.0.0" }).pipe(
    Effect.provide(NodeServices.layer),
    NodeRuntime.runMain,
  );
}
