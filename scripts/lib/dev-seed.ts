/**
 * Copies recent projects and threads from one T3 Code database into another, so
 * an isolated dev server opens on something recognisable instead of an empty
 * sidebar.
 *
 * Projections only — never `orchestration_events`. The projector cursor is
 * exclusive (`WHERE sequence > cursor`), so an empty event log means bootstrap
 * streams nothing and leaves the copied rows alone. Copying a *partial* event
 * range is the actual hazard: the projector would replay a tail whose creating
 * events are missing. See .agents/skills/test-t3-app/references/sqlite-fixtures.md.
 */

import * as NodeSqlite from "node:sqlite";

/** Must match ORCHESTRATION_PROJECTOR_NAMES in apps/server/src/orchestration/Layers/ProjectionPipeline.ts. */
const PROJECTOR_NAMES = [
  "projection.projects",
  "projection.threads",
  "projection.thread-messages",
  "projection.thread-proposed-plans",
  "projection.thread-activities",
  "projection.thread-sessions",
  "projection.thread-turns",
  "projection.checkpoints",
  "projection.pending-approvals",
] as const;

/** Deleted in this order so a row never outlives what it points at. */
const TABLES_IN_DEPENDENCY_ORDER = [
  "projection_pending_approvals",
  "projection_thread_proposed_plans",
  "projection_thread_activities",
  "projection_thread_messages",
  "projection_thread_sessions",
  "projection_turns",
  "projection_threads",
  "projection_projects",
  "projection_state",
] as const;

export interface DevSeedOptions {
  readonly sourceDbPath: string;
  readonly targetDbPath: string;
  /** How many recent threads to copy. */
  readonly threadLimit: number;
  /**
   * Newest activities kept per thread. The real table runs to six figures, and
   * the tail is what makes a thread look alive, so a cap keeps the copy quick
   * without making it look empty.
   */
  readonly activityLimit: number;
  /** ISO-8601 timestamp stamped on the projector cursor rows. */
  readonly seededAt: string;
}

export interface DevSeedSummary {
  readonly projects: number;
  readonly threads: number;
  readonly messages: number;
  readonly activities: number;
  readonly turns: number;
  readonly sessions: number;
  readonly skippedColumns: ReadonlyArray<string>;
}

export class DevSeedError extends Error {
  override readonly name = "DevSeedError";
  readonly hint: string | undefined;
  constructor(message: string, hint?: string) {
    super(message);
    this.hint = hint;
  }
}

const columnsOf = (database: NodeSqlite.DatabaseSync, table: string): ReadonlyArray<string> =>
  database
    .prepare(`SELECT name FROM pragma_table_info(?)`)
    .all(table)
    .map((row) => String((row as { name: unknown }).name));

/**
 * Columns present in both databases. The two can sit on different migrations —
 * a dev worktree is often a migration behind or ahead of the installed app — so
 * `SELECT *` would fail on the first schema change. Copying the intersection
 * degrades gracefully instead: a column only the target knows about keeps its
 * default.
 */
function sharedColumns(
  source: NodeSqlite.DatabaseSync,
  target: NodeSqlite.DatabaseSync,
  table: string,
): { readonly shared: ReadonlyArray<string>; readonly skipped: ReadonlyArray<string> } {
  const sourceColumns = columnsOf(source, table);
  const targetColumns = new Set(columnsOf(target, table));
  const shared = sourceColumns.filter((column) => targetColumns.has(column));
  const skipped = sourceColumns
    .filter((column) => !targetColumns.has(column))
    .map((column) => `${table}.${column}`);
  return { shared, skipped };
}

const placeholders = (count: number) => Array.from({ length: count }, () => "?").join(", ");

const quote = (values: ReadonlyArray<string>) => values.map((value) => `'${value}'`).join(", ");

/**
 * Copies rows for `table` whose `keyColumn` is in `keys`, optionally keeping
 * only the newest `perKeyLimit` rows per key.
 */
function copyRows(input: {
  readonly source: NodeSqlite.DatabaseSync;
  readonly target: NodeSqlite.DatabaseSync;
  readonly table: string;
  readonly keyColumn: string;
  readonly keys: ReadonlyArray<string>;
  readonly omitColumns?: ReadonlyArray<string>;
  readonly perKeyLimit?: { readonly orderBy: string; readonly limit: number };
  readonly overrides?: Readonly<Record<string, unknown>>;
}): { readonly copied: number; readonly skipped: ReadonlyArray<string> } {
  if (input.keys.length === 0) {
    return { copied: 0, skipped: [] };
  }

  const { shared, skipped } = sharedColumns(input.source, input.target, input.table);
  const omit = new Set(input.omitColumns ?? []);
  const columns = shared.filter((column) => !omit.has(column));
  if (columns.length === 0) {
    return { copied: 0, skipped };
  }

  const selectList = columns.map((column) => `"${column}"`).join(", ");
  const rows: Array<Record<string, unknown>> = [];

  if (input.perKeyLimit) {
    // Per-key cap: one bounded query per key beats a window function, and keeps
    // this working on any SQLite build.
    const statement = input.source.prepare(
      `SELECT ${selectList} FROM ${input.table} WHERE "${input.keyColumn}" = ?
       ORDER BY ${input.perKeyLimit.orderBy} DESC LIMIT ?`,
    );
    for (const key of input.keys) {
      rows.push(...(statement.all(key, input.perKeyLimit.limit) as Array<Record<string, unknown>>));
    }
  } else {
    rows.push(
      ...(input.source
        .prepare(
          `SELECT ${selectList} FROM ${input.table} WHERE "${input.keyColumn}" IN (${quote(input.keys)})`,
        )
        .all() as Array<Record<string, unknown>>),
    );
  }

  if (rows.length === 0) {
    return { copied: 0, skipped };
  }

  const insert = input.target.prepare(
    `INSERT OR REPLACE INTO ${input.table} (${selectList}) VALUES (${placeholders(columns.length)})`,
  );
  for (const row of rows) {
    insert.run(
      ...columns.map((column) => {
        const value = Object.hasOwn(input.overrides ?? {}, column)
          ? (input.overrides ?? {})[column]
          : row[column];
        // node:sqlite binds only null/number/bigint/string/Uint8Array; every
        // projection column is one of those, and undefined means "absent".
        return (value ?? null) as null | number | bigint | string | Uint8Array;
      }),
    );
  }

  return { copied: rows.length, skipped };
}

export function seedDevDatabase(options: DevSeedOptions): DevSeedSummary {
  let source: NodeSqlite.DatabaseSync;
  try {
    source = new NodeSqlite.DatabaseSync(options.sourceDbPath, { readOnly: true });
  } catch (cause) {
    throw new DevSeedError(
      `could not open the source database at ${options.sourceDbPath}`,
      `${String(cause)}. Has T3 Code run at least once?`,
    );
  }

  let target: NodeSqlite.DatabaseSync;
  try {
    target = new NodeSqlite.DatabaseSync(options.targetDbPath);
  } catch (cause) {
    source.close();
    throw new DevSeedError(
      `could not open the target database at ${options.targetDbPath}`,
      `${String(cause)}. Start the dev server once so migrations run, then retry.`,
    );
  }

  try {
    // Threads the user actually touched most recently. Mirrors the sidebar's own
    // ordering (packages/client-runtime/src/state/threadSort.ts).
    const threadIds = (
      source
        .prepare(
          `SELECT thread_id FROM projection_threads
           WHERE deleted_at IS NULL AND archived_at IS NULL
           ORDER BY COALESCE(latest_user_message_at, updated_at, created_at) DESC
           LIMIT ?`,
        )
        .all(options.threadLimit) as Array<{ thread_id: string }>
    ).map((row) => row.thread_id);

    if (threadIds.length === 0) {
      throw new DevSeedError(
        "the source database has no active threads to copy",
        "Use T3 Code normally first, or point --from at a different data directory.",
      );
    }

    const projectIds = (
      source
        .prepare(
          `SELECT DISTINCT project_id FROM projection_threads
           WHERE thread_id IN (${quote(threadIds)})`,
        )
        .all() as Array<{ project_id: string }>
    ).map((row) => row.project_id);

    const skipped: Array<string> = [];
    const record = (result: {
      readonly copied: number;
      readonly skipped: ReadonlyArray<string>;
    }) => {
      skipped.push(...result.skipped);
      return result.copied;
    };

    target.exec("BEGIN IMMEDIATE");

    for (const table of TABLES_IN_DEPENDENCY_ORDER) {
      target.exec(`DELETE FROM ${table}`);
    }

    const projects = record(
      copyRows({
        source,
        target,
        table: "projection_projects",
        keyColumn: "project_id",
        keys: projectIds,
      }),
    );
    const threads = record(
      copyRows({
        source,
        target,
        table: "projection_threads",
        keyColumn: "thread_id",
        keys: threadIds,
        // Approvals are not copied (see below), so the badge must not claim any.
        overrides: { pending_approval_count: 0, pending_user_input_count: 0 },
      }),
    );
    // row_id is an AUTOINCREMENT surrogate; let the target assign its own.
    const turns = record(
      copyRows({
        source,
        target,
        table: "projection_turns",
        keyColumn: "thread_id",
        keys: threadIds,
        omitColumns: ["row_id"],
      }),
    );
    const messages = record(
      copyRows({
        source,
        target,
        table: "projection_thread_messages",
        keyColumn: "thread_id",
        keys: threadIds,
      }),
    );
    const activities = record(
      copyRows({
        source,
        target,
        table: "projection_thread_activities",
        keyColumn: "thread_id",
        keys: threadIds,
        perKeyLimit: { orderBy: "created_at", limit: options.activityLimit },
      }),
    );
    const sessions = record(
      copyRows({
        source,
        target,
        table: "projection_thread_sessions",
        keyColumn: "thread_id",
        keys: threadIds,
        // No agent process is attached in the copy. A carried-over "running"
        // status with an active turn renders a thread that spins forever, and
        // ProviderSessionReaper skips reaping anything with an active turn.
        overrides: { status: "stopped", active_turn_id: null, last_error: null },
      }),
    );
    record(
      copyRows({
        source,
        target,
        table: "projection_thread_proposed_plans",
        keyColumn: "thread_id",
        keys: threadIds,
      }),
    );
    // projection_pending_approvals is deliberately skipped: migration 025 deletes
    // approvals with no matching `approval.requested` activity, and the activity
    // cap above can easily drop it.

    // Required: computeSnapshotSequence returns 0 unless every projector has a
    // row, which makes every shell snapshot advertise sequence 0.
    const insertState = target.prepare(
      `INSERT OR REPLACE INTO projection_state (projector, last_applied_sequence, updated_at)
       VALUES (?, ?, ?)`,
    );
    for (const [index, projector] of PROJECTOR_NAMES.entries()) {
      insertState.run(projector, index + 1, options.seededAt);
    }

    target.exec("COMMIT");

    return {
      projects,
      threads,
      messages,
      activities,
      turns,
      sessions,
      skippedColumns: [...new Set(skipped)].sort(),
    };
  } catch (cause) {
    try {
      target.exec("ROLLBACK");
    } catch {
      // Already rolled back, or the transaction never opened.
    }
    throw cause instanceof DevSeedError
      ? cause
      : new DevSeedError(`could not seed the dev database: ${String(cause)}`);
  } finally {
    source.close();
    target.close();
  }
}
