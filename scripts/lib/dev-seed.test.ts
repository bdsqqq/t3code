// @effect-diagnostics nodeBuiltinImport:off - test fixtures build real SQLite files on disk.
import { assert, describe, it } from "@effect/vitest";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import { DevSeedError, seedDevDatabase } from "./dev-seed.ts";

const SEEDED_AT = "2026-07-26T00:00:00.000Z";

/**
 * The subset of the real schema the seeder touches. `monitor_json` is included
 * only in the source, to stand in for the migration drift between an installed
 * app and a worktree that is a migration behind.
 */
function createSchema(
  database: NodeSqlite.DatabaseSync,
  options: { readonly withMonitor: boolean },
) {
  database.exec(`CREATE TABLE projection_projects (
    project_id TEXT PRIMARY KEY, title TEXT NOT NULL, workspace_root TEXT NOT NULL,
    scripts_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, deleted_at TEXT)`);
  database.exec(`CREATE TABLE projection_threads (
    thread_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, title TEXT NOT NULL,
    latest_turn_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    deleted_at TEXT, archived_at TEXT, latest_user_message_at TEXT,
    pending_approval_count INTEGER NOT NULL DEFAULT 0,
    pending_user_input_count INTEGER NOT NULL DEFAULT 0
    ${options.withMonitor ? ", monitor_json TEXT" : ""})`);
  database.exec(`CREATE TABLE projection_thread_messages (
    message_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, role TEXT NOT NULL,
    text TEXT NOT NULL, is_streaming INTEGER NOT NULL,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`);
  database.exec(`CREATE TABLE projection_thread_activities (
    activity_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, tone TEXT NOT NULL,
    kind TEXT NOT NULL, summary TEXT NOT NULL, payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL)`);
  database.exec(`CREATE TABLE projection_thread_sessions (
    thread_id TEXT PRIMARY KEY, status TEXT NOT NULL, provider_name TEXT,
    active_turn_id TEXT, last_error TEXT, updated_at TEXT NOT NULL)`);
  database.exec(`CREATE TABLE projection_turns (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT, thread_id TEXT NOT NULL, turn_id TEXT,
    state TEXT NOT NULL, requested_at TEXT NOT NULL, checkpoint_files_json TEXT NOT NULL,
    UNIQUE (thread_id, turn_id))`);
  database.exec(`CREATE TABLE projection_thread_proposed_plans (
    plan_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, plan_markdown TEXT NOT NULL,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL)`);
  database.exec(`CREATE TABLE projection_pending_approvals (
    request_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, status TEXT NOT NULL,
    created_at TEXT NOT NULL)`);
  database.exec(`CREATE TABLE projection_state (
    projector TEXT PRIMARY KEY, last_applied_sequence INTEGER NOT NULL, updated_at TEXT NOT NULL)`);
}

/** Source DB with `threadCount` threads, oldest first so recency ordering is testable. */
function makeSource(path: string, threadCount: number, activitiesPerThread = 3) {
  const database = new NodeSqlite.DatabaseSync(path);
  createSchema(database, { withMonitor: true });
  database
    .prepare(
      `INSERT INTO projection_projects VALUES ('p1','Project','/repo','[]','${SEEDED_AT}','${SEEDED_AT}',NULL)`,
    )
    .run();

  for (let index = 0; index < threadCount; index += 1) {
    const threadId = `t${String(index)}`;
    // Later index → later timestamp → more recent.
    const at = `2026-07-${String(10 + index).padStart(2, "0")}T00:00:00.000Z`;
    database
      .prepare(
        `INSERT INTO projection_threads (thread_id, project_id, title, latest_turn_id,
         created_at, updated_at, latest_user_message_at, pending_approval_count,
         pending_user_input_count, monitor_json)
         VALUES (?,?,?,?,?,?,?,?,?,?)`,
      )
      .run(threadId, "p1", `Thread ${String(index)}`, `turn-${threadId}`, at, at, at, 4, 2, "{}");
    database
      .prepare(
        `INSERT INTO projection_turns (thread_id, turn_id, state, requested_at, checkpoint_files_json)
         VALUES (?,?,?,?,'[]')`,
      )
      .run(threadId, `turn-${threadId}`, "completed", at);
    database
      .prepare(`INSERT INTO projection_thread_sessions VALUES (?,'running','claude',?,'boom',?)`)
      .run(threadId, `turn-${threadId}`, at);
    database
      .prepare(`INSERT INTO projection_thread_messages VALUES (?,?, 'user','hello',0,?,?)`)
      .run(`m-${threadId}`, threadId, at, at);
    for (let a = 0; a < activitiesPerThread; a += 1) {
      database
        .prepare(`INSERT INTO projection_thread_activities VALUES (?,?,'neutral','tool','ran',?,?)`)
        .run(`a-${threadId}-${String(a)}`, threadId, "{}", `2026-07-10T00:00:0${String(a)}.000Z`);
    }
  }
  database.close();
}

function makeTarget(path: string) {
  const database = new NodeSqlite.DatabaseSync(path);
  // No monitor_json: the target is a migration behind, as a worktree often is.
  createSchema(database, { withMonitor: false });
  database
    .prepare(
      `INSERT INTO projection_projects VALUES ('stale','Stale','/old','[]','${SEEDED_AT}','${SEEDED_AT}',NULL)`,
    )
    .run();
  database.close();
}

const withDatabases = <A>(
  run: (paths: { readonly source: string; readonly target: string }) => A,
  options?: { readonly threads?: number; readonly activities?: number },
): A => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-dev-seed-"));
  const source = NodePath.join(directory, "source.sqlite");
  const target = NodePath.join(directory, "target.sqlite");
  makeSource(source, options?.threads ?? 5, options?.activities ?? 3);
  makeTarget(target);
  try {
    return run({ source, target });
  } finally {
    NodeFS.rmSync(directory, { recursive: true, force: true });
  }
};

const query = <T>(path: string, sql: string): Array<T> => {
  const database = new NodeSqlite.DatabaseSync(path, { readOnly: true });
  try {
    return database.prepare(sql).all() as Array<T>;
  } finally {
    database.close();
  }
};

describe("seedDevDatabase", () => {
  it("copies the most recent threads and their project", () => {
    withDatabases(({ source, target }) => {
      const summary = seedDevDatabase({
        sourceDbPath: source,
        targetDbPath: target,
        threadLimit: 2,
        activityLimit: 10,
        seededAt: SEEDED_AT,
      });

      assert.equal(summary.threads, 2);
      assert.equal(summary.projects, 1);

      const titles = query<{ title: string }>(
        target,
        "SELECT title FROM projection_threads ORDER BY latest_user_message_at DESC",
      ).map((row) => row.title);
      // Threads 4 and 3 are the newest of the five.
      assert.deepStrictEqual(titles, ["Thread 4", "Thread 3"]);
    });
  });

  it("replaces whatever the target held before", () => {
    withDatabases(({ source, target }) => {
      seedDevDatabase({
        sourceDbPath: source,
        targetDbPath: target,
        threadLimit: 1,
        activityLimit: 10,
        seededAt: SEEDED_AT,
      });

      const projects = query<{ project_id: string }>(
        target,
        "SELECT project_id FROM projection_projects",
      ).map((row) => row.project_id);
      assert.deepStrictEqual(projects, ["p1"]);
    });
  });

  // The target can be a migration behind the source; copying its columns
  // blindly would fail on the first schema change.
  it("skips columns the target does not have", () => {
    withDatabases(({ source, target }) => {
      const summary = seedDevDatabase({
        sourceDbPath: source,
        targetDbPath: target,
        threadLimit: 1,
        activityLimit: 10,
        seededAt: SEEDED_AT,
      });

      assert.deepStrictEqual(summary.skippedColumns, ["projection_threads.monitor_json"]);
      assert.equal(summary.threads, 1);
    });
  });

  // A copied "running" session has no agent behind it, so the thread would spin
  // forever and the session reaper would skip it.
  it("neutralizes live session state", () => {
    withDatabases(({ source, target }) => {
      seedDevDatabase({
        sourceDbPath: source,
        targetDbPath: target,
        threadLimit: 3,
        activityLimit: 10,
        seededAt: SEEDED_AT,
      });

      const sessions = query<{ status: string; active_turn_id: string | null }>(
        target,
        "SELECT status, active_turn_id FROM projection_thread_sessions",
      );
      assert.isAbove(sessions.length, 0);
      for (const session of sessions) {
        assert.equal(session.status, "stopped");
        assert.isNull(session.active_turn_id);
      }
    });
  });

  // Approvals are not copied, so the badge counts must not survive.
  it("clears pending counts that have no rows behind them", () => {
    withDatabases(({ source, target }) => {
      seedDevDatabase({
        sourceDbPath: source,
        targetDbPath: target,
        threadLimit: 3,
        activityLimit: 10,
        seededAt: SEEDED_AT,
      });

      const [counts] = query<{ approvals: number; inputs: number }>(
        target,
        "SELECT SUM(pending_approval_count) approvals, SUM(pending_user_input_count) inputs FROM projection_threads",
      );
      assert.equal(counts?.approvals, 0);
      assert.equal(counts?.inputs, 0);
    });
  });

  it("caps activities per thread, keeping the newest", () => {
    withDatabases(
      ({ source, target }) => {
        const summary = seedDevDatabase({
          sourceDbPath: source,
          targetDbPath: target,
          threadLimit: 2,
          activityLimit: 2,
          seededAt: SEEDED_AT,
        });

        assert.equal(summary.activities, 4); // 2 threads × 2 kept
        const ids = query<{ activity_id: string }>(
          target,
          "SELECT activity_id FROM projection_thread_activities ORDER BY activity_id",
        ).map((row) => row.activity_id);
        // Of a-*-0/1/2, the newest two are 1 and 2.
        assert.deepStrictEqual(ids, ["a-t3-1", "a-t3-2", "a-t4-1", "a-t4-2"]);
      },
      { activities: 3 },
    );
  });

  // Required, or computeSnapshotSequence reports 0 for every shell snapshot.
  it("writes a cursor row for every projector", () => {
    withDatabases(({ source, target }) => {
      seedDevDatabase({
        sourceDbPath: source,
        targetDbPath: target,
        threadLimit: 1,
        activityLimit: 10,
        seededAt: SEEDED_AT,
      });

      const rows = query<{ projector: string; last_applied_sequence: number }>(
        target,
        "SELECT projector, last_applied_sequence FROM projection_state ORDER BY last_applied_sequence",
      );
      assert.equal(rows.length, 9);
      assert.equal(rows[0]?.last_applied_sequence, 1);
      assert.equal(rows[8]?.last_applied_sequence, 9);
    });
  });

  it("reports a source with nothing to copy", () => {
    withDatabases(
      ({ source, target }) => {
        assert.throws(
          () =>
            seedDevDatabase({
              sourceDbPath: source,
              targetDbPath: target,
              threadLimit: 5,
              activityLimit: 10,
              seededAt: SEEDED_AT,
            }),
          DevSeedError,
        );
      },
      { threads: 0 },
    );
  });

  it("leaves the target untouched when the source cannot be opened", () => {
    withDatabases(({ target }) => {
      assert.throws(
        () =>
          seedDevDatabase({
            sourceDbPath: NodePath.join(NodePath.dirname(target), "missing.sqlite"),
            targetDbPath: target,
            threadLimit: 5,
            activityLimit: 10,
            seededAt: SEEDED_AT,
          }),
        DevSeedError,
      );

      // The pre-existing row survives: nothing was deleted before the failure.
      const projects = query<{ project_id: string }>(
        target,
        "SELECT project_id FROM projection_projects",
      );
      assert.deepStrictEqual(
        projects.map((row) => row.project_id),
        ["stale"],
      );
    });
  });
});
