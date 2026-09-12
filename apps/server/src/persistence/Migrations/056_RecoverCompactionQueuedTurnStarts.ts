import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Old projectors advanced past accepted compaction-queued starts without storing
 * them. Repair only those placeholders: replaying the turns projector against
 * today's session projection would change historical turn completion states.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* sql<{
    sequence: number;
    threadId: string;
    type: string;
    commandId: string | null;
    messageId: string | null;
    compact: number;
    status: string | null;
    turnId: string | null;
    activityKind: string | null;
    requestId: string | null;
  }>`
    SELECT sequence, stream_id AS "threadId", event_type AS type,
      command_id AS "commandId",
      json_extract(payload_json, '$.messageId') AS "messageId",
      CASE WHEN json_extract(payload_json, '$.role') = 'user'
        AND lower(trim(json_extract(payload_json, '$.text'))) = '/compact'
        AND json_array_length(COALESCE(json_extract(payload_json, '$.attachments'), '[]')) = 0
        THEN 1 ELSE 0 END AS compact,
      json_extract(payload_json, '$.session.status') AS status,
      json_extract(payload_json, '$.session.activeTurnId') AS "turnId",
      json_extract(payload_json, '$.activity.kind') AS "activityKind",
      json_extract(payload_json, '$.activity.payload.requestId') AS "requestId"
    FROM orchestration_events
    WHERE aggregate_kind = 'thread'
      AND sequence <= COALESCE((
        SELECT last_applied_sequence FROM projection_state
        WHERE projector = 'projection.thread-turns'
      ), 0)
      AND event_type IN (
        'thread.created', 'thread.deleted', 'thread.message-sent',
        'thread.turn-start-requested', 'thread.session-set', 'thread.activity-appended',
        'thread.turn-interrupt-requested', 'thread.session-stop-requested', 'thread.reverted'
      )
    ORDER BY sequence
  `;
  type Accepted = { sequence: number; threadId: string; messageId: string };
  const threads = new Map<
    string,
    {
      compactMessages: Set<string>;
      compact: string | null;
      replayed: Array<string>;
      runningTurns: Set<string>;
      queued: Map<string, Accepted>;
    }
  >();
  for (const event of events) {
    let thread = threads.get(event.threadId);
    if (!thread || event.type === "thread.created") {
      thread = {
        compactMessages: new Set(),
        compact: null,
        replayed: [],
        runningTurns: new Set(),
        queued: new Map(),
      };
      threads.set(event.threadId, thread);
    }
    switch (event.type) {
      case "thread.message-sent":
        if (event.compact && event.messageId) thread.compactMessages.add(event.messageId);
        break;
      case "thread.turn-start-requested":
        if (!event.messageId) break;
        if (event.commandId?.startsWith("server:after-compaction:")) {
          if (thread.queued.has(event.messageId) && !thread.replayed.includes(event.messageId)) {
            thread.replayed.push(event.messageId);
          }
        } else if (thread.compact !== null || thread.queued.size > 0) {
          thread.queued.set(event.messageId, {
            sequence: event.sequence,
            threadId: event.threadId,
            messageId: event.messageId,
          });
        } else if (thread.compactMessages.has(event.messageId)) {
          thread.compact = event.messageId;
        }
        break;
      case "thread.activity-appended":
        if (event.activityKind === "provider.turn.start.accepted" && event.requestId) {
          thread.queued.delete(event.requestId);
          thread.replayed = thread.replayed.filter((messageId) => messageId !== event.requestId);
        }
        if (event.activityKind === "context-compaction" && event.requestId === thread.compact) {
          thread.compact = null;
        }
        if (event.activityKind === "provider.turn.start.failed" && event.requestId) {
          if (thread.compact === event.requestId) {
            thread.compact = null;
            thread.replayed.length = 0;
            thread.queued.clear();
          } else {
            thread.queued.delete(event.requestId);
            thread.replayed = thread.replayed.filter((messageId) => messageId !== event.requestId);
          }
        }
        break;
      case "thread.session-set":
        if (event.status === "running" && event.turnId) {
          if (!thread.runningTurns.has(event.turnId)) {
            const messageId = thread.replayed.shift();
            if (messageId !== undefined) thread.queued.delete(messageId);
          }
          thread.runningTurns.add(event.turnId);
        }
        if (
          event.status === "ready" &&
          event.commandId?.startsWith("server:provider-session-set:")
        ) {
          thread.compact = null;
        }
        if (!["error", "stopped", "interrupted"].includes(event.status ?? "")) break;
      // Fall through: explicit cancellation must not resurrect accepted work.
      case "thread.turn-interrupt-requested":
      case "thread.session-stop-requested":
      case "thread.reverted":
      case "thread.deleted":
        thread.compact = null;
        thread.replayed.length = 0;
        thread.queued.clear();
        break;
    }
  }
  const outstanding = [...threads.values()]
    .flatMap((thread) => [...thread.queued.values()])
    .sort((left, right) => left.sequence - right.sequence);
  for (const request of outstanding) {
    // Concrete attribution is stronger delivery evidence than a placeholder.
    const delivered = yield* sql`
      SELECT 1 FROM projection_turns WHERE thread_id = ${request.threadId}
        AND pending_message_id = ${request.messageId} AND turn_id IS NOT NULL LIMIT 1
    `;
    // Reinsert only this recovered queue in event order, including synthetic
    // placeholders whose newer row ids would otherwise reverse the FIFO.
    yield* sql`
      DELETE FROM projection_turns WHERE thread_id = ${request.threadId}
        AND pending_message_id = ${request.messageId} AND turn_id IS NULL
        AND state IN ('pending', 'queued') AND checkpoint_turn_count IS NULL
    `;
    if (delivered.length > 0) continue;
    yield* sql`
      INSERT INTO projection_turns (
        thread_id, pending_message_id, pending_operation_id, pending_model_selection_json,
        pending_title_seed, pending_interaction_mode, pending_admission_protocol,
        source_proposed_plan_thread_id, source_proposed_plan_id, state, requested_at,
        checkpoint_files_json
      )
      SELECT stream_id, json_extract(payload_json, '$.messageId'), command_id,
        json_extract(payload_json, '$.modelSelection'), json_extract(payload_json, '$.titleSeed'),
        json_extract(payload_json, '$.interactionMode'),
        json_extract(payload_json, '$.admissionProtocol'),
        json_extract(payload_json, '$.sourceProposedPlan.threadId'),
        json_extract(payload_json, '$.sourceProposedPlan.planId'),
        'queued', COALESCE(json_extract(payload_json, '$.createdAt'), occurred_at), '[]'
      FROM orchestration_events WHERE sequence = ${request.sequence}
    `;
  }
});
