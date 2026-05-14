// Temporary V2 session projector path: builds session message rows from
// session.next events while storage is still backed by legacy sync projectors.
// EventV2.project(...) returns SyncEvent-shaped projector entries so this file
// can be mixed into the legacy projector list. Remove this file once
// session.next events are the primary session storage model.
import { and, desc, eq } from "@/storage/db"
import type { Database } from "@/storage/db"
import { SessionMessage } from "@opencode-ai/core/session-message"
import { SessionMessageUpdater } from "@opencode-ai/core/session-message-updater"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionEvent } from "@opencode-ai/core/session-event"
import * as DateTime from "effect/DateTime"
import { SessionMessageTable, SessionTable } from "./session.sql"
import type { SessionID } from "./schema"
import { Schema } from "effect"

const decodeMessage = Schema.decodeUnknownSync(SessionMessage.Message)
type SessionMessageData = NonNullable<(typeof SessionMessageTable.$inferInsert)["data"]>

function encodeDateTimes(value: unknown): unknown {
  if (DateTime.isDateTime(value)) return DateTime.toEpochMillis(value)
  if (Array.isArray(value)) return value.map(encodeDateTimes)
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, encodeDateTimes(item)]))
  }
  return value
}

function encodeMessageData(value: unknown): SessionMessageData {
  return encodeDateTimes(value) as SessionMessageData
}

function sqlite(db: Database.TxOrDb, sessionID: SessionID): SessionMessageUpdater.Adapter<void> {
  return {
    getCurrentAssistant() {
      return db
        .select()
        .from(SessionMessageTable)
        .where(and(eq(SessionMessageTable.session_id, sessionID), eq(SessionMessageTable.type, "assistant")))
        .orderBy(desc(SessionMessageTable.id))
        .all()
        .map((row) => decodeMessage({ ...row.data, id: row.id, type: row.type }))
        .find((message): message is SessionMessage.Assistant => message.type === "assistant" && !message.time.completed)
    },
    getCurrentCompaction() {
      return db
        .select()
        .from(SessionMessageTable)
        .where(and(eq(SessionMessageTable.session_id, sessionID), eq(SessionMessageTable.type, "compaction")))
        .orderBy(desc(SessionMessageTable.id))
        .all()
        .map((row) => decodeMessage({ ...row.data, id: row.id, type: row.type }))
        .find((message): message is SessionMessage.Compaction => message.type === "compaction")
    },
    getCurrentShell(callID) {
      return db
        .select()
        .from(SessionMessageTable)
        .where(and(eq(SessionMessageTable.session_id, sessionID), eq(SessionMessageTable.type, "shell")))
        .orderBy(desc(SessionMessageTable.id))
        .all()
        .map((row) => decodeMessage({ ...row.data, id: row.id, type: row.type }))
        .find((message): message is SessionMessage.Shell => message.type === "shell" && message.callID === callID)
    },
    updateAssistant(assistant) {
      const { id, type, ...data } = assistant
      db.update(SessionMessageTable)
        .set({ data: encodeMessageData(data) })
        .where(
          and(
            eq(SessionMessageTable.id, id),
            eq(SessionMessageTable.session_id, sessionID),
            eq(SessionMessageTable.type, type),
          ),
        )
        .run()
    },
    updateCompaction(compaction) {
      const { id, type, ...data } = compaction
      db.update(SessionMessageTable)
        .set({ data: encodeMessageData(data) })
        .where(
          and(
            eq(SessionMessageTable.id, id),
            eq(SessionMessageTable.session_id, sessionID),
            eq(SessionMessageTable.type, type),
          ),
        )
        .run()
    },
    updateShell(shell) {
      const { id, type, ...data } = shell
      db.update(SessionMessageTable)
        .set({ data: encodeMessageData(data) })
        .where(
          and(
            eq(SessionMessageTable.id, id),
            eq(SessionMessageTable.session_id, sessionID),
            eq(SessionMessageTable.type, type),
          ),
        )
        .run()
    },
    appendMessage(message) {
      const { id, type, ...data } = message
      db.insert(SessionMessageTable)
        .values([
          {
            id,
            session_id: sessionID,
            type,
            time_created: DateTime.toEpochMillis(message.time.created),
            data: encodeMessageData(data),
          },
        ])
        .run()
    },
    finish() {},
  }
}

function update(db: Database.TxOrDb, event: SessionEvent.Event) {
  SessionMessageUpdater.update(sqlite(db, event.data.sessionID), event)
}

export default [
  EventV2.project(SessionEvent.AgentSwitched, (db: Database.TxOrDb, data, event) => {
    db.update(SessionTable)
      .set({
        agent: data.agent,
        time_updated: DateTime.toEpochMillis(data.timestamp),
      })
      .where(eq(SessionTable.id, data.sessionID))
      .run()
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.agent.switched", data })
  }),
  EventV2.project(SessionEvent.ModelSwitched, (db: Database.TxOrDb, data, event) => {
    db.update(SessionTable)
      .set({
        model: data.model,
        time_updated: DateTime.toEpochMillis(data.timestamp),
      })
      .where(eq(SessionTable.id, data.sessionID))
      .run()
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.model.switched", data })
  }),
  EventV2.project(SessionEvent.Prompted, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.prompted", data })
  }),
  EventV2.project(SessionEvent.Synthetic, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.synthetic", data })
  }),
  EventV2.project(SessionEvent.Shell.Started, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.shell.started", data })
  }),
  EventV2.project(SessionEvent.Shell.Ended, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.shell.ended", data })
  }),
  EventV2.project(SessionEvent.Step.Started, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.step.started", data })
  }),
  EventV2.project(SessionEvent.Step.Ended, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.step.ended", data })
  }),
  EventV2.project(SessionEvent.Step.Failed, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.step.failed", data })
  }),
  EventV2.project(SessionEvent.Text.Started, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.text.started", data })
  }),
  EventV2.project(SessionEvent.Text.Delta, () => {}),
  EventV2.project(SessionEvent.Text.Ended, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.text.ended", data })
  }),
  EventV2.project(SessionEvent.Tool.Input.Started, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.tool.input.started", data })
  }),
  EventV2.project(SessionEvent.Tool.Input.Delta, () => {}),
  EventV2.project(SessionEvent.Tool.Input.Ended, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.tool.input.ended", data })
  }),
  EventV2.project(SessionEvent.Tool.Called, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.tool.called", data })
  }),
  EventV2.project(SessionEvent.Tool.Success, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.tool.success", data })
  }),
  EventV2.project(SessionEvent.Tool.Failed, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.tool.failed", data })
  }),
  EventV2.project(SessionEvent.Reasoning.Started, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.reasoning.started", data })
  }),
  EventV2.project(SessionEvent.Reasoning.Delta, () => {}),
  EventV2.project(SessionEvent.Reasoning.Ended, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.reasoning.ended", data })
  }),
  EventV2.project(SessionEvent.Retried, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.retried", data })
  }),
  EventV2.project(SessionEvent.Compaction.Started, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.compaction.started", data })
  }),
  EventV2.project(SessionEvent.Compaction.Delta, () => {}),
  EventV2.project(SessionEvent.Compaction.Ended, (db: Database.TxOrDb, data, event) => {
    update(db, { id: SessionMessage.ID.make(event.id), type: "session.next.compaction.ended", data })
  }),
]
