// Temporary V2 bridge: core events are the publish path, but the rest of
// opencode and the HTTP event stream still expect legacy bus/sync payloads.
// This layer goes away once consumers subscribe to core EventV2 directly and
// sync persistence/projectors are no longer used for session.next events.
import { Bus as ProjectBus } from "@/bus"
import { GlobalBus } from "@/bus/global"
import { Event } from "@opencode-ai/core/event"
import "@opencode-ai/core/catalog"
import "@opencode-ai/core/session-event"
import { Effect, Layer, Stream } from "effect"

function emitNormal(event: Event.Payload) {
  GlobalBus.emit("event", {
    directory: event.location?.directory,
    workspace: event.location?.workspaceID,
    payload: {
      id: event.id,
      type: event.type,
      properties: event.data,
    },
  })
}

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const events = yield* Event.Service
    const bus = yield* ProjectBus.Service

    yield* events.subscribeAll().pipe(Stream.runForEach(republish(bus)), Effect.forkScoped)
  }),
)

export const defaultLayer: Layer.Layer<never> = layer.pipe(
  Layer.provideMerge(Event.defaultLayer),
  Layer.provide(ProjectBus.defaultLayer),
) as unknown as Layer.Layer<never>

const republish = (bus: ProjectBus.Interface) => (event: Event.Payload) => {
  const definition = Event.registry.get(event.type)
  if (!definition) return Effect.void

  const publishNormal = bus.publish({ type: definition.type, properties: definition.schema }, event.data, { id: event.id }).pipe(
    Effect.catch(() => Effect.sync(() => emitNormal(event))),
  )
  if (definition.version === undefined) return publishNormal

  return Effect.gen(function* () {
    yield* publishNormal
    yield* Effect.sync(() => {
      GlobalBus.emit("event", {
        directory: event.location?.directory,
        workspace: event.location?.workspaceID,
        payload: {
          type: "sync",
          name: `${definition.type}.${definition.version}`,
          id: event.id,
          seq: 0,
          aggregateID: aggregateID(definition, event),
          data: event.data,
        },
      })
    })
  })
}

function aggregateID(definition: Event.Definition, event: Event.Payload) {
  if (!definition.aggregate) return event.id
  const value = (event.data as Record<string, unknown>)[definition.aggregate]
  return typeof value === "string" ? value : event.id
}

export * as EventLegacy from "./event-legacy"
