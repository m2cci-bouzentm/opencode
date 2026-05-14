import { Context, Effect, Layer, Option, PubSub, Schema, Stream } from "effect"
import { Location } from "./location"
import { withStatics } from "./schema"
import { Identifier } from "./util/identifier"

export const ID = Schema.String.pipe(
  Schema.brand("Event.ID"),
  withStatics((schema) => ({ create: () => schema.make("evt_" + Identifier.ascending()) })),
)
export type ID = typeof ID.Type

export const LocationRef = Schema.Struct({
  directory: Schema.String,
  workspaceID: Schema.optional(Schema.String),
}).annotate({ identifier: "Event.Location" })
export type LocationRef = Location.Ref

export type Definition<
  Type extends string = string,
  Data = unknown,
  DataSchema extends Schema.Schema<Data> = Schema.Schema<Data>,
> = Schema.Schema<{
  readonly id: ID
  readonly metadata?: Record<string, unknown>
  readonly type: Type
  readonly version?: number
  readonly location?: LocationRef
  readonly data: Data
}> & {
  readonly type: Type
  readonly version?: number
  readonly aggregate?: string
  readonly schema: DataSchema
  readonly fields: {
    readonly data: DataSchema
  }
}

export type Payload<D extends Definition = Definition> = Schema.Schema.Type<D>
export type Data<D extends Definition> = Payload<D>["data"]

export type ProjectorDefinition<
  Type extends string = string,
  DataSchema extends Schema.Top = Schema.Top,
> = {
  readonly type: Type
  readonly version: number
  readonly aggregate: string
  readonly schema: DataSchema
  readonly properties: DataSchema
}

export type ProjectorEvent<D extends Definition> = {
  readonly id: string
  readonly seq: number
  readonly aggregateID: string
  readonly data: Data<D>
}

export type ProjectorFunc<D extends Definition, DB> = (db: DB, data: Data<D>, event: ProjectorEvent<D>) => void
export type ProjectorEntry = [
  ProjectorDefinition,
  (db: unknown, data: unknown, event: { readonly id: string; readonly seq: number; readonly aggregateID: string; readonly data: unknown }) => void,
]

export const registry = new Map<string, Definition>()

export function define<const Type extends string, Fields extends Schema.Struct.Fields>(input: {
  readonly type: Type
  readonly version?: number
  readonly aggregate?: string
  readonly schema: Fields
}): Definition<Type, Schema.Schema.Type<Schema.Struct<Fields>>, Schema.Struct<Fields>> {
  const Data = Schema.Struct(input.schema)
  const Payload = Schema.Struct({
    id: ID,
    metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
    type: Schema.Literal(input.type),
    version: Schema.optional(Schema.Number),
    location: Schema.optional(LocationRef),
    data: Data,
  }).annotate({ identifier: input.type })

  const definition = Object.assign(Payload, {
    type: input.type,
    ...(input.version === undefined ? {} : { version: input.version }),
    ...(input.aggregate === undefined ? {} : { aggregate: input.aggregate }),
    schema: Data,
  })
  registry.set(input.type, definition)
  return definition as Definition<Type, Schema.Schema.Type<Schema.Struct<Fields>>, Schema.Struct<Fields>>
}

export function definitions() {
  return registry.values().toArray()
}

export function project<DB, D extends Definition>(
  definition: D,
  func: ProjectorFunc<D, DB>,
): ProjectorEntry {
  if (definition.version === undefined) throw new Error(`Event.project: version required for ${definition.type}`)
  if (!definition.aggregate) throw new Error(`Event.project: aggregate required for ${definition.type}`)

  return [
    {
      type: definition.type,
      version: definition.version,
      aggregate: definition.aggregate,
      schema: definition.schema,
      properties: definition.schema,
    },
    func as ProjectorEntry[1],
  ]
}

export interface PublishOptions<D extends Definition> {
  readonly id?: ID
  readonly metadata?: Record<string, unknown>
}

export interface Interface {
  readonly publish: <D extends Definition>(
    definition: D,
    data: Data<D>,
    options?: PublishOptions<D>,
  ) => Effect.Effect<Payload<D>>
  readonly publishEvent: <D extends Definition>(event: Payload<D>) => Effect.Effect<Payload<D>>
  readonly subscribe: <D extends Definition>(definition: D) => Stream.Stream<Payload<D>>
  readonly subscribeAll: () => Stream.Stream<Payload>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/Event") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const all = yield* PubSub.unbounded<Payload>()
    const typed = new Map<string, PubSub.PubSub<Payload>>()

    const getOrCreate = (definition: Definition) =>
      Effect.gen(function* () {
        const existing = typed.get(definition.type)
        if (existing) return existing
        const pubsub = yield* PubSub.unbounded<Payload>()
        typed.set(definition.type, pubsub)
        return pubsub
      })

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        yield* PubSub.shutdown(all)
        yield* Effect.forEach(typed.values(), PubSub.shutdown, { discard: true })
      }),
    )

    function publishEvent<D extends Definition>(event: Payload<D>) {
      return Effect.gen(function* () {
        const pubsub = typed.get(event.type)
        if (pubsub) yield* PubSub.publish(pubsub, event as Payload)
        yield* PubSub.publish(all, event as Payload)
        return event
      })
    }

    function publish<D extends Definition>(
      definition: D,
      data: Data<D>,
      options?: PublishOptions<D>,
    ) {
      return Effect.gen(function* () {
        const location = Option.getOrUndefined(yield* Effect.serviceOption(Location.Service))
        const event = {
          id: options?.id ?? ID.create(),
          ...(options?.metadata ? { metadata: options.metadata } : {}),
          type: definition.type,
          ...(definition.version === undefined ? {} : { version: definition.version }),
          ...(location ? { location } : {}),
          data,
        } as Payload<D>
        return yield* publishEvent(event)
      })
    }

    const subscribe = <D extends Definition>(definition: D): Stream.Stream<Payload<D>> =>
      Stream.unwrap(getOrCreate(definition).pipe(Effect.map((pubsub) => Stream.fromPubSub(pubsub)))).pipe(
        Stream.map((event) => event as Payload<D>),
      )

    const subscribeAll = (): Stream.Stream<Payload> => Stream.fromPubSub(all)

    return Service.of({ publish, publishEvent, subscribe, subscribeAll })
  }),
)

export const defaultLayer = layer

export * as Event from "./event"
export * as EventV2 from "./event"
