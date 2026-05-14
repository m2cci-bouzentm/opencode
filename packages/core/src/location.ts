import { Context } from "effect"

export * as Location from "./location"

export type Ref = {
  readonly directory: string
  readonly workspaceID?: string
}

export class Service extends Context.Service<Service, Ref>()("@opencode/Location") {}
