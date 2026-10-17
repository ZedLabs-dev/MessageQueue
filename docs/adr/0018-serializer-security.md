# 0018. Serializer security for broker input

- Status: Accepted (2026-10-17); records decisions made in Phases 2, 4 and 7
- Context: OPTIMIZATION-PLAN S1, S9; ADR 0004 (dispatcher), ADR 0007 (RabbitMQ client)

## Context

Every body a consumer reads was written by whoever could publish to the queue, not necessarily this library.
MessagePack 3.1.4 had high-severity advisories for exactly that input (hash-collision DoS, deep nesting), and the
library deserialized with `ContractlessStandardResolver` and no security options.

## Decision

- **One serializer** (`MessageSerializer`, public SPI since ADR 0015), JSON or MessagePack, chosen per queue by
  `SerializationFormat`. No type names on the wire and no polymorphic deserialization: the handler's `T` decides.
- **MessagePack**: 3.1.12 or later, `ContractlessStandardResolver` with `MessagePackSecurity.UntrustedData`
  (collision-resistant hashing for maps, depth limit). Specs check the option and that a hostile dictionary and a
  deeply nested payload throw instead of crashing the process.
- **JSON**: System.Text.Json with an explicit `MaxDepth = 64` (also enforced inside ignored properties), camelCase,
  invalid UTF-8 rejected. A spec checks that 100 levels of nesting throw.
- **Size first** (S9): the dispatcher compares the body with `MaxMessageSizeBytes` (default 1 MiB) before
  deserializing and dead-letters it as `MessageTooLarge` without reading it. On the wire, RabbitMQ.Client buffers at
  most `max(MaxMessageSizeBytes, 16 MiB)` per body (ADR 0007 note); Azure Service Bus is capped by the entity's
  maximum message size (256 KB Standard, up to 100 MB Premium), which the operator sets on the namespace.
- **Failures are data, not crashes:** a body that does not deserialize, or yields null, is dead-lettered as
  `DeserializationFailed` with the exception type only (S6). An unknown format throws `ArgumentOutOfRangeException`.

## Consequences

- Payloads that need more than 64 levels of JSON nesting are rejected; raise `MaxDepth` only with a new decision.
- Changing serializer options is a wire-format and security change: it needs an ADR and a spec, not just a bump.
