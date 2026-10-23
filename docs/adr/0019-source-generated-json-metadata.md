# 0019. Source-generated JSON metadata per queue

- Status: Accepted (2026-10-22)
- Context: OPTIMIZATION-PLAN §4.1 (deferred `JsonSerializerContext` overload); ADR 0018 (serializer security), ADR 0015

## Context

`MessageSerializer` used reflection-based System.Text.Json, which a trimmed or native AOT application cannot rely on.
Metadata is code (a `JsonSerializerContext`), but a queue's settings come from configuration binding, which cannot
carry it, and the provider that reads them is chosen at run time by name.

## Decision

- **`MessageQueueConfig.JsonTypeInfoResolver`** (`IJsonTypeInfoResolver?`, code only, not bindable) carries the
  metadata on the config every provider already receives, so it reaches publish and consume without a new seam.
- **`services.AddMessageQueueJsonMetadata<T>(resolver)`** registers it for `T`'s queue. `QueueRegistration` attaches
  it with a named `PostConfigure`, so it works for every provider and entry point, before or after `AddMessageQueue<T>`.
  A second registration for `T`, or one for a queue registered as an instance (`AddInMemoryMessageQueue(queue)`, in
  either order), throws `ConfigurationException`. Directly constructed queues set the property; the two-argument
  `MessageSerializer` overloads remain the reflection path.
- **Startup checks:** `ValidateOnStart` fails when the resolver has no metadata for `T`, or when it is set and
  `SerializationFormat` is not `Json` (silently ignoring it would hide a misconfiguration).
- **Serializer:** `Serialize`/`Deserialize` overloads take the resolver (the public SPI; the loopback sample uses
  them). With one, the library builds its own `JsonSerializerOptions` (camelCase, `MaxDepth = 64`, the same factory as
  the reflection path so the rules cannot drift), sets the resolver as the **only** `TypeInfoResolver` (no
  reflection fallback: an unknown type throws `NotSupportedException`), makes it read-only, caches it per resolver
  (weakly), and calls the `JsonTypeInfo<T>` overloads, which need no unreferenced code. Invalid UTF-8 is rejected as
  before. The context's own generation options (naming, depth) do not apply: the library's options decide the wire
  format, checked by a spec with a context that sets no naming policy.

## Consequences

- An AOT application gets reflection-free JSON for its messages; the core package is not yet annotated as trimmable
  (MessagePack contractless resolution and the reflection path remain for other apps).
- The wire format is unchanged, so publishers with and without metadata interoperate.
