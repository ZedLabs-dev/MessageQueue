using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace ESE.MessageQueue.Tests.Serialization;

public sealed class DeepNode
{
    public DeepNode? Child { get; set; }
}

// Source-generated metadata, as an AOT or trimmed application supplies it. No naming option on purpose: the library's
// options (camelCase) must apply anyway.
[JsonSerializable(typeof(TestMessage))]
[JsonSerializable(typeof(DeepNode))]
sealed partial class TestJsonContext : JsonSerializerContext;

// Records which types the library asked metadata for, so specs can tell that the supplied resolver was used.
sealed class RecordingJsonResolver : IJsonTypeInfoResolver
{
    readonly IJsonTypeInfoResolver inner;

    public RecordingJsonResolver(IJsonTypeInfoResolver? inner = null)
    {
        this.inner = inner ?? TestJsonContext.Default;
    }

    public ConcurrentQueue<Type> Requested { get; } = new();

    public JsonTypeInfo? GetTypeInfo(Type type, JsonSerializerOptions options)
    {
        Requested.Enqueue(type);
        return inner.GetTypeInfo(type, options);
    }
}

// Knows no type at all: with it, nothing may fall back to reflection.
sealed class EmptyJsonResolver : IJsonTypeInfoResolver
{
    public JsonTypeInfo? GetTypeInfo(Type type, JsonSerializerOptions options) => null;
}
