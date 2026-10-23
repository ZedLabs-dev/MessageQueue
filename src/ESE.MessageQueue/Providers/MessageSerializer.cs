using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using ESE.MessageQueue.Serialization;
using MessagePack;
using MessagePack.Resolvers;

namespace ESE.MessageQueue.Providers;

/// <summary>
/// The library's wire format: JSON (System.Text.Json, camelCase) or MessagePack (contractless, with
/// <c>MessagePackSecurity.UntrustedData</c>). Providers serialize with it on publish so that
/// <see cref="MessageDispatcher{T}"/> can deserialize on consume. Thread-safe.
/// </summary>
public static class MessageSerializer
{
    static readonly JsonSerializerOptions JsonOptions = CreateJsonOptions(null);

    // One options instance per supplied resolver (ADR 0019), built once and read-only; weakly keyed so a resolver the
    // application drops is not kept alive.
    static readonly ConditionalWeakTable<IJsonTypeInfoResolver, JsonSerializerOptions> ResolverOptions = new();

    // ContractlessStandardResolver: types don't need [MessagePackObject]. UntrustedData: broker input is hostile.
    internal static readonly MessagePackSerializerOptions MsgPackOptions =
        MessagePackSerializerOptions.Standard
            .WithResolver(ContractlessStandardResolver.Instance)
            .WithSecurity(MessagePackSecurity.UntrustedData);

    /// <summary>
    /// Serializes <paramref name="message"/> to bytes in <paramref name="format"/>. JSON goes through reflection; providers
    /// pass the queue's <c>JsonTypeInfoResolver</c> to the three-argument overload instead (ADR 0019).
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="format"/> is not a defined format.</exception>
    public static byte[] Serialize<T>(T message, SerializationFormat format) where T : class
    {
        return format switch
        {
            SerializationFormat.Json => JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions),
            SerializationFormat.MessagePack => MessagePackSerializer.Serialize(message, MsgPackOptions),
            _ => throw new ArgumentOutOfRangeException(nameof(format), format, "Unsupported serialization format")
        };
    }

    /// <summary>
    /// Serializes <paramref name="message"/> to bytes in <paramref name="format"/>. For JSON, a non-null
    /// <paramref name="jsonTypeInfoResolver"/> (such as a source-generated <c>JsonSerializerContext</c>) supplies the
    /// metadata instead of reflection, with the same camelCase names and depth limit; pass
    /// <c>MessageQueueConfig.JsonTypeInfoResolver</c>. It is ignored for MessagePack.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="format"/> is not a defined format.</exception>
    /// <exception cref="NotSupportedException">The resolver has no metadata for <typeparamref name="T"/>; there is no reflection fallback.</exception>
    public static byte[] Serialize<T>(T message, SerializationFormat format, IJsonTypeInfoResolver? jsonTypeInfoResolver) where T : class
    {
        if (format == SerializationFormat.Json && jsonTypeInfoResolver is not null)
            return JsonSerializer.SerializeToUtf8Bytes(message, TypeInfo<T>(jsonTypeInfoResolver));
        return Serialize(message, format);
    }

    /// <summary>
    /// Deserializes a body without copying it, with the JSON metadata of <paramref name="jsonTypeInfoResolver"/> when
    /// it is not null (see <see cref="Serialize{T}(T, SerializationFormat, IJsonTypeInfoResolver?)"/>).
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="format"/> is not a defined format.</exception>
    /// <exception cref="NotSupportedException">The resolver has no metadata for <typeparamref name="T"/>; there is no reflection fallback.</exception>
    public static T? Deserialize<T>(ReadOnlyMemory<byte> data, SerializationFormat format, IJsonTypeInfoResolver? jsonTypeInfoResolver) where T : class
    {
        if (format == SerializationFormat.Json && jsonTypeInfoResolver is not null)
            return JsonSerializer.Deserialize(data.Span, TypeInfo<T>(jsonTypeInfoResolver));
        return Deserialize<T>(data, format);
    }

    // True when the resolver has metadata for the type, under the library's options (startup validation).
    internal static bool HasJsonMetadata(IJsonTypeInfoResolver resolver, Type type) => OptionsFor(resolver).TryGetTypeInfo(type, out _);

    // The resolver is the only source of metadata: GetTypeInfo throws NotSupportedException for a type it does not
    // know, instead of falling back to reflection (which a trimmed or AOT application may not have).
    static JsonTypeInfo<T> TypeInfo<T>(IJsonTypeInfoResolver resolver) => (JsonTypeInfo<T>)OptionsFor(resolver).GetTypeInfo(typeof(T));

    static JsonSerializerOptions OptionsFor(IJsonTypeInfoResolver resolver) =>
        ResolverOptions.GetValue(resolver, static r =>
        {
            var options = CreateJsonOptions(r);
            options.MakeReadOnly();
            return options;
        });

    // The library's JSON rules, the same with and without a resolver.
    static JsonSerializerOptions CreateJsonOptions(IJsonTypeInfoResolver? resolver) => new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        // Broker input is untrusted (ADR 0018): stated explicitly so a change of default cannot loosen it.
        MaxDepth = 64,
        TypeInfoResolver = resolver
    };

    /// <summary>
    /// Deserializes a body without copying it. Throws on malformed or hostile input (for example excessive nesting).
    /// JSON goes through reflection; providers pass the queue's <c>JsonTypeInfoResolver</c> to the three-argument
    /// overload instead (ADR 0019).
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="format"/> is not a defined format.</exception>
    public static T? Deserialize<T>(ReadOnlyMemory<byte> data, SerializationFormat format) where T : class
    {
        return format switch
        {
            SerializationFormat.Json => JsonSerializer.Deserialize<T>(data.Span, JsonOptions),
            SerializationFormat.MessagePack => MessagePackSerializer.Deserialize<T>(data, MsgPackOptions),
            _ => throw new ArgumentOutOfRangeException(nameof(format), format, "Unsupported serialization format")
        };
    }

    /// <summary>The MIME type for <paramref name="format"/>: <c>application/json</c>, <c>application/x-msgpack</c>, else <c>application/octet-stream</c>.</summary>
    public static string GetContentType(SerializationFormat format)
    {
        return format switch
        {
            SerializationFormat.Json => "application/json",
            SerializationFormat.MessagePack => "application/x-msgpack",
            _ => "application/octet-stream"
        };
    }
}
