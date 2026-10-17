using System.Text.Json;
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
    static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
        // Broker input is untrusted (ADR 0018): stated explicitly so a change of default cannot loosen it.
        MaxDepth = 64
    };

    // ContractlessStandardResolver: types don't need [MessagePackObject]. UntrustedData: broker input is hostile.
    internal static readonly MessagePackSerializerOptions MsgPackOptions =
        MessagePackSerializerOptions.Standard
            .WithResolver(ContractlessStandardResolver.Instance)
            .WithSecurity(MessagePackSecurity.UntrustedData);

    /// <summary>Serializes <paramref name="message"/> to bytes in <paramref name="format"/>.</summary>
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

    /// <summary>Deserializes a body without copying it. Throws on malformed or hostile input (for example excessive nesting).</summary>
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
