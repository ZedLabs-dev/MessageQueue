using System.Text.Json;
using MessagePack;
using MessagePack.Resolvers;

namespace ESE.MessageQueue.Serialization;

// Serializes straight to and from UTF-8 / MessagePack bytes; consumers pass the broker body as
// ReadOnlyMemory<byte> so no intermediate string or array copy is made.
static class MessageSerializer
{
    static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    // ContractlessStandardResolver: types don't need [MessagePackObject]. UntrustedData: broker input is hostile.
    internal static readonly MessagePackSerializerOptions MsgPackOptions =
        MessagePackSerializerOptions.Standard
            .WithResolver(ContractlessStandardResolver.Instance)
            .WithSecurity(MessagePackSecurity.UntrustedData);

    public static byte[] Serialize<T>(T message, SerializationFormat format) where T : class
    {
        return format switch
        {
            SerializationFormat.Json => JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions),
            SerializationFormat.MessagePack => MessagePackSerializer.Serialize(message, MsgPackOptions),
            _ => throw new ArgumentOutOfRangeException(nameof(format), format, "Unsupported serialization format")
        };
    }

    public static T? Deserialize<T>(ReadOnlyMemory<byte> data, SerializationFormat format) where T : class
    {
        return format switch
        {
            SerializationFormat.Json => JsonSerializer.Deserialize<T>(data.Span, JsonOptions),
            SerializationFormat.MessagePack => MessagePackSerializer.Deserialize<T>(data, MsgPackOptions),
            _ => throw new ArgumentOutOfRangeException(nameof(format), format, "Unsupported serialization format")
        };
    }

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
