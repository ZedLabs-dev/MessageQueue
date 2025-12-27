using System.Text;
using System.Text.Json;
using MessagePack;
using MessagePack.Resolvers;

namespace ESE.MessageQueue.Serialization;

/// <summary>
/// Internal message serializer. Uses JSON by default, MessagePack for high-performance scenarios.
/// </summary>
internal static class MessageSerializer
{
    static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    // Use ContractlessStandardResolver so types don't need [MessagePackObject] attributes
    static readonly MessagePackSerializerOptions MsgPackOptions =
        MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance);

    public static byte[] Serialize<T>(T message, SerializationFormat format) where T : class
    {
        return format switch
        {
            SerializationFormat.Json => SerializeJson(message),
            SerializationFormat.MessagePack => SerializeMessagePack(message),
            _ => throw new ArgumentOutOfRangeException(nameof(format), format, "Unsupported serialization format")
        };
    }

    public static T? Deserialize<T>(byte[] data, SerializationFormat format) where T : class
    {
        return format switch
        {
            SerializationFormat.Json => DeserializeJson<T>(data),
            SerializationFormat.MessagePack => DeserializeMessagePack<T>(data),
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

    static byte[] SerializeJson<T>(T message) where T : class
    {
        var json = JsonSerializer.Serialize(message, JsonOptions);
        return Encoding.UTF8.GetBytes(json);
    }

    static T? DeserializeJson<T>(byte[] data) where T : class
    {
        var json = Encoding.UTF8.GetString(data);
        return JsonSerializer.Deserialize<T>(json, JsonOptions);
    }

    static byte[] SerializeMessagePack<T>(T message) where T : class
    {
        return MessagePackSerializer.Serialize(message, MsgPackOptions);
    }

    static T? DeserializeMessagePack<T>(byte[] data) where T : class
    {
        return MessagePackSerializer.Deserialize<T>(data, MsgPackOptions);
    }
}
