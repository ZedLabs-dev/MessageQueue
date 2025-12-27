using System.Text;
using System.Text.Json;

namespace ESE.MessageQueue.Serialization;

/// <summary>
/// JSON message serializer using System.Text.Json.
/// </summary>
public class JsonMessageSerializer : IMessageSerializer
{
    readonly JsonSerializerOptions options;

    public JsonMessageSerializer() : this(null)
    {
    }

    public JsonMessageSerializer(JsonSerializerOptions? options)
    {
        this.options = options ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
    }

    public string ContentType => "application/json";

    public byte[] Serialize<T>(T message) where T : class
    {
        var json = JsonSerializer.Serialize(message, options);
        return Encoding.UTF8.GetBytes(json);
    }

    public T? Deserialize<T>(byte[] data) where T : class
    {
        var json = Encoding.UTF8.GetString(data);
        return JsonSerializer.Deserialize<T>(json, options);
    }
}

