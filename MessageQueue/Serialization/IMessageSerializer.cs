namespace ESE.MessageQueue.Serialization;

/// <summary>
/// Abstraction for message serialization/deserialization.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// Serializes a message to bytes.
    /// </summary>
    byte[] Serialize<T>(T message) where T : class;

    /// <summary>
    /// Deserializes bytes to a message.
    /// </summary>
    T? Deserialize<T>(byte[] data) where T : class;

    /// <summary>
    /// Gets the content type for this serializer (e.g., "application/json").
    /// </summary>
    string ContentType { get; }
}

/// <summary>
/// Supported serialization formats.
/// </summary>
public enum SerializationFormat
{
    /// <summary>JSON serialization using System.Text.Json.</summary>
    Json,

    /// <summary>MessagePack binary serialization.</summary>
    MessagePack
}

