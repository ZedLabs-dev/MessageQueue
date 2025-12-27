namespace ESE.MessageQueue.Serialization;

/// <summary>
/// Supported serialization formats.
/// </summary>
public enum SerializationFormat
{
    /// <summary>JSON serialization using System.Text.Json.</summary>
    Json,

    /// <summary>MessagePack binary serialization for high-performance scenarios.</summary>
    MessagePack
}
