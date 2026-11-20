
namespace ZedLabs.MessageQueue.Tests.Serialization;

public record PublicTestMessage(string Id, string Name, int Value);

public sealed class KeyedMessage
{
    public Dictionary<string, int> Counts { get; set; } = [];
}

public sealed class NestedNode
{
    public NestedNode? Child { get; set; }
}
