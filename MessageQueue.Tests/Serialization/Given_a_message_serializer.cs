using System.Text.Json;
using AwesomeAssertions;
using ESE.MessageQueue.Serialization;
using MessagePack;

namespace ESE.MessageQueue.Tests.Serialization;

[Trait("Category", "Unit")]
public class Given_a_message_serializer
{
    sealed record TestMessage(string Id, string Name, int Value);

    [Fact]
    public void When_Serializing_With_Json()
    {
        var message = new TestMessage("123", "Test", 42);

        var bytes = MessageSerializer.Serialize(message, SerializationFormat.Json);

        bytes.Should().NotBeEmpty("it should produce serialized bytes");
    }

    [Fact]
    public void When_Deserializing_With_Json()
    {
        var original = new TestMessage("123", "Test", 42);
        var bytes = MessageSerializer.Serialize(original, SerializationFormat.Json);

        var result = MessageSerializer.Deserialize<TestMessage>(bytes, SerializationFormat.Json);

        result.Should().NotBeNull("it should deserialize the message");
        result!.Id.Should().Be("123", "it should preserve the Id");
        result.Name.Should().Be("Test", "it should preserve the Name");
        result.Value.Should().Be(42, "it should preserve the Value");
    }

    [Fact]
    public void When_Serializing_With_Json_Property_Names()
    {
        var message = new TestMessage("123", "Test", 42);

        var json = System.Text.Encoding.UTF8.GetString(MessageSerializer.Serialize(message, SerializationFormat.Json));

        json.Should().Contain("\"id\":\"123\"", "it should write camelCase property names");
        json.Should().NotContain("\"Id\"", "it should not write PascalCase property names");
    }

    [Fact]
    public void When_Getting_ContentType_For_Json()
    {
        var contentType = MessageSerializer.GetContentType(SerializationFormat.Json);

        contentType.Should().Be("application/json", "it should return the JSON content type");
    }

    [Fact]
    public void When_Deserializing_Invalid_Json()
    {
        var invalidBytes = System.Text.Encoding.UTF8.GetBytes("not valid json");

        var act = () => MessageSerializer.Deserialize<TestMessage>(invalidBytes, SerializationFormat.Json);

        act.Should().Throw<JsonException>("it should throw on invalid JSON");
    }

    [Fact]
    public void When_Serializing_With_MessagePack()
    {
        var message = new PublicTestMessage("123", "Test", 42);

        var bytes = MessageSerializer.Serialize(message, SerializationFormat.MessagePack);

        bytes.Should().NotBeEmpty("it should produce serialized bytes");
    }

    [Fact]
    public void When_Deserializing_With_MessagePack()
    {
        var original = new PublicTestMessage("123", "Test", 42);
        var bytes = MessageSerializer.Serialize(original, SerializationFormat.MessagePack);

        var result = MessageSerializer.Deserialize<PublicTestMessage>(bytes, SerializationFormat.MessagePack);

        result.Should().NotBeNull("it should deserialize the message");
        result!.Id.Should().Be("123", "it should preserve the Id");
        result.Name.Should().Be("Test", "it should preserve the Name");
        result.Value.Should().Be(42, "it should preserve the Value");
    }

    [Fact]
    public void When_Getting_ContentType_For_MessagePack()
    {
        var contentType = MessageSerializer.GetContentType(SerializationFormat.MessagePack);

        contentType.Should().Be("application/x-msgpack", "it should return the MessagePack content type");
    }

    [Fact]
    public void When_Comparing_MessagePack_To_Json_Size()
    {
        var message = new PublicTestMessage("123", "Test", 42);

        var jsonBytes = MessageSerializer.Serialize(message, SerializationFormat.Json);
        var msgpackBytes = MessageSerializer.Serialize(message, SerializationFormat.MessagePack);

        msgpackBytes.Length.Should().BeLessThan(jsonBytes.Length, "it should produce smaller payloads than JSON");
    }

    [Fact]
    public void When_Configuring_MessagePack()
    {
        MessageSerializer.MsgPackOptions.Security.Should().BeSameAs(MessagePackSecurity.UntrustedData,
            "it should treat broker payloads as untrusted data");
    }

    [Fact]
    public void When_Deserializing_A_Deeply_Nested_MessagePack_Payload()
    {
        var hostilePayload = BuildNestedPayload(depth: 1_000);

        var act = () => MessageSerializer.Deserialize<NestedNode>(hostilePayload, SerializationFormat.MessagePack);

        act.Should().Throw<MessagePackSerializationException>(
            "it should reject a hostile object graph instead of overflowing the stack");
    }

    [Fact]
    public void When_Deserializing_A_MessagePack_Dictionary()
    {
        var original = new KeyedMessage { Counts = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 } };
        var bytes = MessageSerializer.Serialize(original, SerializationFormat.MessagePack);

        var result = MessageSerializer.Deserialize<KeyedMessage>(bytes, SerializationFormat.MessagePack);

        result!.Counts.Should().BeEquivalentTo(original.Counts, "it should round-trip the dictionary");
        result.Counts.Comparer.Should().NotBeSameAs(EqualityComparer<string>.Default,
            "it should use a hash-collision resistant comparer for untrusted keys");
    }

    static byte[] BuildNestedPayload(int depth)
    {
        var childKey = "Child"u8.ToArray();
        var payload = new List<byte>(depth * (2 + childKey.Length) + 1);
        for (var i = 0; i < depth; i++)
        {
            payload.Add(0x81);
            payload.Add((byte)(0xA0 | childKey.Length));
            payload.AddRange(childKey);
        }
        payload.Add(0xC0);
        return [.. payload];
    }
}
