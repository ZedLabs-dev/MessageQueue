using System.Text;
using System.Text.Json;
using AwesomeAssertions;
using ZedLabs.MessageQueue.Providers;
using ZedLabs.MessageQueue.Serialization;

namespace ZedLabs.MessageQueue.Tests.Serialization;

// ADR 0019: a JsonTypeInfoResolver (a source-generated JsonSerializerContext) replaces reflection for a queue's
// messages, with the library's JSON rules (camelCase, depth limit, strict UTF-8) unchanged.
[Trait("Category", "Unit")]
public class Given_source_generated_json_metadata
{
    static readonly TestMessage Message = new("order-1", "content");

    [Fact]
    public void When_Serializing_With_A_Resolver()
    {
        var resolver = new RecordingJsonResolver();

        var json = Encoding.UTF8.GetString(MessageSerializer.Serialize(Message, SerializationFormat.Json, resolver));

        json.Should().Be("""{"id":"order-1","content":"content"}""", "it should write camelCase names with the supplied metadata");
        resolver.Requested.Should().Contain(typeof(TestMessage), "it should ask the supplied resolver for the message type's metadata");
    }

    [Fact]
    public void When_Deserializing_With_A_Resolver()
    {
        var resolver = new RecordingJsonResolver();

        var message = MessageSerializer.Deserialize<TestMessage>("""{"id":"order-1","content":"content"}"""u8.ToArray(), SerializationFormat.Json, resolver);

        message.Should().Be(Message, "it should read camelCase names with the supplied metadata");
        resolver.Requested.Should().Contain(typeof(TestMessage), "it should ask the supplied resolver for the message type's metadata");
    }

    [Fact]
    public void When_The_Resolver_Has_No_Metadata_For_The_Type()
    {
        var resolver = new EmptyJsonResolver();

        var serialize = () => MessageSerializer.Serialize(Message, SerializationFormat.Json, resolver);
        var deserialize = () => MessageSerializer.Deserialize<TestMessage>("{}"u8.ToArray(), SerializationFormat.Json, resolver);

        serialize.Should().Throw<NotSupportedException>("it should not fall back to reflection when the resolver lacks the type");
        deserialize.Should().Throw<NotSupportedException>("it should not fall back to reflection when the resolver lacks the type");
    }

    [Fact]
    public void When_The_Body_Nests_Deeper_Than_The_Limit()
    {
        var json = string.Concat(Enumerable.Repeat("""{"child":""", 100)) + "null" + new string('}', 100);

        var act = () => MessageSerializer.Deserialize<DeepNode>(Encoding.UTF8.GetBytes(json), SerializationFormat.Json, TestJsonContext.Default);

        act.Should().Throw<JsonException>("it should keep the depth limit (64) with source-generated metadata");
    }

    [Fact]
    public void When_The_Body_Is_Not_Valid_Utf8()
    {
        byte[] invalidUtf8 = [.. """{"id":" """u8, 0xFF, 0xFE, .. """ ","content":"x"}"""u8];

        var act = () => MessageSerializer.Deserialize<TestMessage>(invalidUtf8, SerializationFormat.Json, TestJsonContext.Default);

        act.Should().Throw<JsonException>("it should reject invalid UTF-8 with source-generated metadata");
    }

    [Fact]
    public void When_No_Resolver_Is_Supplied()
    {
        var json = Encoding.UTF8.GetString(MessageSerializer.Serialize(Message, SerializationFormat.Json, jsonTypeInfoResolver: null));

        json.Should().Be("""{"id":"order-1","content":"content"}""", "it should serialize with reflection as before");
    }

    [Fact]
    public void When_The_Format_Is_MessagePack()
    {
        var resolver = new RecordingJsonResolver();

        var bytes = MessageSerializer.Serialize(Message, SerializationFormat.MessagePack, resolver);

        MessageSerializer.Deserialize<TestMessage>(bytes, SerializationFormat.MessagePack).Should().Be(Message,
            "it should serialize MessagePack as before");
        resolver.Requested.Should().BeEmpty("it should use JSON metadata only for JSON");
    }
}
