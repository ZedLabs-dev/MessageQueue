using AwesomeAssertions;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.InMemory;

namespace ZedLabs.MessageQueue.Tests.InMemory;

[Trait("Category", "Unit")]
public class Given_an_in_memory_dead_letter
{
    static readonly MessageContext Context = new() { MessageId = "m-1" };
    static readonly ReadOnlyMemory<byte> Body = new byte[] { 1, 2, 3 };
    static readonly TestMessage Message = new("1", "content");

    static InMemoryDeadLetter<TestMessage> Letter(string reason = "Rejected", string? detail = "d") =>
        new(Message, Body, Context, reason, detail);

    [Fact]
    public void When_Comparing_Equal_Values()
    {
        var letter = Letter();
        var same = Letter();

        letter.Equals(same).Should().BeTrue("it should compare by value");
        letter.Equals((object)same).Should().BeTrue("it should compare by value when boxed");
        (letter == same).Should().BeTrue("it should compare by value with ==");
        (letter != same).Should().BeFalse("it should compare by value with !=");
        letter.GetHashCode().Should().Be(same.GetHashCode(), "it should hash equal values alike");
    }

    [Fact]
    public void When_Comparing_Different_Values()
    {
        var letter = Letter();

        letter.Should().NotBe(Letter(reason: "MaxRetriesExceeded"), "it should compare the reason");
        letter.Should().NotBe(Letter(detail: null), "it should compare the detail");
        letter.Should().NotBe(new InMemoryDeadLetter<TestMessage>(null, Body, Context, "Rejected", "d"), "it should compare the message");
        letter.Should().NotBe(new InMemoryDeadLetter<TestMessage>(Message, new byte[] { 1, 2, 3 }, Context, "Rejected", "d"),
            "it should compare the body as a memory region, as a record does");
        letter.Should().NotBe(new InMemoryDeadLetter<TestMessage>(Message, Body, new MessageContext { MessageId = "m-2" }, "Rejected", "d"),
            "it should compare the context");
        letter.Equals(null).Should().BeFalse("it should not equal null");
        (letter == null).Should().BeFalse("it should not equal null with ==");
        (null == (InMemoryDeadLetter<TestMessage>?)null).Should().BeTrue("it should treat two nulls as equal");
    }

    [Fact]
    public void When_Deconstructing_It()
    {
        var (message, body, context, reason, detail) = Letter();

        message.Should().BeSameAs(Message, "it should deconstruct the message");
        body.Should().Be(Body, "it should deconstruct the body");
        context.Should().BeSameAs(Context, "it should deconstruct the context");
        reason.Should().Be("Rejected", "it should deconstruct the reason");
        detail.Should().Be("d", "it should deconstruct the detail");
    }

    [Fact]
    public void When_Formatting_It() =>
        Letter().ToString().Should().StartWith("InMemoryDeadLetter { Message = ", "it should print like a record")
            .And.EndWith(", Reason = Rejected, Detail = d }", "it should print like a record");

    [Fact]
    public void When_Message_And_Context_Are_Distinct_But_Equal_Values()
    {
        var copy = new InMemoryDeadLetter<TestMessage>(Message with { }, Body, Context with { }, "Rejected", "d");

        copy.Message.Should().NotBeSameAs(Message, "the spec needs a distinct message instance");
        copy.Should().Be(Letter(), "it should compare the message and the context by value, as a record does");
        copy.GetHashCode().Should().Be(Letter().GetHashCode(), "it should hash equal values alike");
    }

    [Fact]
    public void When_Reason_Or_Detail_Differ_Only_In_Case()
    {
        Letter(reason: "rejected").Should().NotBe(Letter(), "it should compare the reason ordinally");
        Letter(detail: "D").Should().NotBe(Letter(), "it should compare the detail ordinally");
    }

    [Fact]
    public void When_Formatting_All_Of_It() =>
        Letter().ToString().Should().Be(
            $"InMemoryDeadLetter {{ Message = {Message}, Body = System.ReadOnlyMemory<Byte>[3], Context = {Context}, Reason = Rejected, Detail = d }}",
            "it should print every value in order, like a record");
}
