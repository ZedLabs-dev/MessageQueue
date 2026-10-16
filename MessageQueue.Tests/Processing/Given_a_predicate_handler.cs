using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Processing;

namespace ESE.MessageQueue.Tests.Processing;

[Trait("Category", "Unit")]
public class Given_a_predicate_handler
{
    readonly MessageContext context = new() { MessageId = "m-1" };

    [Fact]
    public async Task When_It_Returns_True()
    {
        var handler = MessageHandlers.FromPredicate<TestMessage>(_ => Task.FromResult(true));

        var result = await handler(new TestMessage("1", "x"), context, TestContext.Current.CancellationToken);

        result.Should().Be(MessageResult.Acknowledge, "it should acknowledge when the predicate returns true");
    }

    [Fact]
    public async Task When_It_Returns_False()
    {
        var handler = MessageHandlers.FromPredicate<TestMessage>(_ => Task.FromResult(false));

        var result = await handler(new TestMessage("1", "x"), context, TestContext.Current.CancellationToken);

        result.Should().Be(MessageResult.Retry, "it should ask for a retry when the predicate returns false");
    }
}
