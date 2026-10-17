using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.InMemory;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Tests.InMemory;

[Trait("Category", "Unit")]
public class Given_an_in_memory_queue_owned_by_the_caller
{
    [Fact]
    public async Task When_The_Container_Is_Disposed()
    {
        await using var queue = new InMemoryMessageQueue<TestMessage>();
        var services = new ServiceCollection();
        services.AddInMemoryMessageQueue(queue);
        var provider = services.BuildServiceProvider();
        provider.GetRequiredService<IMessageQueue<TestMessage>>();
        provider.GetRequiredService<IMessagePublisher<TestMessage>>();

        await provider.DisposeAsync();
        var act = () => queue.PublishAsync(new TestMessage("1", "still mine"), TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync("it should not dispose a queue instance that the caller created and owns");
    }
}
