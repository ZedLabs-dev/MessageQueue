using AwesomeAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Exceptions;
using ZedLabs.MessageQueue.Extensions;
using ZedLabs.MessageQueue.InMemory;
using ZedLabs.MessageQueue.Tests.Serialization;

namespace ZedLabs.MessageQueue.Tests.Configuration;

// ADR 0019: AddMessageQueueJsonMetadata<T> hands a queue's JSON metadata to whichever provider the configuration
// selects; options binding cannot carry it, so it is attached to the bound config in code and checked on start.
[Trait("Category", "Unit")]
public class Given_json_metadata_for_a_queue
{
    static IConfiguration Configuration(string format = "Json") => new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["MessageQueue:Provider"] = "InMemory",
        ["MessageQueue:QueueName"] = "orders",
        ["MessageQueue:SerializationFormat"] = format
    }).Build();

    static string Name => typeof(TestMessage).FullName!;

    [Fact(Timeout = 10_000)]
    public async Task When_Registered_For_A_Configured_Queue()
    {
        var resolver = new RecordingJsonResolver();
        var services = new ServiceCollection().AddMessageQueueJsonMetadata<TestMessage>(resolver).AddMessageQueue<TestMessage>(Configuration());
        await using var provider = services.BuildServiceProvider();
        var queue = (InMemoryMessageQueue<TestMessage>)provider.GetRequiredService<IMessageQueue<TestMessage>>();
        var received = new TaskCompletionSource<TestMessage>();
        await queue.SubscribeAsync((m, _, _) =>
        {
            received.TrySetResult(m);
            return Task.FromResult(MessageResult.Acknowledge);
        }, TestContext.Current.CancellationToken);

        await queue.PublishAsync(new TestMessage("order-1", "content"), TestContext.Current.CancellationToken);

        (await received.Task).Should().Be(new TestMessage("order-1", "content"), "it should deliver the message");
        provider.GetRequiredService<IOptionsMonitor<InMemoryConfig>>().Get(Name).JsonTypeInfoResolver.Should().BeSameAs(resolver,
            "it should attach the resolver to the queue's config");
        resolver.Requested.Should().Contain(typeof(TestMessage), "it should serialize and deserialize with the supplied metadata");
    }

    [Fact]
    public async Task When_Registered_After_The_Queue()
    {
        var resolver = new RecordingJsonResolver();
        var services = new ServiceCollection().AddMessageQueue<TestMessage>(Configuration()).AddMessageQueueJsonMetadata<TestMessage>(resolver);
        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IOptionsMonitor<InMemoryConfig>>().Get(Name).JsonTypeInfoResolver.Should().BeSameAs(resolver,
            "it should not depend on the order of the registrations");
    }

    [Fact]
    public async Task When_Registered_For_A_Queue_Configured_In_Code()
    {
        var resolver = new RecordingJsonResolver();
        var services = new ServiceCollection().AddInMemoryMessageQueue<TestMessage>().AddMessageQueueJsonMetadata<TestMessage>(resolver);
        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IOptionsMonitor<InMemoryConfig>>().Get(Name).JsonTypeInfoResolver.Should().BeSameAs(resolver,
            "it should attach the resolver to a queue registered without configuration too");
    }

    [Fact]
    public async Task When_The_Resolver_Has_No_Metadata_For_The_Message_Type()
    {
        var services = new ServiceCollection().AddMessageQueueJsonMetadata<TestMessage>(new EmptyJsonResolver()).AddMessageQueue<TestMessage>(Configuration());
        await using var provider = services.BuildServiceProvider();

        var act = () => provider.GetRequiredService<IStartupValidator>().Validate();

        act.Should().Throw<OptionsValidationException>("it should fail on start, not on the first message")
            .WithMessage($"*JsonTypeInfoResolver*{nameof(TestMessage)}*");
    }

    [Fact]
    public async Task When_The_Queue_Uses_MessagePack()
    {
        var services = new ServiceCollection().AddMessageQueueJsonMetadata<TestMessage>(new RecordingJsonResolver())
            .AddMessageQueue<TestMessage>(Configuration("MessagePack"));
        await using var provider = services.BuildServiceProvider();

        var act = () => provider.GetRequiredService<IStartupValidator>().Validate();

        act.Should().Throw<OptionsValidationException>("it should not silently ignore JSON metadata for a MessagePack queue")
            .WithMessage("*JsonTypeInfoResolver*SerializationFormat*");
    }

    [Fact]
    public void When_Registered_Twice_For_A_Message_Type()
    {
        var services = new ServiceCollection().AddMessageQueueJsonMetadata<TestMessage>(new RecordingJsonResolver());

        var act = () => services.AddMessageQueueJsonMetadata<TestMessage>(new RecordingJsonResolver());

        act.Should().Throw<ConfigurationException>("it should not let a second registration silently replace the first")
            .WithMessage($"*{nameof(TestMessage)}*");
    }

    [Fact]
    public void When_The_Resolver_Is_Null()
    {
        var act = () => new ServiceCollection().AddMessageQueueJsonMetadata<TestMessage>(null!);

        act.Should().Throw<ArgumentNullException>("it should require a resolver");
    }

    [Fact]
    public async Task When_Another_Message_Type_Has_Metadata()
    {
        var services = new ServiceCollection().AddMessageQueueJsonMetadata<DeepNode>(TestJsonContext.Default).AddMessageQueue<TestMessage>(Configuration());
        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IOptionsMonitor<InMemoryConfig>>().Get(Name).JsonTypeInfoResolver.Should().BeNull(
            "it should attach metadata only to the queue of its own message type");
    }

    [Fact]
    public async Task When_The_Queue_Is_A_Registered_Instance()
    {
        await using var queue = new InMemoryMessageQueue<TestMessage>(new InMemoryConfig { QueueName = "orders" });
        var services = new ServiceCollection().AddInMemoryMessageQueue(queue);

        var act = () => services.AddMessageQueueJsonMetadata<TestMessage>(new RecordingJsonResolver());

        act.Should().Throw<ConfigurationException>("it should not silently ignore metadata a caller-created queue cannot receive")
            .WithMessage("*JsonTypeInfoResolver*");
    }

    [Fact]
    public async Task When_The_Queue_Instance_Is_Registered_After_The_Metadata()
    {
        await using var queue = new InMemoryMessageQueue<TestMessage>(new InMemoryConfig { QueueName = "orders" });
        var services = new ServiceCollection().AddMessageQueueJsonMetadata<TestMessage>(new RecordingJsonResolver());

        var act = () => services.AddInMemoryMessageQueue(queue);

        act.Should().Throw<ConfigurationException>("it should refuse the instance in either registration order")
            .WithMessage("*JsonTypeInfoResolver*");
    }

    [Fact]
    public async Task When_The_Resolver_Is_Set_In_Code_Without_A_Registration()
    {
        var resolver = new RecordingJsonResolver();
        var services = new ServiceCollection().AddInMemoryMessageQueue<TestMessage>(config => config.JsonTypeInfoResolver = resolver);
        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IOptionsMonitor<InMemoryConfig>>().Get(Name).JsonTypeInfoResolver.Should().BeSameAs(resolver,
            "it should keep a resolver set in code when none is registered");
    }
}
