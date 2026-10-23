using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.Samples;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace ESE.MessageQueue.Tests.Samples;

// The sample must keep working: it boots exactly as `dotnet run` does (appsettings.json, in-memory provider) and
// processes a published order end to end.
[Trait("Category", "Unit")]
public class Given_the_sample_host
{
    [Fact(Timeout = 30_000)]
    public async Task When_It_Runs_On_The_In_Memory_Provider()
    {
        using var host = SampleHost.CreateBuilder(
        [
            "--Sample:OrdersToPublish=1",
            "--Telemetry:ConsoleExporter=false",
            "--Logging:LogLevel:Default=Warning"
        ]).Build();

        await host.StartAsync(TestContext.Current.CancellationToken);
        var queue = host.Services.GetRequiredService<IMessageQueue<OrderCreated>>();
        var inMemory = queue.Should().BeOfType<InMemoryMessageQueue<OrderCreated>>("it should default to the in-memory provider").Subject;
        await Wait.UntilAsync(() => inMemory.ProcessedMessages.Count == 1, Wait.DeliveryTimeout);
        await host.StopAsync(TestContext.Current.CancellationToken);

        inMemory.ProcessedMessages.Should().ContainSingle("it should publish and process one order")
            .Which.OrderId.Should().Be("order-0", "it should acknowledge the first sample order");
        inMemory.DeadLetters.Should().BeEmpty("it should not dead-letter a valid order");
    }

    [Fact(Timeout = 30_000)]
    public async Task When_It_Peeks_And_Replays_Dead_Letters()
    {
        using var host = SampleHost.CreateBuilder(
        [
            "--Sample:OrdersToPublish=3",
            "--Sample:DeadLetters=Replay",
            "--Sample:DeadLetterDelay=00:00:01",
            "--Telemetry:ConsoleExporter=false",
            "--Logging:LogLevel:Default=Warning"
        ]).Build();

        await host.StartAsync(TestContext.Current.CancellationToken);
        var inspector = host.Services.GetServices<IHostedService>().OfType<DeadLetterInspector>().Single();
        await inspector.ExecuteTask!.WaitAsync(TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);

        inspector.Peeked.Should().ContainSingle("it should peek the order the processor rejected")
            .Which.MessageId.Should().Be("order-2", "it should show the rejected order's id");
        inspector.Previewed.Should().BeEquivalentTo(new { DryRun = true, Matched = 1 }, "it should count with a dry run before replaying");
        inspector.Replayed!.Replayed.Should().Be(1, "it should replay it");
    }

    [Fact(Timeout = 30_000)]
    public async Task When_It_Runs_With_The_Default_Dead_Letter_Setting()
    {
        using var host = SampleHost.CreateBuilder(
        [
            "--Sample:OrdersToPublish=3",
            "--Sample:DeadLetterDelay=00:00:01",
            "--Telemetry:ConsoleExporter=false",
            "--Logging:LogLevel:Default=Warning"
        ]).Build();

        await host.StartAsync(TestContext.Current.CancellationToken);
        var inspector = host.Services.GetServices<IHostedService>().OfType<DeadLetterInspector>().Single();
        await inspector.ExecuteTask!.WaitAsync(TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);

        inspector.Peeked.Should().ContainSingle("it should peek by default").Which.MessageId.Should().Be("order-2");
        inspector.Previewed.Should().BeNull("it should not count a replay by default");
        inspector.Replayed.Should().BeNull("it should never replay by default");
    }

    [Fact]
    public void When_Dead_Letter_Settings_Are_Missing() =>
        new SampleOptions().DeadLetters.Should().Be(DeadLetterMode.None,
            "it should not touch dead letters unless the configuration asks for it");

    [Fact(Timeout = 30_000)]
    public async Task When_The_Provider_Has_No_Dead_Letter_Operations()
    {
        var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton<IMessageQueue<OrderCreated>, QueueWithoutDeadLetterOperations>()
            .AddSingleton<IDeadLetterQueue<OrderCreated>, QueueWithoutDeadLetterOperations>()
            .Configure<SampleOptions>(options =>
            {
                options.DeadLetters = DeadLetterMode.Replay;
                options.DeadLetterDelay = TimeSpan.Zero;
            });
        await using var provider = services.BuildServiceProvider();
        var inspector = ActivatorUtilities.CreateInstance<DeadLetterInspector>(provider);

        await inspector.StartAsync(TestContext.Current.CancellationToken);
        await inspector.ExecuteTask!.WaitAsync(TestContext.Current.CancellationToken);

        inspector.ExecuteTask.IsCompletedSuccessfully.Should().BeTrue("it should log and stop, not crash the host, when the provider has no dead-letter operations");
        inspector.Peeked.Should().BeEmpty("it should not peek");
    }

    sealed class QueueWithoutDeadLetterOperations : IMessageQueue<OrderCreated>, IDeadLetterQueue<OrderCreated>
    {
        public MessageQueueCapabilities Capabilities { get; } = new(Durable: true, DeadLetter: false, DelayedRedelivery: false);
        public Task PublishAsync(OrderCreated message, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task PublishAsync(OrderCreated message, PublishOptions options, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task PublishAsync(IEnumerable<OrderCreated> messages, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task SubscribeAsync(Func<OrderCreated, MessageContext, CancellationToken, Task<MessageResult>> handler, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task SubscribeAsync(Func<OrderCreated, Task<bool>> handler, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task UnsubscribeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<IReadOnlyList<DeadLetterMessage<OrderCreated>>> PeekAsync(int maxCount, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<DeadLetterReplayResult> ReplayAsync(DeadLetterReplayOptions<OrderCreated> options, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<int> PurgeAsync(DeadLetterPurgeOptions<OrderCreated> options, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }

    [Fact]
    public void When_It_Is_Pointed_At_RabbitMq_Without_Credentials()
    {
        var build = () => SampleHost.CreateBuilder(["--environment", "RabbitMq"]);

        build.Should().Throw<InvalidOperationException>("it should fail fast instead of connecting with default credentials")
            .WithMessage("*MessageQueue__UserName*MessageQueue__Password*", "it should say which settings to provide");
    }

}

// `dotnet run --project samples/...` starts in the repository root, not the output folder. Changing the working
// directory is process-wide, so this class runs alone (DisableParallelization).
[Trait("Category", "Unit")]
[Collection(WorkingDirectoryCollection.Name)]
public class Given_the_sample_host_started_elsewhere
{
    [Fact]
    public void When_It_Starts_From_Another_Working_Directory()
    {
        var original = Directory.GetCurrentDirectory();
        Directory.SetCurrentDirectory(Path.GetTempPath());
        try
        {
            var builder = SampleHost.CreateBuilder([]);

            builder.Configuration["MessageQueue:Provider"].Should().Be("InMemory",
                "it should read appsettings.json from the output folder, whatever the working directory");
            builder.Environment.ContentRootPath.TrimEnd(Path.DirectorySeparatorChar)
                .Should().Be(AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar), "it should use the output folder as content root");
        }
        finally
        {
            Directory.SetCurrentDirectory(original);
        }
    }
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class WorkingDirectoryCollection
{
    public const string Name = "Working directory";
}
