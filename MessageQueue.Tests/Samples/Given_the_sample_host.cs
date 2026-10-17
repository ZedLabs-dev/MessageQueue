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

    [Fact]
    public void When_It_Is_Pointed_At_RabbitMq_Without_Credentials()
    {
        var build = () => SampleHost.CreateBuilder(["--environment", "RabbitMq"]);

        build.Should().Throw<InvalidOperationException>("it should fail fast instead of connecting with default credentials")
            .WithMessage("*MessageQueue__UserName*MessageQueue__Password*", "it should say which settings to provide");
    }
}
