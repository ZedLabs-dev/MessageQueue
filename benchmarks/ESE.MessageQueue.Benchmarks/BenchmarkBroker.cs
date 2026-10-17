using DotNet.Testcontainers.Builders;
using Testcontainers.RabbitMq;

namespace ESE.MessageQueue.Benchmarks;

sealed class BenchmarkBroker : IAsyncDisposable
{
    public const string Variable = "MQ_BENCH_RABBITMQ";

    readonly RabbitMqContainer? container;

    BenchmarkBroker(RabbitMqContainer? container, bool available)
    {
        this.container = container;
        Available = available;
    }

    public bool Available { get; }

    public static async Task<BenchmarkBroker> StartAsync()
    {
        if (!string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Variable)))
            return new BenchmarkBroker(null, available: true);

        var container = new RabbitMqBuilder("rabbitmq:4.3.6-management").Build();
        try
        {
            await container.StartAsync();
        }
        catch (DockerUnavailableException)
        {
            await container.DisposeAsync();
            await Console.Error.WriteLineAsync($"Docker is not available and {Variable} is not set: skipping the RabbitMQ benchmarks.");
            return new BenchmarkBroker(null, available: false);
        }

        Environment.SetEnvironmentVariable(Variable, container.GetConnectionString());
        return new BenchmarkBroker(container, available: true);
    }

    public async ValueTask DisposeAsync()
    {
        if (container is not null)
            await container.DisposeAsync();
    }
}
