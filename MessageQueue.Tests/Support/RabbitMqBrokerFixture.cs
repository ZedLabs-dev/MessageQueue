using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using DotNet.Testcontainers.Builders;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;

namespace ESE.MessageQueue.Tests;

// One RabbitMQ container for every test in the RabbitMQ collection. xunit creates collection fixtures
// only when a test in the collection runs, so `--filter Category=Unit` never touches Docker.
// No Docker: the tests are reported as skipped, unless MQ_REQUIRE_DOCKER is "true" or "1" (CI), where it is a failure.
public sealed class RabbitMqBrokerFixture : IAsyncLifetime
{
    public const string Image = "rabbitmq:4.3.6-management";
    const string RequireDockerVariable = "MQ_REQUIRE_DOCKER";
    const ushort ManagementPort = 15672;

    RabbitMqContainer? container;
    string? unavailableReason;

    public string ConnectionString => container?.GetConnectionString()
        ?? throw new InvalidOperationException("The RabbitMQ container is not running.");

    static bool DockerRequired =>
        Environment.GetEnvironmentVariable(RequireDockerVariable)?.Trim() is { } value
        && (value == "1" || value.Equals("true", StringComparison.OrdinalIgnoreCase));

    public async ValueTask InitializeAsync()
    {
        try
        {
            container = new RabbitMqBuilder(Image).WithPortBinding(ManagementPort, true).Build();
            await container.StartAsync();
        }
        catch (DockerUnavailableException ex) when (!DockerRequired)
        {
            unavailableReason = $"Docker is not available, so the RabbitMQ integration tests cannot run ({ex.Message}). " +
                $"Start Docker, or set {RequireDockerVariable}=true to make this a failure.";
            return;
        }

        TestContext.Current.SendDiagnosticMessage(
            $"RabbitMQ container {container.Id} ({Image}) listening at {container.Hostname}:{container.GetMappedPublicPort(5672)}");
    }

    public async ValueTask DisposeAsync()
    {
        if (container is not null)
        {
            await container.DisposeAsync();
        }
    }

    public void SkipUnlessAvailable() => Assert.SkipWhen(unavailableReason is not null, unavailableReason ?? string.Empty);

    public void DeleteQueuesAndExchanges(IEnumerable<string> queues, IEnumerable<string> exchanges)
    {
        using var connection = new ConnectionFactory { Uri = new Uri(ConnectionString) }.CreateConnection();
        using var channel = connection.CreateModel();
        foreach (var queue in queues)
        {
            channel.QueueDelete(queue);
        }
        foreach (var exchange in exchanges)
        {
            channel.ExchangeDelete(exchange);
        }
    }

    public void DeclareBoundQueue(string queue, string exchange, string routingKey)
    {
        using var connection = new ConnectionFactory { Uri = new Uri(ConnectionString) }.CreateConnection();
        using var channel = connection.CreateModel();
        channel.QueueDeclare(queue, durable: true, exclusive: false, autoDelete: false, arguments: null);
        channel.QueueBind(queue, exchange, routingKey);
    }

    // Reads the queue type ("classic" or "quorum") from the management HTTP API.
    public async Task<string?> GetQueueTypeAsync(string queue)
    {
        var amqp = new Uri(ConnectionString);
        using var http = new HttpClient
        {
            BaseAddress = new UriBuilder("http", amqp.Host, container!.GetMappedPublicPort(ManagementPort)).Uri
        };
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.UTF8.GetBytes(Uri.UnescapeDataString(amqp.UserInfo))));

        await using var body = await http.GetStreamAsync(
            new Uri($"api/queues/%2F/{Uri.EscapeDataString(queue)}", UriKind.Relative), TestContext.Current.CancellationToken);
        using var json = await JsonDocument.ParseAsync(body, cancellationToken: TestContext.Current.CancellationToken);
        return json.RootElement.GetProperty("type").GetString();
    }

    public uint GetMessageCount(string queue)
    {
        using var connection = new ConnectionFactory { Uri = new Uri(ConnectionString) }.CreateConnection();
        using var channel = connection.CreateModel();
        return channel.MessageCount(queue);
    }
}

[CollectionDefinition(Name)]
public sealed class RabbitMqCollection : ICollectionFixture<RabbitMqBrokerFixture>
{
    public const string Name = "RabbitMQ broker";
}
