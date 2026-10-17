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

    async Task<T> WithChannelAsync<T>(Func<IChannel, Task<T>> action)
    {
        await using var connection = await new ConnectionFactory { Uri = new Uri(ConnectionString) }
            .CreateConnectionAsync(TestContext.Current.CancellationToken);
        await using var channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            TestContext.Current.CancellationToken);
        return await action(channel);
    }

    public Task DeleteQueuesAndExchangesAsync(IEnumerable<string> queues, IEnumerable<string> exchanges) =>
        WithChannelAsync(async channel =>
        {
            foreach (var queue in queues)
            {
                await channel.QueueDeleteAsync(queue, cancellationToken: TestContext.Current.CancellationToken);
            }
            foreach (var exchange in exchanges)
            {
                await channel.ExchangeDeleteAsync(exchange, cancellationToken: TestContext.Current.CancellationToken);
            }
            return true;
        });

    public Task DeclareBoundQueueAsync(string queue, string exchange, string routingKey) =>
        WithChannelAsync(async channel =>
        {
            await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false,
                cancellationToken: TestContext.Current.CancellationToken);
            await channel.QueueBindAsync(queue, exchange, routingKey, cancellationToken: TestContext.Current.CancellationToken);
            return true;
        });

    // Ready messages only (unacknowledged deliveries are not counted), read through AMQP so it is never stale.
    public Task<uint> GetMessageCountAsync(string queue) =>
        WithChannelAsync(channel => channel.MessageCountAsync(queue, TestContext.Current.CancellationToken));

    // Publishes straight to `queue` through the default exchange, the way a foreign (non-library) publisher would.
    public Task PublishRawAsync(string queue, byte[] body, IReadOnlyDictionary<string, string> headers) =>
        WithChannelAsync(async channel =>
        {
            var properties = new BasicProperties
            {
                Persistent = true,
                MessageId = Guid.NewGuid().ToString(),
                Headers = headers.ToDictionary(h => h.Key, h => (object?)h.Value)
            };
            await channel.BasicPublishAsync(string.Empty, queue, mandatory: true, properties, body, TestContext.Current.CancellationToken);
            return true;
        });

    // Takes the next message off `queue` (acknowledging it) and returns its headers as strings, or null when empty.
    public Task<IReadOnlyDictionary<string, string>?> TakeMessageHeadersAsync(string queue) =>
        WithChannelAsync<IReadOnlyDictionary<string, string>?>(async channel =>
        {
            var result = await channel.BasicGetAsync(queue, autoAck: true, TestContext.Current.CancellationToken);
            if (result is null)
                return null;
            return (result.BasicProperties.Headers ?? new Dictionary<string, object?>())
                .ToDictionary(h => h.Key, h => h.Value is byte[] bytes ? Encoding.UTF8.GetString(bytes) : h.Value?.ToString() ?? string.Empty);
        });

    // Simulates a consumer that crashes mid-handler: takes the next message without acknowledging it,
    // then drops the connection so the broker requeues it.
    public async Task TakeWithoutAckAndDisconnectAsync(string queue)
    {
        var connection = await new ConnectionFactory { Uri = new Uri(ConnectionString), AutomaticRecoveryEnabled = false }
            .CreateConnectionAsync(TestContext.Current.CancellationToken);
        var channel = await connection.CreateChannelAsync(cancellationToken: TestContext.Current.CancellationToken);
        var deadline = DateTime.UtcNow + Wait.DeliveryTimeout;
        while (await channel.BasicGetAsync(queue, autoAck: false, TestContext.Current.CancellationToken) is null)
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"No message arrived on {queue}");
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }
        await connection.AbortAsync(TestContext.Current.CancellationToken);
        await connection.DisposeAsync();
    }

    // Publishes straight to `queue` with properties a foreign publisher chose (e.g. malformed ones).
    public Task PublishRawWithAsync(string queue, byte[] body, Action<BasicProperties> configure) =>
        WithChannelAsync(async channel =>
        {
            var properties = new BasicProperties { Persistent = true, MessageId = Guid.NewGuid().ToString() };
            configure(properties);
            await channel.BasicPublishAsync(string.Empty, queue, mandatory: true, properties, body, TestContext.Current.CancellationToken);
            return true;
        });

    // Declares `queue` with the given arguments, e.g. to put an incompatible queue where the library expects its own.
    public Task DeclareQueueAsync(string queue, IDictionary<string, object?> arguments) =>
        WithChannelAsync(async channel =>
        {
            await channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false, arguments: arguments,
                cancellationToken: TestContext.Current.CancellationToken);
            return true;
        });

    // Raw JSON from the management HTTP API (topology and arguments; its message counters lag).
    public async Task<string> GetRawJsonAsync(string path)
    {
        using var http = ManagementClient();
        return await http.GetStringAsync(new Uri(path, UriKind.Relative), TestContext.Current.CancellationToken);
    }

    HttpClient ManagementClient()
    {
        var amqp = new Uri(ConnectionString);
        var http = new HttpClient
        {
            BaseAddress = new UriBuilder("http", amqp.Host, container!.GetMappedPublicPort(ManagementPort)).Uri
        };
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.UTF8.GetBytes(Uri.UnescapeDataString(amqp.UserInfo))));
        return http;
    }

    // Reads the queue type ("classic" or "quorum") from the management HTTP API.
    public async Task<string?> GetQueueTypeAsync(string queue)
    {
        using var http = ManagementClient();
        await using var body = await http.GetStreamAsync(
            new Uri($"api/queues/%2F/{Uri.EscapeDataString(queue)}", UriKind.Relative), TestContext.Current.CancellationToken);
        using var json = await JsonDocument.ParseAsync(body, cancellationToken: TestContext.Current.CancellationToken);
        return json.RootElement.GetProperty("type").GetString();
    }

    // Names of every queue whose name starts with `prefix` (topology only; the management API's counters lag).
    public async Task<IReadOnlyList<string>> ListQueuesAsync(string prefix)
    {
        using var http = ManagementClient();
        await using var body = await http.GetStreamAsync(
            new Uri("api/queues/%2F?columns=name", UriKind.Relative), TestContext.Current.CancellationToken);
        using var json = await JsonDocument.ParseAsync(body, cancellationToken: TestContext.Current.CancellationToken);
        return json.RootElement.EnumerateArray()
            .Select(q => q.GetProperty("name").GetString()!)
            .Where(name => name.StartsWith(prefix, StringComparison.Ordinal))
            .ToList();
    }
}

[CollectionDefinition(Name)]
public sealed class RabbitMqCollection : ICollectionFixture<RabbitMqBrokerFixture>
{
    public const string Name = "RabbitMQ broker";
}
