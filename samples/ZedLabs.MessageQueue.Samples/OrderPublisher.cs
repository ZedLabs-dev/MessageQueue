using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ZedLabs.MessageQueue.Abstractions;

namespace ZedLabs.MessageQueue.Samples;

// Publishes a few orders at startup through IMessagePublisher<T>, which is all application code needs to know.
public sealed partial class OrderPublisher : BackgroundService
{
    public const string FlakyCustomer = "flaky-customer";

    readonly IMessagePublisher<OrderCreated> publisher;
    readonly SampleOptions options;
    readonly ILogger<OrderPublisher> logger;

    public OrderPublisher(IMessagePublisher<OrderCreated> publisher, IOptions<SampleOptions> options, ILogger<OrderPublisher> logger)
    {
        this.publisher = publisher;
        this.options = options.Value;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        for (var i = 0; i < options.OrdersToPublish; i++)
        {
            var order = (i % 3) switch
            {
                0 => new OrderCreated($"order-{i}", "customer-1", 42.50m, DateTimeOffset.UtcNow),
                1 => new OrderCreated($"order-{i}", FlakyCustomer, 10m, DateTimeOffset.UtcNow),
                _ => new OrderCreated($"order-{i}", "customer-2", -1m, DateTimeOffset.UtcNow)
            };

            // MessageId doubles as the idempotency key; CorrelationId ties the message to the workflow that sent it.
            await publisher.PublishAsync(order, new PublishOptions { MessageId = order.OrderId, CorrelationId = $"checkout-{i}" }, stoppingToken);
            LogPublished(logger, order.OrderId);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Published {OrderId}")]
    static partial void LogPublished(ILogger logger, string orderId);
}
