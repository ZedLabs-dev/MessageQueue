using AwesomeAssertions;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.AzureServiceBus;
using ZedLabs.MessageQueue.InMemory;

namespace ZedLabs.MessageQueue.Tests.Abstractions;

// PublishOptions are validated the same way by every provider, before anything is sent.
// The Azure Service Bus queue connects lazily, so invalid options are rejected without a broker.
// RabbitMQ is covered by Given_a_rabbitmq_queue_with_a_subscriber.When_Publishing_A_Reserved_Header.
[Trait("Category", "Unit")]
public class Given_a_publisher
{
    public static TheoryData<string> Providers => ["InMemory", "AzureServiceBus"];

    static IMessageQueue<TestMessage> Create(string provider) => provider switch
    {
        "InMemory" => new InMemoryMessageQueue<TestMessage>(),
        _ => new AzureServiceBusMessageQueue<TestMessage>(new AzureServiceBusConfig
        {
            ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=v",
            QueueName = "orders"
        })
    };

    static async Task Publish(string provider, PublishOptions options)
    {
        var queue = Create(provider);
        try
        {
            await queue.PublishAsync(new TestMessage("1", "x"), options, TestContext.Current.CancellationToken);
        }
        finally
        {
            await ((IAsyncDisposable)queue).DisposeAsync();
        }
    }

    static Dictionary<string, string> Header(string name, string value = "v") => new() { [name] = value };

    [Theory]
    [MemberData(nameof(Providers))]
    public async Task When_The_Message_Is_Null(string provider)
    {
        var queue = Create(provider);
        await using var _ = (IAsyncDisposable)queue;

        var act = () => queue.PublishAsync((TestMessage)null!, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ArgumentNullException>("it should reject a null message");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public async Task When_The_Options_Are_Null(string provider)
    {
        var act = () => Publish(provider, null!);

        await act.Should().ThrowAsync<ArgumentNullException>("it should reject null options");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public async Task When_The_MessageId_Is_Blank(string provider)
    {
        var act = () => Publish(provider, new PublishOptions { MessageId = "  " });

        await act.Should().ThrowAsync<ArgumentException>("it should reject a blank idempotency key").WithMessage("*MessageId*");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public async Task When_The_MessageId_Is_Too_Long(string provider)
    {
        var act = () => Publish(provider, new PublishOptions { MessageId = new string('m', PublishOptions.MaxMessageIdLength + 1) });

        await act.Should().ThrowAsync<ArgumentException>("it should keep the id within every broker's limit").WithMessage("*MessageId*");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public async Task When_The_CorrelationId_Is_Too_Long(string provider)
    {
        var act = () => Publish(provider, new PublishOptions { CorrelationId = new string('c', PublishOptions.MaxMessageIdLength + 1) });

        await act.Should().ThrowAsync<ArgumentException>("it should keep the correlation id within every broker's limit, not cut it")
            .WithMessage("*CorrelationId*");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public async Task When_The_CorrelationId_Has_Control_Characters(string provider)
    {
        var act = () => Publish(provider, new PublishOptions { CorrelationId = "order-1\r\nforged: yes" });

        await act.Should().ThrowAsync<ArgumentException>("it should refuse a correlation id that could forge log lines")
            .WithMessage("*CorrelationId*");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public async Task When_A_Header_Uses_The_Delivery_Attempt_Name(string provider)
    {
        var act = () => Publish(provider, new PublishOptions { Headers = Header("x-delivery-attempt", "1") });

        await act.Should().ThrowAsync<ArgumentException>("it should not let a publisher forge the retry counter")
            .WithMessage("*x-delivery-attempt*");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public async Task When_A_Header_Uses_The_Broker_Prefix(string provider)
    {
        var act = () => Publish(provider, new PublishOptions { Headers = Header("X-Death") });

        await act.Should().ThrowAsync<ArgumentException>("it should reserve the x- prefix for brokers and the library");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public async Task When_A_Header_Uses_A_Trace_Context_Name(string provider)
    {
        var act = () => Publish(provider, new PublishOptions { Headers = Header("traceparent") });

        await act.Should().ThrowAsync<ArgumentException>("it should reserve the W3C trace context headers for telemetry");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public async Task When_There_Are_Too_Many_Headers(string provider)
    {
        var headers = Enumerable.Range(0, PublishOptions.MaxHeaders + 1).ToDictionary(i => $"h{i}", _ => "v");

        var act = () => Publish(provider, new PublishOptions { Headers = headers });

        await act.Should().ThrowAsync<ArgumentException>("it should limit the number of headers");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public async Task When_A_Header_Value_Is_Too_Long(string provider)
    {
        var act = () => Publish(provider, new PublishOptions { Headers = Header("tenant", new string('v', PublishOptions.MaxHeaderValueLength + 1)) });

        await act.Should().ThrowAsync<ArgumentException>("it should limit the size of a header value");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public async Task When_A_Header_Name_Is_Blank(string provider)
    {
        var act = () => Publish(provider, new PublishOptions { Headers = Header(" ") });

        await act.Should().ThrowAsync<ArgumentException>("it should reject a blank header name");
    }

    [Fact]
    public async Task When_The_Options_Are_Valid()
    {
        var act = () => Publish("InMemory", new PublishOptions
        {
            MessageId = new string('m', PublishOptions.MaxMessageIdLength),
            CorrelationId = new string('c', PublishOptions.MaxMessageIdLength),
            Headers = Header("tenant", "acme")
        });

        await act.Should().NotThrowAsync("it should accept options within the limits");
    }
}
