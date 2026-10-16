using System.Diagnostics;
using System.Text.Json;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.HealthChecks;
using ESE.MessageQueue.Retry;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Telemetry;
using ESE.MessageQueue.Telemetry.OpenTelemetry;
using ESE.MessageQueue.Testing;
using FluentAssertions;
using MessagePack;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ESE.MessageQueue.Tests;

// Public record for MessagePack tests (MessagePack requires public types)
public record PublicTestMessage(string Id, string Name, int Value);

public class TelemetryTests
{
    [Fact]
    public void When_Creating_Publish_Activity()
    {
        var provider = new TestTelemetryProvider();

        using var activity = provider.StartPublishActivity("test-queue", "TestMessage");

        activity.Should().NotBeNull("it should create an activity for publish operations");
        activity!.OperationName.Should().Be("publish", "it should set the operation name");
        activity.QueueName.Should().Be("test-queue", "it should set the queue name");
        activity.MessageType.Should().Be("TestMessage", "it should set the message type");
    }

    [Fact]
    public void When_Creating_Consume_Activity()
    {
        var provider = new TestTelemetryProvider();

        using var activity = provider.StartConsumeActivity("test-queue", "TestMessage", "msg-123");

        activity.Should().NotBeNull("it should create an activity for consume operations");
        activity!.OperationName.Should().Be("consume", "it should set the operation name");
        activity.QueueName.Should().Be("test-queue", "it should set the queue name");
        activity.MessageId.Should().Be("msg-123", "it should set the message id");
    }

    [Fact]
    public void When_Recording_Message_Published()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessagePublished("test-queue", "TestMessage");

        provider.PublishedCount.Should().Be(1, "it should increment the published counter");
    }

    [Fact]
    public void When_Recording_Message_Consumed()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessageConsumed("test-queue", "TestMessage", success: true);

        provider.ConsumedSuccessCount.Should().Be(1, "it should increment the success counter");
    }

    [Fact]
    public void When_Recording_Message_Failed()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessageConsumed("test-queue", "TestMessage", success: false);

        provider.ConsumedFailureCount.Should().Be(1, "it should increment the failure counter");
    }

    [Fact]
    public void When_Recording_Message_Retried()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessageRetried("test-queue", "TestMessage", attemptNumber: 2);

        provider.RetriedCount.Should().Be(1, "it should increment the retry counter");
    }

    [Fact]
    public void When_Recording_Message_DeadLettered()
    {
        var provider = new TestTelemetryProvider();

        provider.RecordMessageDeadLettered("test-queue", "TestMessage", "Processing failed");

        provider.DeadLetteredCount.Should().Be(1, "it should increment the dead-letter counter");
    }

    [Fact]
    public void When_Using_NullTelemetryProvider()
    {
        var provider = NullTelemetryProvider.Instance;

        var activity = provider.StartPublishActivity("test-queue", "TestMessage");

        activity.Should().BeNull("it should return null for no-op provider");
    }

    [Fact]
    public void When_Extracting_Trace_Context()
    {
        var provider = new TestTelemetryProvider();
        var headers = new Dictionary<string, object>
        {
            ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        };

        var context = provider.ExtractTraceContext(headers);

        context.Should().NotBeNull("it should extract trace context from headers");
        context!.TraceId.Should().Be("0af7651916cd43dd8448eb211c80319c", "it should extract the trace id");
        context.SpanId.Should().Be("b7ad6b7169203331", "it should extract the span id");
    }

    [Fact]
    public void When_Injecting_Trace_Context()
    {
        var provider = new TestTelemetryProvider();
        var headers = new Dictionary<string, object>();

        provider.InjectTraceContext(headers);

        headers.Should().ContainKey("traceparent", "it should inject traceparent header");
    }
}

public class TestTelemetryProvider : ITelemetryProvider
{
    public int PublishedCount { get; private set; }
    public int ConsumedSuccessCount { get; private set; }
    public int ConsumedFailureCount { get; private set; }
    public int RetriedCount { get; private set; }
    public int DeadLetteredCount { get; private set; }

    public TelemetryActivity? StartPublishActivity(string queueName, string messageType)
    {
        return new TelemetryActivity("publish", queueName, messageType);
    }

    public TelemetryActivity? StartConsumeActivity(string queueName, string messageType, string? messageId)
    {
        return new TelemetryActivity("consume", queueName, messageType) { MessageId = messageId };
    }

    public void RecordMessagePublished(string queueName, string messageType)
    {
        PublishedCount++;
    }

    public void RecordMessageConsumed(string queueName, string messageType, bool success)
    {
        if (success)
            ConsumedSuccessCount++;
        else
            ConsumedFailureCount++;
    }

    public void RecordMessageRetried(string queueName, string messageType, int attemptNumber)
    {
        RetriedCount++;
    }

    public void RecordMessageDeadLettered(string queueName, string messageType, string reason)
    {
        DeadLetteredCount++;
    }

    public TraceContext? ExtractTraceContext(IDictionary<string, object> headers)
    {
        if (headers.TryGetValue("traceparent", out var value) && value is string traceparent)
        {
            var parts = traceparent.Split('-');
            if (parts.Length >= 3)
                return new TraceContext(parts[1], parts[2]);
        }
        return null;
    }

    public void InjectTraceContext(IDictionary<string, object> headers)
    {
        headers["traceparent"] = "00-generated-trace-id-generated-span-id-01";
    }
}

public class OpenTelemetryProviderTests
{
    [Fact]
    public void When_Creating_Provider_With_Source_Name()
    {
        var provider = new OpenTelemetryProvider("ESE.MessageQueue");

        provider.Should().NotBeNull("it should create the provider");
    }

    [Fact]
    public void When_Starting_Publish_Activity_Without_Listener()
    {
        var provider = new OpenTelemetryProvider("ESE.MessageQueue.Tests.NoListener");

        using var activity = provider.StartPublishActivity("test-queue", "TestMessage");

        activity.Should().BeNull("it should return null when no listener is registered");
    }

    [Fact]
    public void When_Starting_Publish_Activity_With_Listener()
    {
        const string sourceName = "ESE.MessageQueue.Tests.WithListener";
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == sourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
        };
        ActivitySource.AddActivityListener(listener);
        var provider = new OpenTelemetryProvider(sourceName);

        using var activity = provider.StartPublishActivity("test-queue", "TestMessage");

        activity.Should().NotBeNull("it should create an activity when listener is registered");
        activity!.OperationName.Should().Be("publish", "it should set operation name");
    }

    [Fact]
    public void When_Recording_Metrics()
    {
        var provider = new OpenTelemetryProvider("ESE.MessageQueue.Tests");

        var act = () =>
        {
            provider.RecordMessagePublished("test-queue", "TestMessage");
            provider.RecordMessageConsumed("test-queue", "TestMessage", success: true);
            provider.RecordMessageRetried("test-queue", "TestMessage", attemptNumber: 1);
            provider.RecordMessageDeadLettered("test-queue", "TestMessage", "test reason");
        };

        act.Should().NotThrow("it should record metrics without error");
    }

    [Fact]
    public void When_Registering_Telemetry_Via_DI()
    {
        var services = new ServiceCollection();

        services.AddMessageQueueTelemetry("Test.Source");
        var provider = services.BuildServiceProvider();

        var telemetry = provider.GetService<ITelemetryProvider>();
        telemetry.Should().NotBeNull("it should register the telemetry provider");
        telemetry.Should().BeOfType<OpenTelemetryProvider>("it should use OpenTelemetry by default");
    }

    [Fact]
    public void When_Registering_Custom_Telemetry_Provider()
    {
        var services = new ServiceCollection();

        services.AddSingleton<ITelemetryProvider>(NullTelemetryProvider.Instance);
        var provider = services.BuildServiceProvider();

        var telemetry = provider.GetService<ITelemetryProvider>();
        telemetry.Should().Be(NullTelemetryProvider.Instance, "it should use the custom provider");
    }
}

public class MessageSerializerTests
{
    sealed record TestMessage(string Id, string Name, int Value);

    [Fact]
    public void When_Serializing_With_Json()
    {
        var message = new TestMessage("123", "Test", 42);

        var bytes = MessageSerializer.Serialize(message, SerializationFormat.Json);

        bytes.Should().NotBeEmpty("it should produce serialized bytes");
    }

    [Fact]
    public void When_Deserializing_With_Json()
    {
        var original = new TestMessage("123", "Test", 42);
        var bytes = MessageSerializer.Serialize(original, SerializationFormat.Json);

        var result = MessageSerializer.Deserialize<TestMessage>(bytes, SerializationFormat.Json);

        result.Should().NotBeNull("it should deserialize the message");
        result!.Id.Should().Be("123", "it should preserve the Id");
        result.Name.Should().Be("Test", "it should preserve the Name");
        result.Value.Should().Be(42, "it should preserve the Value");
    }

    [Fact]
    public void When_Getting_ContentType_For_Json()
    {
        var contentType = MessageSerializer.GetContentType(SerializationFormat.Json);

        contentType.Should().Be("application/json", "it should return the JSON content type");
    }

    [Fact]
    public void When_Deserializing_Invalid_Json()
    {
        var invalidBytes = System.Text.Encoding.UTF8.GetBytes("not valid json");

        var act = () => MessageSerializer.Deserialize<TestMessage>(invalidBytes, SerializationFormat.Json);

        act.Should().Throw<JsonException>("it should throw on invalid JSON");
    }

    [Fact]
    public void When_Serializing_With_MessagePack()
    {
        var message = new PublicTestMessage("123", "Test", 42);

        var bytes = MessageSerializer.Serialize(message, SerializationFormat.MessagePack);

        bytes.Should().NotBeEmpty("it should produce serialized bytes");
    }

    [Fact]
    public void When_Deserializing_With_MessagePack()
    {
        var original = new PublicTestMessage("123", "Test", 42);
        var bytes = MessageSerializer.Serialize(original, SerializationFormat.MessagePack);

        var result = MessageSerializer.Deserialize<PublicTestMessage>(bytes, SerializationFormat.MessagePack);

        result.Should().NotBeNull("it should deserialize the message");
        result!.Id.Should().Be("123", "it should preserve the Id");
        result.Name.Should().Be("Test", "it should preserve the Name");
        result.Value.Should().Be(42, "it should preserve the Value");
    }

    [Fact]
    public void When_Getting_ContentType_For_MessagePack()
    {
        var contentType = MessageSerializer.GetContentType(SerializationFormat.MessagePack);

        contentType.Should().Be("application/x-msgpack", "it should return the MessagePack content type");
    }

    [Fact]
    public void When_Comparing_MessagePack_To_Json_Size()
    {
        var message = new PublicTestMessage("123", "Test", 42);

        var jsonBytes = MessageSerializer.Serialize(message, SerializationFormat.Json);
        var msgpackBytes = MessageSerializer.Serialize(message, SerializationFormat.MessagePack);

        msgpackBytes.Length.Should().BeLessThan(jsonBytes.Length, "it should produce smaller payloads than JSON");
    }

    [Fact]
    public void When_Configuring_MessagePack()
    {
        MessageSerializer.MsgPackOptions.Security.Should().BeSameAs(MessagePackSecurity.UntrustedData,
            "it should treat broker payloads as untrusted data");
    }

    [Fact]
    public void When_Deserializing_A_Deeply_Nested_MessagePack_Payload()
    {
        var hostilePayload = BuildNestedPayload(depth: 1_000);

        var act = () => MessageSerializer.Deserialize<NestedNode>(hostilePayload, SerializationFormat.MessagePack);

        act.Should().Throw<MessagePackSerializationException>(
            "it should reject a hostile object graph instead of overflowing the stack");
    }

    [Fact]
    public void When_Deserializing_A_MessagePack_Dictionary()
    {
        var original = new KeyedMessage { Counts = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 } };
        var bytes = MessageSerializer.Serialize(original, SerializationFormat.MessagePack);

        var result = MessageSerializer.Deserialize<KeyedMessage>(bytes, SerializationFormat.MessagePack);

        result!.Counts.Should().BeEquivalentTo(original.Counts, "it should round-trip the dictionary");
        result.Counts.Comparer.Should().NotBeSameAs(EqualityComparer<string>.Default,
            "it should use a hash-collision resistant comparer for untrusted keys");
    }

    static byte[] BuildNestedPayload(int depth)
    {
        var childKey = "Child"u8.ToArray();
        var payload = new List<byte>(depth * (2 + childKey.Length) + 1);
        for (var i = 0; i < depth; i++)
        {
            payload.Add(0x81);
            payload.Add((byte)(0xA0 | childKey.Length));
            payload.AddRange(childKey);
        }
        payload.Add(0xC0);
        return [.. payload];
    }
}

public sealed class KeyedMessage
{
    public Dictionary<string, int> Counts { get; set; } = [];
}

public sealed class NestedNode
{
    public NestedNode? Child { get; set; }
}

public class RetryPolicyTests
{
    [Fact]
    public void When_Using_Default_Policy()
    {
        var policy = RetryPolicy.Default;

        policy.Strategy.Should().Be(RetryStrategy.Exponential, "it should use exponential backoff");
        policy.MaxAttempts.Should().Be(3, "it should allow 3 attempts");
        policy.InitialDelay.Should().Be(TimeSpan.FromSeconds(1), "it should start with 1 second delay");
        policy.BackoffMultiplier.Should().Be(2.0, "it should double the delay each time");
        policy.UseJitter.Should().BeTrue("it should add jitter by default");
    }

    [Fact]
    public void When_Using_None_Policy()
    {
        var policy = RetryPolicy.None;

        policy.MaxAttempts.Should().Be(0, "it should not retry");
    }

    [Fact]
    public void When_Using_Aggressive_Policy()
    {
        var policy = RetryPolicy.Aggressive;

        policy.MaxAttempts.Should().Be(5, "it should allow more attempts");
        policy.InitialDelay.Should().BeLessThan(TimeSpan.FromSeconds(1), "it should start with a shorter delay");
    }
}

public class RetryExecutorTests
{
    [Fact]
    public void When_Calculating_Delay_With_Fixed_Strategy()
    {
        var policy = new RetryPolicy
        {
            Strategy = RetryStrategy.Fixed,
            InitialDelay = TimeSpan.FromSeconds(5),
            UseJitter = false
        };
        var executor = new RetryExecutor(policy);

        var delay1 = executor.CalculateDelay(1);
        var delay2 = executor.CalculateDelay(2);
        var delay3 = executor.CalculateDelay(3);

        delay1.Should().Be(TimeSpan.FromSeconds(5), "it should return the same delay for attempt 1");
        delay2.Should().Be(TimeSpan.FromSeconds(5), "it should return the same delay for attempt 2");
        delay3.Should().Be(TimeSpan.FromSeconds(5), "it should return the same delay for attempt 3");
    }

    [Fact]
    public void When_Calculating_Delay_With_Linear_Strategy()
    {
        var policy = new RetryPolicy
        {
            Strategy = RetryStrategy.Linear,
            InitialDelay = TimeSpan.FromSeconds(1),
            UseJitter = false
        };
        var executor = new RetryExecutor(policy);

        var delay1 = executor.CalculateDelay(1);
        var delay2 = executor.CalculateDelay(2);
        var delay3 = executor.CalculateDelay(3);

        delay1.Should().Be(TimeSpan.FromSeconds(1), "it should return 1 second for attempt 1");
        delay2.Should().Be(TimeSpan.FromSeconds(2), "it should return 2 seconds for attempt 2");
        delay3.Should().Be(TimeSpan.FromSeconds(3), "it should return 3 seconds for attempt 3");
    }

    [Fact]
    public void When_Calculating_Delay_With_Exponential_Strategy()
    {
        var policy = new RetryPolicy
        {
            Strategy = RetryStrategy.Exponential,
            InitialDelay = TimeSpan.FromSeconds(1),
            BackoffMultiplier = 2.0,
            UseJitter = false
        };
        var executor = new RetryExecutor(policy);

        var delay1 = executor.CalculateDelay(1);
        var delay2 = executor.CalculateDelay(2);
        var delay3 = executor.CalculateDelay(3);

        delay1.Should().Be(TimeSpan.FromSeconds(1), "it should return 1 second for attempt 1");
        delay2.Should().Be(TimeSpan.FromSeconds(2), "it should return 2 seconds for attempt 2");
        delay3.Should().Be(TimeSpan.FromSeconds(4), "it should return 4 seconds for attempt 3");
    }

    [Fact]
    public void When_Calculating_Delay_Exceeds_MaxDelay()
    {
        var policy = new RetryPolicy
        {
            Strategy = RetryStrategy.Exponential,
            InitialDelay = TimeSpan.FromSeconds(1),
            MaxDelay = TimeSpan.FromSeconds(5),
            UseJitter = false
        };
        var executor = new RetryExecutor(policy);

        var delay10 = executor.CalculateDelay(10);

        delay10.Should().Be(TimeSpan.FromSeconds(5), "it should cap at the max delay");
    }

    [Fact]
    public void When_Calculating_Delay_With_Custom_Strategy()
    {
        var policy = new RetryPolicy
        {
            Strategy = RetryStrategy.Custom,
            CustomDelayCalculator = attempt => TimeSpan.FromSeconds(attempt * 10),
            UseJitter = false
        };
        var executor = new RetryExecutor(policy);

        var delay1 = executor.CalculateDelay(1);
        var delay2 = executor.CalculateDelay(2);

        delay1.Should().Be(TimeSpan.FromSeconds(10), "it should use the custom calculator for attempt 1");
        delay2.Should().Be(TimeSpan.FromSeconds(20), "it should use the custom calculator for attempt 2");
    }

    [Fact]
    public void When_MaxAttempts_Is_Reached()
    {
        var policy = new RetryPolicy { MaxAttempts = 3 };
        var executor = new RetryExecutor(policy);

        executor.ShouldRetry(1).Should().BeTrue("it should allow retry on attempt 1");
        executor.ShouldRetry(2).Should().BeTrue("it should allow retry on attempt 2");
        executor.ShouldRetry(3).Should().BeFalse("it should not allow retry after max attempts");
    }

    [Fact]
    public void When_Checking_If_Exception_Is_Retryable()
    {
        var policy = new RetryPolicy
        {
            MaxAttempts = 5,
            IsRetryable = ex => ex is InvalidOperationException
        };
        var executor = new RetryExecutor(policy);

        executor.ShouldRetry(1, new InvalidOperationException()).Should().BeTrue("it should retry InvalidOperationException");
        executor.ShouldRetry(1, new ArgumentException()).Should().BeFalse("it should not retry ArgumentException");
    }

    [Fact]
    public async Task When_Executing_With_Transient_Failures()
    {
        var policy = new RetryPolicy
        {
            MaxAttempts = 3,
            InitialDelay = TimeSpan.FromMilliseconds(10),
            UseJitter = false
        };
        var executor = new RetryExecutor(policy);
        var attempts = 0;

        var result = await executor.ExecuteAsync<int>(_ =>
        {
            attempts++;
            if (attempts < 3)
                throw new InvalidOperationException("Transient failure");
            return Task.FromResult(42);
        });

        result.Should().Be(42, "it should return the result after successful retry");
        attempts.Should().Be(3, "it should have attempted 3 times");
    }

    [Fact]
    public async Task When_Executing_With_Permanent_Failure()
    {
        var policy = new RetryPolicy
        {
            MaxAttempts = 2,
            InitialDelay = TimeSpan.FromMilliseconds(10),
            UseJitter = false
        };
        var executor = new RetryExecutor(policy);
        var attempts = 0;

        var act = async () => await executor.ExecuteAsync<int>(_ =>
        {
            attempts++;
            throw new InvalidOperationException("Permanent failure");
        });

        await act.Should().ThrowAsync<InvalidOperationException>("it should throw after exhausting retries");
        attempts.Should().Be(2, "it should have attempted the max number of times");
    }
}

public class HealthCheckTests
{
    [Fact]
    public async Task When_Health_Check_Succeeds()
    {
        var healthCheck = new MessageQueueHealthCheck("test-queue", _ => Task.FromResult(true));
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("test", healthCheck, null, null)
        };

        var result = await healthCheck.CheckHealthAsync(context);

        result.Status.Should().Be(HealthStatus.Healthy, "it should report healthy status");
        result.Description.Should().Contain("test-queue", "it should include the queue name in the description");
    }

    [Fact]
    public async Task When_Health_Check_Fails()
    {
        var healthCheck = new MessageQueueHealthCheck("test-queue", _ => Task.FromResult(false));
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("test", healthCheck, null, null)
        };

        var result = await healthCheck.CheckHealthAsync(context);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should report unhealthy status");
    }

    [Fact]
    public async Task When_Health_Check_Throws_Exception()
    {
        var healthCheck = new MessageQueueHealthCheck("test-queue",
            _ => throw new InvalidOperationException("Connection failed"));
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("test", healthCheck, null, null)
        };

        var result = await healthCheck.CheckHealthAsync(context);

        result.Status.Should().Be(HealthStatus.Unhealthy, "it should report unhealthy status on exception");
        result.Exception.Should().BeOfType<InvalidOperationException>("it should include the exception");
    }

    [Fact]
    public void When_Registering_Health_Check_Via_DI()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks()
            .AddMessageQueueHealthCheck("my-queue", _ => Task.FromResult(true));

        var provider = services.BuildServiceProvider();
        var healthCheckService = provider.GetService<HealthCheckService>();

        healthCheckService.Should().NotBeNull("it should register the health check service");
    }
}

public class InMemoryMessageQueueTests
{
    sealed record TestMessage(string Id, string Content);

    [Fact]
    public async Task When_Publishing_A_Single_Message()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();
        var message = new TestMessage("1", "Hello");

        await queue.PublishAsync(message);

        queue.Count.Should().Be(1, "it should add the message to the queue");
        queue.PublishedMessages.Should().ContainSingle().Which.Should().Be(message, "it should store the published message");
    }

    [Fact]
    public async Task When_Publishing_A_Batch_Of_Messages()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();
        var messages = new[]
        {
            new TestMessage("1", "First"),
            new TestMessage("2", "Second"),
            new TestMessage("3", "Third")
        };

        await queue.PublishAsync(messages);

        queue.Count.Should().Be(3, "it should add all messages to the queue");
        queue.PublishedMessages.Should().HaveCount(3, "it should store all published messages");
    }

    [Fact]
    public async Task When_Subscribing_To_Messages()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();

        await queue.SubscribeAsync(_ => Task.FromResult(true));

        queue.IsSubscribed.Should().BeTrue("it should register the handler");
    }

    [Fact]
    public async Task When_Processing_All_Messages()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();
        var processed = new List<TestMessage>();

        await queue.PublishAsync(new TestMessage("1", "First"));
        await queue.PublishAsync(new TestMessage("2", "Second"));

        await queue.SubscribeAsync(msg =>
        {
            processed.Add(msg);
            return Task.FromResult(true);
        });

        await queue.ProcessAllAsync();

        processed.Should().HaveCount(2, "it should process all messages");
        queue.ProcessedMessages.Should().HaveCount(2, "it should track processed messages");
        queue.Count.Should().Be(0, "it should remove processed messages from the queue");
    }

    [Fact]
    public async Task When_Processing_One_Message()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();

        await queue.PublishAsync(new TestMessage("1", "First"));
        await queue.PublishAsync(new TestMessage("2", "Second"));

        await queue.SubscribeAsync(_ => Task.FromResult(true));

        var result = await queue.ProcessOneAsync();

        result.Should().BeTrue("it should return true on successful processing");
        queue.ProcessedMessages.Should().HaveCount(1, "it should process only one message");
        queue.Count.Should().Be(1, "it should leave remaining messages in the queue");
    }

    [Fact]
    public async Task When_Handler_Returns_False()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();

        await queue.PublishAsync(new TestMessage("1", "Will fail"));

        await queue.SubscribeAsync(_ => Task.FromResult(false));

        var result = await queue.ProcessOneAsync();

        result.Should().BeFalse("it should return false when handler fails");
        queue.ProcessedMessages.Should().BeEmpty("it should not mark failed messages as processed");
    }

    [Fact]
    public async Task When_Clearing_The_Queue()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();
        await queue.PublishAsync(new TestMessage("1", "Test"));

        queue.Clear();

        queue.Count.Should().Be(0, "it should remove all messages");
        queue.PublishedMessages.Should().BeEmpty("it should clear published messages");
        queue.ProcessedMessages.Should().BeEmpty("it should clear processed messages");
    }

    [Fact]
    public void When_Registering_Via_DI()
    {
        var services = new ServiceCollection();
        services.AddInMemoryMessageQueue<TestMessage>();

        var provider = services.BuildServiceProvider();

        var queue = provider.GetService<IMessageQueue<TestMessage>>();
        var inMemory = provider.GetService<InMemoryMessageQueue<TestMessage>>();

        queue.Should().NotBeNull("it should register IMessageQueue");
        inMemory.Should().NotBeNull("it should register InMemoryMessageQueue");
        queue.Should().BeSameAs(inMemory, "it should use the same instance for both registrations");
    }
}
