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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests;

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

public class JsonMessageSerializerTests
{
    record TestMessage(string Id, string Name, int Value);

    [Fact]
    public void When_Serializing_Message_Returns_Bytes()
    {
        var serializer = new JsonMessageSerializer();
        var message = new TestMessage("123", "Test", 42);

        var bytes = serializer.Serialize(message);

        bytes.Should().NotBeEmpty("it should produce serialized bytes");
    }

    [Fact]
    public void When_Deserializing_Message_Returns_Object()
    {
        var serializer = new JsonMessageSerializer();
        var original = new TestMessage("123", "Test", 42);
        var bytes = serializer.Serialize(original);

        var result = serializer.Deserialize<TestMessage>(bytes);

        result.Should().NotBeNull("it should deserialize the message");
        result!.Id.Should().Be("123");
        result.Name.Should().Be("Test");
        result.Value.Should().Be(42);
    }

    [Fact]
    public void When_Using_Custom_Options_Respects_Settings()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = null, // PascalCase
            WriteIndented = true
        };
        var serializer = new JsonMessageSerializer(options);
        var message = new TestMessage("123", "Test", 42);

        var bytes = serializer.Serialize(message);
        var json = System.Text.Encoding.UTF8.GetString(bytes);

        json.Should().Contain("\"Id\"", "it should use PascalCase");
        json.Should().Contain("\n", "it should be indented");
    }

    [Fact]
    public void ContentType_Returns_ApplicationJson()
    {
        var serializer = new JsonMessageSerializer();

        serializer.ContentType.Should().Be("application/json");
    }

    [Fact]
    public void When_Deserializing_Invalid_Json_Returns_Null_Or_Throws()
    {
        var serializer = new JsonMessageSerializer();
        var invalidBytes = System.Text.Encoding.UTF8.GetBytes("not valid json");

        var act = () => serializer.Deserialize<TestMessage>(invalidBytes);

        act.Should().Throw<JsonException>("it should throw on invalid JSON");
    }
}

public class RetryPolicyTests
{
    [Fact]
    public void Default_Policy_Has_Expected_Values()
    {
        var policy = RetryPolicy.Default;

        policy.Strategy.Should().Be(RetryStrategy.Exponential);
        policy.MaxAttempts.Should().Be(3);
        policy.InitialDelay.Should().Be(TimeSpan.FromSeconds(1));
        policy.BackoffMultiplier.Should().Be(2.0);
        policy.UseJitter.Should().BeTrue();
    }

    [Fact]
    public void None_Policy_Has_Zero_Attempts()
    {
        var policy = RetryPolicy.None;

        policy.MaxAttempts.Should().Be(0);
    }

    [Fact]
    public void Aggressive_Policy_Has_More_Attempts()
    {
        var policy = RetryPolicy.Aggressive;

        policy.MaxAttempts.Should().Be(5);
        policy.InitialDelay.Should().BeLessThan(TimeSpan.FromSeconds(1));
    }
}

public class RetryExecutorTests
{
    [Fact]
    public void CalculateDelay_Fixed_Returns_Same_Delay()
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

        delay1.Should().Be(TimeSpan.FromSeconds(5));
        delay2.Should().Be(TimeSpan.FromSeconds(5));
        delay3.Should().Be(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void CalculateDelay_Linear_Increases_Linearly()
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

        delay1.Should().Be(TimeSpan.FromSeconds(1));
        delay2.Should().Be(TimeSpan.FromSeconds(2));
        delay3.Should().Be(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public void CalculateDelay_Exponential_Doubles_Each_Time()
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

        delay1.Should().Be(TimeSpan.FromSeconds(1));
        delay2.Should().Be(TimeSpan.FromSeconds(2));
        delay3.Should().Be(TimeSpan.FromSeconds(4));
    }

    [Fact]
    public void CalculateDelay_Respects_MaxDelay()
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

        delay10.Should().Be(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void CalculateDelay_Custom_Uses_Calculator()
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

        delay1.Should().Be(TimeSpan.FromSeconds(10));
        delay2.Should().Be(TimeSpan.FromSeconds(20));
    }

    [Fact]
    public void ShouldRetry_Returns_False_When_MaxAttempts_Reached()
    {
        var policy = new RetryPolicy { MaxAttempts = 3 };
        var executor = new RetryExecutor(policy);

        executor.ShouldRetry(1).Should().BeTrue();
        executor.ShouldRetry(2).Should().BeTrue();
        executor.ShouldRetry(3).Should().BeFalse();
    }

    [Fact]
    public void ShouldRetry_Uses_IsRetryable_Predicate()
    {
        var policy = new RetryPolicy
        {
            MaxAttempts = 5,
            IsRetryable = ex => ex is InvalidOperationException
        };
        var executor = new RetryExecutor(policy);

        executor.ShouldRetry(1, new InvalidOperationException()).Should().BeTrue();
        executor.ShouldRetry(1, new ArgumentException()).Should().BeFalse();
    }

    [Fact]
    public async Task ExecuteAsync_Retries_On_Failure()
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

        result.Should().Be(42);
        attempts.Should().Be(3);
    }

    [Fact]
    public async Task ExecuteAsync_Throws_After_MaxAttempts()
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

        await act.Should().ThrowAsync<InvalidOperationException>();
        attempts.Should().Be(2);
    }
}

public class HealthCheckTests
{
    [Fact]
    public async Task MessageQueueHealthCheck_Returns_Healthy_When_Check_Succeeds()
    {
        var healthCheck = new MessageQueueHealthCheck("test-queue", _ => Task.FromResult(true));
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("test", healthCheck, null, null)
        };

        var result = await healthCheck.CheckHealthAsync(context);

        result.Status.Should().Be(HealthStatus.Healthy);
        result.Description.Should().Contain("test-queue");
    }

    [Fact]
    public async Task MessageQueueHealthCheck_Returns_Unhealthy_When_Check_Fails()
    {
        var healthCheck = new MessageQueueHealthCheck("test-queue", _ => Task.FromResult(false));
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("test", healthCheck, null, null)
        };

        var result = await healthCheck.CheckHealthAsync(context);

        result.Status.Should().Be(HealthStatus.Unhealthy);
    }

    [Fact]
    public async Task MessageQueueHealthCheck_Returns_Unhealthy_On_Exception()
    {
        var healthCheck = new MessageQueueHealthCheck("test-queue",
            _ => throw new InvalidOperationException("Connection failed"));
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("test", healthCheck, null, null)
        };

        var result = await healthCheck.CheckHealthAsync(context);

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Exception.Should().BeOfType<InvalidOperationException>();
    }

    [Fact]
    public void HealthCheckExtensions_Registers_Custom_Check()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHealthChecks()
            .AddMessageQueueHealthCheck("my-queue", _ => Task.FromResult(true));

        var provider = services.BuildServiceProvider();
        var healthCheckService = provider.GetService<HealthCheckService>();

        healthCheckService.Should().NotBeNull();
    }
}

public class InMemoryMessageQueueTests
{
    record TestMessage(string Id, string Content);

    [Fact]
    public async Task PublishAsync_Adds_Message_To_Queue()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();
        var message = new TestMessage("1", "Hello");

        await queue.PublishAsync(message);

        queue.Count.Should().Be(1);
        queue.PublishedMessages.Should().ContainSingle().Which.Should().Be(message);
    }

    [Fact]
    public async Task PublishAsync_Batch_Adds_All_Messages()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();
        var messages = new[]
        {
            new TestMessage("1", "First"),
            new TestMessage("2", "Second"),
            new TestMessage("3", "Third")
        };

        await queue.PublishAsync(messages);

        queue.Count.Should().Be(3);
        queue.PublishedMessages.Should().HaveCount(3);
    }

    [Fact]
    public async Task SubscribeAsync_Registers_Handler()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();

        await queue.SubscribeAsync(_ => Task.FromResult(true));

        queue.IsSubscribed.Should().BeTrue();
    }

    [Fact]
    public async Task ProcessAllAsync_Processes_All_Messages()
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

        processed.Should().HaveCount(2);
        queue.ProcessedMessages.Should().HaveCount(2);
        queue.Count.Should().Be(0);
    }

    [Fact]
    public async Task ProcessOneAsync_Processes_Single_Message()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();

        await queue.PublishAsync(new TestMessage("1", "First"));
        await queue.PublishAsync(new TestMessage("2", "Second"));

        await queue.SubscribeAsync(_ => Task.FromResult(true));

        var result = await queue.ProcessOneAsync();

        result.Should().BeTrue();
        queue.ProcessedMessages.Should().HaveCount(1);
        queue.Count.Should().Be(1);
    }

    [Fact]
    public async Task ProcessOneAsync_Returns_False_On_Handler_Failure()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();

        await queue.PublishAsync(new TestMessage("1", "Will fail"));

        await queue.SubscribeAsync(_ => Task.FromResult(false));

        var result = await queue.ProcessOneAsync();

        result.Should().BeFalse("handler returned false");
        queue.ProcessedMessages.Should().BeEmpty("failed messages are not marked as processed");
    }

    [Fact]
    public async Task Clear_Removes_All_Messages()
    {
        var queue = new InMemoryMessageQueue<TestMessage>();
        await queue.PublishAsync(new TestMessage("1", "Test"));

        queue.Clear();

        queue.Count.Should().Be(0);
        queue.PublishedMessages.Should().BeEmpty();
        queue.ProcessedMessages.Should().BeEmpty();
    }

    [Fact]
    public void DI_Registration_Works()
    {
        var services = new ServiceCollection();
        services.AddInMemoryMessageQueue<TestMessage>();

        var provider = services.BuildServiceProvider();

        var queue = provider.GetService<IMessageQueue<TestMessage>>();
        var inMemory = provider.GetService<InMemoryMessageQueue<TestMessage>>();

        queue.Should().NotBeNull();
        inMemory.Should().NotBeNull();

        // Both should be the same instance
        queue.Should().BeSameAs(inMemory);
    }
}
