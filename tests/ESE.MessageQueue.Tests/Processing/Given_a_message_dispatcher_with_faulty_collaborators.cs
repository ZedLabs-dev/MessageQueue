using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Providers;
using ESE.MessageQueue.Retry;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Telemetry;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests.Processing;

// Telemetry, logging and user-supplied retry callbacks must never make a message disappear: the dispatcher
// still settles every delivery exactly once.
[Trait("Category", "Unit")]
public class Given_a_message_dispatcher_with_faulty_collaborators
{
    static readonly RetryPolicy ThreeRetries = new()
    {
        MaxRetries = 3,
        InitialDelay = TimeSpan.FromSeconds(2),
        MaxDelay = TimeSpan.FromSeconds(30),
        UseJitter = false
    };

    readonly FakeDeliveryActions actions = new();
    readonly TestMessage message = new("order-1", "content");

    Task<DispatchOutcome> Dispatch(
        MessageResult result,
        RetryPolicy? retry = null,
        ITelemetryProvider? telemetry = null,
        ILogger? logger = null,
        int attempt = 1,
        Exception? throws = null,
        bool enableDeadLetterQueue = true)
    {
        var config = new InMemoryConfig { QueueName = "orders", Retry = retry ?? ThreeRetries, EnableDeadLetterQueue = enableDeadLetterQueue };
        var dispatcher = new MessageDispatcher<TestMessage>(config, new ESE.MessageQueue.Telemetry.TelemetryTarget("test", "orders", nameof(TestMessage)),
            (_, _, _) => throws is null ? Task.FromResult(result) : throw throws, telemetry, logger);
        return dispatcher.DispatchAsync(MessageSerializer.Serialize(message, SerializationFormat.Json),
            new MessageContext { MessageId = "m-1", DeliveryAttempt = attempt }, actions, TestContext.Current.CancellationToken);
    }

    [Theory]
    [InlineData(MessageResult.Acknowledge, "Complete")]
    [InlineData(MessageResult.Retry, "Reschedule")]
    [InlineData(MessageResult.DeadLetter, "DeadLetter")]
    public async Task When_Telemetry_Throws(MessageResult result, string expectedAction)
    {
        await Dispatch(result, telemetry: new ThrowingTelemetryProvider());

        actions.Calls.Should().Equal([expectedAction], "it should settle the delivery even when every telemetry call throws");
    }

    [Fact]
    public async Task When_Telemetry_Throws_On_The_Last_Attempt()
    {
        await Dispatch(MessageResult.Retry, telemetry: new ThrowingTelemetryProvider(), attempt: 4);

        actions.Calls.Should().Equal(["DeadLetter"], "it should still dead-letter when the dead-letter metric throws");
    }

    [Theory]
    [InlineData(MessageResult.Acknowledge, "Complete")]
    [InlineData(MessageResult.Retry, "Reschedule")]
    [InlineData(MessageResult.DeadLetter, "DeadLetter")]
    public async Task When_The_Logger_Throws(MessageResult result, string expectedAction)
    {
        await Dispatch(result, logger: new ThrowingLogger(), throws: result == MessageResult.Retry ? new TimeoutException() : null);

        actions.Calls.Should().Equal([expectedAction], "it should settle the delivery even when logging throws");
    }

    [Fact]
    public async Task When_Logging_A_Drop_Throws()
    {
        await Dispatch(MessageResult.DeadLetter, logger: new ThrowingLogger(), enableDeadLetterQueue: false);

        actions.Calls.Should().Equal(["Complete"], "it should still drop (complete) the message when the warning cannot be logged");
    }

    [Fact]
    public async Task When_IsRetryable_Throws()
    {
        var outcome = await Dispatch(MessageResult.Retry,
            retry: ThreeRetries with { IsRetryable = _ => throw new InvalidOperationException("predicate bug") },
            throws: new TimeoutException());

        outcome.Should().Be(DispatchOutcome.DeadLettered, "it should treat a failing IsRetryable as non-retryable");
        actions.DeadLetterReason!.Value.Code.Should().Be(DeadLetterReasons.NonRetryableError, "it should say why");
    }

    [Fact]
    public async Task When_The_Custom_Delay_Calculator_Throws()
    {
        var logger = new RecordingLogger();

        var outcome = await Dispatch(MessageResult.Retry,
            retry: ThreeRetries with { Strategy = RetryStrategy.Custom, CustomDelayCalculator = _ => throw new InvalidOperationException() },
            logger: logger);

        outcome.Should().Be(DispatchOutcome.Rescheduled, "it should still retry the message");
        actions.RescheduleDelay.Should().Be(TimeSpan.FromSeconds(2), "it should fall back to InitialDelay");
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Exception is InvalidOperationException,
            "it should log the broken calculator");
    }

    [Fact]
    public async Task When_IsRetryable_Refuses_On_The_Last_Retry()
    {
        var outcome = await Dispatch(MessageResult.Retry,
            retry: ThreeRetries with { IsRetryable = _ => false },
            attempt: 3,
            throws: new ArgumentException());

        outcome.Should().Be(DispatchOutcome.DeadLettered, "it should dead-letter a non-retryable failure");
        actions.DeadLetterReason!.Value.Code.Should().Be(DeadLetterReasons.NonRetryableError,
            "it should blame the exception, not the retry budget, while a retry was still allowed");
    }
}
