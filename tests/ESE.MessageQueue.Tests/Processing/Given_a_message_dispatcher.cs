using System.Text;
using AwesomeAssertions;
using ESE.MessageQueue.Abstractions;
using ESE.MessageQueue.InMemory;
using ESE.MessageQueue.Processing;
using ESE.MessageQueue.Providers;
using ESE.MessageQueue.Retry;
using ESE.MessageQueue.Serialization;
using ESE.MessageQueue.Tests.Serialization;
using ESE.MessageQueue.Tests.Telemetry;
using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests.Processing;

[Trait("Category", "Unit")]
public class Given_a_message_dispatcher
{
    static readonly RetryPolicy ThreeRetries = new()
    {
        MaxRetries = 3,
        InitialDelay = TimeSpan.FromSeconds(2),
        BackoffMultiplier = 2,
        MaxDelay = TimeSpan.FromSeconds(30),
        UseJitter = false
    };

    readonly InMemoryConfig config = new() { QueueName = "orders", Retry = ThreeRetries };
    readonly FakeDeliveryActions actions = new();
    readonly TestTelemetryProvider telemetry = new();
    readonly RecordingLogger logger = new();
    readonly TestMessage message = new("order-1", "content");
    int handlerCalls;

    MessageDispatcher<TestMessage> Dispatcher(
        Func<TestMessage, MessageContext, CancellationToken, Task<MessageResult>> handler,
        InMemoryConfig? overrides = null) =>
        new(overrides ?? config, new ESE.MessageQueue.Telemetry.TelemetryTarget("test", "orders", nameof(TestMessage)), async (m, c, ct) =>
        {
            handlerCalls++;
            return await handler(m, c, ct);
        }, telemetry, logger);

    Task<DispatchOutcome> Dispatch(MessageDispatcher<TestMessage> dispatcher, int attempt = 1, ReadOnlyMemory<byte>? body = null) =>
        DispatchUntil(dispatcher, TestContext.Current.CancellationToken, attempt, body);

    Task<DispatchOutcome> DispatchUntil(
        MessageDispatcher<TestMessage> dispatcher,
        CancellationToken stopping,
        int attempt = 1,
        ReadOnlyMemory<byte>? body = null) =>
        dispatcher.DispatchAsync(
            body ?? MessageSerializer.Serialize(message, SerializationFormat.Json),
            new MessageContext { MessageId = "m-1", DeliveryAttempt = attempt },
            actions,
            stopping);

    static Func<TestMessage, MessageContext, CancellationToken, Task<MessageResult>> Result(MessageResult result) =>
        (_, _, _) => Task.FromResult(result);

    static Func<TestMessage, MessageContext, CancellationToken, Task<MessageResult>> Throws(Exception exception) =>
        (_, _, _) => throw exception;

    [Fact]
    public async Task When_Handler_Acknowledges()
    {
        var outcome = await Dispatch(Dispatcher(Result(MessageResult.Acknowledge)));

        outcome.Should().Be(DispatchOutcome.Completed, "it should complete an acknowledged message");
        actions.Calls.Should().Equal(["Complete"], "it should settle the delivery exactly once, by completing it");
        telemetry.ConsumedSuccessCount.Should().Be(1, "it should record a successful consume");
    }

    [Fact]
    public async Task When_Handler_Receives_The_Message_And_Context()
    {
        TestMessage? received = null;
        MessageContext? receivedContext = null;
        var context = new MessageContext
        {
            MessageId = "m-42",
            CorrelationId = "c-1",
            DeliveryAttempt = 2,
            Headers = new Dictionary<string, string> { ["tenant"] = "a" }
        };
        var dispatcher = Dispatcher((m, c, _) =>
        {
            received = m;
            receivedContext = c;
            return Task.FromResult(MessageResult.Acknowledge);
        });

        await dispatcher.DispatchAsync(MessageSerializer.Serialize(message, SerializationFormat.Json), context, actions,
            TestContext.Current.CancellationToken);

        received.Should().Be(message, "it should pass the deserialized message to the handler");
        receivedContext.Should().BeSameAs(context, "it should pass the transport's metadata through unchanged");
    }

    [Fact]
    public async Task When_Handler_Requests_A_Retry()
    {
        var outcome = await Dispatch(Dispatcher(Result(MessageResult.Retry)), attempt: 1);

        outcome.Should().Be(DispatchOutcome.Rescheduled, "it should reschedule while retries remain");
        actions.Calls.Should().Equal(["Reschedule"], "it should only reschedule");
        actions.RescheduleDelay.Should().Be(TimeSpan.FromSeconds(2), "it should wait the policy's first backoff delay");
        actions.NextAttempt.Should().Be(2, "it should redeliver as attempt 2");
        telemetry.RetriedCount.Should().Be(1, "it should record the retry");
        telemetry.ConsumedFailureCount.Should().Be(1, "it should record an unsuccessful consume");
    }

    [Fact]
    public async Task When_Retrying_A_Later_Attempt()
    {
        await Dispatch(Dispatcher(Result(MessageResult.Retry)), attempt: 3);

        actions.RescheduleDelay.Should().Be(TimeSpan.FromSeconds(8), "it should back off exponentially (2s, 4s, 8s)");
        actions.NextAttempt.Should().Be(4, "it should redeliver as the next attempt");
    }

    [Fact]
    public async Task When_Handler_Requests_A_Retry_On_The_Last_Attempt()
    {
        var outcome = await Dispatch(Dispatcher(Result(MessageResult.Retry)), attempt: 4);

        outcome.Should().Be(DispatchOutcome.DeadLettered, "it should dead-letter once MaxRetries retries were made");
        actions.Calls.Should().Equal(["DeadLetter"], "it should not reschedule again");
        actions.DeadLetterReason!.Value.Code.Should().Be(DeadLetterReasons.MaxRetriesExceeded, "it should say why");
        telemetry.DeadLetterReasons.Should().Equal([DeadLetterReasons.MaxRetriesExceeded], "it should tag the metric with the reason code");
    }

    [Fact]
    public async Task When_Retries_Are_Disabled()
    {
        var outcome = await Dispatch(Dispatcher(Result(MessageResult.Retry), config with { Retry = RetryPolicy.None }));

        outcome.Should().Be(DispatchOutcome.DeadLettered, "it should dead-letter the first failure when MaxRetries is zero");
    }

    [Fact]
    public async Task When_Handler_Requests_A_DeadLetter()
    {
        var outcome = await Dispatch(Dispatcher(Result(MessageResult.DeadLetter)), attempt: 1);

        outcome.Should().Be(DispatchOutcome.DeadLettered, "it should dead-letter immediately");
        actions.Calls.Should().Equal(["DeadLetter"], "it should not retry a rejected message");
        actions.DeadLetterReason!.Value.Code.Should().Be(DeadLetterReasons.Rejected, "it should record that the handler rejected it");
    }

    [Fact]
    public async Task When_Handler_Throws()
    {
        var outcome = await Dispatch(Dispatcher(Throws(new TimeoutException("db password=hunter2"))), attempt: 1);

        outcome.Should().Be(DispatchOutcome.Rescheduled, "it should treat an exception as a retry");
        actions.NextAttempt.Should().Be(2, "it should redeliver as attempt 2");
        telemetry.Activities.Should().ContainSingle("it should start one consume span")
            .Which.Error.Should().BeOfType<TimeoutException>("it should mark the span as failed");
    }

    [Fact]
    public async Task When_Handler_Throws_On_The_Last_Attempt()
    {
        await Dispatch(Dispatcher(Throws(new TimeoutException("db password=hunter2"))), attempt: 4);

        var reason = actions.DeadLetterReason!.Value;
        reason.Code.Should().Be(DeadLetterReasons.MaxRetriesExceeded, "it should dead-letter after the last retry");
        reason.Detail.Should().Be(typeof(TimeoutException).FullName, "it should record the exception type");
        reason.Detail.Should().NotContain("hunter2", "it should never copy exception text to the broker");
    }

    [Fact]
    public async Task When_Handler_Throws_A_NonRetryable_Exception()
    {
        var policy = ThreeRetries with { IsRetryable = ex => ex is not ArgumentException };

        var outcome = await Dispatch(Dispatcher(Throws(new ArgumentException("bad")), config with { Retry = policy }), attempt: 1);

        outcome.Should().Be(DispatchOutcome.DeadLettered, "it should not retry what the policy says is permanent");
        actions.DeadLetterReason!.Value.Code.Should().Be(DeadLetterReasons.NonRetryableError, "it should say why");
        actions.DeadLetterReason!.Value.Detail.Should().Be(typeof(ArgumentException).FullName, "it should record the exception type");
    }

    [Fact]
    public async Task When_Handler_Throws_And_Logs()
    {
        await Dispatch(Dispatcher(Throws(new TimeoutException("boom"))), attempt: 1);

        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Exception is TimeoutException,
            "it should log the handler failure with the exception for the operator");
    }

    [Fact]
    public async Task When_Message_Exceeds_MaxMessageSizeBytes()
    {
        var oversized = Encoding.UTF8.GetBytes(new string('x', 101));

        var outcome = await Dispatch(Dispatcher(Result(MessageResult.Acknowledge), config with { MaxMessageSizeBytes = 100 }),
            body: oversized);

        outcome.Should().Be(DispatchOutcome.DeadLettered, "it should dead-letter an oversized message");
        actions.DeadLetterReason!.Value.Code.Should().Be(DeadLetterReasons.MessageTooLarge,
            "it should reject the size before trying to deserialize the (invalid JSON) body");
        handlerCalls.Should().Be(0, "it should not call the handler");
    }

    [Fact]
    public async Task When_Message_Is_Exactly_MaxMessageSizeBytes()
    {
        var body = MessageSerializer.Serialize(message, SerializationFormat.Json);

        var outcome = await Dispatch(Dispatcher(Result(MessageResult.Acknowledge), config with { MaxMessageSizeBytes = body.Length }),
            body: body);

        outcome.Should().Be(DispatchOutcome.Completed, "it should accept a message at the size limit");
    }

    [Fact]
    public void When_Using_The_Default_Size_Limit()
    {
        new InMemoryConfig().MaxMessageSizeBytes.Should().Be(1024 * 1024, "it should limit messages to 1 MB by default");
    }

    [Fact]
    public async Task When_Body_Cannot_Be_Deserialized()
    {
        var outcome = await Dispatch(Dispatcher(Result(MessageResult.Acknowledge)), body: "not json"u8.ToArray());

        outcome.Should().Be(DispatchOutcome.DeadLettered, "it should dead-letter a poison message instead of retrying it");
        actions.DeadLetterReason!.Value.Code.Should().Be(DeadLetterReasons.DeserializationFailed, "it should say why");
        actions.DeadLetterReason!.Value.Detail.Should().Be(typeof(System.Text.Json.JsonException).FullName,
            "it should record the exception type");
        handlerCalls.Should().Be(0, "it should not call the handler");
    }

    [Fact]
    public async Task When_Body_Deserializes_To_Null()
    {
        var outcome = await Dispatch(Dispatcher(Result(MessageResult.Acknowledge)), body: "null"u8.ToArray());

        outcome.Should().Be(DispatchOutcome.DeadLettered, "it should dead-letter a body that holds no message");
        actions.DeadLetterReason!.Value.Code.Should().Be(DeadLetterReasons.DeserializationFailed, "it should say why");
        handlerCalls.Should().Be(0, "it should not call the handler with null");
    }

    [Fact]
    public async Task When_Dead_Lettering_Is_Disabled_And_Retries_Are_Exhausted()
    {
        var outcome = await Dispatch(Dispatcher(Result(MessageResult.Retry), config with { EnableDeadLetterQueue = false }),
            attempt: 4);

        outcome.Should().Be(DispatchOutcome.Dropped, "it should drop the message when there is no dead-letter queue");
        actions.Calls.Should().Equal(["Complete"], "it should complete the delivery so the broker forgets it");
        logger.Entries.Should().Contain(e => e.Level == LogLevel.Warning && e.Message.Contains(DeadLetterReasons.MaxRetriesExceeded),
            "it should warn that a message was dropped, and why");
        telemetry.DeadLetteredCount.Should().Be(0, "it should not count a dropped message as dead-lettered");
    }

    [Fact]
    public async Task When_Dead_Lettering_Is_Disabled_And_Handler_Rejects()
    {
        var outcome = await Dispatch(Dispatcher(Result(MessageResult.DeadLetter), config with { EnableDeadLetterQueue = false }));

        outcome.Should().Be(DispatchOutcome.Dropped, "it should drop the rejected message");
        actions.Calls.Should().Equal(["Complete"], "it should complete the delivery");
    }

    [Fact]
    public async Task When_Dead_Lettering_Is_Disabled_And_Body_Is_Poison()
    {
        var outcome = await Dispatch(Dispatcher(Result(MessageResult.Acknowledge), config with { EnableDeadLetterQueue = false }),
            body: "not json"u8.ToArray());

        outcome.Should().Be(DispatchOutcome.Dropped, "it should drop a poison message rather than redeliver it forever");
        actions.Calls.Should().Equal(["Complete"], "it should complete the delivery");
    }

    [Fact]
    public async Task When_Handler_Is_Cancelled_By_Shutdown()
    {
        using var shutdown = new CancellationTokenSource();
        var dispatcher = Dispatcher(async (_, _, ct) =>
        {
            await shutdown.CancelAsync();
            ct.ThrowIfCancellationRequested();
            return MessageResult.Acknowledge;
        });

        var act = () => DispatchUntil(dispatcher, shutdown.Token);

        await act.Should().ThrowAsync<OperationCanceledException>("it should let shutdown abort the delivery");
        actions.Calls.Should().BeEmpty("it should leave the message unsettled so the broker redelivers it");
    }

    [Fact]
    public async Task When_Handler_Throws_OperationCanceled_Without_Shutdown()
    {
        var outcome = await Dispatch(Dispatcher(Throws(new OperationCanceledException("handler timeout"))));

        outcome.Should().Be(DispatchOutcome.Rescheduled, "it should treat a handler's own cancellation as a failure to retry");
    }

    [Fact]
    public async Task When_A_Handler_Throws_An_Exception_With_A_Very_Long_Type_Name()
    {
        var limit = PublishOptions.MaxHeaderValueLength;
        var error = LongNamedExceptions.Create();
        error.GetType().FullName!.Length.Should().BeGreaterThan(limit * 2, "the spec needs a type name beyond the limit");

        var outcome = await Dispatch(Dispatcher(Throws(error), config with { Retry = RetryPolicy.None }));

        outcome.Should().Be(DispatchOutcome.DeadLettered, "it should dead-letter after the only attempt");
        actions.DeadLetterReason!.Value.Detail.Should().HaveLength(limit, "it should cut the dead-letter detail to the shared limit");
        telemetry.Consumed.Should().ContainSingle().Which.ErrorType.Should().HaveLength(limit,
            "it should cut the error type it reports to telemetry");
        logger.Entries.Should().OnlyContain(e => e.Message.Length < limit + 300,
            "it should not write an unbounded exception type name into a log message");
    }

    [Fact]
    public async Task When_The_Correlation_Id_Is_Long_Or_Has_Control_Characters()
    {
        MessageContext? received = null;
        var dispatcher = Dispatcher((_, c, _) =>
        {
            received = c;
            return Task.FromResult(MessageResult.Acknowledge);
        });

        await dispatcher.DispatchAsync(
            MessageSerializer.Serialize(message, SerializationFormat.Json),
            new MessageContext { MessageId = "m-1", CorrelationId = "line\r\nforged" + new string('c', 500) },
            actions,
            TestContext.Current.CancellationToken);

        received!.CorrelationId.Should().StartWith("lineforged", "it should strip control characters from a correlation id")
            .And.HaveLength(PublishOptions.MaxMessageIdLength, "it should cut a correlation id to the id limit");
        telemetry.Consumes.Should().ContainSingle().Which.Context!.CorrelationId.Should().Be(received.CorrelationId,
            "it should start the consume span with the bounded correlation id");
    }

    [Fact]
    public async Task When_The_Correlation_Id_Is_Short_And_Clean()
    {
        MessageContext? received = null;
        var dispatcher = Dispatcher((_, c, _) =>
        {
            received = c;
            return Task.FromResult(MessageResult.Acknowledge);
        });
        var context = new MessageContext { MessageId = "m-1", CorrelationId = "c-1" };

        await dispatcher.DispatchAsync(MessageSerializer.Serialize(message, SerializationFormat.Json), context, actions,
            TestContext.Current.CancellationToken);

        received.Should().BeSameAs(context, "it should pass a context with clean ids through unchanged");
    }

    [Fact]
    public async Task When_The_Queue_Has_Json_Metadata()
    {
        var resolver = new RecordingJsonResolver();
        TestMessage? received = null;
        var dispatcher = Dispatcher((m, _, _) =>
        {
            received = m;
            return Task.FromResult(MessageResult.Acknowledge);
        }, config with { JsonTypeInfoResolver = resolver });

        await Dispatch(dispatcher);

        received.Should().Be(message, "it should deserialize the message");
        resolver.Requested.Should().Contain(typeof(TestMessage), "it should deserialize with the queue's JSON metadata");
    }

    async Task<MessageContext> ReceivedContext(MessageContext context)
    {
        MessageContext? received = null;
        var dispatcher = Dispatcher((_, c, _) =>
        {
            received = c;
            return Task.FromResult(MessageResult.Acknowledge);
        });
        await dispatcher.DispatchAsync(MessageSerializer.Serialize(message, SerializationFormat.Json), context, actions,
            TestContext.Current.CancellationToken);
        return received!;
    }

    [Fact]
    public async Task When_There_Is_No_Correlation_Id() =>
        (await ReceivedContext(new MessageContext { MessageId = "m-1\n", CorrelationId = null })).CorrelationId
            .Should().BeNull("it should keep an absent correlation id absent, not turn it into an empty one");

    [Fact]
    public async Task When_An_Id_Has_C1_Control_Characters() =>
        (await ReceivedContext(new MessageContext { MessageId = "m\u0085-1", CorrelationId = "c\u009b-1" }))
            .Should().Match<MessageContext>(c => c.MessageId == "m-1" && c.CorrelationId == "c-1",
                "it should strip C1 control characters too, also from short ids");

    [Fact]
    public async Task When_The_Cut_Would_Split_A_Surrogate_Pair_In_An_Id()
    {
        var limit = PublishOptions.MaxMessageIdLength;
        var id = new string('c', limit - 1) + "\U0001F600" + "tail";

        var context = await ReceivedContext(new MessageContext { MessageId = id, CorrelationId = id });

        context.CorrelationId.Should().Be(new string('c', limit - 1), "it should not leave half of a surrogate pair at the end");
        context.MessageId.Should().Be(new string('c', limit - 1), "it should cut a message id the same way");
    }
}
