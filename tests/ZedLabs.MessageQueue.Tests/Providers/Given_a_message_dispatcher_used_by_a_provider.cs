using AwesomeAssertions;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.InMemory;
using ZedLabs.MessageQueue.Processing;
using ZedLabs.MessageQueue.Providers;
using ZedLabs.MessageQueue.Serialization;
using ZedLabs.MessageQueue.Telemetry;
using ZedLabs.MessageQueue.Tests.Processing;

namespace ZedLabs.MessageQueue.Tests.Providers;

// MessageDispatcher<T> is public SPI (ADR 0015): a third-party provider may hand it anything a broker delivered, and
// may keep using the config object it passed in.
[Trait("Category", "Unit")]
public class Given_a_message_dispatcher_used_by_a_provider
{
    static readonly TelemetryTarget Target = new("test", "orders", nameof(TestMessage));
    readonly FakeDeliveryActions actions = new();
    readonly RecordingLogger logger = new();

    static Func<TestMessage, MessageContext, CancellationToken, Task<MessageResult>> Returns(MessageResult result) =>
        (_, _, _) => Task.FromResult(result);

    static byte[] Body() => MessageSerializer.Serialize(new TestMessage("1", "x"), SerializationFormat.Json);

    [Fact]
    public void When_Constructed_Without_Its_Collaborators()
    {
        var config = new InMemoryConfig { QueueName = "orders" };

        var noConfig = () => new MessageDispatcher<TestMessage>(null!, Target, Returns(MessageResult.Acknowledge));
        var noTarget = () => new MessageDispatcher<TestMessage>(config, null!, Returns(MessageResult.Acknowledge));
        var noHandler = () => new MessageDispatcher<TestMessage>(config, Target, null!);

        noConfig.Should().Throw<ArgumentNullException>("it should reject a missing config at construction, not on the first delivery");
        noTarget.Should().Throw<ArgumentNullException>("it should reject a missing telemetry target");
        noHandler.Should().Throw<ArgumentNullException>("it should reject a missing handler");
    }

    [Fact]
    public async Task When_Dispatching_Without_A_Context_Or_Actions()
    {
        var dispatcher = new MessageDispatcher<TestMessage>(new InMemoryConfig { QueueName = "orders" }, Target, Returns(MessageResult.Acknowledge));
        var context = new MessageContext { MessageId = "m-1" };

        var noContext = () => dispatcher.DispatchAsync(Body(), null!, actions, TestContext.Current.CancellationToken);
        var noActions = () => dispatcher.DispatchAsync(Body(), context, null!, TestContext.Current.CancellationToken);

        await noContext.Should().ThrowAsync<ArgumentNullException>("it should reject a missing context");
        await noActions.Should().ThrowAsync<ArgumentNullException>("it should reject missing delivery actions");
    }

    [Fact]
    public async Task When_The_Provider_Changes_The_Config_Afterwards()
    {
        var config = new InMemoryConfig { QueueName = "orders", EnableDeadLetterQueue = true };
        var dispatcher = new MessageDispatcher<TestMessage>(config, Target, Returns(MessageResult.DeadLetter));
        config.EnableDeadLetterQueue = false;

        var outcome = await dispatcher.DispatchAsync(Body(), new MessageContext { MessageId = "m-1" }, actions, TestContext.Current.CancellationToken);

        outcome.Should().Be(DispatchOutcome.DeadLettered, "it should keep using the config as it was when the subscription started");
    }

    [Fact]
    public async Task When_The_Broker_Message_Id_Carries_Control_Characters()
    {
        var dispatcher = new MessageDispatcher<TestMessage>(new InMemoryConfig { QueueName = "orders" }, Target, Returns(MessageResult.DeadLetter),
            logger: logger);
        var forged = "m-1\r\n[Error] forged log entry " + new string('x', 500);

        await dispatcher.DispatchAsync(Body(), new MessageContext { MessageId = forged }, actions, TestContext.Current.CancellationToken);

        logger.Entries.Should().NotBeEmpty("it should log the dead letter");
        logger.Entries.Should().AllSatisfy(e => e.Message.Should().NotContainAny(["\r", "\n"],
            "it should not let a broker-supplied id forge log lines"));
        logger.Entries.Should().AllSatisfy(e => e.Message.Should().NotContain(new string('x', 200),
            "it should truncate a broker-supplied id before logging it"));
    }

    [Fact]
    public void When_A_Dead_Letter_Reason_Is_Default()
    {
        var reason = default(DeadLetterReason);

        reason.Code.Should().Be(DeadLetterReasons.DeliveryFailed, "it should never hand a provider a null reason code");
    }

    [Fact]
    public void When_A_Dead_Letter_Reason_Has_No_Code()
    {
        var act = () => new DeadLetterReason(" ");

        act.Should().Throw<ArgumentException>("it should require one of the DeadLetterReasons codes");
    }
}
