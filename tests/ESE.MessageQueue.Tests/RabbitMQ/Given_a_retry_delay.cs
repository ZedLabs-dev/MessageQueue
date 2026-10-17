using AwesomeAssertions;
using ESE.MessageQueue.RabbitMQ;
using ESE.MessageQueue.Retry;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// Broker-side backoff uses one retry queue per delay bucket (ADR 0006). Buckets follow the policy's un-jittered
// schedule, with three jitter levels per step, so the number of retry queues stays small and predictable.
[Trait("Category", "Unit")]
public class Given_a_retry_delay
{
    static readonly RetryPolicy Default = new();
    static readonly RetryPolicy NoJitter = new() { UseJitter = false };

    static long Bucket(RetryPolicy policy, int retry, double delayMilliseconds) =>
        RetryQueues.BucketMilliseconds(policy, retry, TimeSpan.FromMilliseconds(delayMilliseconds));

    [Theory]
    [InlineData(1, 2_000)]
    [InlineData(2, 4_000)]
    [InlineData(3, 8_000)]
    [InlineData(4, 16_000)]
    [InlineData(5, 30_000)]
    public void When_The_Policy_Has_No_Jitter(int retry, long expected) =>
        Bucket(NoJitter, retry, expected).Should().Be(expected, "it should use the policy's own schedule step");

    [Theory]
    [InlineData(1, 1_830, 1_800)]
    [InlineData(1, 2_040, 2_000)]
    [InlineData(1, 2_150, 2_200)]
    [InlineData(5, 28_000, 27_000)]
    [InlineData(5, 29_600, 30_000)]
    public void When_The_Delay_Is_Jittered(int retry, double delay, long expected) =>
        Bucket(Default, retry, delay).Should().Be(expected, "it should pick the nearest of step x (1 - J), step and step x (1 + J), capped at MaxDelay");

    [Fact]
    public void When_Counting_The_Retry_Queues_Of_The_Default_Policy()
    {
        var executor = new RetryExecutor(Default);
        var buckets = Enumerable.Range(0, 2_000)
            .SelectMany(_ => Enumerable.Range(1, Default.MaxRetries).Select(retry => Bucket(Default, retry, executor.CalculateDelay(retry).TotalMilliseconds)))
            .Distinct()
            .Count();

        buckets.Should().BeLessThanOrEqualTo(14, "it should need at most three queues per step (two for the capped step)");
    }

    [Theory]
    [InlineData(37, 37)]
    [InlineData(237, 240)]
    [InlineData(29_876, 30_000)]
    public void When_A_Custom_Calculator_Chooses_The_Delay(double delay, long expected) =>
        Bucket(new RetryPolicy { Strategy = RetryStrategy.Custom, CustomDelayCalculator = _ => TimeSpan.FromMilliseconds(delay) }, 1, delay)
            .Should().Be(expected, "it should round a custom delay to two significant digits");

    [Theory]
    [InlineData(0, 0)]
    [InlineData(0.4, 0)]
    [InlineData(0.6, 1)]
    [InlineData(99.4, 99)]
    [InlineData(1_949, 1_900)]
    [InlineData(86_400_000, 86_000_000)]
    public void When_Rounding_To_Two_Significant_Digits(double delay, long expected) =>
        RetryQueues.RoundToTwoSignificantDigits(TimeSpan.FromMilliseconds(delay)).Should().Be(expected, "it should keep two significant digits");

    [Fact]
    public void When_Rounding_Every_Delay_Up_To_A_Minute()
    {
        var delays = Enumerable.Range(1, 60_000).Select(ms => TimeSpan.FromMilliseconds(ms)).ToList();

        var worstError = delays.Max(d => Math.Abs(RetryQueues.RoundToTwoSignificantDigits(d) - d.TotalMilliseconds) / d.TotalMilliseconds);

        worstError.Should().BeLessThanOrEqualTo(0.05, "it should keep every custom delay within 5%");
    }

    [Fact]
    public void When_The_Delay_Is_Zero() =>
        Bucket(new RetryPolicy { InitialDelay = TimeSpan.Zero, UseJitter = false }, 1, 0).Should()
            .Be(0, "it should still go through a retry queue, with a TTL of zero");

    [Fact]
    public void When_Naming_The_Retry_Queue() =>
        RetryQueues.Name("orders", 2_300).Should().Be("orders.retry.2300", "it should name the queue after its delay in milliseconds");

    [Fact]
    public void When_Declaring_A_Quorum_Retry_Queue()
    {
        var arguments = RetryQueues.Arguments(new RabbitMqConfig { QueueName = "orders", UseQuorumQueues = true, QuorumInitialGroupSize = 3 }, 2_300);

        arguments.Should().Contain("x-message-ttl", 2_300L, "it should hold every message for the bucket's delay");
        arguments.Should().Contain("x-dead-letter-exchange", string.Empty, "it should route expired messages through the default exchange");
        arguments.Should().Contain("x-dead-letter-routing-key", "orders", "it should send expired messages back to the main queue");
        arguments.Should().Contain("x-queue-type", "quorum", "it should be as durable as the main queue");
        arguments.Should().Contain("x-dead-letter-strategy", "at-least-once", "it should not lose a retry on its way back");
        arguments.Should().Contain("x-overflow", "reject-publish", "it should be what at-least-once dead-lettering requires");
    }

    [Theory]
    [InlineData(2_300, 3_600_000)]
    [InlineData(86_000_000, 344_000_000)]
    public void When_A_Classic_Retry_Queue_Is_No_Longer_Used(long bucket, long expires) =>
        RetryQueues.Arguments(new RabbitMqConfig { QueueName = "orders", UseQuorumQueues = false }, bucket).Should()
            .Contain("x-expires", expires, "it should let the broker delete an unused retry queue, never sooner than 4 x its TTL or an hour");

    [Fact]
    public void When_A_Quorum_Retry_Queue_Is_No_Longer_Used() =>
        RetryQueues.Arguments(new RabbitMqConfig { QueueName = "orders", UseQuorumQueues = true }, 2_300).Should()
            .NotContainKey("x-expires", "it should never expire a quorum queue: declaring one does not reset its expiry, so it could vanish holding messages");

    [Fact]
    public void When_Declaring_A_Classic_Retry_Queue() =>
        RetryQueues.Arguments(new RabbitMqConfig { QueueName = "orders", UseQuorumQueues = false }, 100).Should()
            .NotContainKey("x-queue-type", "it should declare a classic queue like the main queue");

    [Theory]
    [InlineData(2_300, 900_000, false)]
    [InlineData(2_300, 900_001, true)]
    [InlineData(86_000_000, 86_000_000, false)]
    [InlineData(86_000_000, 86_000_001, true)]
    public void When_A_Cached_Retry_Queue_Ages(long bucket, long ageMilliseconds, bool stale) =>
        RetryQueues.IsStale(declaredAtMilliseconds: 1_000, nowMilliseconds: 1_000 + ageMilliseconds, bucket).Should().Be(stale,
            "it should declare a retry queue again once a quarter of its x-expires has passed, so it never expires holding a message");
}
