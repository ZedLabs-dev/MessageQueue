using AwesomeAssertions;
using ZedLabs.MessageQueue.Retry;

namespace ZedLabs.MessageQueue.Tests.Retry;

[Trait("Category", "Unit")]
public class Given_a_retry_policy
{
    [Fact]
    public void When_Using_Default_Policy()
    {
        var policy = RetryPolicy.Default;

        policy.Strategy.Should().Be(RetryStrategy.Exponential, "it should use exponential backoff");
        policy.MaxRetries.Should().Be(5, "it should retry 5 times after the first attempt");
        policy.InitialDelay.Should().Be(TimeSpan.FromSeconds(2), "it should wait 2 seconds before the first retry");
        policy.BackoffMultiplier.Should().Be(2.0, "it should double the delay each time");
        policy.MaxDelay.Should().Be(TimeSpan.FromSeconds(30), "it should cap a single delay at 30 seconds");
        policy.UseJitter.Should().BeTrue("it should add jitter by default");
        policy.JitterFactor.Should().Be(0.1, "it should jitter by 10%");
    }

    [Fact]
    public void When_Summing_The_Default_Backoff_Schedule()
    {
        var executor = new RetryExecutor(RetryPolicy.Default with { UseJitter = false });

        var total = Enumerable.Range(1, RetryPolicy.Default.MaxRetries)
            .Aggregate(TimeSpan.Zero, (sum, retry) => sum + executor.CalculateDelay(retry));

        total.Should().Be(TimeSpan.FromSeconds(60), "it should spread 5 retries over about 60 seconds (2+4+8+16+30)");
    }

    [Fact]
    public void When_Using_None_Policy()
    {
        var policy = RetryPolicy.None;

        policy.MaxRetries.Should().Be(0, "it should not retry");
    }

    [Fact]
    public void When_Using_Aggressive_Policy()
    {
        var policy = RetryPolicy.Aggressive;

        policy.MaxRetries.Should().Be(5, "it should allow more retries");
        policy.InitialDelay.Should().BeLessThan(TimeSpan.FromSeconds(1), "it should start with a shorter delay");
    }
}
