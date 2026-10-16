using AwesomeAssertions;
using ESE.MessageQueue.Retry;

namespace ESE.MessageQueue.Tests.Retry;

[Trait("Category", "Unit")]
public class Given_a_retry_policy
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
