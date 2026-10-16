using AwesomeAssertions;
using ESE.MessageQueue.Retry;

namespace ESE.MessageQueue.Tests.Retry;

[Trait("Category", "Unit")]
public class Given_a_retry_executor
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

    [Fact(Timeout = 10_000)]
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
        }, TestContext.Current.CancellationToken);

        result.Should().Be(42, "it should return the result after successful retry");
        attempts.Should().Be(3, "it should have attempted 3 times");
    }

    [Fact(Timeout = 10_000)]
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
        }, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<InvalidOperationException>("it should throw after exhausting retries");
        attempts.Should().Be(2, "it should have attempted the max number of times");
    }
}
