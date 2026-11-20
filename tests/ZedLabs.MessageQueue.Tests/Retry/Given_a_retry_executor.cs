using AwesomeAssertions;
using ZedLabs.MessageQueue.Retry;

namespace ZedLabs.MessageQueue.Tests.Retry;

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
    public void When_MaxRetries_Is_Reached()
    {
        var policy = new RetryPolicy { MaxRetries = 3 };
        var executor = new RetryExecutor(policy);

        executor.ShouldRetry(1).Should().BeTrue("it should retry after the first attempt");
        executor.ShouldRetry(3).Should().BeTrue("it should allow the third retry after the third attempt");
        executor.ShouldRetry(4).Should().BeFalse("it should stop once MaxRetries retries have been made");
    }

    [Fact]
    public void When_MaxRetries_Is_Zero()
    {
        var executor = new RetryExecutor(RetryPolicy.None);

        executor.ShouldRetry(1).Should().BeFalse("it should not retry at all when MaxRetries is zero");
    }

    [Fact]
    public void When_Checking_If_Exception_Is_Retryable()
    {
        var policy = new RetryPolicy
        {
            MaxRetries = 5,
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
            MaxRetries = 2,
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
        attempts.Should().Be(3, "it should make the first attempt plus two retries");
    }

    [Fact(Timeout = 10_000)]
    public async Task When_Executing_With_Permanent_Failure()
    {
        var policy = new RetryPolicy
        {
            MaxRetries = 2,
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
        attempts.Should().Be(3, "it should make MaxRetries + 1 attempts in total");
    }

    [Fact]
    public void When_Linear_Delay_Would_Overflow()
    {
        var policy = new RetryPolicy
        {
            Strategy = RetryStrategy.Linear,
            InitialDelay = TimeSpan.FromDays(1),
            MaxDelay = TimeSpan.FromMinutes(5),
            UseJitter = false
        };
        var executor = new RetryExecutor(policy);

        var delay = executor.CalculateDelay(int.MaxValue);

        delay.Should().Be(TimeSpan.FromMinutes(5), "it should cap a huge linear delay instead of overflowing to a negative value");
    }

    [Fact]
    public void When_Exponential_Delay_Would_Overflow()
    {
        var policy = new RetryPolicy
        {
            Strategy = RetryStrategy.Exponential,
            InitialDelay = TimeSpan.FromSeconds(1),
            BackoffMultiplier = 10,
            MaxDelay = TimeSpan.FromMinutes(5),
            UseJitter = true
        };
        var executor = new RetryExecutor(policy);

        var delay = executor.CalculateDelay(int.MaxValue);

        delay.Should().BeGreaterThan(TimeSpan.Zero, "it should never produce a zero or negative delay for a huge attempt count")
            .And.BeLessThanOrEqualTo(TimeSpan.FromMinutes(5), "it should stay within MaxDelay");
    }

    [Fact]
    public void When_Custom_Calculator_Returns_Out_Of_Range_Delays()
    {
        var policy = new RetryPolicy
        {
            Strategy = RetryStrategy.Custom,
            CustomDelayCalculator = attempt => attempt == 1 ? TimeSpan.FromSeconds(-5) : TimeSpan.FromDays(365),
            MaxDelay = TimeSpan.FromSeconds(30),
            UseJitter = false
        };
        var executor = new RetryExecutor(policy);

        executor.CalculateDelay(1).Should().Be(TimeSpan.Zero, "it should not return a negative delay");
        executor.CalculateDelay(2).Should().Be(TimeSpan.FromSeconds(30), "it should cap a custom delay at MaxDelay");
    }

    [Fact]
    public void When_Jitter_Is_Applied_At_MaxDelay()
    {
        var policy = new RetryPolicy
        {
            Strategy = RetryStrategy.Fixed,
            InitialDelay = TimeSpan.FromSeconds(30),
            MaxDelay = TimeSpan.FromSeconds(30),
            JitterFactor = 0.5
        };
        var executor = new RetryExecutor(policy);

        var delays = Enumerable.Range(0, 200).Select(_ => executor.CalculateDelay(1)).ToList();

        delays.Should().OnlyContain(d => d <= TimeSpan.FromSeconds(30), "it should never let jitter push a delay past MaxDelay");
        delays.Should().OnlyContain(d => d >= TimeSpan.FromSeconds(15), "it should jitter downward by at most JitterFactor");
    }

    [Fact]
    public void When_Calculating_Delays_Concurrently()
    {
        var executor = new RetryExecutor(new RetryPolicy
        {
            Strategy = RetryStrategy.Fixed,
            InitialDelay = TimeSpan.FromSeconds(10),
            MaxDelay = TimeSpan.FromSeconds(20),
            JitterFactor = 0.1
        });
        var delays = new System.Collections.Concurrent.ConcurrentBag<TimeSpan>();

        Parallel.For(0, 200_000, _ => delays.Add(executor.CalculateDelay(1)));

        delays.Should().OnlyContain(d => d >= TimeSpan.FromSeconds(9) && d <= TimeSpan.FromSeconds(11),
            "it should keep every jittered delay within the jitter range under concurrent use");
        delays.Distinct().Count().Should().BeGreaterThan(1000,
            "it should keep producing random jitter instead of a corrupted constant");
    }

    [Fact]
    public void When_Jitter_Is_Applied()
    {
        var executor = new RetryExecutor(new RetryPolicy
        {
            Strategy = RetryStrategy.Fixed,
            InitialDelay = TimeSpan.FromSeconds(10),
            MaxDelay = TimeSpan.FromSeconds(30),
            JitterFactor = 0.1
        });

        var delays = Enumerable.Range(0, 1000).Select(_ => executor.CalculateDelay(1)).ToList();

        delays.Should().Contain(d => d < TimeSpan.FromSeconds(10), "it should jitter some delays below the base delay");
        delays.Should().Contain(d => d > TimeSpan.FromSeconds(10), "it should jitter some delays above the base delay");
    }
}
