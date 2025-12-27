namespace ESE.MessageQueue.Retry;

/// <summary>
/// Executes operations with retry logic.
/// </summary>
public class RetryExecutor
{
    readonly RetryPolicy policy;
    readonly Random random = new();

    public RetryExecutor(RetryPolicy policy)
    {
        this.policy = policy;
    }

    /// <summary>
    /// Calculates the delay for a given attempt number.
    /// </summary>
    public TimeSpan CalculateDelay(int attemptNumber)
    {
        if (attemptNumber < 1)
            return TimeSpan.Zero;

        var baseDelay = policy.Strategy switch
        {
            RetryStrategy.Fixed => policy.InitialDelay,
            RetryStrategy.Linear => TimeSpan.FromTicks(policy.InitialDelay.Ticks * attemptNumber),
            RetryStrategy.Exponential => TimeSpan.FromTicks(
                (long)(policy.InitialDelay.Ticks * Math.Pow(policy.BackoffMultiplier, attemptNumber - 1))),
            RetryStrategy.Custom when policy.CustomDelayCalculator != null =>
                policy.CustomDelayCalculator(attemptNumber),
            _ => policy.InitialDelay
        };

        // Cap at max delay
        if (baseDelay > policy.MaxDelay)
            baseDelay = policy.MaxDelay;

        // Add jitter if enabled
        if (policy.UseJitter && policy.JitterFactor > 0)
        {
            var jitterRange = baseDelay.TotalMilliseconds * policy.JitterFactor;
            var jitter = (random.NextDouble() * 2 - 1) * jitterRange; // -jitterRange to +jitterRange
            baseDelay = TimeSpan.FromMilliseconds(Math.Max(0, baseDelay.TotalMilliseconds + jitter));
        }

        return baseDelay;
    }

    /// <summary>
    /// Determines if a retry should be attempted.
    /// </summary>
    public bool ShouldRetry(int attemptNumber, Exception? exception = null)
    {
        if (attemptNumber >= policy.MaxAttempts)
            return false;

        if (exception != null && policy.IsRetryable != null)
            return policy.IsRetryable(exception);

        return true;
    }

    /// <summary>
    /// Executes an async operation with retry logic.
    /// </summary>
    public async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken = default)
    {
        var attempt = 0;
        Exception? lastException = null;

        while (true)
        {
            try
            {
                attempt++;
                return await operation(cancellationToken);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                lastException = ex;

                if (!ShouldRetry(attempt, ex))
                    throw;

                var delay = CalculateDelay(attempt);
                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    /// <summary>
    /// Executes an async operation with retry logic (no return value).
    /// </summary>
    public async Task ExecuteAsync(
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken = default)
    {
        await ExecuteAsync(async ct =>
        {
            await operation(ct);
            return true;
        }, cancellationToken);
    }
}

