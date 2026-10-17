namespace ESE.MessageQueue.Retry;

/// <summary>
/// Computes backoff delays and retry decisions for a <see cref="RetryPolicy"/>, and runs operations with retries.
/// Thread-safe.
/// </summary>
public class RetryExecutor
{
    readonly RetryPolicy policy;

    public RetryExecutor(RetryPolicy policy)
    {
        this.policy = policy;
    }

    /// <summary>
    /// The delay before retry number <paramref name="retryNumber"/> (1-based), clamped to [0, MaxDelay] after jitter.
    /// </summary>
    public TimeSpan CalculateDelay(int retryNumber)
    {
        if (retryNumber < 1)
            return TimeSpan.Zero;

        var maxMs = policy.MaxDelay.TotalMilliseconds;
        var delayMs = ScheduledMilliseconds(retryNumber);

        if (policy.UseJitter && policy.JitterFactor > 0)
        {
            var jitterRange = delayMs * policy.JitterFactor;
            delayMs = Clamp(delayMs + (Random.Shared.NextDouble() * 2 - 1) * jitterRange, maxMs);
        }

        return TimeSpan.FromMilliseconds(delayMs);
    }

    /// <summary>The delay before retry <paramref name="retryNumber"/> without jitter, clamped to [0, MaxDelay].</summary>
    internal TimeSpan ScheduledDelay(int retryNumber) =>
        retryNumber < 1 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(ScheduledMilliseconds(retryNumber));

    double ScheduledMilliseconds(int retryNumber)
    {
        var initialMs = policy.InitialDelay.TotalMilliseconds;

        // Computed in double and clamped before converting, so huge retry numbers cannot overflow.
        var delayMs = policy.Strategy switch
        {
            RetryStrategy.Fixed => initialMs,
            RetryStrategy.Linear => initialMs * retryNumber,
            RetryStrategy.Exponential => initialMs * Math.Pow(policy.BackoffMultiplier, retryNumber - 1),
            RetryStrategy.Custom when policy.CustomDelayCalculator != null =>
                policy.CustomDelayCalculator(retryNumber).TotalMilliseconds,
            _ => initialMs
        };

        return Clamp(delayMs, policy.MaxDelay.TotalMilliseconds);
    }

    /// <summary>
    /// Whether another attempt may follow attempt number <paramref name="attemptNumber"/> (1-based).
    /// True while fewer than MaxRetries retries have been made and the exception, if any, is retryable.
    /// An <c>IsRetryable</c> predicate that throws counts as "not retryable".
    /// </summary>
    public bool ShouldRetry(int attemptNumber, Exception? exception = null)
    {
        if (attemptNumber > policy.MaxRetries)
            return false;

        if (exception != null && policy.IsRetryable != null)
        {
            try
            {
                return policy.IsRetryable(exception);
            }
            catch (Exception)
            {
                // A broken predicate cannot vouch for a retry: treat the failure as permanent.
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Runs <paramref name="operation"/> up to MaxRetries + 1 times, waiting the backoff delay between attempts.
    /// </summary>
    public async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken = default)
    {
        var attempt = 0;

        while (true)
        {
            try
            {
                attempt++;
                return await operation(cancellationToken);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested && ShouldRetry(attempt, ex))
            {
                await Task.Delay(CalculateDelay(attempt), cancellationToken);
            }
        }
    }

    /// <summary>
    /// Runs <paramref name="operation"/> up to MaxRetries + 1 times, waiting the backoff delay between attempts.
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

    static double Clamp(double delayMs, double maxMs) =>
        double.IsNaN(delayMs) ? maxMs : Math.Clamp(delayMs, 0, maxMs);
}
