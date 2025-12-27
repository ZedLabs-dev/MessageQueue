namespace ESE.MessageQueue.Retry;

/// <summary>
/// Retry strategy types.
/// </summary>
public enum RetryStrategy
{
    /// <summary>Fixed delay between retries.</summary>
    Fixed,

    /// <summary>Linear increase in delay (delay * attempt).</summary>
    Linear,

    /// <summary>Exponential backoff (delay * 2^attempt).</summary>
    Exponential,

    /// <summary>Custom delay calculation.</summary>
    Custom
}

/// <summary>
/// Configuration for retry behavior.
/// </summary>
public record RetryPolicy
{
    /// <summary>
    /// The retry strategy to use.
    /// </summary>
    public RetryStrategy Strategy { get; init; } = RetryStrategy.Exponential;

    /// <summary>
    /// Maximum number of retry attempts before giving up.
    /// </summary>
    public int MaxAttempts { get; init; } = 3;

    /// <summary>
    /// Initial delay before the first retry.
    /// </summary>
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Maximum delay between retries (caps exponential/linear growth).
    /// </summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Multiplier for exponential backoff (default 2.0).
    /// </summary>
    public double BackoffMultiplier { get; init; } = 2.0;

    /// <summary>
    /// Whether to add random jitter to delays to prevent thundering herd.
    /// </summary>
    public bool UseJitter { get; init; } = true;

    /// <summary>
    /// Maximum jitter as a percentage of the delay (0.0 to 1.0).
    /// </summary>
    public double JitterFactor { get; init; } = 0.1;

    /// <summary>
    /// Custom delay calculator for Custom strategy.
    /// </summary>
    public Func<int, TimeSpan>? CustomDelayCalculator { get; init; }

    /// <summary>
    /// Predicate to determine if an exception is retryable.
    /// </summary>
    public Func<Exception, bool>? IsRetryable { get; init; }

    /// <summary>
    /// Default policy with exponential backoff.
    /// </summary>
    public static RetryPolicy Default => new();

    /// <summary>
    /// No retries - fail immediately.
    /// </summary>
    public static RetryPolicy None => new() { MaxAttempts = 0 };

    /// <summary>
    /// Aggressive retry policy for transient failures.
    /// </summary>
    public static RetryPolicy Aggressive => new()
    {
        MaxAttempts = 5,
        InitialDelay = TimeSpan.FromMilliseconds(100),
        MaxDelay = TimeSpan.FromSeconds(30),
        BackoffMultiplier = 2.0,
        UseJitter = true
    };
}

