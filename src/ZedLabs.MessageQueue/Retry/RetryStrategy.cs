namespace ZedLabs.MessageQueue.Retry;

/// <summary>How <see cref="RetryPolicy"/> computes the delay before retry <c>n</c> (1-based).</summary>
public enum RetryStrategy
{
    /// <summary><c>InitialDelay</c> before every retry.</summary>
    Fixed,

    /// <summary><c>InitialDelay × n</c>.</summary>
    Linear,

    /// <summary><c>InitialDelay × BackoffMultiplier^(n-1)</c>.</summary>
    Exponential,

    /// <summary><c>CustomDelayCalculator(n)</c>.</summary>
    Custom
}
