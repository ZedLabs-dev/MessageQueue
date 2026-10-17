using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Processing;

// Logging must never change what happens to a message: a throwing logging provider is ignored.
sealed class SafeLogger : ILogger
{
    readonly ILogger inner;

    public SafeLogger(ILogger inner)
    {
        this.inner = inner;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        try
        {
            return inner.BeginScope(state);
        }
        catch (Exception)
        {
            return null;
        }
    }

    public bool IsEnabled(LogLevel logLevel)
    {
        try
        {
            return inner.IsEnabled(logLevel);
        }
        catch (Exception)
        {
            return false;
        }
    }

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        try
        {
            inner.Log(logLevel, eventId, state, exception, formatter);
        }
        catch (Exception)
        {
            // Nothing sensible to do: the logging pipeline itself is broken.
        }
    }
}
