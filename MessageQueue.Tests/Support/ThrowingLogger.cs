using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.Tests;

// A logger whose sink throws, standing in for a broken logging provider.
public sealed class ThrowingLogger : ILogger
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
        throw new InvalidOperationException("log sink failed");
}
