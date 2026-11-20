using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace ZedLabs.MessageQueue.Tests;

// Captures log entries so specs can assert that a warning or error was logged.
public sealed class RecordingLogger : ILogger
{
    public ConcurrentQueue<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = new();

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        Entries.Enqueue((logLevel, formatter(state, exception), exception));
    }
}

public sealed class RecordingLogger<T> : ILogger<T>
{
    public RecordingLogger Inner { get; } = new();

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => Inner.BeginScope(state);

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        => Inner.Log(logLevel, eventId, state, exception, formatter);
}
