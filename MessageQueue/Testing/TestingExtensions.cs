using ESE.MessageQueue.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Testing;

/// <summary>
/// Extension methods for registering test message queues.
/// </summary>
public static class TestingExtensions
{
    /// <summary>
    /// Adds an in-memory message queue for testing.
    /// </summary>
    public static IServiceCollection AddInMemoryMessageQueue<T>(this IServiceCollection services)
        where T : class
    {
        var queue = new InMemoryMessageQueue<T>();
        services.AddSingleton(queue);
        services.AddSingleton<IMessageQueue<T>>(queue);
        return services;
    }

    /// <summary>
    /// Adds an in-memory message queue with a specific instance for testing.
    /// </summary>
    public static IServiceCollection AddInMemoryMessageQueue<T>(
        this IServiceCollection services,
        InMemoryMessageQueue<T> queue)
        where T : class
    {
        services.AddSingleton(queue);
        services.AddSingleton<IMessageQueue<T>>(queue);
        return services;
    }
}

