using Microsoft.Extensions.DependencyInjection;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.Extensions;

/// <summary>Registers the RabbitMQ provider.</summary>
public static class RabbitMqServiceCollectionExtensions
{
    /// <summary>
    /// Makes <c>Provider = "RabbitMq"</c> selectable in <c>AddMessageQueue&lt;T&gt;</c>. Call it before
    /// <c>AddMessageQueue&lt;T&gt;</c>; calling it more than once is a no-op.
    /// </summary>
    public static IServiceCollection AddRabbitMqProvider(this IServiceCollection services) =>
        services.AddMessageQueueProvider(new RabbitMqProvider());
}
