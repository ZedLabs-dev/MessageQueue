using Microsoft.Extensions.DependencyInjection;
using ZedLabs.MessageQueue.Extensions;
using ZedLabs.MessageQueue.Providers;

namespace ZedLabs.MessageQueue.Samples.LoopbackProvider;

// The provider's own rules on top of the shared ones (retry policy, serialization format).
sealed class LoopbackConfigValidator : MessageQueueConfigValidator<LoopbackConfig>
{
    protected override void ValidateProvider(LoopbackConfig options, ICollection<string> errors)
    {
        if (options.MaxDeadLetters < 0)
            errors.Add("MaxDeadLetters must be >= 0");
    }
}

// Selectable as Provider = "Loopback" once AddLoopbackProvider() has registered it.
sealed class LoopbackProvider : IMessageQueueProvider
{
    public string Name => LoopbackConfig.ProviderName;

    public void AddQueue<T>(MessageQueueRegistration<T> registration) where T : class =>
        registration.AddQueue<LoopbackConfig, LoopbackConfigValidator, LoopbackMessageQueue<T>>(
            context => new LoopbackMessageQueue<T>(context.Config, context.Telemetry, context.Logger));
}

/// <summary>Registers the loopback sample provider.</summary>
public static class LoopbackServiceCollectionExtensions
{
    /// <summary>Makes <c>Provider = "Loopback"</c> selectable in <c>AddMessageQueue&lt;T&gt;</c>.</summary>
    public static IServiceCollection AddLoopbackProvider(this IServiceCollection services) =>
        services.AddMessageQueueProvider(new LoopbackProvider());
}
