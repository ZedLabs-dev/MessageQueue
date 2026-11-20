#region snippet:telemetry
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using ZedLabs.MessageQueue.Extensions;
#endregion

namespace ZedLabs.MessageQueue.Samples.Docs;

#region snippet:telemetry
public static class OrderTelemetry
{
    public const string SourceName = "MyApp.Messaging";

    public static IServiceCollection AddOrderTelemetry(this IServiceCollection services)
    {
        // The library's spans and metrics, reported through an ActivitySource and a Meter of this name.
        services.AddMessageQueueTelemetry(SourceName);

        // Your OpenTelemetry pipeline subscribes to the same name; add your exporter (OTLP, Prometheus, ...) here.
        services.AddOpenTelemetry()
            .WithTracing(tracing => tracing.AddSource(SourceName))
            .WithMetrics(metrics => metrics.AddMeter(SourceName));
        return services;
    }
}
#endregion
