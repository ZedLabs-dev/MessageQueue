using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace ESE.MessageQueue.Samples;

// Everything the sample registers, in one place, so the smoke test boots exactly what `dotnet run` does.
public static class SampleHost
{
    // The ActivitySource and Meter name the library reports under; OpenTelemetry subscribes to it below.
    public const string TelemetrySource = "ESE.MessageQueue.Samples";

    public static HostApplicationBuilder CreateBuilder(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        Configure(builder.Services, builder.Configuration);
        return builder;
    }

    public static void Configure(IServiceCollection services, IConfiguration configuration)
    {
        ThrowIfRabbitMqCredentialsMissing(configuration.GetSection("MessageQueue"));

        // The broker comes from MessageQueue:Provider (InMemory, RabbitMq or AzureServiceBus); the code is the same.
        services.AddMessageQueue<OrderCreated>(configuration);
        services.AddMessageProcessor<OrderCreated, OrderCreatedProcessor>();
        services.AddHostedService<MessageQueueHostedService<OrderCreated>>();
        services.AddHostedService<OrderPublisher>();
        services.AddOptions<SampleOptions>().Bind(configuration.GetSection(SampleOptions.SectionName));

        // The library only emits System.Diagnostics spans and metrics; choosing exporters is the host's job.
        services.AddMessageQueueTelemetry(TelemetrySource);
        if (configuration.GetValue<bool>("Telemetry:ConsoleExporter"))
        {
            services.AddOpenTelemetry()
                .WithTracing(tracing => tracing.AddSource(TelemetrySource).AddConsoleExporter())
                .WithMetrics(metrics => metrics.AddMeter(TelemetrySource).AddConsoleExporter());
        }
    }

    // The dev broker has no default user (MessageQueue/.env sets one), and committed settings carry no credentials:
    // say what to set instead of failing later with an authentication error.
    static void ThrowIfRabbitMqCredentialsMissing(IConfigurationSection section)
    {
        if (!string.Equals(section["Provider"], "RabbitMq", StringComparison.OrdinalIgnoreCase)
            || !string.IsNullOrWhiteSpace(section["ConnectionString"]))
            return;

        if (string.IsNullOrWhiteSpace(section["UserName"]) || string.IsNullOrWhiteSpace(section["Password"]))
            throw new InvalidOperationException(
                "The RabbitMQ sample needs credentials: set the environment variables MessageQueue__UserName and " +
                "MessageQueue__Password to the user in MessageQueue/.env (or set MessageQueue__ConnectionString).");
    }
}
