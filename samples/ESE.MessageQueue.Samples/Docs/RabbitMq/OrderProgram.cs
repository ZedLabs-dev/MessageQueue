#region snippet:rabbitmq-program
using ESE.MessageQueue.Samples.Docs;
using ESE.MessageQueue.Samples.Docs.RabbitMq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
#endregion

namespace ESE.MessageQueue.Samples.Docs.RabbitMq;

// What Program.cs holds; a method here so that it compiles next to the other guides' entry points.
public static class OrderProgram
{
    public static async Task RunAsync(string[] args)
    {
        #region snippet:rabbitmq-program
        var builder = Host.CreateApplicationBuilder(args);   // appsettings.json, environment variables, command line
        builder.Services.AddOrderMessaging(builder.Configuration);
        builder.Services.AddOrderTelemetry();
        // Give running handlers time to finish: more than MessageQueue:ShutdownTimeout (25 s by default).
        builder.Services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(45));
        using var host = builder.Build();
        await host.RunAsync();
        #endregion
    }
}
