#region snippet:inmemory-program
using Microsoft.Extensions.Hosting;
using ZedLabs.MessageQueue.Samples.Docs;
using ZedLabs.MessageQueue.Samples.Docs.InMemory;
#endregion

namespace ZedLabs.MessageQueue.Samples.Docs.InMemory;

// What Program.cs holds; a method here so that it compiles next to the other guides' entry points.
public static class OrderProgram
{
    public static async Task RunAsync(string[] args)
    {
        #region snippet:inmemory-program
        var builder = Host.CreateApplicationBuilder(args);   // appsettings.json, environment variables, command line
        builder.Services.AddOrderMessaging(builder.Configuration);
        builder.Services.AddOrderTelemetry();
        using var host = builder.Build();
        await host.RunAsync();
        #endregion
    }
}
