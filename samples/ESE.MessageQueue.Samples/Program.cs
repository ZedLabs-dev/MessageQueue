using ESE.MessageQueue.Samples;
using Microsoft.Extensions.Hosting;

// dotnet run --project samples/ESE.MessageQueue.Samples                                # in-memory (default)
// dotnet run --project samples/ESE.MessageQueue.Samples -- --environment RabbitMq      # appsettings.RabbitMq.json
// dotnet run --project samples/ESE.MessageQueue.Samples -- --environment AzureServiceBus
// (the `--` passes --environment to the app; `dotnet run --environment` means something else)
using var host = SampleHost.CreateBuilder(args).Build();
await host.RunAsync();
