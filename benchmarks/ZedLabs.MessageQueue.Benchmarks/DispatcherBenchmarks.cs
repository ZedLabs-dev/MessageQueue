using System.Diagnostics;
using System.Diagnostics.Metrics;
using BenchmarkDotNet.Attributes;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.InMemory;
using ZedLabs.MessageQueue.Processing;
using ZedLabs.MessageQueue.Providers;
using ZedLabs.MessageQueue.Serialization;
using ZedLabs.MessageQueue.Telemetry;
using ZedLabs.MessageQueue.Telemetry.OpenTelemetry;

namespace ZedLabs.MessageQueue.Benchmarks;

// The shared pipeline's cost per message (ADR 0004) on top of deserializing and calling the handler: size check,
// telemetry, outcome. "Baseline" deserializes and calls the same handler directly.
[MemoryDiagnoser]
public class DispatcherBenchmarks : IDisposable
{
    static readonly Task<MessageResult> Acknowledged = Task.FromResult(MessageResult.Acknowledge);

    readonly byte[] body = MessageSerializer.Serialize(Order.Sample(1), SerializationFormat.Json);
    readonly MessageContext context = new()
    {
        MessageId = "m-1",
        TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    };
    readonly NoOpActions actions = new();
    MessageDispatcher<Order> dispatcher = null!;
    OpenTelemetryProvider? provider;
    ActivityListener? spans;
    MeterListener? metrics;

    // None: NullTelemetryProvider. Unobserved: OpenTelemetryProvider nobody listens to. Recorded: an ActivityListener
    // samples every span and a MeterListener takes every measurement (what a traced, metered host pays).
    [Params("None", "Unobserved", "Recorded")]
    public string Telemetry { get; set; } = "None";

    [GlobalSetup]
    public void Setup()
    {
        var source = $"ZedLabs.MessageQueue.Benchmarks.{Telemetry}";
        provider = Telemetry == "None" ? null : new OpenTelemetryProvider(source);
        if (Telemetry == "Recorded")
        {
            spans = new ActivityListener
            {
                ShouldListenTo = s => s.Name == source,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(spans);
            metrics = new MeterListener
            {
                InstrumentPublished = (instrument, listener) =>
                {
                    if (instrument.Meter.Name == source)
                        listener.EnableMeasurementEvents(instrument);
                }
            };
            metrics.SetMeasurementEventCallback<long>(static (_, _, _, _) => { });
            metrics.SetMeasurementEventCallback<double>(static (_, _, _, _) => { });
            metrics.Start();
        }

        dispatcher = new MessageDispatcher<Order>(new InMemoryConfig { QueueName = "orders" }, new TelemetryTarget("benchmark", "orders", nameof(Order)),
            (_, _, _) => Acknowledged, (ITelemetryProvider?)provider ?? NullTelemetryProvider.Instance);
    }

    [GlobalCleanup]
    public void Dispose()
    {
        spans?.Dispose();
        metrics?.Dispose();
        provider?.Dispose();
        GC.SuppressFinalize(this);
    }

    [Benchmark(Baseline = true)]
    public Task<MessageResult> Baseline() =>
        MessageSerializer.Deserialize<Order>(body, SerializationFormat.Json) is { } message ? Handle(message) : Acknowledged;

    [Benchmark]
    public Task Dispatch() => dispatcher.DispatchAsync(body, context, actions, CancellationToken.None);

    static Task<MessageResult> Handle(Order message) => message.Amount >= 0 ? Acknowledged : Acknowledged;

    sealed class NoOpActions : IDeliveryActions
    {
        public Task CompleteAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task RescheduleAsync(TimeSpan delay, int nextAttempt, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task DeadLetterAsync(DeadLetterReason reason, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
