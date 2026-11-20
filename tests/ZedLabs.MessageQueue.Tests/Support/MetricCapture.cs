using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace ZedLabs.MessageQueue.Tests;

// Listens to one Meter (use a meter name unique to the test) and keeps every measurement, by instrument name.
sealed class MetricCapture : IDisposable
{
    readonly MeterListener listener = new();
    readonly ConcurrentQueue<Measurement> measurements = new();

    public MetricCapture(string meterName)
    {
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == meterName)
            {
                Instruments[instrument.Name] = instrument;
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument, value, tags));
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument, value, tags));
        listener.Start();
    }

    public ConcurrentDictionary<string, Instrument> Instruments { get; } = new();

    public IReadOnlyList<Measurement> For(string instrument) => [.. measurements.Where(m => m.Instrument == instrument)];

    public double Sum(string instrument) => For(instrument).Sum(m => m.Value);

    void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags) =>
        measurements.Enqueue(new Measurement(instrument.Name, value, tags.ToArray().ToDictionary(t => t.Key, t => t.Value)));

    public void Dispose() => listener.Dispose();

    public sealed record Measurement(string Instrument, double Value, IReadOnlyDictionary<string, object?> Tags);
}
