using BenchmarkDotNet.Attributes;
using ZedLabs.MessageQueue.Providers;
using ZedLabs.MessageQueue.Serialization;

namespace ZedLabs.MessageQueue.Benchmarks;

// §4.1: serialize to and deserialize from bytes (no string round trip, no body copy).
[MemoryDiagnoser]
public class SerializerBenchmarks
{
    readonly Order order = Order.Sample(1);
    byte[] body = [];

    [Params(SerializationFormat.Json, SerializationFormat.MessagePack)]
    public SerializationFormat Format { get; set; }

    [GlobalSetup]
    public void Setup() => body = MessageSerializer.Serialize(order, Format);

    [Benchmark]
    public byte[] Serialize() => MessageSerializer.Serialize(order, Format);

    [Benchmark]
    public Order? Deserialize() => MessageSerializer.Deserialize<Order>(body, Format);
}
