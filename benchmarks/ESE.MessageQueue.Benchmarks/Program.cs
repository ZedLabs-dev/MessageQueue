using BenchmarkDotNet.Running;
using ESE.MessageQueue.Benchmarks;

// The RabbitMQ benchmarks need a broker: MQ_BENCH_RABBITMQ (an amqp:// connection string), else a Testcontainers
// container started here and shared with BenchmarkDotNet's child processes through that variable. It is started only
// when the --filter selects a RabbitMQ benchmark. Without Docker they are left out, with a message.
Type[] rabbitMqBenchmarks = [typeof(RabbitMqPublishBenchmarks), typeof(RabbitMqConsumeBenchmarks)];
var wantsRabbitMq = rabbitMqBenchmarks.Any(type => BenchmarkFilter.Selects(args, type));
await using var broker = wantsRabbitMq ? await BenchmarkBroker.StartAsync() : null;
var benchmarks = typeof(Program).Assembly.GetTypes()
    .Where(t => t.Name.EndsWith("Benchmarks", StringComparison.Ordinal) && !t.IsAbstract)
    .Where(t => broker?.Available == true || !rabbitMqBenchmarks.Contains(t))
    .ToArray();
BenchmarkSwitcher.FromTypes(benchmarks).Run(args);
