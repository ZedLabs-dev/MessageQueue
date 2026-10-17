using System.Text;

namespace ESE.MessageQueue.RabbitMQ;

// RabbitMQ rejects queue and exchange names longer than 255 bytes (UTF-8). The library derives
// "{QueueName}.retry.{ms}" (at most 15 more bytes: delays are capped at one day) and "{QueueName}.deadletter".
static class RabbitMqNames
{
    public const int MaxNameBytes = 255;
    public const int MaxQueueNameBytes = MaxNameBytes - 15;

    public static void Validate(RabbitMqConfig config, ICollection<string> errors)
    {
        if (Bytes(config.QueueName) > MaxQueueNameBytes)
            errors.Add($"QueueName must be at most {MaxQueueNameBytes} bytes (UTF-8), leaving room for the '.retry.<ms>' and '.deadletter' queues");

        if (Bytes(config.DeadLetterQueueName) > MaxNameBytes)
            errors.Add($"DeadLetterQueueName must be at most {MaxNameBytes} bytes (UTF-8)");

        if (Bytes(config.ExchangeName) > MaxNameBytes)
            errors.Add($"ExchangeName must be at most {MaxNameBytes} bytes (UTF-8)");
    }

    static int Bytes(string? name) => name is null ? 0 : Encoding.UTF8.GetByteCount(name);
}
