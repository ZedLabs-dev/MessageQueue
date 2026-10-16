using RabbitMQ.Client;

namespace ESE.MessageQueue.Tests;

public static class Helper
{
    public static bool IsRabbitMqAvailable()
    {
        try
        {
            var factory = new ConnectionFactory { Uri = new Uri("amqp://admin:password123@localhost:5672/") };
            using var connection = factory.CreateConnection();
            return connection.CreateModel() != null;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
