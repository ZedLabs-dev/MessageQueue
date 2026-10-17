using ESE.MessageQueue.Providers;

namespace ESE.MessageQueue.RabbitMQ;

// RabbitMQ's rules on top of the shared ones (connection and TLS, names, timeouts).
sealed class RabbitMqConfigValidator : MessageQueueConfigValidator<RabbitMqConfig>
{
    protected override void ValidateProvider(RabbitMqConfig options, ICollection<string> errors)
    {
        foreach (var error in RabbitMqConnections.Validate(options))
            errors.Add(error);
        RabbitMqNames.Validate(options, errors);

        if (options.ConfirmTimeout <= TimeSpan.Zero)
            errors.Add("ConfirmTimeout must be > 0");

        if (options.HeartbeatInterval < TimeSpan.Zero)
            errors.Add("HeartbeatInterval must be >= 0");

        if (options.NetworkRecoveryInterval <= TimeSpan.Zero)
            errors.Add("NetworkRecoveryInterval must be > 0");

        if (options.ShutdownTimeout <= TimeSpan.Zero)
            errors.Add("ShutdownTimeout must be > 0");
    }
}
