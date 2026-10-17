using System.ComponentModel.DataAnnotations;
using System.Net.Security;
using System.Text;
using ESE.MessageQueue.Configuration;

namespace ESE.MessageQueue.RabbitMQ;

public record RabbitMqConfig : MessageQueueConfig
{
    public override MessageQueueProvider Provider => MessageQueueProvider.RabbitMq;

    public static readonly TimeSpan DefaultShutdownTimeout = TimeSpan.FromSeconds(30);

    public string ConnectionString { get; set; } = string.Empty;

    public string HostName { get; set; } = string.Empty;

    [Range(0, 65535)]
    public int Port { get; set; }

    public string UserName { get; set; } = string.Empty;

    public string Password { get; set; } = string.Empty;

    public string VirtualHost { get; set; } = string.Empty;

    public bool? RequireTls { get; set; }

    [Required]
    public RabbitMqTlsConfig Tls { get; set; } = new();

    public string DeadLetterQueueName { get; set; } = string.Empty;

    public bool UseQuorumQueues { get; set; } = true;

    [Range(1, int.MaxValue)]
    public int QuorumInitialGroupSize { get; set; } = 3;

    [Range(1, ushort.MaxValue)]
    public int PrefetchCount { get; set; } = 10;

    public string ExchangeName { get; set; } = string.Empty;

    [Required]
    public string ExchangeType { get; set; } = "direct";

    public bool EnablePublisherConfirms { get; set; } = true;

    [Range(1, 64)]
    public int PublisherChannelPoolSize { get; set; } = 2;

    [Range(1, int.MaxValue)]
    public int MaxOutstandingConfirms { get; set; } = 1000;

    public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public TimeSpan ShutdownTimeout { get; set; } = DefaultShutdownTimeout;

    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(60);

    public TimeSpan NetworkRecoveryInterval { get; set; } = TimeSpan.FromSeconds(10);

    protected override bool PrintMembers(StringBuilder builder) =>
        ConfigValidation.PrintMembers(this, builder, nameof(ConnectionString), nameof(Password));

    // Test seam: shortens the retry queues' x-expires so expiry can be observed in seconds.
    internal TimeSpan? RetryQueueExpiresOverride { get; init; }

    internal string EffectiveDeadLetterQueueName =>
        string.IsNullOrWhiteSpace(DeadLetterQueueName) ? $"{QueueName}.deadletter" : DeadLetterQueueName;
}

public record RabbitMqTlsConfig
{
    public bool Enabled { get; set; }

    public string ServerName { get; set; } = string.Empty;

    public SslPolicyErrors AcceptablePolicyErrors { get; set; } = SslPolicyErrors.None;

    public bool AllowInsecureCertificates { get; set; }
}
