using System.Net.Security;
using AwesomeAssertions;
using Microsoft.Extensions.Options;
using ZedLabs.MessageQueue.Configuration;
using ZedLabs.MessageQueue.Exceptions;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.Tests.RabbitMQ;

// Endpoint, credential and TLS settings (S7). Validation runs at startup and in the constructor; nothing here connects.
[Trait("Category", "Unit")]
public class Given_a_rabbitmq_connection_config
{
    static RabbitMqConfig Config(string connectionString) => new() { ConnectionString = connectionString, QueueName = "orders" };

    static ValidateOptionsResult Validate(RabbitMqConfig config) => new RabbitMqConfigValidator().Validate(null, config);

    [Theory]
    [InlineData("amqp://user:pw@localhost/")]
    [InlineData("amqp://user:pw@LOCALHOST:5672/")]
    [InlineData("amqp://user:pw@127.0.0.1/")]
    [InlineData("amqp://user:pw@[::1]/")]
    public void When_Plain_Amqp_Targets_The_Local_Machine(string connectionString) =>
        Validate(Config(connectionString)).Succeeded.Should().BeTrue("it should allow unencrypted AMQP for local development by default");

    [Theory]
    [InlineData("amqp://user:pw@rabbit.internal/")]
    [InlineData("amqp://user:pw@10.0.0.5:5672/")]
    [InlineData("amqp://user:pw@localhost.example.com/")]
    [InlineData("amqp://user:pw@localhostile/")]
    public void When_Plain_Amqp_Targets_A_Remote_Host(string connectionString)
    {
        var result = Validate(Config(connectionString));

        result.Failed.Should().BeTrue("it should require TLS for any host other than localhost unless told otherwise");
        result.FailureMessage.Should().Contain("RequireTls", "it should name the setting that rejected the connection")
            .And.Contain("amqps://", "it should say how to fix it");
    }

    [Fact]
    public void When_RequireTls_Is_Turned_Off_For_A_Remote_Host() =>
        Validate(Config("amqp://user:pw@rabbit.internal/") with { RequireTls = false }).Succeeded
            .Should().BeTrue("it should let an explicit RequireTls = false accept plain AMQP");

    [Fact]
    public void When_RequireTls_Is_Turned_On_For_Localhost() =>
        Validate(Config("amqp://user:pw@localhost/") with { RequireTls = true }).Failed
            .Should().BeTrue("it should enforce an explicit RequireTls even for localhost");

    [Fact]
    public void When_Amqps_Targets_A_Remote_Host() =>
        Validate(Config("amqps://user:pw@rabbit.internal/") with { RequireTls = true }).Succeeded
            .Should().BeTrue("it should accept an encrypted connection");

    [Fact]
    public void When_Certificate_Errors_Are_Accepted_Without_The_Insecure_Flag()
    {
        var config = Config("amqps://user:pw@rabbit.internal/");
        config.Tls.AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors | SslPolicyErrors.RemoteCertificateNameMismatch;

        var result = Validate(config);

        result.Failed.Should().BeTrue("it should not let a configuration value quietly switch off certificate validation");
        result.FailureMessage.Should().Contain("AllowInsecureCertificates", "it should name the explicit opt-in");
    }

    [Fact]
    public void When_Certificate_Errors_Are_Accepted_With_The_Insecure_Flag()
    {
        var config = Config("amqps://user:pw@rabbit.internal/");
        config.Tls.AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateNameMismatch;
        config.Tls.AllowInsecureCertificates = true;

        Validate(config).Succeeded.Should().BeTrue("it should accept certificate errors once insecure certificates are explicitly allowed");
    }

    [Fact]
    public void When_Unknown_Certificate_Error_Flags_Are_Accepted()
    {
        var config = Config("amqps://user:pw@rabbit.internal/");
        config.Tls.AcceptablePolicyErrors = (SslPolicyErrors)0xFF;
        config.Tls.AllowInsecureCertificates = true;

        Validate(config).FailureMessage.Should().Contain("unknown flags", "it should reject values that are not certificate policy errors");
    }

    [Fact]
    public void When_Using_Separate_Endpoint_Settings()
    {
        var config = new RabbitMqConfig
        {
            QueueName = "orders",
            HostName = "rabbit.internal",
            Port = 5671,
            UserName = "app",
            Password = "S3CRET",
            VirtualHost = "sales",
            Tls = new RabbitMqTlsConfig { Enabled = true, ServerName = "rabbit.example.com" }
        };

        Validate(config).Succeeded.Should().BeTrue("it should accept HostName, Port, credentials and TLS instead of a connection string");
        var factory = RabbitMqConnections.CreateFactory(config);
        factory.HostName.Should().Be("rabbit.internal", "it should connect to the configured host");
        factory.Port.Should().Be(5671, "it should use the configured port");
        factory.UserName.Should().Be("app", "it should use the configured user");
        factory.Password.Should().Be("S3CRET", "it should use the configured password");
        factory.VirtualHost.Should().Be("sales", "it should use the configured virtual host");
        factory.Ssl.Enabled.Should().BeTrue("it should switch TLS on");
        factory.Ssl.ServerName.Should().Be("rabbit.example.com", "it should validate the certificate against the configured server name");
        factory.Ssl.AcceptablePolicyErrors.Should().Be(SslPolicyErrors.None, "it should accept no certificate errors by default");
    }

    [Fact]
    public void When_Separate_Endpoint_Settings_Have_No_Tls_For_A_Remote_Host() =>
        Validate(new RabbitMqConfig { QueueName = "orders", HostName = "rabbit.internal" }).FailureMessage
            .Should().Contain("RequireTls", "it should apply the same TLS rule to HostName settings");

    [Fact]
    public void When_Using_An_Amqps_Connection_String()
    {
        var config = Config("amqps://user:pw@rabbit.internal/");
        config.Tls.ServerName = "rabbit.example.com";

        var factory = RabbitMqConnections.CreateFactory(config);

        factory.Ssl.Enabled.Should().BeTrue("it should keep the TLS that amqps:// switches on");
        factory.Ssl.ServerName.Should().Be("rabbit.example.com", "it should apply the server name override");
    }

    [Fact]
    public void When_Both_A_Connection_String_And_A_HostName_Are_Set() =>
        Validate(Config("amqp://localhost/") with { HostName = "localhost" }).FailureMessage
            .Should().Contain("either ConnectionString or HostName", "it should not guess which endpoint wins");

    [Fact]
    public void When_Credentials_Are_Set_Alongside_A_Connection_String() =>
        Validate(Config("amqp://localhost/") with { Password = "other" }).FailureMessage
            .Should().Contain("only with HostName", "it should not silently ignore credentials that the connection string overrides");

    [Fact]
    public void When_Tls_Is_Enabled_Alongside_A_Plain_Connection_String()
    {
        var config = Config("amqp://localhost/");
        config.Tls.Enabled = true;

        Validate(config).FailureMessage.Should().Contain("amqps://", "it should ask for the scheme instead of a conflicting flag");
    }

    [Theory]
    [InlineData("rabbit.internal")]
    [InlineData("http://rabbit.internal/")]
    public void When_The_Connection_String_Is_Not_An_Amqp_Uri(string connectionString) =>
        Validate(Config(connectionString)).FailureMessage.Should().Contain("amqp://", "it should require an AMQP URI");

    [Fact]
    public void When_Constructing_A_Queue_For_A_Remote_Host_Over_Plain_Amqp()
    {
        var act = () => new RabbitMqMessageQueue<TestMessage>(Config("amqp://user:pw@rabbit.internal/"));

        act.Should().Throw<OptionsValidationException>("it should validate the TLS rule before connecting").WithMessage("*RequireTls*");
    }

    [Fact]
    public async Task When_The_Config_Is_Changed_To_Plain_Amqp_After_Construction()
    {
        var config = Config("amqps://user:pw@rabbit.internal/");
        await using var queue = new RabbitMqMessageQueue<TestMessage>(config);
        config.ConnectionString = "amqp://user:pw@rabbit.internal/";

        var act = () => queue.InitializeAsync(TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ConfigurationException>("it should re-check TLS when it connects, without dialling")
            .WithMessage("*RequireTls*");
    }

    [Fact]
    public void When_Printing_A_Config_With_A_Password()
    {
        var config = new RabbitMqConfig { QueueName = "orders", HostName = "rabbit", Password = "S3CRET" };

        config.ToString().Should().NotContain("S3CRET", "it should never print the password").And.Contain("Password = ***",
            "it should show that a password is set");
    }

    [Fact]
    public void When_The_Shutdown_Timeout_Is_Zero() =>
        Validate(Config("amqp://localhost/") with { ShutdownTimeout = TimeSpan.Zero }).FailureMessage
            .Should().Contain("ShutdownTimeout", "it should require a positive shutdown timeout");

    [Fact]
    public void When_Nothing_Says_Where_The_Broker_Is() =>
        ((Action)(() => ConfigValidation.ThrowIfInvalid(new RabbitMqConfig { QueueName = "orders" }, new RabbitMqConfigValidator())))
            .Should().Throw<OptionsValidationException>("it should require an endpoint").WithMessage("*ConnectionString*HostName*");

    [Theory]
    [InlineData(240, true)]
    [InlineData(241, false)]
    public void When_The_Queue_Name_Is_Long(int length, bool valid)
    {
        var config = Config("amqp://localhost/") with { QueueName = new string('q', length) };

        Validate(config).Succeeded.Should().Be(valid,
            "it should leave room for the '.retry.<ms>' and '.deadletter' suffixes within RabbitMQ's 255-byte limit");
    }

    [Fact]
    public void When_The_Queue_Name_Is_Long_In_Bytes() =>
        Validate(Config("amqp://localhost/") with { QueueName = new string('\u00e9', 121) }).FailureMessage
            .Should().Contain("QueueName", "it should count UTF-8 bytes, not characters");

    [Fact]
    public void When_The_Dead_Letter_Queue_Name_Is_Too_Long() =>
        Validate(Config("amqp://localhost/") with { DeadLetterQueueName = new string('d', 256) }).FailureMessage
            .Should().Contain("DeadLetterQueueName", "it should reject a name RabbitMQ cannot declare");

    [Fact]
    public void When_The_Exchange_Name_Is_Too_Long() =>
        Validate(Config("amqp://localhost/") with { ExchangeName = new string('e', 256) }).FailureMessage
            .Should().Contain("ExchangeName", "it should reject a name RabbitMQ cannot declare");

    [Fact]
    public async Task When_More_Handlers_May_Run_Than_Messages_Are_Prefetched()
    {
        var logger = new RecordingLogger();

        await using var queue = new RabbitMqMessageQueue<TestMessage>(
            Config("amqp://localhost/") with { MaxConcurrentCalls = 20, PrefetchCount = 10 }, logger: logger);

        logger.Entries.Should().Contain(e => e.Level == Microsoft.Extensions.Logging.LogLevel.Warning
                && e.Message.Contains("MaxConcurrentCalls", StringComparison.Ordinal) && e.Message.Contains("PrefetchCount", StringComparison.Ordinal),
            "it should warn that PrefetchCount, not MaxConcurrentCalls, will bound concurrency");
    }

    // S9 on the wire: the client never buffers a body larger than the library would accept, or larger than RabbitMQ's
    // own default max_message_size (16 MiB), whichever is larger. Below that floor an oversized body would make the
    // client drop the connection instead of letting the pipeline dead-letter it.
    [Theory]
    [InlineData(1024 * 1024, 16 * 1024 * 1024)]
    [InlineData(32 * 1024 * 1024, 32 * 1024 * 1024)]
    public void When_Limiting_Inbound_Message_Bodies(int maxMessageSizeBytes, int expected) =>
        RabbitMqConnections.CreateFactory(Config("amqp://user:pw@localhost/") with { MaxMessageSizeBytes = maxMessageSizeBytes })
            .MaxInboundMessageBodySize.Should().Be((uint)expected, "it should cap what the client reads to the configured size limit");
}
