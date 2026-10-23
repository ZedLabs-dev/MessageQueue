using System.Collections.Concurrent;
using System.Net.Security;
using AwesomeAssertions;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.RabbitMQ;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// Mutual TLS against a real broker (ADR 0022): RabbitMQ with ssl_options.verify = verify_peer and
// fail_if_no_peer_cert = true, a CA, server and client certificate generated for the run, and the broker's certificate
// trusted through Tls:CaCertificatePath rather than by switching validation off.
[Trait("Category", "Integration")]
[Collection(RabbitMqMutualTlsCollection.Name)]
public sealed class Given_a_rabbitmq_broker_requiring_client_certificates : IAsyncLifetime
{
    const int Timeout = 60_000;

    readonly RabbitMqMutualTlsBrokerFixture broker;
    readonly List<RabbitMqMessageQueue<TestMessage>> created = [];

    public Given_a_rabbitmq_broker_requiring_client_certificates(RabbitMqMutualTlsBrokerFixture broker)
    {
        this.broker = broker;
    }

    public ValueTask InitializeAsync()
    {
        broker.SkipUnlessAvailable();
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var queue in created)
            await queue.DisposeAsync();
    }

    RabbitMqMessageQueue<TestMessage> Create(Action<RabbitMqTlsConfig>? tls = null)
    {
        var queue = new RabbitMqMessageQueue<TestMessage>(broker.NewConfig("mtls", tls));
        created.Add(queue);
        return queue;
    }

    void WithClientCertificate(RabbitMqTlsConfig tls)
    {
        tls.ClientCertificatePath = broker.Authority.ClientPfxPath;
        tls.ClientCertificatePassword = TestCertificateAuthority.Password;
    }

    static async Task ShouldBeRefusedAsync(RabbitMqMessageQueue<TestMessage> queue, string because, CancellationToken cancellationToken)
    {
        var act = () => queue.InitializeAsync(cancellationToken);

        var thrown = await act.Should().ThrowAsync<MessageQueueException>(because);
        thrown.WithMessage("*Failed to connect to RabbitMQ*");
        thrown.Which.ToString().Should().NotContain(RabbitMqMutualTlsBrokerFixture.Password, "it should not leak the connection password")
            .And.NotContain(TestCertificateAuthority.Password, "it should not leak the client certificate password");
    }

    public static TheoryData<string> CertificateFiles => ["pfx", "pem"];

    [Theory(Timeout = Timeout)]
    [MemberData(nameof(CertificateFiles))]
    public async Task When_The_Client_Presents_Its_Certificate(string format)
    {
        var queue = Create(tls =>
        {
            if (format == "pfx")
            {
                WithClientCertificate(tls);
                return;
            }
            tls.ClientCertificatePath = broker.Authority.ClientCertificatePemPath;
            tls.ClientCertificateKeyPath = broker.Authority.ClientKeyPemPath;
        });
        var received = new ConcurrentQueue<TestMessage>();
        var message = new TestMessage($"mtls-{format}", "over amqps with a client certificate");

        await queue.SubscribeAsync(m =>
        {
            received.Enqueue(m);
            return Task.FromResult(true);
        }, TestContext.Current.CancellationToken);
        await queue.PublishAsync(message, TestContext.Current.CancellationToken);
        await Wait.ForCountAsync(() => received.Count, 1, Wait.DeliveryTimeout);

        received.Should().ContainSingle($"it should connect with the {format} client certificate and round-trip a message")
            .Which.Should().Be(message, "it should deliver the published message");
    }

    [Fact(Timeout = Timeout)]
    public async Task When_The_Client_Presents_No_Certificate() =>
        await ShouldBeRefusedAsync(Create(), "it should report that the broker refused a client without a certificate", TestContext.Current.CancellationToken);

    [Fact(Timeout = Timeout)]
    public async Task When_The_Broker_Ca_Is_Not_Trusted() =>
        await ShouldBeRefusedAsync(Create(tls =>
        {
            WithClientCertificate(tls);
            tls.CaCertificatePath = string.Empty;
        }), "it should refuse a broker certificate the machine's trust store does not know", TestContext.Current.CancellationToken);

    [Fact(Timeout = Timeout)]
    public async Task When_Another_Ca_Is_Configured()
    {
        using var unrelated = new TestCertificateAuthority("ese-messagequeue-unrelated-ca");

        await ShouldBeRefusedAsync(Create(tls =>
        {
            WithClientCertificate(tls);
            tls.CaCertificatePath = unrelated.CaPemPath;
        }), "it should refuse a broker certificate that does not chain to the configured CA", TestContext.Current.CancellationToken);
    }

    [Fact(Timeout = Timeout)]
    public async Task When_The_Server_Name_Does_Not_Match() =>
        await ShouldBeRefusedAsync(Create(tls =>
        {
            WithClientCertificate(tls);
            tls.ServerName = "not-the-broker.example";
        }), "it should refuse a trusted certificate issued for another name", TestContext.Current.CancellationToken);

    [Fact(Timeout = Timeout)]
    public async Task When_A_Name_Mismatch_Is_Explicitly_Accepted()
    {
        var queue = Create(tls =>
        {
            WithClientCertificate(tls);
            tls.ServerName = "not-the-broker.example";
            tls.AllowInsecureCertificates = true;
            tls.AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateNameMismatch;
        });

        var act = () => queue.InitializeAsync(TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync("it should connect when AllowInsecureCertificates and AcceptablePolicyErrors accept the name mismatch");
    }

    [Fact(Timeout = Timeout)]
    public async Task When_An_Untrusted_Ca_Is_Explicitly_Accepted()
    {
        var queue = Create(tls =>
        {
            WithClientCertificate(tls);
            tls.CaCertificatePath = string.Empty;
            tls.AllowInsecureCertificates = true;
            tls.AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors;
        });

        var act = () => queue.InitializeAsync(TestContext.Current.CancellationToken);

        await act.Should().NotThrowAsync("it should connect when AllowInsecureCertificates and AcceptablePolicyErrors accept the unknown CA");
    }
}
