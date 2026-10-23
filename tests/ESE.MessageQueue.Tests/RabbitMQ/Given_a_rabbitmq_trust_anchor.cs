using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using AwesomeAssertions;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.RabbitMQ;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace ESE.MessageQueue.Tests.RabbitMQ;

// Tls:CaCertificatePath (ADR 0022): the broker's certificate must chain to the configured CA, and only to it, instead of
// the machine's trust store; the server name is still checked, and AcceptablePolicyErrors still decides what may be
// overlooked. The validation callback is called directly with generated certificates: nothing connects.
[Trait("Category", "Unit")]
public sealed class Given_a_rabbitmq_trust_anchor : IDisposable
{
    readonly TestCertificateAuthority authority = new();
    readonly TestCertificateAuthority unrelated = new("ese-messagequeue-unrelated-ca");

    public void Dispose()
    {
        authority.Dispose();
        unrelated.Dispose();
    }

    static RabbitMqConfig Config(Action<RabbitMqTlsConfig> tls)
    {
        var config = new RabbitMqConfig { ConnectionString = "amqps://user:pw@rabbit.internal/", QueueName = "orders" };
        tls(config.Tls);
        return config;
    }

    static ValidateOptionsResult Validate(RabbitMqConfig config) => new RabbitMqConfigValidator().Validate(null, config);

    static bool Accepts(ConnectionFactory factory, X509Certificate? certificate, SslPolicyErrors errors)
    {
        var callback = factory.Ssl.CertificateValidationCallback;
        callback.Should().NotBeNull("it should validate the broker's certificate against the configured CA");
        return callback!(factory, certificate, null, errors);
    }

    static ConnectionFactory Trusting(string caPath, Action<RabbitMqTlsConfig>? more = null) =>
        RabbitMqConnections.CreateFactory(Config(tls =>
        {
            tls.CaCertificatePath = caPath;
            more?.Invoke(tls);
        }));

    [Fact]
    public void When_The_Ca_Is_A_Pem_File()
    {
        var config = Config(tls => tls.CaCertificatePath = authority.CaPemPath);

        Validate(config).Succeeded.Should().BeTrue("it should accept a readable PEM CA certificate");
        Accepts(RabbitMqConnections.CreateFactory(config), authority.Server, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeTrue("it should trust a broker certificate issued by the configured CA although the machine does not");
    }

    [Fact]
    public void When_The_Ca_Is_A_Der_File()
    {
        var config = Config(tls => tls.CaCertificatePath = authority.CaDerPath);

        Validate(config).Succeeded.Should().BeTrue("it should accept a DER CA certificate");
        Accepts(RabbitMqConnections.CreateFactory(config), authority.Server, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeTrue("it should trust a broker certificate issued by the configured DER CA");
    }

    [Fact]
    public void When_The_Pem_File_Bundles_Several_Cas()
    {
        var bundle = authority.Write("bundle.crt", unrelated.Certificate.ExportCertificatePem() + "\n" + authority.Certificate.ExportCertificatePem());

        Accepts(Trusting(bundle), authority.Server, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeTrue("it should trust every CA in a PEM bundle, not only the first");
    }

    [Fact]
    public void When_The_Broker_Certificate_Is_From_Another_Ca()
    {
        Accepts(Trusting(unrelated.CaPemPath), authority.Server, SslPolicyErrors.None)
            .Should().BeFalse("it should trust only the configured CA, even when the machine's trust store accepts the certificate");
    }

    [Fact]
    public void When_The_Server_Name_Does_Not_Match()
    {
        Accepts(Trusting(authority.CaPemPath), authority.Server, SslPolicyErrors.RemoteCertificateNameMismatch | SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeFalse("it should still refuse a certificate issued for another name by the trusted CA");
    }

    [Fact]
    public void When_A_Name_Mismatch_Is_Explicitly_Acceptable()
    {
        var factory = Trusting(authority.CaPemPath, tls =>
        {
            tls.AllowInsecureCertificates = true;
            tls.AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateNameMismatch;
        });

        Accepts(factory, authority.Server, SslPolicyErrors.RemoteCertificateNameMismatch | SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeTrue("it should overlook a name mismatch that AcceptablePolicyErrors allows");
        Accepts(factory, unrelated.Server, SslPolicyErrors.RemoteCertificateNameMismatch)
            .Should().BeFalse("it should not overlook an untrusted CA because a name mismatch is acceptable");
    }

    [Fact]
    public void When_Chain_Errors_Are_Explicitly_Acceptable()
    {
        var factory = Trusting(authority.CaPemPath, tls =>
        {
            tls.AllowInsecureCertificates = true;
            tls.AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors;
        });

        Accepts(factory, unrelated.Server, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeTrue("it should overlook an untrusted CA that AcceptablePolicyErrors allows");
    }

    [Fact]
    public void When_The_Broker_Sends_An_Intermediate_Ca()
    {
        var (intermediate, server) = authority.IssueThroughIntermediate();
        using (intermediate)
        using (server)
        using (var presented = new X509Chain())
        {
            presented.ChainPolicy.ExtraStore.Add(intermediate);
            var callback = Trusting(authority.CaPemPath).Ssl.CertificateValidationCallback!;

            callback(this, server, presented, SslPolicyErrors.RemoteCertificateChainErrors)
                .Should().BeTrue("it should build the chain through the intermediate CA the broker sent along");
            callback(this, server, null, SslPolicyErrors.RemoteCertificateChainErrors)
                .Should().BeFalse("it should not trust a certificate whose issuer is neither configured nor sent");
        }
    }

    // The broker's certificate checked with the certificates it sent along (an SslStream chain's ExtraStore).
    bool Accepts(X509Certificate2 certificate, params X509Certificate2[] sent)
    {
        using var presented = new X509Chain();
        foreach (var each in sent)
            presented.ChainPolicy.ExtraStore.Add(each);
        return Trusting(authority.CaPemPath).Ssl.CertificateValidationCallback!(this, certificate, presented, SslPolicyErrors.RemoteCertificateChainErrors);
    }

    [Fact]
    public void When_An_Intermediate_Exceeds_Its_Path_Length()
    {
        using var first = TestCertificateBuilder.Ca("CN=intermediate-path-length-0", authority.Certificate, pathLength: 0);
        using var second = TestCertificateBuilder.Ca("CN=intermediate-below-the-limit", first);
        using var leaf = TestCertificateBuilder.Leaf(second, [TestCertificateBuilder.ServerAuthentication]);

        Accepts(leaf, first, second).Should().BeFalse("it should refuse a chain through an intermediate that its issuer's path length forbids");
    }

    [Fact]
    public void When_The_Broker_Certificate_Is_Signed_By_A_Server_Certificate()
    {
        using var server = TestCertificateBuilder.Leaf(authority.Certificate, [TestCertificateBuilder.ServerAuthentication]);
        using var forged = TestCertificateBuilder.Leaf(server, [TestCertificateBuilder.ServerAuthentication], ["evil.localhost", "localhost"]);

        Accepts(forged, server).Should().BeFalse("it should refuse a certificate signed with the key of a leaf certificate, which is not a CA");
    }

    [Fact]
    public void When_The_Broker_Certificate_Is_Signed_By_A_Client_Certificate_Without_Constraints()
    {
        using var client = TestCertificateBuilder.Leaf(authority.Certificate, null, ["client"], isCa: null);
        using var forged = TestCertificateBuilder.Leaf(client, [TestCertificateBuilder.ServerAuthentication]);

        Accepts(forged, client).Should().BeFalse("it should refuse a certificate signed by a CA-issued certificate that is not marked as a CA");
    }

    [Fact]
    public void When_The_Broker_Sends_A_Root_Named_Like_The_Anchor()
    {
        using var rogue = TestCertificateBuilder.Ca(authority.Certificate.Subject, subjectKeyIdentifier: TestCertificateBuilder.SubjectKeyIdentifier(authority.Certificate));
        using var intermediate = TestCertificateBuilder.Ca("CN=rogue-intermediate", rogue);
        using var leaf = TestCertificateBuilder.Leaf(intermediate, [TestCertificateBuilder.ServerAuthentication]);

        Accepts(leaf, rogue, intermediate).Should().BeFalse("it should never trust a certificate the broker sent, even one named like the anchor");
    }

    [Fact]
    public void When_Another_Ca_Forges_The_Anchor_Name()
    {
        var identifier = TestCertificateBuilder.SubjectKeyIdentifier(authority.Certificate);
        using var rogue = TestCertificateBuilder.Ca(authority.Certificate.Subject, subjectKeyIdentifier: identifier);
        using var leaf = TestCertificateBuilder.Leaf(rogue, [TestCertificateBuilder.ServerAuthentication], authorityKeyIdentifier: identifier);

        Accepts(leaf, rogue).Should().BeFalse("it should refuse a certificate from a CA that only copies the anchor's name and key identifier, sent along");
        Accepts(leaf).Should().BeFalse("it should refuse a certificate from a CA that only copies the anchor's name and key identifier");
    }

    [Fact]
    public void When_The_Presented_Chain_Is_Valid_For_Another_Root()
    {
        using var other = TestCertificateBuilder.Ca("CN=machine-trusted-stand-in");
        using var leaf = TestCertificateBuilder.Leaf(other, [TestCertificateBuilder.ServerAuthentication]);
        using var presented = new X509Chain();
        presented.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        presented.ChainPolicy.CustomTrustStore.Add(other);
        presented.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        presented.Build(leaf).Should().BeTrue("the stand-in for the machine's trust store accepts the chain");

        Trusting(authority.CaPemPath).Ssl.CertificateValidationCallback!(this, leaf, presented, SslPolicyErrors.None)
            .Should().BeFalse("it should judge the chain against the configured CA, not reuse the chain the TLS stack built against the machine's roots");
    }

    [Fact]
    public void When_An_Intermediate_Has_Name_Constraints()
    {
        using var constrained = TestCertificateBuilder.Ca("CN=constrained-intermediate", authority.Certificate,
            nameConstraints: TestCertificateBuilder.PermittedDnsSubtree("example.internal"));
        using var inside = TestCertificateBuilder.Leaf(constrained, [TestCertificateBuilder.ServerAuthentication], ["rabbit.example.internal"]);
        using var outside = TestCertificateBuilder.Leaf(constrained, [TestCertificateBuilder.ServerAuthentication], ["localhost"]);

        Accepts(inside, constrained).Should().BeTrue("it should accept a name the intermediate is permitted to certify");
        Accepts(outside, constrained).Should().BeFalse("it should refuse a name outside the intermediate's name constraints");
    }

    [Fact]
    public void When_The_Ca_File_Holds_Only_An_Intermediate()
    {
        var (intermediate, server) = authority.IssueThroughIntermediate();
        using (intermediate)
        using (server)
        {
            var intermediateOnly = authority.Write("intermediate-only.crt", intermediate.ExportCertificatePem());

            Trusting(intermediateOnly).Ssl.CertificateValidationCallback!(this, server, null, SslPolicyErrors.RemoteCertificateChainErrors)
                .Should().BeFalse("it should need the root in the CA file: an intermediate alone is not a trust anchor");
        }
    }

    [Fact]
    public void When_The_Broker_Certificate_Declares_Any_Usage_Or_None()
    {
        using var anyUsage = TestCertificateBuilder.Leaf(authority.Certificate, [TestCertificateBuilder.AnyExtendedKeyUsage]);
        using var noUsage = TestCertificateBuilder.Leaf(authority.Certificate, null);

        Accepts(anyUsage).Should().BeFalse("it should refuse a certificate whose only usage is anyExtendedKeyUsage");
        Accepts(noUsage).Should().BeTrue("it should accept a certificate that declares no extended key usage");
    }

    [Fact(Timeout = 60_000)]
    public async Task When_The_Broker_Certificate_Points_At_A_Download_Location()
    {
        using var intermediate = TestCertificateBuilder.Ca("CN=downloadable-intermediate", authority.Certificate);
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var fetches = 0;
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var serving = Task.Run(async () =>
        {
            var body = intermediate.RawData;
            while (!stop.IsCancellationRequested)
            {
                using var client = await listener.AcceptTcpClientAsync(stop.Token);
                Interlocked.Increment(ref fetches);
                var stream = client.GetStream();
                _ = await stream.ReadAsync(new byte[4096], stop.Token);
                await stream.WriteAsync(System.Text.Encoding.ASCII.GetBytes(
                    $"HTTP/1.1 200 OK\r\nContent-Type: application/pkix-cert\r\nContent-Length: {body.Length}\r\nConnection: close\r\n\r\n"), stop.Token);
                await stream.WriteAsync(body, stop.Token);
            }
        }, TestContext.Current.CancellationToken);
        using var leaf = TestCertificateBuilder.Leaf(intermediate, [TestCertificateBuilder.ServerAuthentication],
            authorityInformationAccess: $"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}/intermediate.cer");

        var accepted = Accepts(leaf);
        await stop.CancelAsync();
        listener.Stop();
        await serving.ContinueWith(_ => { }, TaskScheduler.Default);

        accepted.Should().BeFalse("it should not complete the chain with an intermediate the broker did not send");
        fetches.Should().Be(0, "it should never download a certificate from a location the broker's certificate names");
    }

    [Fact]
    public void When_Insecure_Acceptance_Is_Withdrawn_After_Validation()
    {
        var config = Config(tls =>
        {
            tls.CaCertificatePath = authority.CaPemPath;
            tls.AllowInsecureCertificates = true;
            tls.AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateNameMismatch;
        });
        Validate(config).Succeeded.Should().BeTrue("the settings are valid while AllowInsecureCertificates confirms them");
        config.Tls.AllowInsecureCertificates = false;

        var act = () => RabbitMqConnections.CreateFactory(config);

        act.Should().Throw<ConfigurationException>("it should re-check at connect time that AcceptablePolicyErrors is still confirmed")
            .WithMessage("*AllowInsecureCertificates*");
    }

    [Fact]
    public void When_The_Broker_Presents_No_Certificate()
    {
        Accepts(Trusting(authority.CaPemPath), null, SslPolicyErrors.RemoteCertificateNotAvailable)
            .Should().BeFalse("it should refuse a broker that presents no certificate");
    }

    [Fact]
    public void When_The_Broker_Certificate_Is_Not_For_Server_Authentication()
    {
        using var clientOnly = authority.IssueServer(new Oid("1.3.6.1.5.5.7.3.2"));

        Accepts(Trusting(authority.CaPemPath), clientOnly, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeFalse("it should refuse a certificate from the trusted CA that is not issued for server authentication");
    }

    [Fact]
    public void When_The_Broker_Certificate_Has_Expired()
    {
        using var expired = authority.IssueServer(new Oid("1.3.6.1.5.5.7.3.1"), DateTimeOffset.UtcNow.AddMinutes(-1));

        Accepts(Trusting(authority.CaPemPath), expired, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeFalse("it should refuse an expired certificate from the trusted CA");
    }

    [Fact]
    public void When_The_Broker_Certificate_Is_Not_An_X509Certificate2()
    {
        using var plain = new X509Certificate(authority.Server);

        Accepts(Trusting(authority.CaPemPath), plain, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeTrue("it should validate the certificate whatever type the TLS stack hands over");
    }

    [Fact]
    public void When_No_Ca_Is_Configured() =>
        RabbitMqConnections.CreateFactory(Config(_ => { })).Ssl.CertificateValidationCallback
            .Should().BeNull("it should keep the machine's trust store and AcceptablePolicyErrors when no CA is configured");

    [Fact]
    public void When_Tls_Is_Enabled_With_HostName()
    {
        var config = new RabbitMqConfig { HostName = "rabbit.internal", QueueName = "orders" };
        config.Tls.Enabled = true;
        config.Tls.CaCertificatePath = authority.CaPemPath;

        Validate(config).Succeeded.Should().BeTrue("it should accept a CA with Tls:Enabled on the HostName path");
        Accepts(RabbitMqConnections.CreateFactory(config), authority.Server, SslPolicyErrors.RemoteCertificateChainErrors)
            .Should().BeTrue("it should trust the configured CA on the HostName path too");
    }

    [Fact]
    public void When_A_Ca_Is_Configured_Without_Tls()
    {
        var config = new RabbitMqConfig { ConnectionString = "amqp://user:pw@localhost/", QueueName = "orders" };
        config.Tls.CaCertificatePath = authority.CaPemPath;

        Validate(config).Failures.Should().ContainSingle(f => f.Contains("Tls:CaCertificatePath") && f.Contains("TLS"),
            "it should refuse a CA that would never be used because the connection is not encrypted");
    }

    [Fact]
    public void When_The_Ca_File_Does_Not_Exist()
    {
        var missing = Path.Combine(authority.Folder, "missing.crt");
        var config = Config(tls => tls.CaCertificatePath = missing);

        Validate(config).Failures.Should().ContainSingle(f => f.Contains("Tls:CaCertificatePath") && f.Contains(missing),
            "it should name the setting and the path of a CA file that cannot be read");
        var act = () => RabbitMqConnections.CreateFactory(config);
        act.Should().Throw<ConfigurationException>("it should refuse to connect without the CA it was told to trust")
            .WithMessage("*Tls:CaCertificatePath*");
    }

    [Fact]
    public void When_The_Ca_File_Holds_No_Certificate()
    {
        var empty = authority.Write("empty.crt", "not a certificate");

        Validate(Config(tls => tls.CaCertificatePath = empty)).Failures
            .Should().ContainSingle(f => f.Contains("Tls:CaCertificatePath") && f.Contains(empty),
                "it should refuse a CA file without a certificate");
    }

    [Fact]
    public void When_The_Pem_File_Holds_Only_A_Key()
    {
        var keyOnly = authority.Write("key-only.pem", File.ReadAllText(authority.ServerKeyPemPath));

        Validate(Config(tls => tls.CaCertificatePath = keyOnly)).Failures
            .Should().ContainSingle(f => f.Contains("Tls:CaCertificatePath") && f.Contains("no certificate"),
                "it should refuse a PEM file that holds no certificate");
    }

    [Fact]
    public void When_The_Ca_Path_Is_Blank() =>
        Validate(Config(tls => tls.CaCertificatePath = "  ")).Succeeded
            .Should().BeTrue("it should treat a blank CA path as not configured");

    [Fact]
    public void When_Printing_The_Config() =>
        Config(tls => tls.CaCertificatePath = authority.CaPemPath).ToString()
            .Should().Contain(authority.CaPemPath, "it should show the CA path, which is not a secret");

    [Fact]
    public async Task When_A_Queue_Connects_More_Than_Once()
    {
        var queue = new RabbitMqMessageQueue<TestMessage>(Config(tls => tls.CaCertificatePath = authority.CaPemPath));

        queue.CreateConnectionFactory();
        var first = queue.TrustAnchors;
        queue.CreateConnectionFactory();

        first.Should().NotBeNull("it should load the CA on the first connect").And.ContainSingle();
        queue.TrustAnchors.Should().BeSameAs(first, "it should load the CA once per queue, not on every reconnect");
        var anchor = first!.Single();
        await queue.DisposeAsync();
        anchor.Handle.Should().Be(IntPtr.Zero, "it should dispose the CA certificates with the queue");
    }
}
