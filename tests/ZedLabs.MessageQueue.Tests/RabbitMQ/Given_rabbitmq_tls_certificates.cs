using AwesomeAssertions;
using ZedLabs.MessageQueue.RabbitMQ;

namespace ZedLabs.MessageQueue.Tests.RabbitMQ;

// The certificates a connection owner (queue or standalone health check) loads once and disposes (ADR 0022).
[Trait("Category", "Unit")]
public sealed class Given_rabbitmq_tls_certificates : IDisposable
{
    readonly TestCertificateAuthority authority = new();

    public void Dispose() => authority.Dispose();

    RabbitMqTlsConfig Tls() => new()
    {
        ClientCertificatePath = authority.ClientPfxPath,
        ClientCertificatePassword = TestCertificateAuthority.Password,
        CaCertificatePath = authority.CaPemPath
    };

    [Fact]
    public void When_Asked_For_The_Client_Certificate_After_Disposal()
    {
        var certificates = new RabbitMqTlsCertificates();
        certificates.ClientCertificate(Tls());
        certificates.Dispose();

        var act = () => certificates.ClientCertificate(Tls());

        act.Should().Throw<ObjectDisposedException>("it should not hand out a client certificate it has disposed");
    }

    [Fact]
    public void When_Asked_For_The_Ca_After_Disposal()
    {
        var certificates = new RabbitMqTlsCertificates();
        certificates.TrustAnchors(Tls());
        certificates.Dispose();

        var act = () => certificates.TrustAnchors(Tls());

        act.Should().Throw<ObjectDisposedException>("it should not hand out CA certificates it has disposed");
    }

    // Every thread waits at the gate, then all ask at once for the first load.
    static List<T> AllAtOnce<T>(Func<T> load)
    {
        const int callers = 64;
        using var gate = new ManualResetEventSlim();
        var results = new T[callers];
        var threads = Enumerable.Range(0, callers).Select(i => new Thread(() =>
        {
            gate.Wait();
            results[i] = load();
        })).ToList();
        threads.ForEach(t => t.Start());
        gate.Set();
        threads.ForEach(t => t.Join());
        return [.. results];
    }

    [Fact]
    public void When_The_Client_Certificate_Is_First_Asked_For_Concurrently()
    {
        using var certificates = new RabbitMqTlsCertificates();
        var tls = Tls();

        AllAtOnce<object>(() => certificates.ClientCertificate(tls)).Distinct(ReferenceEqualityComparer.Instance).Should()
            .ContainSingle("it should load the client certificate once however many connections ask for it at the same time");
    }

    [Fact]
    public void When_The_Ca_Is_First_Asked_For_Concurrently()
    {
        using var certificates = new RabbitMqTlsCertificates();
        var tls = Tls();

        AllAtOnce<object>(() => certificates.TrustAnchors(tls)).Distinct(ReferenceEqualityComparer.Instance).Should()
            .ContainSingle("it should load the CA once however many connections ask for it at the same time");
    }

    [Fact]
    public void When_A_Load_Fails()
    {
        using var certificates = new RabbitMqTlsCertificates();
        var tls = Tls();
        var path = tls.ClientCertificatePath;
        tls.ClientCertificatePath = Path.Combine(authority.Folder, "missing.pfx");
        var failed = () => certificates.ClientCertificate(tls);
        failed.Should().Throw<ZedLabs.MessageQueue.Exceptions.ConfigurationException>("it should report the missing file");
        tls.ClientCertificatePath = path;

        certificates.ClientCertificate(tls).Thumbprint
            .Should().Be(authority.ClientThumbprint, "it should load again after a failure instead of caching it");
    }
}
