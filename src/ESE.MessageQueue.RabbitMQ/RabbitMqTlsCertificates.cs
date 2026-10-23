using System.Security.Cryptography.X509Certificates;

namespace ESE.MessageQueue.RabbitMQ;

// The certificates one connection owner (a queue or a standalone health check) presents and trusts (ADR 0022): each is
// loaded on first use, reused for every later connection and disposed with the owner. A failed load is not cached, so
// a fixed file is picked up on the next connect. Thread-safe: health check probes may run concurrently.
sealed class RabbitMqTlsCertificates : IDisposable
{
    readonly Lock gate = new();
    X509Certificate2? clientCertificate;
    X509Certificate2Collection? trustAnchors;
    bool disposed;

    public X509Certificate2? LoadedClientCertificate
    {
        get
        {
            lock (gate)
                return clientCertificate;
        }
    }

    public X509Certificate2Collection? LoadedTrustAnchors
    {
        get
        {
            lock (gate)
                return trustAnchors;
        }
    }

    public X509Certificate2 ClientCertificate(RabbitMqTlsConfig tls)
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            return clientCertificate ??= RabbitMqClientCertificates.Load(tls);
        }
    }

    public X509Certificate2Collection TrustAnchors(RabbitMqTlsConfig tls)
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            return trustAnchors ??= RabbitMqTrustAnchors.Load(tls);
        }
    }

    public void Dispose()
    {
        lock (gate)
        {
            if (disposed)
                return;
            disposed = true;
            clientCertificate?.Dispose();
            if (trustAnchors is not null)
                RabbitMqTrustAnchors.Dispose(trustAnchors);
        }
    }
}
