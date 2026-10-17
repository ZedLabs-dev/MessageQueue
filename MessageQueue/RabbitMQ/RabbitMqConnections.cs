using System.Net;
using System.Net.Security;
using ESE.MessageQueue.Exceptions;
using RabbitMQ.Client;

namespace ESE.MessageQueue.RabbitMQ;

// Endpoint, credential and TLS rules (S7), and the ConnectionFactory built from them. The validator reports the
// same rules at startup; CreateFactory re-checks them at connect time because config records stay mutable.
static class RabbitMqConnections
{
    const string PlainScheme = "amqp";
    const string TlsScheme = "amqps";

    public static List<string> Validate(RabbitMqConfig config)
    {
        var errors = new List<string>();
        var hasUri = !string.IsNullOrWhiteSpace(config.ConnectionString);
        var hasHost = !string.IsNullOrWhiteSpace(config.HostName);

        if (hasUri == hasHost)
        {
            errors.Add(hasUri
                ? "Set either ConnectionString or HostName, not both"
                : "ConnectionString (amqps://user:password@host/vhost) or HostName is required");
            return errors;
        }

        if (hasUri)
        {
            if (!TryParse(config.ConnectionString, out _))
                errors.Add("ConnectionString must be an absolute amqp:// or amqps:// URI");
            if (config.Port != 0 || config.UserName.Length > 0 || config.Password.Length > 0 || config.VirtualHost.Length > 0)
                errors.Add("Port, UserName, Password and VirtualHost apply only with HostName; put them in the ConnectionString instead");
            if (TlsOf(config).Enabled)
                errors.Add("Tls:Enabled applies only with HostName; use an amqps:// ConnectionString instead");
        }

        if (TlsOf(config).AcceptablePolicyErrors != SslPolicyErrors.None && !TlsOf(config).AllowInsecureCertificates)
            errors.Add("Tls:AcceptablePolicyErrors accepts invalid server certificates; set Tls:AllowInsecureCertificates = true to confirm that this is intended");

        if ((TlsOf(config).AcceptablePolicyErrors & ~AllPolicyErrors) != 0)
            errors.Add("Tls:AcceptablePolicyErrors contains unknown flags");

        if (errors.Count == 0 && RequiresTls(config) && !UsesTls(config))
            errors.Add("RequireTls is on (the default for any host other than localhost), but the connection is not encrypted: " +
                "use an amqps:// ConnectionString (or Tls:Enabled with HostName), or set RequireTls = false explicitly");

        return errors;
    }

    public static ConnectionFactory CreateFactory(RabbitMqConfig config)
    {
        var errors = Validate(config);
        if (errors.Count > 0)
            throw new ConfigurationException($"Invalid RabbitMQ connection settings for queue '{config.QueueName}': {string.Join("; ", errors)}");

        var factory = new ConnectionFactory
        {
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            RequestedHeartbeat = config.HeartbeatInterval,
            NetworkRecoveryInterval = config.NetworkRecoveryInterval,
            ClientProvidedName = $"ESE.MessageQueue:{config.QueueName}"
        };

        if (!string.IsNullOrWhiteSpace(config.ConnectionString))
        {
            // amqps:// switches TLS on and sets the server name to the URI's host.
            factory.Uri = new Uri(config.ConnectionString.Trim());
        }
        else
        {
            factory.HostName = config.HostName;
            if (config.Port != 0)
                factory.Port = config.Port;
            if (config.UserName.Length > 0)
                factory.UserName = config.UserName;
            if (config.Password.Length > 0)
                factory.Password = config.Password;
            if (config.VirtualHost.Length > 0)
                factory.VirtualHost = config.VirtualHost;
            factory.Ssl = new SslOption(config.HostName, enabled: TlsOf(config).Enabled);
        }

        if (factory.Ssl.Enabled)
        {
            if (TlsOf(config).ServerName.Length > 0)
                factory.Ssl.ServerName = TlsOf(config).ServerName;
            factory.Ssl.AcceptablePolicyErrors = TlsOf(config).AcceptablePolicyErrors;
        }

        return factory;
    }

    // Plain AMQP is only the default for the local machine (development); everywhere else TLS is required
    // unless RequireTls is explicitly false.
    static bool RequiresTls(RabbitMqConfig config) => config.RequireTls ?? !IsLoopback(Host(config));

    static bool UsesTls(RabbitMqConfig config) =>
        string.IsNullOrWhiteSpace(config.ConnectionString)
            ? TlsOf(config).Enabled
            : TryParse(config.ConnectionString, out var uri) && uri.Scheme == TlsScheme;

    static string Host(RabbitMqConfig config) =>
        string.IsNullOrWhiteSpace(config.ConnectionString)
            ? config.HostName.Trim()
            : TryParse(config.ConnectionString, out var uri) ? uri.Host : string.Empty;

    static bool IsLoopback(string host)
    {
        var bare = host.Trim('[', ']');
        return bare.Equals("localhost", StringComparison.OrdinalIgnoreCase)
            || (IPAddress.TryParse(bare, out var address) && IPAddress.IsLoopback(address));
    }

    static bool TryParse(string connectionString, out Uri uri)
    {
        if (Uri.TryCreate(connectionString.Trim(), UriKind.Absolute, out var parsed)
            && (parsed.Scheme == PlainScheme || parsed.Scheme == TlsScheme)
            && parsed.Host.Length > 0)
        {
            uri = parsed;
            return true;
        }

        uri = null!;
        return false;
    }

    static RabbitMqTlsConfig TlsOf(RabbitMqConfig config) => config.Tls ?? new RabbitMqTlsConfig();

    const SslPolicyErrors AllPolicyErrors =
        SslPolicyErrors.RemoteCertificateNotAvailable | SslPolicyErrors.RemoteCertificateNameMismatch | SslPolicyErrors.RemoteCertificateChainErrors;
}
