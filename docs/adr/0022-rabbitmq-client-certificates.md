# 0022. RabbitMQ client certificates (mutual TLS)

- Status: Accepted (2026-10-22); amends ADR 0008
- Context: OPTIMIZATION-PLAN §9 (blocked item "client certificates"); ADR 0008 (TLS enforcement), ADR 0002

## Context

ADR 0008 validates the broker's certificate but the client could not present one, so brokers that authenticate or
authorize clients by certificate (`ssl_options.verify = verify_peer`, the EXTERNAL mechanism) could not be used.

## Decision

- **Sources:** `Tls:ClientCertificatePath` (PKCS#12, or PEM with the key in the file or `Tls:ClientCertificateKeyPath`;
  `Tls:ClientCertificatePassword` for the PFX or an encrypted key), or `Tls:ClientCertificateThumbprint` in
  `Tls:ClientCertificateStore{Location,Name}` (default `CurrentUser/My`); not both, and only with TLS. Thumbprints
  ignore whitespace, `:`, `-` and the direction marks certificate dialogs copy along; case does not matter.
- **Startup** loads the certificate once to prove it is usable: a missing file, wrong password, missing private key,
  unknown thumbprint or unopenable store fails with the setting and path, never the password or loader text. Store
  settings must be enum names (the generic enum-name rule covers every provider's nested enum settings).
- **Lifetime:** each queue loads its certificate on the first connect, reuses it for every reconnect, and disposes it
  in `DisposeAsync`. It goes into `Ssl.Certs` on both the `amqps://` and the `HostName` path. File contents, decoded
  PEM text and the PKCS#12 round trip (PEM keys are ephemeral, which Windows `SslStream` cannot use) are zeroed after
  use; certificates the store hands out and the queue does not keep are disposed.
- The password is redacted from `ToString()`. Standalone connection-string health checks carry no client
  certificate; use the queue-backed check against a broker that requires one.

## Consequences

- Configuration only: no new public types.
- Store lookups are specified on Linux (CI), where `CurrentUser/My` is a folder; macOS and Windows stores are not
  written by the specs. No broker requiring client certificates has been tested.
