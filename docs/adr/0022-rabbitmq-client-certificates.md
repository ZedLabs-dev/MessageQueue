# 0022. RabbitMQ client certificates (mutual TLS)

- Status: Accepted (2026-10-22), amended 2026-10-23 (trust anchor, standalone health check, real-broker specs); amends ADR 0008
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
- The password is redacted from `ToString()`.
- **Trust anchor (2026-10-23):** `Tls:CaCertificatePath` names a PEM file (one or more CA certificates) or a DER
  certificate. When set, the broker's certificate must chain to those CAs **and only to them** (custom root trust, not
  the OS store), carry the server-authentication usage where it declares usages, and be within its validity; the server
  name is still checked; revocation is not checked (the `SslStream` client default). The file must contain the root:
  an intermediate-only file is not an anchor, so every connection is refused. Certificates the broker sends are chain
  candidates, never anchors, and the chain the TLS stack built against the OS store is never reused; issuers are never
  downloaded from the certificate's AIA location (`DisableCertificateDownloads`; Linux would otherwise fetch them).
  Basic constraints, path lengths and name constraints are enforced. A leaf whose only extended key usage is
  `anyExtendedKeyUsage` is refused; a leaf with no extended key usage is accepted (the X.509 default). The chain's
  element certificates are disposed after each validation. Because a validation callback
  replaces RabbitMQ.Client's own `AcceptablePolicyErrors` check, the callback applies `AcceptablePolicyErrors` itself,
  so the second-flag rule is unchanged. Needs TLS; loaded and checked at startup (missing file, no certificate); each
  queue loads it once, with the client certificate, and disposes it with the queue. Considered and rejected: turning on
  `AcceptablePolicyErrors` for a private CA (it accepts any certificate) and adding the CA to the OS store (machine-wide,
  needs privileges, not portable to containers).
- **Standalone health check (2026-10-23):** `AddRabbitMqHealthCheck(connectionString, RabbitMqTlsConfig tls, ...,
  requireTls)` applies the queue's TLS settings and rules. The settings are copied at registration; one check instance
  per registration (a keyed singleton named after the check) loads the certificates on the first probe, reuses them
  and is disposed, with them, by the container. Invalid settings report Unhealthy and are logged (event 6001), like
  every other probe failure. The overload without `RabbitMqTlsConfig` probes as before, but it is now registered the
  same way: one check per registration, a keyed singleton per check name, instead of a new check per run.

## Consequences

- No new public types: one setting (`Tls:CaCertificatePath`) and one health check overload.
- Store lookups are specified on Linux (CI), where `CurrentUser/My` is a folder; macOS and Windows stores are not
  written by the specs.
- Tested against a real broker: `Support/RabbitMqMutualTlsBrokerFixture` starts the pinned test image with TLS on 5671,
  `ssl_options.verify = verify_peer` and `fail_if_no_peer_cert = true`, and a CA, server and client certificate
  generated per run (`Support/TestCertificateAuthority`; nothing is committed). Specs
  `RabbitMQ/Given_a_rabbitmq_broker_requiring_client_certificates` (round trip with PFX and PEM client certificates; no
  certificate refused; untrusted CA, another CA and a wrong server name refused unless explicitly accepted) and
  `HealthChecks/Given_a_rabbitmq_health_check_on_a_mutual_tls_broker`. Hostile chains (a leaf or a non-CA client
  certificate signing the broker's certificate, a violated path length or name constraint, a rogue root named like the
  anchor, a chain valid only for another root, an AIA download location) are specified in
  `RabbitMQ/Given_a_rabbitmq_trust_anchor` and pass on macOS and in the Linux SDK container; mutants
  `mutual-tls.json` and the reviewer's `review-mutual-tls.json`. The EXTERNAL (certificate) login mechanism is not
  exercised: the broker still authenticates the user with PLAIN.
