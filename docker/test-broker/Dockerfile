# The RabbitMQ image the integration tests and benchmarks start with Testcontainers. They read the tag from this
# FROM line at run time, so Dependabot's docker ecosystem (directory /docker/test-broker) can bump it.
# Keep it on the same tag and digest as docker-compose.dev.yml. Pinned by the multi-arch index digest (S8): the tag
# documents the version, the digest fixes the bytes; Dependabot updates both.
FROM rabbitmq:4.4.0-management@sha256:c200fd496482cda18c412be4e6f244830e1e75949b8cf5f0e887cea9ea456109
