# The RabbitMQ image the integration tests and benchmarks start with Testcontainers. They read the tag from this
# FROM line at run time, so Dependabot's docker ecosystem (directory /docker/test-broker) can bump it.
# Keep it on the same tag and digest as docker-compose.dev.yml. Pinned by the multi-arch index digest (S8): the tag
# documents the version, the digest fixes the bytes; Dependabot updates both.
FROM rabbitmq:4.3.6-management@sha256:54cb013d76cd1fd6c0a0708f34062ad61c452bb6f016b434c6c33f1a765d2b58
