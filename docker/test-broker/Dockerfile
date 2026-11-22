# The RabbitMQ image the integration tests and benchmarks start with Testcontainers. They read the tag from this
# FROM line at run time, so Dependabot's docker ecosystem (directory /docker/test-broker) can bump it.
# Keep it on the same tag and digest as docker-compose.dev.yml. Pinned by the multi-arch index digest (S8): the tag
# documents the version, the digest fixes the bytes; Dependabot updates both.
FROM rabbitmq:4.4.1-management@sha256:61f90c401a5b5ed2486771973ea92ad66046471eacab9c9149f1a6e091c91677
