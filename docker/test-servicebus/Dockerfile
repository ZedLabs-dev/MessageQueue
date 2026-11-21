# The images the Azure Service Bus integration tests start with Testcontainers when MQ_ASB_ACCEPT_EULA=Y. They read
# each stage's FROM line at run time, so Dependabot's docker ecosystem (directory /docker/test-servicebus) can bump
# them. Keep them on the same tags and digests as docker-compose.dev.yml. This file is never built.
FROM mcr.microsoft.com/azure-messaging/servicebus-emulator:2.0.1@sha256:5a96d893b245031740f7d46e0fe5ff282d24b78c4b7d761dd57590f3f010a9b3 AS emulator
# amd64 only; on Apple Silicon it runs under Rosetta emulation (Docker Desktop).
FROM mcr.microsoft.com/mssql/server:2022-CU27-ubuntu-22.04@sha256:4402d880dd4c34bfa7d8705e56a86cd6c88da80a1f6bbbe741f999e76264a090 AS sql
