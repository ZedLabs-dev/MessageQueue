#!/bin/bash

echo "Running Message Queue Tests..."

# Check if RabbitMQ is running
if ! docker ps | grep -q rabbitmq-dev; then
    echo "RabbitMQ container not running. Starting development environment..."
    docker-compose -f docker-compose.dev.yml up -d rabbitmq-dev
    
    echo "Waiting for RabbitMQ to be ready..."
    sleep 15
    
    until docker exec rabbitmq-dev rabbitmqctl status > /dev/null 2>&1; do
        echo "Still waiting for RabbitMQ..."
        sleep 5
    done
    
    echo "RabbitMQ is ready!"
fi

# Run unit tests (these don't require RabbitMQ)
echo "Running unit tests..."
dotnet test AgentReview.Tests/AgentReview.Tests.csproj \
  --filter "FullyQualifiedName~MessageQueue&FullyQualifiedName!~Integration" \
  --verbosity normal

# Run integration tests (these require RabbitMQ)
echo "Running integration tests..."
dotnet test AgentReview.Tests/AgentReview.Tests.csproj \
  --filter "FullyQualifiedName~Integration" \
  --verbosity normal

echo "All tests completed!"