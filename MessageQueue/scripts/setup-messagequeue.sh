#!/bin/bash

echo "Setting up Message Queue Library..."

# Check if RabbitMQ container exists and remove it
if docker ps -a --format 'table {{.Names}}' | grep -q rabbitmq-dev; then
    echo "Stopping and removing existing RabbitMQ container..."
    docker stop rabbitmq-dev
    docker rm rabbitmq-dev
fi

# Start RabbitMQ with correct credentials
echo "Starting RabbitMQ container..."
docker run -d --name rabbitmq-dev \
  -p 5672:5672 \
  -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=admin \
  -e RABBITMQ_DEFAULT_PASS=password123 \
  rabbitmq:3-management

# Wait for RabbitMQ to be ready
echo "Waiting for RabbitMQ to start..."
sleep 10

# Check if RabbitMQ is ready
echo "Checking RabbitMQ status..."
until docker exec rabbitmq-dev rabbitmqctl status > /dev/null 2>&1; do
    echo "Waiting for RabbitMQ to be ready..."
    sleep 5
done

echo "RabbitMQ is ready!"

# Restore packages
echo "Restoring NuGet packages..."
dotnet restore

# Build the project
echo "Building project..."
dotnet build

# Run tests
echo "Running tests..."
dotnet test AgentReview.Tests/AgentReview.Tests.csproj --verbosity normal

echo "Message Queue Library setup complete!"
echo "RabbitMQ Management UI available at: http://localhost:15672"
echo "Default credentials: admin/password123"
