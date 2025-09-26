#!/bin/bash

echo "Quick Start - Message Queue Development Environment"

# Start development services
echo "Starting development services..."
docker-compose -f docker-compose.dev.yml up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 15

# Check RabbitMQ
until docker exec rabbitmq-dev rabbitmqctl status > /dev/null 2>&1; do
    echo "Waiting for RabbitMQ..."
    sleep 5
done

# Check MongoDB
until docker exec mongodb-dev mongosh --eval "db.adminCommand('ping')" > /dev/null 2>&1; do
    echo "Waiting for MongoDB..."
    sleep 5
done

echo "Services are ready!"
echo ""
echo "Available services:"
echo "- RabbitMQ Management: http://localhost:15672 (admin/password123)"
echo "- MongoDB: mongodb://admin:password123@localhost:27017"
echo ""

# Build and test
echo "Building project..."
dotnet build

echo "Running tests..."
dotnet test AgentReview.Tests/AgentReview.Tests.csproj --verbosity normal

echo ""
echo "Development environment is ready!"
echo "To stop services: docker-compose -f docker-compose.dev.yml down"