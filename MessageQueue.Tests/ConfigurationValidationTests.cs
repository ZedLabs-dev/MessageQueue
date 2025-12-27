using ESE.MessageQueue.AzureServiceBus;
using ESE.MessageQueue.Exceptions;
using ESE.MessageQueue.Extensions;
using ESE.MessageQueue.RabbitMQ;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ESE.MessageQueue.Tests;

public class ConfigurationValidationTests
{
    [Fact]
    public void When_RabbitMQ_ConnectionString_Is_Empty()
    {
        var config = CreateConfiguration(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "RabbitMq",
            ["MessageQueue:ConnectionString"] = "",
            ["MessageQueue:QueueName"] = "test-queue"
        });
        var services = new ServiceCollection();

        var act = () => services.AddMessageQueue<TestMessage>(config);

        act.Should().Throw<ConfigurationException>("it should fail fast when ConnectionString is empty")
            .WithMessage("*ConnectionString*");
    }

    [Fact]
    public void When_RabbitMQ_QueueName_Is_Empty()
    {
        var config = CreateConfiguration(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "RabbitMq",
            ["MessageQueue:ConnectionString"] = "amqp://localhost",
            ["MessageQueue:QueueName"] = ""
        });
        var services = new ServiceCollection();

        var act = () => services.AddMessageQueue<TestMessage>(config);

        act.Should().Throw<ConfigurationException>("it should fail fast when QueueName is empty")
            .WithMessage("*QueueName*");
    }

    [Fact]
    public void When_RabbitMQ_RetryAttempts_Is_Negative()
    {
        var config = CreateConfiguration(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "RabbitMq",
            ["MessageQueue:ConnectionString"] = "amqp://localhost",
            ["MessageQueue:QueueName"] = "test-queue",
            ["MessageQueue:RetryAttempts"] = "-1"
        });
        var services = new ServiceCollection();

        var act = () => services.AddMessageQueue<TestMessage>(config);

        act.Should().Throw<ConfigurationException>("it should fail fast when RetryAttempts is negative")
            .WithMessage("*RetryAttempts*");
    }

    [Fact]
    public void When_RabbitMQ_PrefetchCount_Is_Zero()
    {
        var config = CreateConfiguration(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "RabbitMq",
            ["MessageQueue:ConnectionString"] = "amqp://localhost",
            ["MessageQueue:QueueName"] = "test-queue",
            ["MessageQueue:PrefetchCount"] = "0"
        });
        var services = new ServiceCollection();

        var act = () => services.AddMessageQueue<TestMessage>(config);

        act.Should().Throw<ConfigurationException>("it should fail fast when PrefetchCount is zero")
            .WithMessage("*PrefetchCount*");
    }

    [Fact]
    public void When_AzureServiceBus_ConnectionString_Is_Empty()
    {
        var config = CreateConfiguration(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "AzureServiceBus",
            ["MessageQueue:ConnectionString"] = "",
            ["MessageQueue:QueueName"] = "test-queue"
        });
        var services = new ServiceCollection();

        var act = () => services.AddMessageQueue<TestMessage>(config);

        act.Should().Throw<ConfigurationException>("it should fail fast when ConnectionString is empty")
            .WithMessage("*ConnectionString*");
    }

    [Fact]
    public void When_AzureServiceBus_MaxConcurrentCalls_Is_Zero()
    {
        var config = CreateConfiguration(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "AzureServiceBus",
            ["MessageQueue:ConnectionString"] = "Endpoint=sb://test.servicebus.windows.net/",
            ["MessageQueue:QueueName"] = "test-queue",
            ["MessageQueue:MaxConcurrentCalls"] = "0"
        });
        var services = new ServiceCollection();

        var act = () => services.AddMessageQueue<TestMessage>(config);

        act.Should().Throw<ConfigurationException>("it should fail fast when MaxConcurrentCalls is zero")
            .WithMessage("*MaxConcurrentCalls*");
    }

    [Fact]
    public void When_AzureServiceBus_MaxBatchSize_Is_Zero()
    {
        var config = CreateConfiguration(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "AzureServiceBus",
            ["MessageQueue:ConnectionString"] = "Endpoint=sb://test.servicebus.windows.net/",
            ["MessageQueue:QueueName"] = "test-queue",
            ["MessageQueue:MaxBatchSize"] = "0"
        });
        var services = new ServiceCollection();

        var act = () => services.AddMessageQueue<TestMessage>(config);

        act.Should().Throw<ConfigurationException>("it should fail fast when MaxBatchSize is zero")
            .WithMessage("*MaxBatchSize*");
    }

    [Fact]
    public void When_RabbitMQ_Configuration_Is_Valid()
    {
        var config = CreateConfiguration(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "RabbitMq",
            ["MessageQueue:ConnectionString"] = "amqp://localhost",
            ["MessageQueue:QueueName"] = "test-queue"
        });
        var services = new ServiceCollection();

        var act = () => services.AddMessageQueue<TestMessage>(config);

        act.Should().NotThrow("it should accept valid configuration");
    }

    [Fact]
    public void When_AzureServiceBus_Configuration_Is_Valid()
    {
        var config = CreateConfiguration(new Dictionary<string, string?>
        {
            ["MessageQueue:Provider"] = "AzureServiceBus",
            ["MessageQueue:ConnectionString"] = "Endpoint=sb://test.servicebus.windows.net/",
            ["MessageQueue:QueueName"] = "test-queue"
        });
        var services = new ServiceCollection();

        var act = () => services.AddMessageQueue<TestMessage>(config);

        act.Should().NotThrow("it should accept valid configuration");
    }

    static IConfiguration CreateConfiguration(Dictionary<string, string?> values)
    {
        return new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build();
    }
}

