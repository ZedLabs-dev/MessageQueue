using AwesomeAssertions;

namespace ZedLabs.MessageQueue.Tests.AzureServiceBus;

// Which Service Bus the integration tests talk to. The emulator's licence (and its SQL Server's) is the owner's to
// accept, so the emulator starts only when they opt in through MQ_ASB_ACCEPT_EULA.
[Trait("Category", "Unit")]
public sealed class Given_the_service_bus_test_target
{
    static ServiceBusTestTarget Resolve(params (string Name, string Value)[] variables) =>
        ServiceBusTestTarget.Resolve(name => variables.FirstOrDefault(v => v.Name == name).Value);

    [Fact]
    public void When_Nothing_Is_Set()
    {
        var target = Resolve();

        target.StartEmulator.Should().BeFalse("it should not start the emulator without the owner accepting its licence");
        target.ConnectionString.Should().BeNull("it should have no namespace to talk to");
        target.SkipReason.Should().Contain(AzureServiceBusEnvironment.ConnectionStringVariable,
            "it should say how to point the tests at a namespace")
            .And.Contain(ServiceBusTestTarget.AcceptEulaVariable, "it should say how to opt in to the emulator");
    }

    [Theory]
    [InlineData("Y")]
    [InlineData("y")]
    [InlineData("true")]
    [InlineData("TRUE")]
    [InlineData("1")]
    [InlineData(" Y ")]
    public void When_The_Licence_Is_Accepted(string value)
    {
        var target = Resolve((ServiceBusTestTarget.AcceptEulaVariable, value));

        target.StartEmulator.Should().BeTrue("it should start the emulator once the owner accepts its licence");
        target.SkipReason.Should().BeNull("it should run the tests");
    }

    [Theory]
    [InlineData("N")]
    [InlineData("false")]
    [InlineData("0")]
    [InlineData("yes please")]
    [InlineData("")]
    public void When_The_Licence_Is_Not_Accepted(string value)
    {
        var target = Resolve((ServiceBusTestTarget.AcceptEulaVariable, value));

        target.StartEmulator.Should().BeFalse("it should not treat anything but an explicit yes as accepting the licence");
        target.SkipReason.Should().NotBeNull("it should skip the tests");
    }

    [Fact]
    public void When_A_Connection_String_Is_Set()
    {
        var target = Resolve(
            (AzureServiceBusEnvironment.ConnectionStringVariable, "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=v"),
            (ServiceBusTestTarget.AcceptEulaVariable, "Y"));

        target.ConnectionString.Should().StartWith("Endpoint=sb://example.", "it should use the namespace the owner provided");
        target.StartEmulator.Should().BeFalse("it should prefer the provided namespace over the emulator");
        target.SkipReason.Should().BeNull("it should run the tests");
    }
}
