namespace ESE.MessageQueue.Samples;

public sealed class SampleOptions
{
    public const string SectionName = "Sample";

    // How many orders OrderPublisher publishes at startup; they cycle through the processor's three outcomes.
    public int OrdersToPublish { get; set; } = 3;
}
