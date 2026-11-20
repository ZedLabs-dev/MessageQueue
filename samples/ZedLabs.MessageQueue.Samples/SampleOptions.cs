namespace ZedLabs.MessageQueue.Samples;

public sealed class SampleOptions
{
    public const string SectionName = "Sample";

    // How many orders OrderPublisher publishes at startup; they cycle through the processor's three outcomes.
    public int OrdersToPublish { get; set; } = 3;

    // What DeadLetterInspector does once, DeadLetterDelay after startup.
    public DeadLetterMode DeadLetters { get; set; } = DeadLetterMode.None;

    public TimeSpan DeadLetterDelay { get; set; } = TimeSpan.FromSeconds(5);
}

public enum DeadLetterMode
{
    None,
    Peek,
    Replay
}
