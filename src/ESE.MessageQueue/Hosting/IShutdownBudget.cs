namespace ESE.MessageQueue.Hosting;

// A first-party queue's ShutdownTimeout: how long its unsubscribe may wait for running handlers. The hosted service
// compares it with the host's HostOptions.ShutdownTimeout at start (ADR 0025). Third-party providers do not implement
// it and get no warning.
interface IShutdownBudget
{
    TimeSpan ShutdownTimeout { get; }
}
