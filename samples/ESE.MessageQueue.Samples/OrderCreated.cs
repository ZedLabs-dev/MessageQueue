namespace ESE.MessageQueue.Samples;

public sealed record OrderCreated(string OrderId, string CustomerId, decimal Amount, DateTimeOffset CreatedAt);
