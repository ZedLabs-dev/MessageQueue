namespace ESE.MessageQueue.Benchmarks;

// A typical event: a few scalars and a small list, about 400 bytes as JSON.
public sealed record Order(string OrderId, string CustomerId, decimal Amount, DateTimeOffset CreatedAt, IReadOnlyList<OrderLine> Lines)
{
    public static Order Sample(int index) => new(
        $"order-{index:D8}",
        "customer-42",
        129.95m,
        new DateTimeOffset(2026, 10, 17, 12, 0, 0, TimeSpan.Zero),
        [new OrderLine("sku-1", 2, 19.99m), new OrderLine("sku-2", 1, 89.97m), new OrderLine("sku-3", 5, 0m)]);
}

public sealed record OrderLine(string Sku, int Quantity, decimal Price);
