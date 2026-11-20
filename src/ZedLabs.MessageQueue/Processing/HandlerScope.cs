namespace ZedLabs.MessageQueue.Processing;

// Marks the async flow of one running handler, so that UnsubscribeAsync or DisposeAsync called from inside that
// handler can avoid waiting for the handler itself (which would deadlock). Per delivery: tasks the handler spawns
// inherit the scope, but it only counts while Active, that is until the handler returns.
sealed class HandlerScope
{
    static readonly AsyncLocal<HandlerScope?> Current = new();

    readonly object owner;
    int active = 1;

    HandlerScope(object owner)
    {
        this.owner = owner;
    }

    // Set at the start of the delivery's own async method: the value does not flow back to the transport's caller.
    public static HandlerScope Enter(object owner)
    {
        var scope = new HandlerScope(owner);
        Current.Value = scope;
        return scope;
    }

    public void Exit() => Volatile.Write(ref active, 0);

    // True only on the flow of an active handler of `owner` (a subscription).
    public static bool IsRunningHandlerOf(object owner) =>
        Current.Value is { } scope && Volatile.Read(ref scope.active) == 1 && ReferenceEquals(scope.owner, owner);

    // For background shutdown work started from inside a handler: it is not that handler.
    public static void Clear() => Current.Value = null;
}
