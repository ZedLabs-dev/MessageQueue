using Azure.Messaging.ServiceBus;

namespace ZedLabs.MessageQueue.AzureServiceBus;

// Settles one received message at most once (the broker refuses a second settlement of the same lock). Each broker
// call is bounded by SendTimeout. If a call fails, the message is open again so it can still be handed back
// (abandoned), except when its lock is lost: then nothing can settle it and the broker redelivers it on its own.
sealed class ServiceBusSettlement
{
    readonly IServiceBusDelivery delivery;
    readonly TimeSpan timeout;
    int state;

    public ServiceBusSettlement(IServiceBusDelivery delivery, TimeSpan timeout)
    {
        this.delivery = delivery;
        this.timeout = timeout;
    }

    public bool IsSettled => Volatile.Read(ref state) == 1;

    // `final`: the last thing anyone will try (handing the message back after a failure). If it fails too, the
    // message stays closed and the broker redelivers it when the lock expires.
    public async Task SettleAsync(Func<IServiceBusDelivery, CancellationToken, Task> operation, bool final = false)
    {
        if (Interlocked.Exchange(ref state, 1) == 1)
            return;

        using var deadline = new CancellationTokenSource(timeout);
        try
        {
            await operation(delivery, deadline.Token);
        }
        catch (Exception ex) when (!final && !IsLockLost(ex))
        {
            Volatile.Write(ref state, 0);
            throw;
        }
    }

    // How the SDK and System.Transactions refuse a transaction: NotAllowed maps to InvalidOperationException,
    // NotImplemented to NotSupportedException, and a failed enlistment to TransactionException. Not a refusal, so never
    // a reason to stop using transactions: a disposed client (ObjectDisposedException derives from
    // InvalidOperationException) or a transaction that aborted (timeout) or is in doubt. The SDK has no failure reason
    // that names a refusal, so a refusal reported as a ServiceBusException is not recognised (ADR 0023).
    public static bool IsTransactionRefusal(Exception exception) =>
        exception is not ObjectDisposedException
            and not System.Transactions.TransactionAbortedException
            and not System.Transactions.TransactionInDoubtException
        && exception is InvalidOperationException or NotSupportedException or System.Transactions.TransactionException;

    public static bool IsLockLost(Exception exception) =>
        exception is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost or ServiceBusFailureReason.SessionLockLost };
}
