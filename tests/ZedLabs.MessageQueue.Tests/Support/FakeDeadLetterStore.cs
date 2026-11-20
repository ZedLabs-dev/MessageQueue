using System.Collections.Concurrent;
using ZedLabs.MessageQueue.Abstractions;
using ZedLabs.MessageQueue.Providers;
using ZedLabs.MessageQueue.Serialization;

namespace ZedLabs.MessageQueue.Tests;

// A provider's dead letters for DeadLetterQueue<T> specs. It behaves like the seam's contract: a pass returns each dead
// letter at most once and no more than the store held when it opened, holds what it returned until it is replayed,
// removed or the pass ends, and returns the rest on dispose. Replays move a dead letter to Replayed; failures can be
// injected per message.
sealed class FakeDeadLetterStore : IDeadLetterStore
{
    readonly object gate = new();
    readonly List<FakeDeadLetter> letters = [];

    public DeadLetterOperations Operations { get; set; } = DeadLetterOperations.All;
    public ConcurrentQueue<string> Log { get; } = new();
    public List<FakeDeadLetter> Replayed { get; } = [];
    public List<FakeDeadLetter> Removed { get; } = [];
    public List<bool> Scans { get; } = [];
    public int Returned;
    public int Examined;
    public Func<FakeDeadLetter, Exception?>? ReplayFailure { get; set; }
    public Func<FakeDeadLetter, Exception?>? RemoveFailure { get; set; }
    public Func<int, Exception?>? NextFailure { get; set; }
    public Func<FakeDeadLetter, CancellationToken, Task>? BeforeReplay { get; set; }
    public Func<Task>? OnOpen { get; set; }
    public Exception? DisposeFailure { get; set; }

    public IReadOnlyList<FakeDeadLetter> Letters
    {
        get
        {
            lock (gate)
                return [.. letters];
        }
    }

    public FakeDeadLetter Add(string id, string? reason = null, TestMessage? message = null, byte[]? body = null,
        IReadOnlyDictionary<string, string>? headers = null, string? correlationId = null)
    {
        var letter = new FakeDeadLetter(
            new DeadLetterMetadata
            {
                MessageId = id,
                CorrelationId = correlationId,
                ReasonCode = reason,
                Headers = headers ?? new Dictionary<string, string>()
            },
            body ?? MessageSerializer.Serialize(message ?? new TestMessage(id, "content"), SerializationFormat.Json));
        lock (gate)
            letters.Add(letter);
        return letter;
    }

    public FakeDeadLetter Add(DeadLetterMetadata metadata, byte[] body)
    {
        var letter = new FakeDeadLetter(metadata, body);
        lock (gate)
            letters.Add(letter);
        return letter;
    }

    public async Task<IDeadLetterScan> OpenScanAsync(bool browseOnly, CancellationToken cancellationToken)
    {
        if (OnOpen is not null)
            await OnOpen();
        lock (gate)
        {
            Scans.Add(browseOnly);
            Log.Enqueue(browseOnly ? "open:browse" : "open");
            return new Scan(this, [.. letters], browseOnly);
        }
    }

    sealed class Scan : IDeadLetterScan
    {
        readonly FakeDeadLetterStore store;
        readonly List<FakeDeadLetter> snapshot;
        readonly bool browseOnly;
        readonly List<Entry> held = [];
        int next;

        public Scan(FakeDeadLetterStore store, List<FakeDeadLetter> snapshot, bool browseOnly)
        {
            this.store = store;
            this.snapshot = snapshot;
            this.browseOnly = browseOnly;
        }

        public Task<IDeadLetterEntry?> NextAsync(CancellationToken cancellationToken)
        {
            if (store.NextFailure?.Invoke(next) is { } failure)
                throw failure;
            if (next >= snapshot.Count)
                return Task.FromResult<IDeadLetterEntry?>(null);
            Interlocked.Increment(ref store.Examined);
            var entry = new Entry(store, snapshot[next++], browseOnly);
            held.Add(entry);
            return Task.FromResult<IDeadLetterEntry?>(entry);
        }

        public ValueTask DisposeAsync()
        {
            foreach (var entry in held.Where(e => !e.Settled))
                Interlocked.Increment(ref store.Returned);
            store.Log.Enqueue("dispose");
            return store.DisposeFailure is { } failure ? ValueTask.FromException(failure) : ValueTask.CompletedTask;
        }
    }

    sealed class Entry : IDeadLetterEntry
    {
        readonly FakeDeadLetterStore store;
        readonly FakeDeadLetter letter;
        readonly bool browseOnly;

        public Entry(FakeDeadLetterStore store, FakeDeadLetter letter, bool browseOnly)
        {
            this.store = store;
            this.letter = letter;
            this.browseOnly = browseOnly;
        }

        public bool Settled { get; private set; }
        public DeadLetterMetadata Metadata => letter.Metadata;
        public ReadOnlyMemory<byte> Body => letter.Body;

        public async Task ReplayAsync(CancellationToken cancellationToken)
        {
            if (browseOnly)
                throw new InvalidOperationException("A browse-only pass cannot replay");
            if (store.BeforeReplay is not null)
                await store.BeforeReplay(letter, cancellationToken);
            store.Log.Enqueue($"replay:{letter.Metadata.MessageId}");
            if (store.ReplayFailure?.Invoke(letter) is { } failure)
                throw failure;
            lock (store.gate)
            {
                store.letters.Remove(letter);
                store.Replayed.Add(letter);
            }
            Settled = true;
        }

        public Task RemoveAsync(CancellationToken cancellationToken)
        {
            if (browseOnly)
                throw new InvalidOperationException("A browse-only pass cannot remove");
            store.Log.Enqueue($"remove:{letter.Metadata.MessageId}");
            if (store.RemoveFailure?.Invoke(letter) is { } failure)
                throw failure;
            lock (store.gate)
            {
                store.letters.Remove(letter);
                store.Removed.Add(letter);
            }
            Settled = true;
            return Task.CompletedTask;
        }
    }
}

// Compared by reference: two dead letters with the same values are still two messages.
sealed class FakeDeadLetter(DeadLetterMetadata metadata, byte[] body)
{
    public DeadLetterMetadata Metadata { get; } = metadata;
    public byte[] Body { get; } = body;
}
