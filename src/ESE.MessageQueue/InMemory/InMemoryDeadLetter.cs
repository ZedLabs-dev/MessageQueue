using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.InMemory;

/// <summary>
/// A message the in-memory provider dead-lettered. Compared by value, like a record.
/// </summary>
/// <remarks>
/// Declared by hand rather than as a record so that every public member carries its nullable annotations (the
/// compiler's synthesized members of a generic record are nullable-oblivious).
/// </remarks>
/// <typeparam name="T">The message type.</typeparam>
public sealed class InMemoryDeadLetter<T> : IEquatable<InMemoryDeadLetter<T>> where T : class
{
    /// <summary>Creates a dead-letter entry.</summary>
    /// <param name="Message">The deserialized message, or null when the body could not be (or was not) deserialized.</param>
    /// <param name="Body">The serialized body as published.</param>
    /// <param name="Context">The metadata of the last delivery.</param>
    /// <param name="Reason">One of <see cref="Processing.DeadLetterReasons"/>.</param>
    /// <param name="Detail">Diagnostic detail such as an exception type name.</param>
#pragma warning disable IDE1006 // PascalCase parameters: the same public signature as the positional record this replaces.
    public InMemoryDeadLetter(T? Message, ReadOnlyMemory<byte> Body, MessageContext Context, string Reason, string? Detail)
#pragma warning restore IDE1006
    {
        this.Message = Message;
        this.Body = Body;
        this.Context = Context;
        this.Reason = Reason;
        this.Detail = Detail;
    }

    /// <summary>The deserialized message, or null when the body could not be (or was not) deserialized.</summary>
    public T? Message { get; init; }

    /// <summary>The serialized body as published.</summary>
    public ReadOnlyMemory<byte> Body { get; init; }

    /// <summary>The metadata of the last delivery.</summary>
    public MessageContext Context { get; init; }

    /// <summary>One of <see cref="Processing.DeadLetterReasons"/>.</summary>
    public string Reason { get; init; }

    /// <summary>Diagnostic detail such as an exception type name.</summary>
    public string? Detail { get; init; }

    /// <summary>Deconstructs the entry into its five values.</summary>
#pragma warning disable IDE1006 // Same names as the constructor's parameters.
    public void Deconstruct(out T? Message, out ReadOnlyMemory<byte> Body, out MessageContext Context, out string Reason, out string? Detail)
#pragma warning restore IDE1006
    {
        Message = this.Message;
        Body = this.Body;
        Context = this.Context;
        Reason = this.Reason;
        Detail = this.Detail;
    }

    /// <summary>True when every value is equal (the body as a memory region, as a record compares it).</summary>
    public bool Equals(InMemoryDeadLetter<T>? other) =>
        ReferenceEquals(this, other)
        || (other is not null
            && EqualityComparer<T?>.Default.Equals(Message, other.Message)
            && EqualityComparer<ReadOnlyMemory<byte>>.Default.Equals(Body, other.Body)
            && EqualityComparer<MessageContext>.Default.Equals(Context, other.Context)
            && string.Equals(Reason, other.Reason, StringComparison.Ordinal)
            && string.Equals(Detail, other.Detail, StringComparison.Ordinal));

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as InMemoryDeadLetter<T>);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(Message, Body, Context, Reason, Detail);

    /// <summary>Formats the entry like a record: <c>InMemoryDeadLetter { Message = ..., ... }</c>.</summary>
    public override string ToString() =>
        $"InMemoryDeadLetter {{ Message = {Message}, Body = {Body}, Context = {Context}, Reason = {Reason}, Detail = {Detail} }}";

    /// <summary>True when both are null or equal.</summary>
    public static bool operator ==(InMemoryDeadLetter<T>? left, InMemoryDeadLetter<T>? right) =>
        left is null ? right is null : left.Equals(right);

    /// <summary>True when they differ.</summary>
    public static bool operator !=(InMemoryDeadLetter<T>? left, InMemoryDeadLetter<T>? right) => !(left == right);
}
