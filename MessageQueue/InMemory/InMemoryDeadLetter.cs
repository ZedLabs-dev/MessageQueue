using ESE.MessageQueue.Abstractions;

namespace ESE.MessageQueue.InMemory;

/// <summary>
/// A message the in-memory provider dead-lettered.
/// </summary>
/// <param name="Message">The deserialized message, or null when the body could not be (or was not) deserialized.</param>
/// <param name="Body">The serialized body as published.</param>
/// <param name="Context">The metadata of the last delivery.</param>
/// <param name="Reason">One of <see cref="Processing.DeadLetterReasons"/>.</param>
/// <param name="Detail">Diagnostic detail such as an exception type name.</param>
public sealed record InMemoryDeadLetter<T>(T? Message, ReadOnlyMemory<byte> Body, MessageContext Context, string Reason, string? Detail)
    where T : class;
