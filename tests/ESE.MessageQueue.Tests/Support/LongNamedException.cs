namespace ESE.MessageQueue.Tests;

// An exception whose type's FullName is far longer than any limit: a generic type's FullName carries the
// assembly-qualified names of its type arguments, so 80 levels of nesting make it about 10 000 characters.
public sealed class LongNamedException<T> : Exception
{
    public LongNamedException() : base("long-named")
    {
    }

    public LongNamedException(string message) : base(message)
    {
    }

    public LongNamedException(string message, Exception inner) : base(message, inner)
    {
    }
}

public sealed class Nest<T>;

public static class LongNamedExceptions
{
    public const int NestingLevels = 80;

    public static Exception Create()
    {
        var type = typeof(int);
        for (var i = 0; i < NestingLevels; i++)
            type = typeof(Nest<>).MakeGenericType(type);
        return (Exception)Activator.CreateInstance(typeof(LongNamedException<>).MakeGenericType(type))!;
    }
}
