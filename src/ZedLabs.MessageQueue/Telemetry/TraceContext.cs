namespace ZedLabs.MessageQueue.Telemetry;

/// <summary>A W3C trace context (https://www.w3.org/TR/trace-context/) carried in message headers.</summary>
public sealed record TraceContext(string TraceId, string SpanId)
{
    /// <summary>Longest <c>traceparent</c> read; longer values are ignored without being parsed.</summary>
    public const int MaxTraceParentLength = 256;

    /// <summary>Longest <c>tracestate</c> kept (the specification's propagation minimum); longer ones are dropped.</summary>
    public const int MaxTraceStateLength = 512;

    const int VersionZeroLength = 55;
    const int MaxTraceStateMembers = 32;
    const byte KnownFlags = 0x03;

    /// <summary>The trace flags byte (bit 0: sampled; bit 1: random, W3C Trace Context level 2).</summary>
    public byte TraceFlags { get; init; }

    /// <summary>The <c>sampled</c> trace flag.</summary>
    public bool Sampled => (TraceFlags & 1) == 1;

    /// <summary>The vendor <c>tracestate</c> list, when the message carried a valid one.</summary>
    public string? TraceState { get; init; }

    /// <summary>
    /// Parses a <c>traceparent</c> and optional <c>tracestate</c> strictly, as the specification requires of a receiver:
    /// lowercase hex, non-zero ids, version <c>ff</c> refused, exactly 55 characters for version 00. A malformed or
    /// oversized traceparent yields null; a malformed tracestate is dropped on its own. Never throws.
    /// </summary>
    public static TraceContext? Parse(string? traceParent, string? traceState)
    {
        if (traceParent is null || traceParent.Length < VersionZeroLength || traceParent.Length > MaxTraceParentLength)
            return null;

        var value = traceParent.AsSpan();
        var version = value[..2];
        if (!IsLowerHex(version) || version is "ff")
            return null;

        // Version 00 has exactly four fields; a later version may append fields after a '-'.
        if (version is "00" ? value.Length != VersionZeroLength : value.Length > VersionZeroLength && value[VersionZeroLength] != '-')
            return null;

        if (value[2] != '-' || value[35] != '-' || value[52] != '-')
            return null;

        var traceId = value[3..35];
        var spanId = value[36..52];
        var flags = value[53..55];
        if (!IsLowerHex(traceId) || !IsLowerHex(spanId) || !IsLowerHex(flags) || IsAllZero(traceId) || IsAllZero(spanId))
            return null;

        return new TraceContext(traceId.ToString(), spanId.ToString())
        {
            // Bits 0 (sampled) and 1 (random, level 2) are defined; unknown flags are cleared before propagating.
            TraceFlags = (byte)(Convert.ToByte(flags.ToString(), 16) & KnownFlags),
            TraceState = IsValidTraceState(traceState) ? traceState : null
        };
    }

    /// <summary>The version 00 <c>traceparent</c> header value.</summary>
    public string ToTraceParent() => $"00-{TraceId}-{SpanId}-{TraceFlags & KnownFlags:x2}";

    static bool IsLowerHex(ReadOnlySpan<char> value)
    {
        foreach (var c in value)
        {
            if (c is not ((>= '0' and <= '9') or (>= 'a' and <= 'f')))
                return false;
        }
        return true;
    }

    static bool IsAllZero(ReadOnlySpan<char> value) => !value.ContainsAnyExcept('0');

    // list = member *( OWS "," OWS member ), at most 32 members, unique keys; empty members are allowed.
    static bool IsValidTraceState(string? traceState)
    {
        if (string.IsNullOrEmpty(traceState) || traceState.Length > MaxTraceStateLength)
            return false;

        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var raw in traceState.Split(','))
        {
            var member = raw.Trim(' ', '\t');
            if (member.Length == 0)
                continue;

            var equals = member.IndexOf('=', StringComparison.Ordinal);
            if (equals <= 0 || !IsValidKey(member.AsSpan(0, equals)) || !IsValidValue(member.AsSpan(equals + 1)))
                return false;

            if (!keys.Add(member[..equals]) || keys.Count > MaxTraceStateMembers)
                return false;
        }
        return keys.Count > 0;
    }

    // key = simple-key / multi-tenant-key; simple-key = lcalpha 0*255( lcalpha / DIGIT / "_" / "-"/ "*" / "/" ),
    // multi-tenant-key = tenant-id "@" system-id (tenant-id may start with a digit, system-id at most 14 characters).
    static bool IsValidKey(ReadOnlySpan<char> key)
    {
        var at = key.IndexOf('@');
        if (at < 0)
            return key.Length <= 256 && key[0] is >= 'a' and <= 'z' && IsKeyTail(key[1..]);

        var tenant = key[..at];
        var system = key[(at + 1)..];
        return tenant.Length is >= 1 and <= 241 && tenant[0] is (>= 'a' and <= 'z') or (>= '0' and <= '9') && IsKeyTail(tenant[1..])
            && system.Length is >= 1 and <= 14 && system[0] is >= 'a' and <= 'z' && IsKeyTail(system[1..]);
    }

    static bool IsKeyTail(ReadOnlySpan<char> value)
    {
        foreach (var c in value)
        {
            if (c is not ((>= 'a' and <= 'z') or (>= '0' and <= '9') or '_' or '-' or '*' or '/'))
                return false;
        }
        return true;
    }

    // value = 0*255(chr) nblk-chr; chr = %x20 / nblk-chr; nblk-chr = %x21-2B / %x2D-3C / %x3E-7E
    static bool IsValidValue(ReadOnlySpan<char> value)
    {
        if (value.Length is 0 or > 256 || value[^1] == ' ')
            return false;

        foreach (var c in value)
        {
            if (c is < ' ' or > '~' or ',' or '=')
                return false;
        }
        return true;
    }
}
