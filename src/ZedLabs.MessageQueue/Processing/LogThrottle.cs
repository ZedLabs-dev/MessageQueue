namespace ZedLabs.MessageQueue.Processing;

// Keeps repeated identical errors from flooding the log: per key, at most one entry per interval. The next entry
// that is let through reports how many were suppressed in between.
sealed class LogThrottle
{
    const int MaxTracked = 1_000;
    static readonly TimeSpan DefaultInterval = TimeSpan.FromSeconds(10);

    readonly Dictionary<string, Entry> entries = new(StringComparer.Ordinal);
    readonly long intervalMilliseconds;

    public LogThrottle(TimeSpan? interval = null)
    {
        intervalMilliseconds = (long)(interval ?? DefaultInterval).TotalMilliseconds;
    }

    public bool ShouldLog(string key, out int suppressed)
    {
        lock (entries)
        {
            var now = Environment.TickCount64;
            if (entries.TryGetValue(key, out var entry) && now - entry.LastLogged < intervalMilliseconds)
            {
                entry.Suppressed++;
                suppressed = 0;
                return false;
            }

            if (entry is null)
            {
                if (entries.Count >= MaxTracked)
                    entries.Clear();
                entry = new Entry();
                entries[key] = entry;
            }

            suppressed = entry.Suppressed;
            entry.Suppressed = 0;
            entry.LastLogged = now;
            return true;
        }
    }

    sealed class Entry
    {
        public long LastLogged;
        public int Suppressed;
    }
}
