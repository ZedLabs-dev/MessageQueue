namespace ESE.MessageQueue.Tests;

public static class Skip
{
    public static void If(bool condition, string reason)
    {
        if (condition)
        {
            //throw new Xunit.SkipException(reason);
        }
    }
}
