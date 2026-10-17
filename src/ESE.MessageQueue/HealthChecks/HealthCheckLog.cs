using Microsoft.Extensions.Logging;

namespace ESE.MessageQueue.HealthChecks;

// S5: a failed probe's exception goes to the application's log, never into the HealthCheckResult, whose description,
// data and exception health endpoints may show to anyone.
static partial class HealthCheckLog
{
    [LoggerMessage(EventId = 6001, Level = LogLevel.Warning,
        Message = "Health check {HealthCheck} failed with {ExceptionType}")]
    static partial void Failed(ILogger logger, string healthCheck, string exceptionType, Exception exception);

    public static void ProbeFailed(ILogger logger, string healthCheck, Exception exception) =>
        Failed(new Processing.SafeLogger(logger), healthCheck, exception.GetType().FullName ?? exception.GetType().Name, exception);
}

// The logger health checks registered through the extensions log to: the container's, or none.
static class HealthCheckLoggers
{
    public static ILogger For(IServiceProvider services) =>
        (services.GetService(typeof(ILoggerFactory)) as ILoggerFactory)?.CreateLogger("ESE.MessageQueue.HealthChecks")
        ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
}
