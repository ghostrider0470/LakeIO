using Microsoft.Extensions.Logging;

namespace LakeIO.Telemetry;

/// <summary>
/// Default implementation of IStorageMetrics that logs metrics using ILogger.
/// Can be replaced with Application Insights or other telemetry providers.
/// </summary>
public class LoggerStorageMetrics : IStorageMetrics
{
    private readonly ILogger<LoggerStorageMetrics> _logger;

    public LoggerStorageMetrics(ILogger<LoggerStorageMetrics> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public void RecordOperationDuration(
        string operationName,
        double durationMs,
        bool success,
        IDictionary<string, string>? properties = null)
    {
        var propertiesStr = properties != null
            ? string.Join(", ", properties.Select(kvp => $"{kvp.Key}={kvp.Value}"))
            : "none";

        _logger.LogInformation(
            "Operation: {OperationName}, Duration: {DurationMs}ms, Success: {Success}, Properties: {Properties}",
            operationName, durationMs, success, propertiesStr);
    }

    public void RecordDataTransfer(
        string operationName,
        long sizeBytes,
        string direction,
        IDictionary<string, string>? properties = null)
    {
        var sizeMB = sizeBytes / 1_048_576.0;
        var propertiesStr = properties != null
            ? string.Join(", ", properties.Select(kvp => $"{kvp.Key}={kvp.Value}"))
            : "none";

        _logger.LogInformation(
            "Data Transfer: {OperationName}, Size: {SizeMB:F2}MB, Direction: {Direction}, Properties: {Properties}",
            operationName, sizeMB, direction, propertiesStr);
    }

    public void RecordCounter(
        string metricName,
        long value = 1,
        IDictionary<string, string>? properties = null)
    {
        var propertiesStr = properties != null
            ? string.Join(", ", properties.Select(kvp => $"{kvp.Key}={kvp.Value}"))
            : "none";

        _logger.LogInformation(
            "Counter: {MetricName}, Value: {Value}, Properties: {Properties}",
            metricName, value, propertiesStr);
    }

    public void RecordGauge(
        string metricName,
        double value,
        IDictionary<string, string>? properties = null)
    {
        var propertiesStr = properties != null
            ? string.Join(", ", properties.Select(kvp => $"{kvp.Key}={kvp.Value}"))
            : "none";

        _logger.LogInformation(
            "Gauge: {MetricName}, Value: {Value}, Properties: {Properties}",
            metricName, value, propertiesStr);
    }
}
