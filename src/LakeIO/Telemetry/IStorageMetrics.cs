namespace LakeIO.Telemetry;

/// <summary>
/// Interface for recording storage operation metrics and telemetry.
/// Implementations can integrate with Application Insights, Prometheus, or other monitoring systems.
/// </summary>
public interface IStorageMetrics
{
    /// <summary>
    /// Records the duration of a storage operation.
    /// </summary>
    /// <param name="operationName">The name of the operation (e.g., "UploadFile", "ReadParquet").</param>
    /// <param name="durationMs">The duration in milliseconds.</param>
    /// <param name="success">Whether the operation succeeded.</param>
    /// <param name="properties">Additional properties to include with the metric.</param>
    void RecordOperationDuration(
        string operationName,
        double durationMs,
        bool success,
        IDictionary<string, string>? properties = null);

    /// <summary>
    /// Records the size of data transferred in a storage operation.
    /// </summary>
    /// <param name="operationName">The name of the operation.</param>
    /// <param name="sizeBytes">The size in bytes.</param>
    /// <param name="direction">The direction of data transfer ("Upload" or "Download").</param>
    /// <param name="properties">Additional properties to include with the metric.</param>
    void RecordDataTransfer(
        string operationName,
        long sizeBytes,
        string direction,
        IDictionary<string, string>? properties = null);

    /// <summary>
    /// Records a counter metric (e.g., number of files processed, errors encountered).
    /// </summary>
    /// <param name="metricName">The name of the counter metric.</param>
    /// <param name="value">The value to record (default is 1).</param>
    /// <param name="properties">Additional properties to include with the metric.</param>
    void RecordCounter(
        string metricName,
        long value = 1,
        IDictionary<string, string>? properties = null);

    /// <summary>
    /// Records a gauge metric (e.g., current file size, buffer queue length).
    /// </summary>
    /// <param name="metricName">The name of the gauge metric.</param>
    /// <param name="value">The current value.</param>
    /// <param name="properties">Additional properties to include with the metric.</param>
    void RecordGauge(
        string metricName,
        double value,
        IDictionary<string, string>? properties = null);
}
