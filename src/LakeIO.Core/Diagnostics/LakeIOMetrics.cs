using System.Diagnostics.Metrics;

namespace LakeIO;

/// <summary>
/// Provides a process-wide <see cref="Meter"/> singleton with pre-defined instruments
/// for recording LakeIO storage operation metrics.
/// </summary>
/// <remarks>
/// <para>
/// All Operations classes record metrics through the static instruments on this class.
/// When no <see cref="MeterListener"/> or OpenTelemetry MeterProvider is configured,
/// the recording calls are effectively no-ops with negligible overhead.
/// </para>
/// <para>
/// To collect metrics, configure an OpenTelemetry MeterProvider with
/// <c>AddMeter("LakeIO")</c>.
/// </para>
/// </remarks>
internal static class LakeIOMetrics
{
    /// <summary>
    /// The well-known name used to identify the LakeIO meter.
    /// Pass this value to <c>AddMeter</c> when configuring an OpenTelemetry MeterProvider.
    /// </summary>
    internal const string MeterName = "LakeIO";

    /// <summary>
    /// The singleton <see cref="Meter"/> instance that owns all LakeIO instruments.
    /// Thread-safe by design (<see cref="Meter"/> is documented as thread-safe).
    /// </summary>
    internal static readonly Meter Meter = new(MeterName, "2.0.0");

    /// <summary>
    /// Counts the total number of LakeIO storage operations (read, write, delete, list, etc.).
    /// Tag with <c>lakeio.operation</c> to distinguish operation types.
    /// </summary>
    internal static readonly Counter<long> OperationsTotal =
        Meter.CreateCounter<long>(
            name: "lakeio.operations.total",
            unit: "{operations}",
            description: "Total number of LakeIO storage operations");

    /// <summary>
    /// Counts the total bytes transferred (uploaded or downloaded) by LakeIO operations.
    /// Tag with <c>lakeio.direction</c> ("upload" or "download") to distinguish direction.
    /// </summary>
    internal static readonly Counter<long> BytesTransferred =
        Meter.CreateCounter<long>(
            name: "lakeio.bytes.transferred",
            unit: "By",
            description: "Total bytes transferred by LakeIO operations");

    /// <summary>
    /// Records the duration (in seconds) of individual LakeIO storage operations.
    /// Tag with <c>lakeio.operation</c> and <c>lakeio.status</c> to distinguish
    /// operation types and success/failure.
    /// </summary>
    internal static readonly Histogram<double> OperationDuration =
        Meter.CreateHistogram<double>(
            name: "lakeio.operation.duration",
            unit: "s",
            description: "Duration of LakeIO storage operations",
            advice: new InstrumentAdvice<double>
            {
                HistogramBucketBoundaries = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
            });
}
