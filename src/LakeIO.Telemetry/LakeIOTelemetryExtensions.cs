using OpenTelemetry.Trace;
using OpenTelemetry.Metrics;

namespace LakeIO.Telemetry;

/// <summary>
/// Extension methods for registering LakeIO instrumentation with OpenTelemetry providers.
/// </summary>
/// <remarks>
/// <para>
/// Use these methods to register LakeIO's <see cref="System.Diagnostics.ActivitySource"/> and
/// <see cref="System.Diagnostics.Metrics.Meter"/> with OpenTelemetry's TracerProvider and MeterProvider
/// respectively. This enables collection of distributed traces and metrics emitted by all LakeIO
/// storage operations (JSON, Parquet, CSV, File, Batch, Directory).
/// </para>
/// <para>
/// Example usage:
/// <code>
/// builder.Services.AddOpenTelemetry()
///     .WithTracing(tracing => tracing.AddLakeIOInstrumentation())
///     .WithMetrics(metrics => metrics.AddLakeIOInstrumentation());
/// </code>
/// </para>
/// </remarks>
public static class LakeIOTelemetryExtensions
{
    /// <summary>
    /// Registers LakeIO's <see cref="System.Diagnostics.ActivitySource"/> with the
    /// <see cref="TracerProviderBuilder"/>, enabling collection of distributed traces
    /// for all LakeIO storage operations.
    /// </summary>
    /// <param name="builder">The <see cref="TracerProviderBuilder"/> to configure.</param>
    /// <returns>The <paramref name="builder"/> for chaining.</returns>
    public static TracerProviderBuilder AddLakeIOInstrumentation(this TracerProviderBuilder builder)
    {
        return builder.AddSource(LakeIOActivitySource.Name);
    }

    /// <summary>
    /// Registers LakeIO's <see cref="System.Diagnostics.Metrics.Meter"/> with the
    /// <see cref="MeterProviderBuilder"/>, enabling collection of operation count,
    /// bytes transferred, and duration metrics for all LakeIO storage operations.
    /// </summary>
    /// <param name="builder">The <see cref="MeterProviderBuilder"/> to configure.</param>
    /// <returns>The <paramref name="builder"/> for chaining.</returns>
    public static MeterProviderBuilder AddLakeIOInstrumentation(this MeterProviderBuilder builder)
    {
        return builder.AddMeter(LakeIOMetrics.MeterName);
    }
}
