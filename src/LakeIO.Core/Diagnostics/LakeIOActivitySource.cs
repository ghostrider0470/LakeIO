using System.Diagnostics;

namespace LakeIO;

/// <summary>
/// Provides a process-wide <see cref="ActivitySource"/> singleton for distributed tracing
/// across all LakeIO storage operations.
/// </summary>
/// <remarks>
/// <para>
/// All Operations classes (JsonOperations, ParquetOperations, CsvOperations, etc.) use
/// <c>LakeIOActivitySource.Source.StartActivity(name)</c> to create spans for each
/// storage operation. When no <see cref="ActivityListener"/> is registered, the calls
/// are effectively no-ops with negligible overhead.
/// </para>
/// <para>
/// To collect traces, register an <see cref="ActivityListener"/> or use an OpenTelemetry
/// TracerProvider configured with <c>AddSource("LakeIO")</c>.
/// </para>
/// </remarks>
internal static class LakeIOActivitySource
{
    /// <summary>
    /// The well-known name used to identify the LakeIO activity source.
    /// Pass this value to <c>AddSource</c> when configuring an OpenTelemetry TracerProvider.
    /// </summary>
    internal const string Name = "LakeIO";

    /// <summary>
    /// The version reported by the activity source, matching the LakeIO library version.
    /// </summary>
    internal const string Version = "2.0.0";

    /// <summary>
    /// The singleton <see cref="ActivitySource"/> instance used by all LakeIO operations.
    /// Thread-safe by design (<see cref="ActivitySource"/> is documented as thread-safe).
    /// Returns <see langword="null"/> from <see cref="ActivitySource.StartActivity(string)"/>
    /// when no listener is sampling, so callers must null-check the returned <see cref="Activity"/>.
    /// </summary>
    internal static readonly ActivitySource Source = new(Name, Version);
}
