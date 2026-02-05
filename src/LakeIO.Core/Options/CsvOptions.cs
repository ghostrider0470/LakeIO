namespace LakeIO;

/// <summary>
/// Per-operation configuration for CSV operations.
/// </summary>
/// <remarks>
/// <para>Properties are nullable with init-only setters (immutable). When a property is null,
/// the operation falls back to the corresponding value on
/// <see cref="LakeClientOptions.Csv"/> (<see cref="CsvFormatOptions"/>), then to library defaults.</para>
/// <para><b>Fallback chain:</b> <see cref="CsvOptions"/> (per-op) ->
/// <see cref="LakeClientOptions.Csv"/> (<see cref="CsvFormatOptions"/>) -> library defaults.</para>
/// </remarks>
public class CsvOptions
{
    /// <summary>
    /// CSV field delimiter for this specific operation.
    /// When null, falls back to <see cref="CsvFormatOptions.Delimiter"/> (default: <c>","</c>).
    /// </summary>
    public string? Delimiter { get; init; }

    /// <summary>
    /// Whether the CSV file includes a header row for this specific operation.
    /// When null, falls back to <see cref="CsvFormatOptions.HasHeader"/> (default: <see langword="true"/>).
    /// </summary>
    public bool? HasHeader { get; init; }

    /// <summary>
    /// Culture name for type conversion (e.g., <c>"en-US"</c>, <c>"de-DE"</c>).
    /// When null, <see cref="System.Globalization.CultureInfo.InvariantCulture"/> is used.
    /// </summary>
    public string? CultureName { get; init; }
}
