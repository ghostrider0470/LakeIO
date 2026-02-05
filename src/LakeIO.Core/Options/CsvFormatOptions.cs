namespace LakeIO;

/// <summary>
/// CSV format configuration options.
/// </summary>
public class CsvFormatOptions
{
    /// <summary>CSV delimiter. Default: comma.</summary>
    public string Delimiter { get; init; } = ",";

    /// <summary>Whether CSV files include a header row. Default: true.</summary>
    public bool HasHeader { get; init; } = true;
}
