namespace LakeIO;

/// <summary>
/// Specifies the depth of Parquet file validation performed by
/// <see cref="LakeIO.Parquet.ParquetOperations.ValidateAsync"/>.
/// Higher levels include all checks from lower levels.
/// </summary>
public enum ParquetValidationLevel
{
    /// <summary>
    /// Checks only that the file size is at least 12 bytes (the minimum size for a valid
    /// Parquet file: 4-byte header magic + 4-byte footer length + 4-byte footer magic).
    /// Cost: 1 HTTP call (GetProperties).
    /// </summary>
    Quick,

    /// <summary>
    /// Quick checks plus verification that the PAR1 magic bytes are present at
    /// both the start (offset 0) and end (offset fileSize-4) of the file.
    /// Cost: 3 HTTP calls (GetProperties + 2 range reads).
    /// </summary>
    Standard,

    /// <summary>
    /// Standard checks plus a full Parquet.Net metadata parse via
    /// <c>ParquetReader.CreateAsync</c>, which validates the footer, schema, and
    /// row group metadata. Populates <see cref="ParquetValidationResult.RowGroupCount"/>
    /// and <see cref="ParquetValidationResult.FieldNames"/>.
    /// Cost: 3+ HTTP calls (GetProperties + 2 range reads + OpenRead for metadata).
    /// </summary>
    Deep
}

/// <summary>
/// Result of a Parquet file validation operation, containing validity status,
/// error details, and optional metadata extracted during deep validation.
/// </summary>
public class ParquetValidationResult
{
    /// <summary>Whether the file passed validation at the requested level.</summary>
    public bool IsValid { get; init; }

    /// <summary>
    /// Human-readable error description when <see cref="IsValid"/> is <see langword="false"/>;
    /// <see langword="null"/> when the file is valid.
    /// </summary>
    public string? ErrorReason { get; init; }

    /// <summary>The file size in bytes from GetProperties, or null if unavailable.</summary>
    public long? FileSize { get; init; }

    /// <summary>The validation level that was performed.</summary>
    public ParquetValidationLevel Level { get; init; }

    /// <summary>
    /// Number of row groups in the Parquet file. Populated only at
    /// <see cref="ParquetValidationLevel.Deep"/> level; null otherwise.
    /// </summary>
    public int? RowGroupCount { get; init; }

    /// <summary>
    /// Data field names from the Parquet schema. Populated only at
    /// <see cref="ParquetValidationLevel.Deep"/> level; null otherwise.
    /// </summary>
    public IReadOnlyList<string>? FieldNames { get; init; }
}
