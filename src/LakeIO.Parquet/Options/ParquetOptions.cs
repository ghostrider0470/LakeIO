using Parquet;

namespace LakeIO.Parquet;

/// <summary>
/// Per-operation configuration for Parquet operations.
/// </summary>
/// <remarks>
/// When null, falls back to <see cref="LakeClientOptions"/> defaults
/// (<c>DefaultParquetCompression</c> and <c>DefaultParquetRowGroupSize</c>),
/// then to library defaults (Snappy compression, 10,000 row group size).
/// </remarks>
public class ParquetOptions
{
    /// <summary>
    /// Parquet compression method for this specific operation.
    /// When null, falls back to <see cref="LakeClientOptions.DefaultParquetCompression"/>,
    /// then to <see cref="CompressionMethod.Snappy"/>.
    /// </summary>
    public CompressionMethod? CompressionMethod { get; set; }

    /// <summary>
    /// Number of rows per row group for this specific operation.
    /// When null, falls back to <see cref="LakeClientOptions.DefaultParquetRowGroupSize"/>,
    /// then to 10,000.
    /// </summary>
    public int? RowGroupSize { get; set; }

    /// <summary>
    /// When true, automatically runs Quick validation after write operations
    /// (WriteAsync, WriteStreamAsync). Catches partial uploads.
    /// Default: null (treated as false).
    /// </summary>
    public bool? ValidateAfterWrite { get; set; }
}
