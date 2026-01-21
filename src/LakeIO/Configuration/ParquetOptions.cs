using System.ComponentModel.DataAnnotations;
using Parquet;

namespace LakeIO.Configuration;

/// <summary>
/// Configuration options for Parquet file operations.
/// Supports Options Pattern for .NET configuration binding.
/// </summary>
public class ParquetOptions
{
    /// <summary>
    /// Configuration section name for binding from appsettings.json
    /// </summary>
    public const string SectionName = "Parquet";

    /// <summary>
    /// Compression method for Parquet files.
    /// Default: Snappy (best balance of speed and compression ratio)
    /// Options: None, Snappy, Gzip, LZ4, Zstd, Brotli
    /// </summary>
    public CompressionMethod CompressionMethod { get; set; } = CompressionMethod.Snappy;

    /// <summary>
    /// Row group size in bytes (internal divisions for parallel processing).
    /// Default: 134217728 (128 MB) - optimal for most scenarios
    /// Recommended range: 64 MB - 256 MB
    /// </summary>
    [Range(67108864, 268435456, ErrorMessage = "RowGroupSize must be between 64 MB and 256 MB")]
    public long RowGroupSize { get; set; } = 134217728; // 128 MB

    /// <summary>
    /// Maximum file size in bytes before recommending file rotation.
    /// Default: 1073741824 (1 GB) - optimal for query performance
    /// For Consumption plan: Consider 536870912 (512 MB)
    /// </summary>
    [Range(134217728, 5368709120, ErrorMessage = "MaxFileSize must be between 128 MB and 5 GB")]
    public long MaxFileSize { get; set; } = 1073741824; // 1 GB

    /// <summary>
    /// Warning threshold as percentage of MaxFileSize (0.0 - 1.0).
    /// Default: 0.9 (90%) - log warning when file reaches this size
    /// </summary>
    [Range(0.5, 1.0, ErrorMessage = "FileSizeWarningThreshold must be between 0.5 and 1.0")]
    public double FileSizeWarningThreshold { get; set; } = 0.9;

    /// <summary>
    /// Enable automatic file rotation when MaxFileSize is exceeded.
    /// Default: false (manual management)
    /// </summary>
    public bool EnableAutoFileRotation { get; set; } = false;

    /// <summary>
    /// Enable file size monitoring and warnings.
    /// Default: true
    /// </summary>
    public bool EnableFileSizeMonitoring { get; set; } = true;

    /// <summary>
    /// Memory budget for compaction operations in bytes.
    /// Default: 524288000 (500 MB) - safe for most Azure Functions plans
    /// </summary>
    [Range(104857600, 2147483648, ErrorMessage = "CompactionMemoryBudget must be between 100 MB and 2 GB")]
    public long CompactionMemoryBudget { get; set; } = 524288000; // 500 MB
}
