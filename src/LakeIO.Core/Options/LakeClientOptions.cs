using System.Text.Json;
using Azure.Storage.Files.DataLake;

namespace LakeIO;

/// <summary>
/// Configuration options for <see cref="LakeClient"/>.
/// </summary>
/// <remarks>
/// This is a standalone options class that converts to Azure SDK options internally.
/// It intentionally does NOT inherit from <c>Azure.Core.ClientOptions</c> to avoid
/// exposing HTTP pipeline details to consumers.
/// </remarks>
public class LakeClientOptions
{
    /// <summary>Retry configuration for Azure storage operations.</summary>
    public LakeRetryOptions Retry { get; } = new();

    /// <summary>
    /// JSON serialization options used by Json operations.
    /// When null, System.Text.Json defaults are used.
    /// </summary>
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }

    /// <summary>CSV format options used by Csv operations.</summary>
    public CsvFormatOptions Csv { get; } = new();

    /// <summary>
    /// Default Parquet compression method name. Default: "Snappy".
    /// </summary>
    /// <remarks>
    /// Stored as a string because LakeIO.Core does not reference Parquet.Net.
    /// ParquetOperations maps this to <c>Parquet.CompressionMethod</c> internally.
    /// Supported values: "None", "Snappy", "Gzip", "Lzo", "Brotli", "LZ4", "Zstd".
    /// </remarks>
    public string DefaultParquetCompression { get; set; } = "Snappy";

    /// <summary>
    /// Default number of rows per Parquet row group. Default: 10,000.
    /// </summary>
    /// <remarks>
    /// A moderate default suited for ADLS upload patterns where entire MemoryStream
    /// is uploaded after write. Larger row groups improve read performance but use more memory.
    /// </remarks>
    public int DefaultParquetRowGroupSize { get; set; } = 10_000;

    /// <summary>
    /// Buffer threshold in bytes for ChunkedUploadStream auto-flush.
    /// When the internal buffer reaches this size, data is flushed to Azure via AppendAsync.
    /// Default: 4MB (4 * 1024 * 1024).
    /// </summary>
    public int ChunkedUploadThreshold { get; set; } = 4 * 1024 * 1024;

    /// <summary>Operation timeout. Default: 5 minutes.</summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Enable detailed diagnostic logging.</summary>
    public bool EnableDiagnostics { get; set; }

    /// <summary>
    /// Converts to Azure SDK <see cref="DataLakeClientOptions"/> for internal use.
    /// </summary>
    internal DataLakeClientOptions ToDataLakeClientOptions()
    {
        var options = new DataLakeClientOptions();
        options.Retry.MaxRetries = Retry.MaxRetries;
        options.Retry.Delay = Retry.Delay;
        options.Retry.MaxDelay = Retry.MaxDelay;
        options.Retry.Mode = Retry.RetryMode;
        return options;
    }
}
