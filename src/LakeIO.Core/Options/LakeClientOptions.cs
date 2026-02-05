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
