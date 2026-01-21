using System.ComponentModel.DataAnnotations;

namespace LakeIO.Configuration;

/// <summary>
/// Configuration options for Azure Data Lake Storage connection.
/// Supports Options Pattern for .NET configuration binding.
/// </summary>
public class LakeOptions
{
    /// <summary>
    /// Configuration section name for binding from appsettings.json
    /// </summary>
    public const string SectionName = "AzureDataLake";

    /// <summary>
    /// Azure Data Lake Storage connection string.
    /// Format: DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey};EndpointSuffix=core.windows.net
    /// </summary>
    [Required(ErrorMessage = "ConnectionString is required")]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Default file system (container) name if not specified in operations.
    /// </summary>
    public string? DefaultFileSystem { get; set; }

    /// <summary>
    /// Maximum number of concurrent operations allowed.
    /// Default: 10
    /// </summary>
    [Range(1, 100, ErrorMessage = "MaxConcurrentOperations must be between 1 and 100")]
    public int MaxConcurrentOperations { get; set; } = 10;

    /// <summary>
    /// Timeout for storage operations in seconds.
    /// Default: 300 seconds (5 minutes)
    /// </summary>
    [Range(30, 3600, ErrorMessage = "OperationTimeoutSeconds must be between 30 and 3600")]
    public int OperationTimeoutSeconds { get; set; } = 300;

    /// <summary>
    /// Enable detailed logging for storage operations.
    /// Default: false
    /// </summary>
    public bool EnableDetailedLogging { get; set; } = false;

    /// <summary>
    /// Cache file system clients to improve performance.
    /// Default: true
    /// </summary>
    public bool CacheFileSystemClients { get; set; } = true;
}
