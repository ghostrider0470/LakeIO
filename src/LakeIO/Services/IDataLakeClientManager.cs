using Azure.Storage.Files.DataLake;

namespace LakeIO.Services;

/// <summary>
/// Manages Azure Data Lake Storage client creation and caching.
/// Provides thread-safe client instances with connection pooling.
/// </summary>
public interface IDataLakeClientManager
{
    /// <summary>
    /// Gets or creates a DataLakeServiceClient for the specified connection string.
    /// Results are cached for performance.
    /// </summary>
    /// <param name="connectionString">The connection string to the storage account.</param>
    /// <returns>A cached or new <see cref="DataLakeServiceClient"/> instance.</returns>
    /// <exception cref="ArgumentException">Thrown when connection string is null or empty.</exception>
    DataLakeServiceClient GetOrCreateServiceClient(string connectionString);

    /// <summary>
    /// Gets or creates a DataLakeFileSystemClient for the specified file system.
    /// Results are cached for performance.
    /// </summary>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="connectionString">
    /// Optional. The connection string to use. If not provided, uses the default connection string.
    /// </param>
    /// <returns>A cached or new <see cref="DataLakeFileSystemClient"/> instance.</returns>
    /// <exception cref="ArgumentException">Thrown when file system name is null or empty.</exception>
    DataLakeFileSystemClient GetOrCreateFileSystemClient(string fileSystemName, string? connectionString = null);

    /// <summary>
    /// Clears all cached clients. Use this when connection strings change or for cleanup.
    /// </summary>
    void ClearCache();

    /// <summary>
    /// Gets the number of cached file system clients.
    /// Useful for monitoring and diagnostics.
    /// </summary>
    int CachedFileSystemCount { get; }

    /// <summary>
    /// Gets the number of cached service clients.
    /// Useful for monitoring and diagnostics.
    /// </summary>
    int CachedServiceClientCount { get; }
}
