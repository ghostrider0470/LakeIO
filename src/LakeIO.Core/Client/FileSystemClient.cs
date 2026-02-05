using System.Collections.Concurrent;
using Azure.Storage.Files.DataLake;

namespace LakeIO;

/// <summary>
/// Client for file system operations in Azure Data Lake.
/// </summary>
/// <remarks>
/// <para>Provides navigation to directories and files, plus format-specific operations
/// via extension methods (<c>fs.Json()</c>, <c>fs.Csv()</c>, etc.).</para>
/// <para>Format operations use extension methods (not properties) because C# stable versions
/// do not support extension properties, and LakeIO.Parquet adds <c>.Parquet()</c> from a
/// separate assembly via <c>InternalsVisibleTo</c>.</para>
/// </remarks>
public class FileSystemClient
{
    private readonly DataLakeFileSystemClient? _azureClient;
    private readonly LakeClientOptions? _options;

    // Thread-safe lazy cache for operation instances (one per type)
    private readonly ConcurrentDictionary<Type, object> _operations = new();

    /// <summary>
    /// Protected parameterless constructor for mocking.
    /// </summary>
    protected FileSystemClient()
    {
    }

    /// <summary>
    /// Internal constructor used by <see cref="LakeClient.GetFileSystemClient"/>.
    /// </summary>
    internal FileSystemClient(DataLakeFileSystemClient azureClient, LakeClientOptions options)
    {
        _azureClient = azureClient ?? throw new ArgumentNullException(nameof(azureClient));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>Gets the file system name.</summary>
    public virtual string Name => _azureClient!.Name;

    /// <summary>Gets the file system URI.</summary>
    public virtual Uri Uri => _azureClient!.Uri;

    /// <summary>
    /// Internal hook for extension methods to get or create operation instances.
    /// Thread-safe via ConcurrentDictionary. Each operation type is created once and cached.
    /// </summary>
    /// <typeparam name="T">The operation type (e.g., JsonOperations, ParquetOperations).</typeparam>
    /// <param name="factory">Factory function that creates the operation instance.</param>
    /// <returns>The cached or newly created operation instance.</returns>
    internal T GetOrCreateOperations<T>(Func<DataLakeFileSystemClient, LakeClientOptions, T> factory)
        where T : class
    {
        return (T)_operations.GetOrAdd(typeof(T), _ => factory(_azureClient!, _options!));
    }

    /// <summary>
    /// Exposes the underlying Azure client for operations that need direct access.
    /// </summary>
    internal DataLakeFileSystemClient AzureClient => _azureClient!;

    /// <summary>
    /// Exposes the options for operations that need configuration.
    /// </summary>
    internal LakeClientOptions Options => _options!;

    /// <summary>
    /// Gets a <see cref="DirectoryClient"/> for the specified directory path.
    /// </summary>
    /// <param name="directoryPath">The directory path within this file system.</param>
    /// <returns>A new <see cref="DirectoryClient"/>.</returns>
    public virtual DirectoryClient GetDirectoryClient(string directoryPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directoryPath);
        var azureClient = _azureClient!.GetDirectoryClient(directoryPath);
        return new DirectoryClient(azureClient, _options!);
    }

    /// <summary>
    /// Gets a <see cref="FileClient"/> for the specified file path.
    /// </summary>
    /// <param name="filePath">The file path within this file system.</param>
    /// <returns>A new <see cref="FileClient"/>.</returns>
    public virtual FileClient GetFileClient(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        var azureClient = _azureClient!.GetFileClient(filePath);
        return new FileClient(azureClient, _options!);
    }
}
