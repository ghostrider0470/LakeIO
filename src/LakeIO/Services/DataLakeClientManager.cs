using System.Collections.Concurrent;
using Azure.Storage.Files.DataLake;
using LakeIO.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LakeIO.Services;

/// <summary>
/// Manages Azure Data Lake Storage client creation and caching.
/// Implements thread-safe client pooling for optimal performance.
/// </summary>
public class DataLakeClientManager : IDataLakeClientManager
{
    private readonly ILogger<DataLakeClientManager> _logger;
    private readonly LakeOptions _options;
    private readonly ConcurrentDictionary<string, DataLakeFileSystemClient> _fileSystemClients = new();
    private readonly ConcurrentDictionary<string, DataLakeServiceClient> _serviceClients = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="DataLakeClientManager"/> class.
    /// </summary>
    /// <param name="logger">Logger instance for diagnostics.</param>
    /// <param name="options">Configuration options for Azure Data Lake.</param>
    /// <exception cref="ArgumentNullException">Thrown when logger or options is null.</exception>
    public DataLakeClientManager(
        ILogger<DataLakeClientManager> logger,
        IOptions<LakeOptions> options)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));

        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
        {
            throw new InvalidOperationException(
                "ConnectionString is required in LakeOptions. " +
                "Please configure it in appsettings.json under 'AzureDataLake:ConnectionString'.");
        }

        // Initialize the default service client
        GetOrCreateServiceClient(_options.ConnectionString);

        if (_options.EnableDetailedLogging)
        {
            _logger.LogInformation("DataLakeClientManager initialized with caching: {CacheEnabled}",
                _options.CacheFileSystemClients);
        }
    }

    /// <inheritdoc />
    public DataLakeServiceClient GetOrCreateServiceClient(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));
        }

        return _serviceClients.GetOrAdd(connectionString, cs =>
        {
            if (_options.EnableDetailedLogging)
            {
                _logger.LogDebug("Creating new DataLakeServiceClient for connection string");
            }

            var client = new DataLakeServiceClient(cs);

            _logger.LogInformation("Created new DataLakeServiceClient. Total service clients cached: {Count}",
                _serviceClients.Count);

            return client;
        });
    }

    /// <inheritdoc />
    public DataLakeFileSystemClient GetOrCreateFileSystemClient(
        string fileSystemName,
        string? connectionString = null)
    {
        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        // Use default connection string if not provided
        var effectiveConnectionString = connectionString ?? _options.ConnectionString;

        // Generate cache key
        var cacheKey = $"{effectiveConnectionString}:{fileSystemName}";

        // Check if caching is enabled
        if (!_options.CacheFileSystemClients)
        {
            if (_options.EnableDetailedLogging)
            {
                _logger.LogDebug("Client caching disabled, creating new client for file system: {FileSystem}",
                    fileSystemName);
            }

            var serviceClient = GetOrCreateServiceClient(effectiveConnectionString);
            return serviceClient.GetFileSystemClient(fileSystemName);
        }

        // Return cached or create new client
        return _fileSystemClients.GetOrAdd(cacheKey, _ =>
        {
            if (_options.EnableDetailedLogging)
            {
                _logger.LogDebug("Creating new DataLakeFileSystemClient for file system: {FileSystem}",
                    fileSystemName);
            }

            var serviceClient = GetOrCreateServiceClient(effectiveConnectionString);
            var fileSystemClient = serviceClient.GetFileSystemClient(fileSystemName);

            _logger.LogInformation(
                "Created new DataLakeFileSystemClient for '{FileSystem}'. Total file system clients cached: {Count}",
                fileSystemName, _fileSystemClients.Count);

            return fileSystemClient;
        });
    }

    /// <inheritdoc />
    public void ClearCache()
    {
        var fileSystemCount = _fileSystemClients.Count;
        var serviceClientCount = _serviceClients.Count;

        _fileSystemClients.Clear();
        _serviceClients.Clear();

        _logger.LogInformation(
            "Cleared client cache. Removed {FileSystemCount} file system clients and {ServiceClientCount} service clients",
            fileSystemCount, serviceClientCount);
    }

    /// <inheritdoc />
    public int CachedFileSystemCount => _fileSystemClients.Count;

    /// <inheritdoc />
    public int CachedServiceClientCount => _serviceClients.Count;
}
