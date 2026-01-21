using LakeIO.Annotations;
using LakeIO.Configuration;
using LakeIO.Formatters;
using LakeIO.Formatters.Interfaces;
using LakeIO.Validation;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LakeIO.Services;

/// <summary>
/// Implementation of Parquet storage operations for Azure Data Lake Storage.
/// Provides optimized Parquet file operations with file size monitoring.
/// </summary>
public class ParquetStorageService : IParquetStorageService
{
    private readonly ILogger<ParquetStorageService> _logger;
    private readonly IDataLakeClientManager _clientManager;
    private readonly IParquetFileFormatter _parquetFormatter;
    private readonly LakeOptions _azureOptions;
    private readonly ParquetOptions _parquetOptions;

    public ParquetStorageService(
        ILogger<ParquetStorageService> logger,
        IDataLakeClientManager clientManager,
        IParquetFileFormatter parquetFormatter,
        IOptions<LakeOptions> azureOptions,
        IOptions<ParquetOptions> parquetOptions)
    {
        _logger = logger.ThrowIfNull();
        _clientManager = clientManager.ThrowIfNull();
        _parquetFormatter = parquetFormatter.ThrowIfNull();
        _azureOptions = azureOptions?.Value.ThrowIfNull() ?? throw new ArgumentNullException(nameof(azureOptions));
        _parquetOptions = parquetOptions?.Value.ThrowIfNull() ?? throw new ArgumentNullException(nameof(parquetOptions));
    }

    public async Task<string> StoreItemsAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName,
        bool overwrite = true) where T : IParquetSerializable<T>, new()
    {
        items.ThrowIfNullOrEmpty();
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var itemsList = items.ToList();

        if (_azureOptions.EnableDetailedLogging)
        {
            _logger.LogDebug("Storing {Count} items as Parquet file to {FilePath} in file system {FileSystem}",
                itemsList.Count, filePath, fileSystemName);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        using var stream = await _parquetFormatter.SerializeItemsAsync(itemsList);
        stream.Position = 0;

        await fileClient.UploadAsync(stream, overwrite);

        // Check file size after upload
        if (_parquetOptions.EnableFileSizeMonitoring)
        {
            await CheckFileSizeAsync(filePath, fileSystemName);
        }

        _logger.LogInformation("Successfully stored {Count} items as Parquet file to {FilePath}",
            itemsList.Count, filePath);

        return filePath;
    }

    public async Task<IEnumerable<T>> ReadItemsAsync<T>(
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new()
    {
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        if (_azureOptions.EnableDetailedLogging)
        {
            _logger.LogDebug("Reading Parquet file from {FilePath} in file system {FileSystem}",
                filePath, fileSystemName);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        if (!await fileClient.ExistsAsync())
        {
            _logger.LogWarning("Parquet file not found: {FilePath}", filePath);
            return Enumerable.Empty<T>();
        }

        using var stream = new MemoryStream();
        await fileClient.ReadToAsync(stream);
        stream.Position = 0;

        var items = await _parquetFormatter.DeserializeItemsAsync<T>(stream);
        var itemsList = items.ToList();

        _logger.LogInformation("Successfully read {Count} items from Parquet file {FilePath}",
            itemsList.Count, filePath);

        return itemsList;
    }

    public async Task<string> UpdateFileAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new()
    {
        // Update is the same as Store with overwrite = true
        return await StoreItemsAsync(items, filePath, fileSystemName, overwrite: true);
    }

    public async Task<string> AppendItemsAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new()
    {
        items.ThrowIfNullOrEmpty();
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var newItems = items.ToList();

        if (_azureOptions.EnableDetailedLogging)
        {
            _logger.LogDebug("Appending {Count} items to Parquet file {FilePath}",
                newItems.Count, filePath);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        List<T> allItems;

        // Check if file exists and read existing data
        if (await fileClient.ExistsAsync())
        {
            var existingItems = await ReadItemsAsync<T>(filePath, fileSystemName);
            var existingList = existingItems.ToList();
            allItems = existingList.Concat(newItems).ToList();

            _logger.LogInformation("Merging {NewCount} new items with {ExistingCount} existing items",
                newItems.Count, existingList.Count);
        }
        else
        {
            allItems = newItems;
            _logger.LogInformation("Creating new Parquet file with {Count} items", newItems.Count);
        }

        // Write combined data
        await UpdateFileAsync(allItems, filePath, fileSystemName);

        return filePath;
    }

    public async Task<long> GetFileSizeAsync(
        string filePath,
        string fileSystemName)
    {
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        try
        {
            var properties = await fileClient.GetPropertiesAsync();
            return properties.Value.ContentLength;
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound" || ex.ErrorCode == "BlobNotFound")
        {
            return 0;
        }
    }

    public async Task<bool> IsFileSizeWarningAsync(
        string filePath,
        string fileSystemName)
    {
        var fileSize = await GetFileSizeAsync(filePath, fileSystemName);
        var warningThreshold = (long)(_parquetOptions.MaxFileSize * _parquetOptions.FileSizeWarningThreshold);

        return fileSize >= warningThreshold;
    }

    public async Task<bool> FileExistsAsync(
        string filePath,
        string fileSystemName)
    {
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        return await fileClient.ExistsAsync();
    }

    private async Task CheckFileSizeAsync(string filePath, string fileSystemName)
    {
        var fileSize = await GetFileSizeAsync(filePath, fileSystemName);
        var fileSizeMB = fileSize / 1_048_576.0; // Convert to MB

        var warningThreshold = (long)(_parquetOptions.MaxFileSize * _parquetOptions.FileSizeWarningThreshold);

        if (fileSize >= _parquetOptions.MaxFileSize)
        {
            _logger.LogWarning(
                "Parquet file {FilePath} size ({SizeMB:F2} MB) exceeds maximum recommended size ({MaxSizeMB:F2} MB). " +
                "Consider implementing file rotation.",
                filePath, fileSizeMB, _parquetOptions.MaxFileSize / 1_048_576.0);
        }
        else if (fileSize >= warningThreshold)
        {
            _logger.LogWarning(
                "Parquet file {FilePath} size ({SizeMB:F2} MB) approaching maximum size ({MaxSizeMB:F2} MB). " +
                "Warning threshold: {ThresholdPercent:P0}",
                filePath, fileSizeMB, _parquetOptions.MaxFileSize / 1_048_576.0,
                _parquetOptions.FileSizeWarningThreshold);
        }
        else if (_azureOptions.EnableDetailedLogging)
        {
            _logger.LogDebug("Parquet file {FilePath} size: {SizeMB:F2} MB", filePath, fileSizeMB);
        }
    }
}
