using LakeIO.Annotations;
using LakeIO.Configuration;
using LakeIO.Formatters;
using LakeIO.Formatters.Interfaces;
using LakeIO.Validation;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LakeIO.Services;

/// <summary>
/// Implementation of buffer storage operations for Azure Data Lake Storage.
/// Manages the Hybrid Buffer + Parquet strategy for high-throughput writes.
/// </summary>
public class BufferStorageService : IBufferStorageService
{
    private readonly ILogger<BufferStorageService> _logger;
    private readonly IDataLakeClientManager _clientManager;
    private readonly IJsonStorageService _jsonStorage;
    private readonly IParquetStorageService _parquetStorage;
    private readonly IFileOperationsService _fileOperations;
    private readonly LakeOptions _azureOptions;
    private readonly BufferOptions _bufferOptions;

    public BufferStorageService(
        ILogger<BufferStorageService> logger,
        IDataLakeClientManager clientManager,
        IJsonStorageService jsonStorage,
        IParquetStorageService parquetStorage,
        IFileOperationsService fileOperations,
        IOptions<LakeOptions> azureOptions,
        IOptions<BufferOptions> bufferOptions)
    {
        _logger = logger.ThrowIfNull();
        _clientManager = clientManager.ThrowIfNull();
        _jsonStorage = jsonStorage.ThrowIfNull();
        _parquetStorage = parquetStorage.ThrowIfNull();
        _fileOperations = fileOperations.ThrowIfNull();
        _azureOptions = azureOptions?.Value.ThrowIfNull() ?? throw new ArgumentNullException(nameof(azureOptions));
        _bufferOptions = bufferOptions?.Value.ThrowIfNull() ?? throw new ArgumentNullException(nameof(bufferOptions));
    }

    public async Task<string> CompactBufferToParquetAsync<T>(
        string bufferFilePath,
        string parquetFilePath,
        string fileSystemName,
        bool deleteBuffer = true) where T : IParquetSerializable<T>, new()
    {
        bufferFilePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        parquetFilePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        if (_azureOptions.EnableDetailedLogging)
        {
            _logger.LogDebug("Compacting buffer {BufferPath} to Parquet {ParquetPath}",
                bufferFilePath, parquetFilePath);
        }

        // Generate temporary path with timestamp for atomic isolation
        var timestamp = DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmssfff");
        var tempBufferPath = $"{bufferFilePath}{_bufferOptions.CompactingSuffix}.{timestamp}";

        try
        {
            // Step 1: Atomically rename buffer to temp file
            // This prevents race condition - new appends will create a fresh buffer
            await _fileOperations.RenameFileAsync(bufferFilePath, tempBufferPath, fileSystemName);

            _logger.LogInformation("Renamed buffer {BufferPath} to {TempPath} for safe compaction",
                bufferFilePath, tempBufferPath);
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
        {
            // Buffer doesn't exist or already being compacted - this is OK, skip
            _logger.LogWarning("Buffer file {BufferPath} not found, already compacted or doesn't exist",
                bufferFilePath);
            return parquetFilePath;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to rename buffer {BufferPath} for compaction", bufferFilePath);
            throw;
        }

        // Step 2: Read from temporary file (new appends are going to fresh buffer)
        try
        {
            var bufferItems = await _jsonStorage.ReadJsonLinesAsync<T>(tempBufferPath, fileSystemName);
            var bufferList = bufferItems.ToList();

            if (bufferList.Count == 0)
            {
                _logger.LogWarning("Buffer file {TempPath} is empty, skipping compaction", tempBufferPath);

                if (deleteBuffer)
                {
                    await DeleteBufferAsync(tempBufferPath, fileSystemName);
                }

                return parquetFilePath;
            }

            _logger.LogInformation("Read {Count} items from temp buffer {TempPath}",
                bufferList.Count, tempBufferPath);

            // Step 3: Merge with existing Parquet file if it exists
            if (await _parquetStorage.FileExistsAsync(parquetFilePath, fileSystemName))
            {
                var existingItems = await _parquetStorage.ReadItemsAsync<T>(parquetFilePath, fileSystemName);
                var existingList = existingItems.ToList();

                var allItems = existingList.Concat(bufferList).ToList();

                _logger.LogInformation(
                    "Merging {BufferCount} buffer items with {ExistingCount} existing Parquet items for total of {TotalCount}",
                    bufferList.Count, existingList.Count, allItems.Count);

                await _parquetStorage.UpdateFileAsync(allItems, parquetFilePath, fileSystemName);
            }
            else
            {
                _logger.LogInformation("Creating new Parquet file with {Count} items from buffer",
                    bufferList.Count);

                await _parquetStorage.StoreItemsAsync(bufferList, parquetFilePath, fileSystemName);
            }

            // Step 4: Delete temporary buffer after successful compaction
            if (deleteBuffer && _bufferOptions.DeleteBufferAfterCompaction)
            {
                await DeleteBufferAsync(tempBufferPath, fileSystemName);
                _logger.LogInformation("Deleted temp buffer file {TempPath} after successful compaction",
                    tempBufferPath);
            }

            _logger.LogInformation("Successfully compacted buffer {BufferPath} to Parquet {ParquetPath}",
                bufferFilePath, parquetFilePath);

            return parquetFilePath;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to compact buffer {TempPath}", tempBufferPath);

            // On failure, optionally rename temp file with .failed suffix for investigation
            if (_bufferOptions.RetryFailedCompactions)
            {
                try
                {
                    var failedPath = $"{bufferFilePath}{_bufferOptions.FailedCompactionSuffix}.{timestamp}";
                    await _fileOperations.RenameFileAsync(tempBufferPath, failedPath, fileSystemName);
                    _logger.LogWarning("Renamed failed buffer to {FailedPath} for later retry", failedPath);
                }
                catch (Exception renameEx)
                {
                    _logger.LogError(renameEx, "Failed to rename temp buffer {TempPath} to failed state", tempBufferPath);
                }
            }

            throw;
        }
    }

    public async Task<List<string>> ListBufferFilesAsync(
        string directoryPath,
        string fileSystemName,
        bool recursive = true)
    {
        directoryPath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        if (_azureOptions.EnableDetailedLogging)
        {
            _logger.LogDebug("Listing buffer files in {DirectoryPath}, recursive: {Recursive}",
                directoryPath, recursive);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var bufferFiles = new List<string>();

        try
        {
            await foreach (var pathItem in fileSystemClient.GetPathsAsync(directoryPath, recursive))
            {
                if (pathItem.IsDirectory == false && IsBufferFile(pathItem.Name))
                {
                    bufferFiles.Add(pathItem.Name);
                }
            }
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
        {
            _logger.LogDebug("Directory does not exist, returning empty list: {DirectoryPath}", directoryPath);
            return bufferFiles;
        }

        _logger.LogInformation("Found {Count} buffer files in {DirectoryPath}",
            bufferFiles.Count, directoryPath);

        return bufferFiles;
    }

    public async Task<long> GetBufferSizeAsync(
        string bufferFilePath,
        string fileSystemName)
    {
        bufferFilePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(bufferFilePath);

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

    public async Task<bool> ShouldCompactAsync(
        string bufferFilePath,
        string fileSystemName)
    {
        if (!_bufferOptions.AutoCompactOnSizeThreshold)
        {
            return false;
        }

        var bufferSize = await GetBufferSizeAsync(bufferFilePath, fileSystemName);
        var shouldCompact = bufferSize >= _bufferOptions.BufferSizeThreshold;

        if (shouldCompact && _azureOptions.EnableDetailedLogging)
        {
            _logger.LogDebug(
                "Buffer {BufferPath} size ({SizeMB:F2} MB) exceeds threshold ({ThresholdMB:F2} MB), should compact",
                bufferFilePath, bufferSize / 1_048_576.0, _bufferOptions.BufferSizeThreshold / 1_048_576.0);
        }

        return shouldCompact;
    }

    public async Task DeleteBufferAsync(
        string bufferFilePath,
        string fileSystemName)
    {
        bufferFilePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(bufferFilePath);

        try
        {
            await fileClient.DeleteAsync();

            if (_azureOptions.EnableDetailedLogging)
            {
                _logger.LogDebug("Deleted buffer file {BufferPath}", bufferFilePath);
            }
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound" || ex.ErrorCode == "BlobNotFound")
        {
            _logger.LogWarning("Buffer file {BufferPath} not found, already deleted", bufferFilePath);
        }
    }

    public string GetBufferPathFromParquetPath(string parquetFilePath)
    {
        parquetFilePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();

        if (parquetFilePath.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
        {
            return parquetFilePath.Substring(0, parquetFilePath.Length - 8) + _bufferOptions.BufferFileSuffix;
        }

        return parquetFilePath + _bufferOptions.BufferFileSuffix;
    }

    public string GetParquetPathFromBufferPath(string bufferFilePath)
    {
        bufferFilePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();

        if (bufferFilePath.EndsWith(_bufferOptions.BufferFileSuffix, StringComparison.OrdinalIgnoreCase))
        {
            return bufferFilePath.Substring(0, bufferFilePath.Length - _bufferOptions.BufferFileSuffix.Length) + ".parquet";
        }

        throw new ArgumentException(
            $"Buffer file path must end with '{_bufferOptions.BufferFileSuffix}'",
            nameof(bufferFilePath));
    }

    public bool IsBufferFile(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            return false;

        return filePath.EndsWith(_bufferOptions.BufferFileSuffix, StringComparison.OrdinalIgnoreCase);
    }
}
