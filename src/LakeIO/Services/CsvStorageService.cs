using LakeIO.Configuration;
using LakeIO.Formatters.Interfaces;
using LakeIO.Validation;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LakeIO.Services;

/// <summary>
/// Implementation of CSV storage operations for Azure Data Lake Storage.
/// Provides CSV file operations with column mapping support.
/// </summary>
public class CsvStorageService : ICsvStorageService
{
    private readonly ILogger<CsvStorageService> _logger;
    private readonly IDataLakeClientManager _clientManager;
    private readonly ICsvFileFormatter _csvFormatter;
    private readonly LakeOptions _options;

    public CsvStorageService(
        ILogger<CsvStorageService> logger,
        IDataLakeClientManager clientManager,
        ICsvFileFormatter csvFormatter,
        IOptions<LakeOptions> options)
    {
        _logger = logger.ThrowIfNull();
        _clientManager = clientManager.ThrowIfNull();
        _csvFormatter = csvFormatter.ThrowIfNull();
        _options = options?.Value.ThrowIfNull() ?? throw new ArgumentNullException(nameof(options));
    }

    public async Task<string> StoreItemsAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName,
        Dictionary<string, string>? columnMapping = null,
        bool overwrite = true)
    {
        items.ThrowIfNullOrEmpty();
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var itemsList = items.ToList();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Storing {Count} items as CSV file to {FilePath} in file system {FileSystem}",
                itemsList.Count, filePath, fileSystemName);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        using var stream = await _csvFormatter.SerializeItemsAsync(itemsList, columnMapping);
        stream.Position = 0;

        await fileClient.UploadAsync(stream, overwrite);

        _logger.LogInformation("Successfully stored {Count} items as CSV file to {FilePath}",
            itemsList.Count, filePath);

        return filePath;
    }

    public async Task<IEnumerable<T>> ReadItemsAsync<T>(
        string filePath,
        string fileSystemName,
        Dictionary<string, string>? columnMapping = null)
    {
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Reading CSV file from {FilePath} in file system {FileSystem}",
                filePath, fileSystemName);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        if (!await fileClient.ExistsAsync())
        {
            _logger.LogWarning("CSV file not found: {FilePath}", filePath);
            return Enumerable.Empty<T>();
        }

        using var stream = new MemoryStream();
        await fileClient.ReadToAsync(stream);
        stream.Position = 0;

        var items = await _csvFormatter.DeserializeItemsAsync<T>(stream);
        var itemsList = items.ToList();

        _logger.LogInformation("Successfully read {Count} items from CSV file {FilePath}",
            itemsList.Count, filePath);

        return itemsList;
    }

    public async Task<string> AppendItemsAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName,
        Dictionary<string, string>? columnMapping = null)
    {
        items.ThrowIfNullOrEmpty();
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var newItems = items.ToList();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Appending {Count} items to CSV file {FilePath}",
                newItems.Count, filePath);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        List<T> allItems;

        // Check if file exists and read existing data
        if (await fileClient.ExistsAsync())
        {
            var existingItems = await ReadItemsAsync<T>(filePath, fileSystemName, columnMapping);
            var existingList = existingItems.ToList();
            allItems = existingList.Concat(newItems).ToList();

            _logger.LogInformation("Merging {NewCount} new items with {ExistingCount} existing items",
                newItems.Count, existingList.Count);
        }
        else
        {
            allItems = newItems;
            _logger.LogInformation("Creating new CSV file with {Count} items", newItems.Count);
        }

        // Write combined data
        await StoreItemsAsync(allItems, filePath, fileSystemName, columnMapping, overwrite: true);

        return filePath;
    }
}
