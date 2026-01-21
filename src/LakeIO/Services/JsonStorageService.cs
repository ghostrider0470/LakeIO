using System.Text.Json;
using LakeIO.Configuration;
using LakeIO.Formatters.Interfaces;
using LakeIO.Serialization;
using LakeIO.Validation;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LakeIO.Services;

/// <summary>
/// Implementation of JSON storage operations for Azure Data Lake Storage.
/// Provides high-performance JSON file operations with streaming support.
/// </summary>
public class JsonStorageService : IJsonStorageService
{
    private readonly ILogger<JsonStorageService> _logger;
    private readonly IDataLakeClientManager _clientManager;
    private readonly IFileFormatter _jsonFormatter;
    private readonly LakeOptions _options;

    public JsonStorageService(
        ILogger<JsonStorageService> logger,
        IDataLakeClientManager clientManager,
        IFileFormatter jsonFormatter,
        IOptions<LakeOptions> options)
    {
        _logger = logger.ThrowIfNull();
        _clientManager = clientManager.ThrowIfNull();
        _jsonFormatter = jsonFormatter.ThrowIfNull();
        _options = options?.Value.ThrowIfNull() ?? throw new ArgumentNullException(nameof(options));
    }

    public async Task<string> StoreItemAsync<T>(
        T item,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        JsonSerializerOptions? jsonOptions = null,
        bool overwrite = true)
    {
        if (item == null)
            throw new ArgumentNullException(nameof(item));

        directoryPath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        fileName ??= $"{Guid.NewGuid()}.json";
        var fullPath = $"{directoryPath.TrimEnd('/')}/{fileName}";

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Storing JSON item to {FilePath} in file system {FileSystem}",
                fullPath, fileSystemName);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(fullPath);

        using var stream = await _jsonFormatter.SerializeAsync(item);
        stream.Position = 0;

        await fileClient.UploadAsync(stream, overwrite);

        _logger.LogInformation("Successfully stored JSON item to {FilePath}", fullPath);

        return fullPath;
    }

    public async Task<List<string>> StoreItemsAsync<T>(
        IEnumerable<T> items,
        string directoryPath,
        string fileSystemName,
        JsonSerializerOptions? jsonOptions = null,
        bool overwrite = true)
    {
        items.ThrowIfNullOrEmpty();
        directoryPath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var itemsList = items.ToList();
        var paths = new List<string>();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Storing {Count} JSON items to {DirectoryPath} in file system {FileSystem}",
                itemsList.Count, directoryPath, fileSystemName);
        }

        foreach (var item in itemsList)
        {
            var path = await StoreItemAsync(item, directoryPath, fileSystemName, null, jsonOptions, overwrite);
            paths.Add(path);
        }

        _logger.LogInformation("Successfully stored {Count} JSON items to {DirectoryPath}",
            itemsList.Count, directoryPath);

        return paths;
    }

    public async Task<T?> ReadItemAsync<T>(
        string filePath,
        string fileSystemName,
        JsonSerializerOptions? jsonOptions = null)
    {
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Reading JSON item from {FilePath} in file system {FileSystem}",
                filePath, fileSystemName);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        if (!await fileClient.ExistsAsync())
        {
            _logger.LogWarning("File not found: {FilePath}", filePath);
            return default;
        }

        using var stream = new MemoryStream();
        await fileClient.ReadToAsync(stream);
        stream.Position = 0;

        var item = await _jsonFormatter.DeserializeAsync<T>(stream);

        _logger.LogInformation("Successfully read JSON item from {FilePath}", filePath);

        return item;
    }

    public async Task<IEnumerable<T>> ReadItemsAsync<T>(
        string directoryPath,
        string fileSystemName,
        string searchPattern = "*.json",
        JsonSerializerOptions? jsonOptions = null)
    {
        directoryPath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Reading JSON items from {DirectoryPath} with pattern {SearchPattern}",
                directoryPath, searchPattern);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var items = new List<T>();

        try
        {
            await foreach (var pathItem in fileSystemClient.GetPathsAsync(directoryPath))
            {
                if (pathItem.IsDirectory == false && MatchesPattern(pathItem.Name, searchPattern))
                {
                    var item = await ReadItemAsync<T>(pathItem.Name, fileSystemName, jsonOptions);
                    if (item != null)
                    {
                        items.Add(item);
                    }
                }
            }
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
        {
            _logger.LogDebug("Directory does not exist, returning empty list: {DirectoryPath}", directoryPath);
            return items;
        }

        _logger.LogInformation("Successfully read {Count} JSON items from {DirectoryPath}",
            items.Count, directoryPath);

        return items;
    }

    public async Task AppendJsonLineAsync<T>(
        T item,
        string filePath,
        string fileSystemName)
    {
        if (item == null)
            throw new ArgumentNullException(nameof(item));

        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Appending JSON line to {FilePath} in file system {FileSystem}",
                filePath, fileSystemName);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Serialize item to JSON line using default options
        var serializerOptions = JsonSerializerOptionsExtensions.CreateDefaultOptions();
        var json = JsonSerializer.Serialize(item, serializerOptions);
        using var memoryStream = new MemoryStream();
        using var writer = new StreamWriter(memoryStream, System.Text.Encoding.UTF8, leaveOpen: true);
        await writer.WriteLineAsync(json);
        await writer.FlushAsync();
        memoryStream.Position = 0;

        // Get current file size for offset
        long offset = 0;
        try
        {
            var properties = await fileClient.GetPropertiesAsync();
            offset = properties.Value.ContentLength;
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound" || ex.ErrorCode == "BlobNotFound")
        {
            await fileClient.CreateAsync();
            offset = 0;
        }

        // Append data using native Azure append operation
        await fileClient.AppendAsync(memoryStream, offset);
        await fileClient.FlushAsync(offset + memoryStream.Length);

        _logger.LogInformation("Successfully appended JSON line to {FilePath} at offset {Offset}",
            filePath, offset);
    }

    public async Task AppendJsonLinesAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName)
    {
        items.ThrowIfNullOrEmpty();
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var itemsList = items.ToList();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Appending {Count} JSON lines to {FilePath} in file system {FileSystem}",
                itemsList.Count, filePath, fileSystemName);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Serialize items to NDJSON format using default options
        var serializerOptions = JsonSerializerOptionsExtensions.CreateDefaultOptions();
        using var memoryStream = new MemoryStream();
        using var writer = new StreamWriter(memoryStream, System.Text.Encoding.UTF8, leaveOpen: true);

        foreach (var item in itemsList)
        {
            var json = JsonSerializer.Serialize(item, serializerOptions);
            await writer.WriteLineAsync(json);
        }

        await writer.FlushAsync();
        memoryStream.Position = 0;

        // Get current file size for offset
        long offset = 0;
        try
        {
            var properties = await fileClient.GetPropertiesAsync();
            offset = properties.Value.ContentLength;
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound" || ex.ErrorCode == "BlobNotFound")
        {
            await fileClient.CreateAsync();
            offset = 0;
        }

        // Append data using native Azure append operation
        await fileClient.AppendAsync(memoryStream, offset);
        await fileClient.FlushAsync(offset + memoryStream.Length);

        _logger.LogInformation("Successfully appended {Count} JSON lines to {FilePath} at offset {Offset}",
            itemsList.Count, filePath, offset);
    }

    public async Task<IEnumerable<T>> ReadJsonLinesAsync<T>(
        string filePath,
        string fileSystemName)
    {
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Reading JSON lines from {FilePath} in file system {FileSystem}",
                filePath, fileSystemName);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        if (!await fileClient.ExistsAsync())
        {
            _logger.LogWarning("File not found: {FilePath}", filePath);
            return Enumerable.Empty<T>();
        }

        using var stream = new MemoryStream();
        await fileClient.ReadToAsync(stream);
        stream.Position = 0;

        var items = new List<T>();
        var serializerOptions = JsonSerializerOptionsExtensions.CreateDefaultOptions();
        using var reader = new StreamReader(stream);

        while (!reader.EndOfStream)
        {
            var line = await reader.ReadLineAsync();
            if (!string.IsNullOrWhiteSpace(line))
            {
                var item = JsonSerializer.Deserialize<T>(line, serializerOptions);
                if (item != null)
                {
                    items.Add(item);
                }
            }
        }

        _logger.LogInformation("Successfully read {Count} JSON lines from {FilePath}",
            items.Count, filePath);

        return items;
    }

    private static bool MatchesPattern(string fileName, string pattern)
    {
        // Simple pattern matching (supports * wildcard)
        if (pattern == "*" || pattern == "*.*")
            return true;

        if (pattern.StartsWith("*."))
        {
            var extension = pattern[1..]; // Remove the *
            return fileName.EndsWith(extension, StringComparison.OrdinalIgnoreCase);
        }

        return fileName.Equals(pattern, StringComparison.OrdinalIgnoreCase);
    }
}
