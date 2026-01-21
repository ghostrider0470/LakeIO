using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Configuration;
using Parquet;
using Parquet.Data;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Parquet.Schema;
using Array = System.Array;
using LakeIO.Annotations;
using LakeIO.Formatters;
using LakeIO.Formatters.Interfaces;
using LakeIO.Formatters.Json;
using LakeIO.Formatters.Parquet;
using LakeIO.Formatters.Csv;

namespace LakeIO;

/// <summary>
/// Provides methods to interact with Azure Data Lake Storage, supporting JSON, Parquet, and CSV file formats.
/// This class implements the Facade pattern, providing a simplified interface to the underlying specialized services.
/// Thread-safe client caching ensures optimal performance for concurrent operations.
///
/// DEPRECATION NOTE: This class is maintained for backward compatibility. New code should use the specialized
/// service interfaces (IJsonStorageService, IParquetStorageService, ICsvStorageService, IBufferStorageService)
/// registered via ServiceCollectionExtensions for better testability, modularity, and maintainability.
/// </summary>
public class LakeContext : ILakeContext, IDisposable
{
    private bool _disposed = false;
    private readonly IConfiguration _configuration;
    private const int DefaultBufferSize = 81920; // 80KB buffer size for uploading
    private readonly ConcurrentDictionary<string, DataLakeFileSystemClient> _fileSystemClients = new();
    private readonly ConcurrentDictionary<string, DataLakeServiceClient> _serviceClients = new();
    private readonly ILogger<LakeContext> _logger;
    private readonly IFileFormatter _jsonFormatter;
    private readonly IParquetFileFormatter _parquetFormatter;
    private readonly ICsvFileFormatter _csvFormatter;

    /// <summary>
    ///     Initializes a new instance of the <see cref="LakeContext" /> class.
    /// </summary>
    /// <param name="configuration">The configuration containing the Data Lake connection string.</param>
    /// <param name="logger">The logger instance.</param>
    /// <param name="jsonFormatter">Optional. The JSON formatter to use. If not provided, a default one will be created.</param>
    /// <param name="parquetFormatter">Optional. The Parquet formatter to use. If not provided, a default one will be created.</param>
    /// <param name="csvFormatter">Optional. The CSV formatter to use. If not provided, a default one will be created.</param>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the Data Lake connection string is not found in the configuration.</exception>
    public LakeContext(
        IConfiguration configuration, 
        ILogger<LakeContext> logger,
        IFileFormatter? jsonFormatter = null,
        IParquetFileFormatter? parquetFormatter = null,
        ICsvFileFormatter? csvFormatter = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

        // Try to get connection string from configuration
        var connectionString = _configuration["DataLakeConnectionString"] ??
                               _configuration["DataLake:ConnectionString"];

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "Data Lake connection string not found. Please set 'DataLakeConnectionString' or 'DataLake:ConnectionString' in the configuration.");
        }

        // Initialize the default service client
        GetOrCreateServiceClient(connectionString);

        // Initialize formatters
        _jsonFormatter = jsonFormatter ?? new SystemTextJsonFormatter();
        _parquetFormatter = parquetFormatter ?? new ParquetFileFormatter();
        _csvFormatter = csvFormatter ?? new CsvFileFormatter();
    }

    /// <summary>
    ///     Gets or creates a DataLakeServiceClient for the specified connection string.
    /// </summary>
    /// <param name="connectionString">The connection string to the storage account.</param>
    /// <returns>A <see cref="DataLakeServiceClient" /> instance.</returns>
    public DataLakeServiceClient GetOrCreateServiceClient(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));
        }

        return _serviceClients.GetOrAdd(connectionString, cs => new DataLakeServiceClient(cs));
    }

    /// <summary>
    ///     Gets or creates a DataLakeFileSystemClient for the specified file system.
    /// </summary>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="connectionString">
    ///     Optional. The connection string to use. If not provided, uses the default connection
    ///     string.
    /// </param>
    /// <returns>A <see cref="DataLakeFileSystemClient" /> instance.</returns>
    public DataLakeFileSystemClient GetOrCreateFileSystemClient(
        string fileSystemName,
        string? connectionString = null)
    {
        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var cacheKey = $"{connectionString ?? "default"}:{fileSystemName}";

        var fileSystemClient = _fileSystemClients.GetOrAdd(cacheKey, _ =>
        {
            var serviceClient = !string.IsNullOrEmpty(connectionString)
                ? GetOrCreateServiceClient(connectionString)
                : _serviceClients.Values.First();

            return serviceClient.GetFileSystemClient(fileSystemName);
        });

        return fileSystemClient;
    }

    

    /// <inheritdoc />
    public async Task<string> StoreItemAsJson<T>(
        T item,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        JsonSerializerOptions? jsonOptions = null,
        bool overwrite = true)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        if (string.IsNullOrWhiteSpace(directoryPath))
        {
            throw new ArgumentException("Directory path cannot be null or empty.", nameof(directoryPath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }


        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);
        await directoryClient.CreateIfNotExistsAsync();

        fileName ??= $"{Guid.NewGuid()}.json";
        if (!fileName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
        {
            fileName += ".json";
        }

        var fileClient = directoryClient.GetFileClient(fileName);

        // Use the JSON formatter to serialize the item
        using var stream = await _jsonFormatter.SerializeAsync(item);
        await fileClient.UploadAsync(stream, overwrite);

        _logger.LogInformation("Successfully stored JSON file at {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);
            
        return fileClient.Path;
    }
    
    /// <inheritdoc />
    public async Task<string> UpdateJsonFile<T>(
        T item,
        string filePath,
        string fileSystemName,
        JsonSerializerOptions? jsonOptions = null)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }


        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Check if file exists
        if (!await fileClient.ExistsAsync())
        {
            throw new FileNotFoundException($"The file {filePath} was not found in the file system {fileSystemName}.");
        }

        // Use the JSON formatter to serialize the item
        using var stream = await _jsonFormatter.SerializeAsync(item);
        await fileClient.UploadAsync(stream, overwrite: true);
        
        _logger.LogInformation("Successfully updated JSON file at {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);

        return fileClient.Path;
    }
    
    /// <summary>
    /// Disposes the resources used by the LakeContext.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
    
    /// <summary>
    /// Disposes the resources used by the LakeContext.
    /// </summary>
    /// <param name="disposing">True to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                _serviceClients.Clear();
                _fileSystemClients.Clear();
            }
            
            _disposed = true;
        }
    }
    
    /// <summary>
    /// Stores an item as a Parquet file in Azure Data Lake Storage.
    /// </summary>
    /// <typeparam name="T">The type of the item to store, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
    /// <param name="item">The item to store.</param>
    /// <param name="directoryPath">The directory path where the file will be stored.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="fileName">Optional. The name of the file. If not provided, a GUID will be used.</param>
    /// <param name="overwrite">Whether to overwrite the file if it already exists.</param>
    /// <returns>The path to the stored file.</returns>
    public async Task<string> StoreItemAsParquet<T>(
        T item,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        bool overwrite = true) where T : IParquetSerializable<T>
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        if (string.IsNullOrWhiteSpace(directoryPath))
        {
            throw new ArgumentException("Directory path cannot be null or empty.", nameof(directoryPath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);
        await directoryClient.CreateIfNotExistsAsync();

        fileName ??= $"{Guid.NewGuid()}.parquet";
        if (!fileName.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
        {
            fileName += ".parquet";
        }

        var fileClient = directoryClient.GetFileClient(fileName);

        // Use the Parquet formatter to serialize the item
        using var stream = await _parquetFormatter.SerializeAsync(item);
        
        // Get the length of the stream for proper upload
        var contentLength = stream.Length;
        stream.Position = 0;
        
        try
        {
            if (overwrite)
            {
                // Try to delete if exists, but don't fail if it doesn't
                try
                {
                    if (await fileClient.ExistsAsync())
                    {
                        await fileClient.DeleteAsync();
                    }
                }
                catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
                {
                    // File doesn't exist, which is fine for our purposes
                    _logger.LogDebug("File {Path} didn't exist when trying to delete it, continuing with upload", 
                        fileClient.Path);
                }
            }
            
            // Upload the entire stream at once
            await fileClient.UploadAsync(stream, overwrite: false);
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathAlreadyExists" && overwrite)
        {
            // If the path already exists and we want to overwrite, try the delete-then-upload approach again
            _logger.LogDebug("Path {Path} already exists, retrying with explicit delete-then-create approach", 
                fileClient.Path);
                
            try
            {
                // Reset stream position
                stream.Position = 0;
                
                // Delete the file explicitly
                await fileClient.DeleteAsync();
                
                // Wait a short time to ensure deletion is processed
                await Task.Delay(100);
                
                // Upload again
                await fileClient.UploadAsync(stream, overwrite: false);
            }
            catch (Exception retryEx)
            {
                throw new InvalidOperationException($"Failed to upload Parquet file after retry: {retryEx.Message}", retryEx);
            }
        }

        _logger.LogInformation("Successfully stored Parquet file at {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);
            
        return fileClient.Path;
    }
    
    /// <summary>
    /// Stores a collection of items as a Parquet file in Azure Data Lake Storage.
    /// </summary>
    /// <typeparam name="T">The type of the items to store, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
    /// <param name="items">The collection of items to store.</param>
    /// <param name="directoryPath">The directory path where the file will be stored.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="fileName">Optional. The name of the file. If not provided, a GUID will be used.</param>
    /// <param name="overwrite">Whether to overwrite the file if it already exists.</param>
    /// <returns>The path to the stored file.</returns>
    public async Task<string> StoreItemsAsParquet<T>(
        IEnumerable<T> items,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        bool overwrite = true) where T : IParquetSerializable<T>
    {
        if (items == null)
        {
            throw new ArgumentNullException(nameof(items));
        }

        if (!items.Any())
        {
            throw new ArgumentException("At least one item is required", nameof(items));
        }

        if (string.IsNullOrWhiteSpace(directoryPath))
        {
            throw new ArgumentException("Directory path cannot be null or empty.", nameof(directoryPath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);
        await directoryClient.CreateIfNotExistsAsync();

        fileName ??= $"{Guid.NewGuid()}.parquet";
        if (!fileName.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase))
        {
            fileName += ".parquet";
        }

        var fileClient = directoryClient.GetFileClient(fileName);

        // Use the Parquet formatter to serialize the items
        using var stream = await _parquetFormatter.SerializeItemsAsync(items);
        
        // Get the length of the stream for proper upload
        var contentLength = stream.Length;
        stream.Position = 0;
        
        // Validate that we have a valid Parquet file
        if (contentLength < 12) // Minimum Parquet file size
        {
            throw new InvalidOperationException($"Serialized Parquet data is too small ({contentLength} bytes). Minimum valid Parquet file is 12 bytes.");
        }
        
        try
        {
            if (overwrite)
            {
                // Try to delete if exists, but don't fail if it doesn't
                try
                {
                    if (await fileClient.ExistsAsync())
                    {
                        await fileClient.DeleteAsync();
                    }
                }
                catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
                {
                    // File doesn't exist, which is fine for our purposes
                    _logger.LogDebug("File {Path} didn't exist when trying to delete it, continuing with upload", 
                        fileClient.Path);
                }
            }
            
            // Upload the entire stream at once
            await fileClient.UploadAsync(stream, overwrite: false);
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathAlreadyExists" && overwrite)
        {
            // If the path already exists and we want to overwrite, try the delete-then-upload approach again
            _logger.LogDebug("Path {Path} already exists, retrying with explicit delete-then-create approach", 
                fileClient.Path);
                
            try
            {
                // Reset stream position
                stream.Position = 0;
                
                // Delete the file explicitly
                await fileClient.DeleteAsync();
                
                // Wait a short time to ensure deletion is processed
                await Task.Delay(100);
                
                // Upload again
                await fileClient.UploadAsync(stream, overwrite: false);
            }
            catch (Exception retryEx)
            {
                throw new InvalidOperationException($"Failed to upload Parquet file after retry: {retryEx.Message}", retryEx);
            }
        }

        _logger.LogInformation("Successfully stored Parquet file with multiple items at {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);
            
        return fileClient.Path;
    }
    
    /// <summary>
    /// Updates an existing Parquet file in Azure Data Lake Storage with new content.
    /// </summary>
    /// <typeparam name="T">The type of the item to update with, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
    /// <param name="item">The item containing the updated data.</param>
    /// <param name="filePath">The path to the existing file to update.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>The path to the updated file.</returns>
    public async Task<string> UpdateParquetFile<T>(
        T item,
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Check if file exists
        if (!await fileClient.ExistsAsync())
        {
            throw new FileNotFoundException($"The file {filePath} was not found in the file system {fileSystemName}.");
        }

        // Use the Parquet formatter to serialize the item
        using var stream = await _parquetFormatter.SerializeAsync(item);
        
        // Get the length of the stream for proper upload
        var contentLength = stream.Length;
        stream.Position = 0;
        
        try
        {
            // Try to delete if exists, but don't fail if it doesn't
            try
            {
                if (await fileClient.ExistsAsync())
                {
                    await fileClient.DeleteAsync();
                }
            }
            catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
            {
                // File doesn't exist, which is fine for our purposes
                _logger.LogDebug("File {Path} didn't exist when trying to delete it, continuing with upload", 
                    fileClient.Path);
            }
            
            // Upload the entire stream at once
            await fileClient.UploadAsync(stream, overwrite: false);
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathAlreadyExists")
        {
            // If the path already exists, try the delete-then-upload approach again
            _logger.LogDebug("Path {Path} already exists, retrying with explicit delete-then-create approach", 
                fileClient.Path);
                
            try
            {
                // Reset stream position
                stream.Position = 0;
                
                // Delete the file explicitly
                await fileClient.DeleteAsync();
                
                // Wait a short time to ensure deletion is processed
                await Task.Delay(100);
                
                // Upload again
                await fileClient.UploadAsync(stream, overwrite: false);
            }
            catch (Exception retryEx)
            {
                throw new InvalidOperationException($"Failed to upload Parquet file after retry: {retryEx.Message}", retryEx);
            }
        }
        
        _logger.LogInformation("Successfully updated Parquet file at {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);

        return fileClient.Path;
    }
    
    /// <summary>
    /// Updates an existing Parquet file in Azure Data Lake Storage with a collection of items.
    /// </summary>
    /// <typeparam name="T">The type of the items to update with, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
    /// <param name="items">The collection of items containing the updated data.</param>
    /// <param name="filePath">The path to the existing file to update.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>The path to the updated file.</returns>
    public async Task<string> UpdateParquetFileWithItems<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>
    {
        if (items == null)
        {
            throw new ArgumentNullException(nameof(items));
        }

        if (!items.Any())
        {
            throw new ArgumentException("At least one item is required", nameof(items));
        }

        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Check if file exists
        if (!await fileClient.ExistsAsync())
        {
            throw new FileNotFoundException($"The file {filePath} was not found in the file system {fileSystemName}.");
        }

        // Use the Parquet formatter to serialize the items
        using var stream = await _parquetFormatter.SerializeItemsAsync(items);
        
        // Get the length of the stream for proper upload
        var contentLength = stream.Length;
        stream.Position = 0;
        
        // Validate that we have a valid Parquet file
        if (contentLength < 12) // Minimum Parquet file size
        {
            throw new InvalidOperationException($"Serialized Parquet data is too small ({contentLength} bytes). Minimum valid Parquet file is 12 bytes.");
        }
        
        try
        {
            // Try to delete if exists, but don't fail if it doesn't
            try
            {
                if (await fileClient.ExistsAsync())
                {
                    await fileClient.DeleteAsync();
                }
            }
            catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
            {
                // File doesn't exist, which is fine for our purposes
                _logger.LogDebug("File {Path} didn't exist when trying to delete it, continuing with upload", 
                    fileClient.Path);
            }
            
            // Upload the entire stream at once
            await fileClient.UploadAsync(stream, overwrite: false);
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathAlreadyExists")
        {
            // If the path already exists, try the delete-then-upload approach again
            _logger.LogDebug("Path {Path} already exists, retrying with explicit delete-then-create approach", 
                fileClient.Path);
                
            try
            {
                // Reset stream position
                stream.Position = 0;
                
                // Delete the file explicitly
                await fileClient.DeleteAsync();
                
                // Wait a short time to ensure deletion is processed
                await Task.Delay(100);
                
                // Upload again
                await fileClient.UploadAsync(stream, overwrite: false);
            }
            catch (Exception retryEx)
            {
                throw new InvalidOperationException($"Failed to upload Parquet file after retry: {retryEx.Message}", retryEx);
            }
        }
        
        _logger.LogInformation("Successfully updated Parquet file with multiple items at {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);

        return fileClient.Path;
    }
    
    /// <summary>
    /// Reads a Parquet file from Azure Data Lake Storage and deserializes it to an object.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to, which must implement IParquetSerializable&lt;T&gt; and have a parameterless constructor.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>The deserialized object.</returns>
    public async Task<T> ReadParquetFile<T>(
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new()
    {
        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Check if file exists
        if (!await fileClient.ExistsAsync())
        {
            throw new FileNotFoundException($"The file {filePath} was not found in the file system {fileSystemName}.");
        }

        // Download the file to a memory stream
        var memoryStream = new MemoryStream();
        await fileClient.ReadToAsync(memoryStream);
        memoryStream.Position = 0;

        // Use the Parquet formatter to deserialize the stream
        var result = await _parquetFormatter.DeserializeAsync<T>(memoryStream);
        
        _logger.LogInformation("Successfully read Parquet file from {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);

        return result;
    }
    
    /// <summary>
    /// Reads a Parquet file from Azure Data Lake Storage and deserializes it to a collection of objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to, which must implement IParquetSerializable&lt;T&gt; and have a parameterless constructor.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>A collection of deserialized objects.</returns>
    public async Task<IEnumerable<T>> ReadParquetItems<T>(
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new()
    {
        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Check if file exists
        if (!await fileClient.ExistsAsync())
        {
            throw new FileNotFoundException($"The file {filePath} was not found in the file system {fileSystemName}.");
        }

        // Download the file to a memory stream
        var memoryStream = new MemoryStream();
        await fileClient.ReadToAsync(memoryStream);
        memoryStream.Position = 0;

        // Use the Parquet formatter to deserialize the stream
        var result = await _parquetFormatter.DeserializeItemsAsync<T>(memoryStream);
        
        _logger.LogInformation("Successfully read Parquet file with multiple items from {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);

        return result;
    }
    
    /// <summary>
    /// Reads a JSON file from Azure Data Lake Storage and deserializes it to an object.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="jsonOptions">Optional. The JSON serializer options.</param>
    /// <returns>The deserialized object.</returns>
    public async Task<T> ReadJsonFile<T>(
        string filePath,
        string fileSystemName,
        JsonSerializerOptions? jsonOptions = null)
    {
        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Check if file exists
        if (!await fileClient.ExistsAsync())
        {
            throw new FileNotFoundException($"The file {filePath} was not found in the file system {fileSystemName}.");
        }

        // Download the file to a memory stream
        var memoryStream = new MemoryStream();
        await fileClient.ReadToAsync(memoryStream);
        memoryStream.Position = 0;

        // Use the JSON formatter to deserialize the stream
        var result = await _jsonFormatter.DeserializeAsync<T>(memoryStream);
        
        _logger.LogInformation("Successfully read JSON file from {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);

        return result;
    }
    
    /// <summary>
    /// Reads a JSON file from Azure Data Lake Storage and deserializes it to a collection of objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="jsonOptions">Optional. The JSON serializer options.</param>
    /// <returns>A collection of deserialized objects.</returns>
    public async Task<IEnumerable<T>> ReadJsonItems<T>(
        string filePath,
        string fileSystemName,
        JsonSerializerOptions? jsonOptions = null)
    {
        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Check if file exists
        if (!await fileClient.ExistsAsync())
        {
            throw new FileNotFoundException($"The file {filePath} was not found in the file system {fileSystemName}.");
        }

        // Download the file to a memory stream
        var memoryStream = new MemoryStream();
        await fileClient.ReadToAsync(memoryStream);
        memoryStream.Position = 0;

        // Use the JSON formatter to deserialize the stream
        var result = await _jsonFormatter.DeserializeItemsAsync<T>(memoryStream);
        
        _logger.LogInformation("Successfully read JSON file with multiple items from {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);

        return result;
    }
    
    /// <summary>
    /// Lists all files in a directory path in Azure Data Lake Storage.
    /// </summary>
    /// <param name="directoryPath">The directory path to list files from.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="recursive">Whether to list files recursively.</param>
    /// <returns>A collection of file paths.</returns>
    public async Task<IEnumerable<string>> ListFiles(
        string directoryPath,
        string fileSystemName,
        bool recursive = false,
        string? suffix = null)
    {
        if (string.IsNullOrWhiteSpace(directoryPath))
        {
            throw new ArgumentException("Directory path cannot be null or empty.", nameof(directoryPath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);

        var files = new List<string>();

        try
        {
            await foreach (var pathItem in directoryClient.GetPathsAsync(recursive: recursive))
            {
                if (!pathItem.IsDirectory.GetValueOrDefault())
                {
                    // Apply suffix filter if provided
                    if (string.IsNullOrEmpty(suffix) ||
                        pathItem.Name.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
                    {
                        files.Add(pathItem.Name);
                    }
                }
            }
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
        {
            _logger.LogDebug("Directory does not exist, returning empty list: {DirectoryPath} in file system {FileSystem}",
                directoryPath, fileSystemName);
            return files;
        }

        _logger.LogInformation("Successfully listed {Count} files from {DirectoryPath} in file system {FileSystem}{SuffixFilter}",
            files.Count, directoryPath, fileSystemName,
            string.IsNullOrEmpty(suffix) ? "" : $" (filtered by suffix: {suffix})");

        return files;
    }

    /// <summary>
    /// Lists all directories (subdirectories) in a directory path in Azure Data Lake Storage.
    /// </summary>
    /// <param name="directoryPath">The directory path to list directories from.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="recursive">Whether to list directories recursively.</param>
    /// <returns>A collection of directory paths.</returns>
    public async Task<IEnumerable<string>> ListDirectories(
        string directoryPath,
        string fileSystemName,
        bool recursive = false)
    {
        if (string.IsNullOrWhiteSpace(directoryPath))
        {
            throw new ArgumentException("Directory path cannot be null or empty.", nameof(directoryPath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);

        var directories = new List<string>();

        try
        {
            await foreach (var pathItem in directoryClient.GetPathsAsync(recursive: recursive))
            {
                if (pathItem.IsDirectory.GetValueOrDefault())
                {
                    directories.Add(pathItem.Name);
                }
            }
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
        {
            _logger.LogDebug("Directory does not exist, returning empty list: {DirectoryPath} in file system {FileSystem}",
                directoryPath, fileSystemName);
            return directories;
        }

        _logger.LogInformation("Successfully listed {Count} directories from {DirectoryPath} in file system {FileSystem}",
            directories.Count, directoryPath, fileSystemName);

        return directories;
    }

    /// <summary>
    /// Lists all files in a directory path in Azure Data Lake Storage that match the specified date range based on file modification time.
    /// </summary>
    /// <param name="directoryPath">The directory path to list files from.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="fromDate">The start date to filter files (inclusive).</param>
    /// <param name="toDate">The end date to filter files (inclusive).</param>
    /// <param name="recursive">Whether to list files recursively.</param>
    /// <returns>A collection of file paths that fall within the specified date range.</returns>
    public async Task<IEnumerable<string>> ListFilesByDateRange(
        string directoryPath,
        string fileSystemName,
        DateTime fromDate,
        DateTime toDate,
        bool recursive = false)
    {
        if (string.IsNullOrWhiteSpace(directoryPath))
        {
            throw new ArgumentException("Directory path cannot be null or empty.", nameof(directoryPath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        if (fromDate > toDate)
        {
            throw new ArgumentException("From date cannot be greater than to date.", nameof(fromDate));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);

        var files = new List<string>();

        try
        {
            await foreach (var pathItem in directoryClient.GetPathsAsync(recursive: recursive))
            {
                if (!pathItem.IsDirectory.GetValueOrDefault())
                {
                    var lastModified = pathItem.LastModified.DateTime;
                    if (lastModified >= fromDate && lastModified <= toDate)
                    {
                        files.Add(pathItem.Name);
                    }
                }
            }
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
        {
            _logger.LogDebug("Directory does not exist, returning empty list: {DirectoryPath} in file system {FileSystem}",
                directoryPath, fileSystemName);
            return files;
        }

        _logger.LogInformation("Successfully listed {Count} files from {DirectoryPath} in date range {FromDate} to {ToDate} in file system {FileSystem}",
            files.Count, directoryPath, fromDate, toDate, fileSystemName);

        return files;
    }

    /// <summary>
    /// Lists all files in a directory path in Azure Data Lake Storage with their metadata.
    /// </summary>
    /// <param name="directoryPath">The directory path to list files from.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="recursive">Whether to list files recursively.</param>
    /// <returns>A collection of file information including paths and metadata.</returns>
    public async Task<IEnumerable<DataLakeFileInfo>> ListFilesWithMetadata(
        string directoryPath,
        string fileSystemName,
        bool recursive = false)
    {
        if (string.IsNullOrWhiteSpace(directoryPath))
        {
            throw new ArgumentException("Directory path cannot be null or empty.", nameof(directoryPath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);

        var files = new List<DataLakeFileInfo>();

        try
        {
            await foreach (var pathItem in directoryClient.GetPathsAsync(recursive: recursive))
            {
                var fileInfo = new DataLakeFileInfo
                {
                    Path = pathItem.Name,
                    Name = System.IO.Path.GetFileName(pathItem.Name),
                    Size = pathItem.ContentLength ?? 0,
                    LastModified = pathItem.LastModified,
                    CreatedOn = null,
                    IsDirectory = pathItem.IsDirectory.GetValueOrDefault()
                };

                files.Add(fileInfo);
            }
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
        {
            _logger.LogDebug("Directory does not exist, returning empty list: {DirectoryPath} in file system {FileSystem}",
                directoryPath, fileSystemName);
            return files;
        }

        _logger.LogInformation("Successfully listed {Count} files with metadata from {DirectoryPath} in file system {FileSystem}",
            files.Count, directoryPath, fileSystemName);

        return files;
    }

    /// <summary>
    /// Checks if a file is a valid Parquet file by checking size and format.
    /// </summary>
    /// <param name="filePath">The file path to check.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>True if the file is valid, false otherwise.</returns>
    public async Task<bool> IsValidParquetFile(string filePath, string fileSystemName)
    {
        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        try
        {
            var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
            var fileClient = fileSystemClient.GetFileClient(filePath);
            
            // Check if file exists
            var response = await fileClient.GetPropertiesAsync();
            if (!response.HasValue)
            {
                return false;
            }
            
            // Check file size - minimum Parquet file is 12 bytes (PAR1 header + PAR1 footer)
            var fileSize = response.Value.ContentLength;
            if (fileSize < 12)
            {
                _logger.LogWarning("File {FilePath} is too small to be a valid Parquet file: {Size} bytes", filePath, fileSize);
                return false;
            }
            
            // For small files, download and check the header/footer
            if (fileSize < 1024) // Check files smaller than 1KB more thoroughly
            {
                using var stream = new MemoryStream();
                await fileClient.ReadToAsync(stream);
                stream.Position = 0;
                
                // Check for PAR1 at the beginning
                var header = new byte[4];
                if (stream.Read(header, 0, 4) < 4)
                {
                    return false;
                }
                
                if (header[0] != 'P' || header[1] != 'A' || header[2] != 'R' || header[3] != '1')
                {
                    _logger.LogWarning("File {FilePath} missing PAR1 header", filePath);
                    return false;
                }
                
                // Check for PAR1 at the end
                stream.Seek(-4, SeekOrigin.End);
                var footer = new byte[4];
                if (stream.Read(footer, 0, 4) < 4)
                {
                    return false;
                }
                
                if (footer[0] != 'P' || footer[1] != 'A' || footer[2] != 'R' || footer[3] != '1')
                {
                    _logger.LogWarning("File {FilePath} missing PAR1 footer", filePath);
                    return false;
                }
            }
            
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking if file {FilePath} is valid Parquet", filePath);
            return false;
        }
    }

    #region CSV Methods

    /// <summary>
    /// Stores an item as a CSV file in Azure Data Lake Storage.
    /// </summary>
    /// <typeparam name="T">The type of the item to store.</typeparam>
    /// <param name="item">The item to store.</param>
    /// <param name="directoryPath">The directory path where the file will be stored.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="fileName">Optional. The name of the file. If not provided, a GUID will be used.</param>
    /// <param name="delimiter">Optional. The delimiter to use for CSV fields (default: comma).</param>
    /// <param name="hasHeader">Optional. Whether to include a header row (default: true).</param>
    /// <param name="columnMapping">Optional. Dictionary mapping property names to CSV column headers.</param>
    /// <param name="overwrite">Whether to overwrite the file if it already exists.</param>
    /// <returns>The path to the stored file.</returns>
    public async Task<string> StoreItemAsCsv<T>(
        T item,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        string delimiter = ",",
        bool hasHeader = true,
        Dictionary<string, string>? columnMapping = null,
        bool overwrite = true)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        return await StoreItemsAsCsv(new[] { item }, directoryPath, fileSystemName, fileName, delimiter, hasHeader, columnMapping, overwrite);
    }

    /// <summary>
    /// Stores a collection of items as a CSV file in Azure Data Lake Storage.
    /// </summary>
    /// <typeparam name="T">The type of the items to store.</typeparam>
    /// <param name="items">The collection of items to store.</param>
    /// <param name="directoryPath">The directory path where the file will be stored.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="fileName">Optional. The name of the file. If not provided, a GUID will be used.</param>
    /// <param name="delimiter">Optional. The delimiter to use for CSV fields (default: comma).</param>
    /// <param name="hasHeader">Optional. Whether to include a header row (default: true).</param>
    /// <param name="columnMapping">Optional. Dictionary mapping property names to CSV column headers.</param>
    /// <param name="overwrite">Whether to overwrite the file if it already exists.</param>
    /// <returns>The path to the stored file.</returns>
    public async Task<string> StoreItemsAsCsv<T>(
        IEnumerable<T> items,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        string delimiter = ",",
        bool hasHeader = true,
        Dictionary<string, string>? columnMapping = null,
        bool overwrite = true)
    {
        if (items == null)
        {
            throw new ArgumentNullException(nameof(items));
        }

        if (string.IsNullOrWhiteSpace(directoryPath))
        {
            throw new ArgumentException("Directory path cannot be null or empty.", nameof(directoryPath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        // Configure CSV formatter
        _csvFormatter.Delimiter = delimiter;
        _csvFormatter.HasHeader = hasHeader;

        // Generate file name if not provided
        fileName = fileName ?? Guid.NewGuid().ToString("N");
        if (!fileName.EndsWith(_csvFormatter.FileExtension))
        {
            fileName += _csvFormatter.FileExtension;
        }

        var filePath = $"{directoryPath.TrimEnd('/')}/{fileName}";

        // Get file system client
        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);

        // Create directory if it doesn't exist
        await directoryClient.CreateIfNotExistsAsync();

        // Get file client
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Serialize items to CSV
        using var stream = await _csvFormatter.SerializeItemsAsync(items, columnMapping);

        // Upload the file
        await fileClient.UploadAsync(stream, overwrite);

        _logger.LogInformation("Successfully stored CSV file with {Count} items at {Path} in file system {FileSystem}", 
            items.Count(), filePath, fileSystemName);

        return filePath;
    }

    /// <summary>
    /// Reads a CSV file from Azure Data Lake Storage and deserializes it to an object.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="delimiter">Optional. The delimiter used in the CSV file (default: comma).</param>
    /// <param name="hasHeader">Optional. Whether the CSV file has a header row (default: true).</param>
    /// <returns>The deserialized object.</returns>
    public async Task<T> ReadCsvFile<T>(
        string filePath,
        string fileSystemName,
        string delimiter = ",",
        bool hasHeader = true)
    {
        var items = await ReadCsvItems<T>(filePath, fileSystemName, delimiter, hasHeader);
        return items.FirstOrDefault();
    }

    /// <summary>
    /// Reads a CSV file from Azure Data Lake Storage and deserializes it to a collection of objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="delimiter">Optional. The delimiter used in the CSV file (default: comma).</param>
    /// <param name="hasHeader">Optional. Whether the CSV file has a header row (default: true).</param>
    /// <returns>A collection of deserialized objects.</returns>
    public async Task<IEnumerable<T>> ReadCsvItems<T>(
        string filePath,
        string fileSystemName,
        string delimiter = ",",
        bool hasHeader = true)
    {
        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        // Configure CSV formatter
        _csvFormatter.Delimiter = delimiter;
        _csvFormatter.HasHeader = hasHeader;

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Check if file exists
        if (!await fileClient.ExistsAsync())
        {
            throw new FileNotFoundException($"The file {filePath} was not found in the file system {fileSystemName}.");
        }

        // Download the file to a memory stream
        var memoryStream = new MemoryStream();
        await fileClient.ReadToAsync(memoryStream);
        memoryStream.Position = 0;

        // Use the CSV formatter to deserialize the stream
        var result = await _csvFormatter.DeserializeItemsAsync<T>(memoryStream);

        _logger.LogInformation("Successfully read CSV file with multiple items from {Path} in file system {FileSystem}", 
            fileClient.Path, fileSystemName);

        return result;
    }

    /// <summary>
    /// Updates an existing CSV file in Azure Data Lake Storage with new content.
    /// </summary>
    /// <typeparam name="T">The type of the item to update with.</typeparam>
    /// <param name="item">The item containing the updated data.</param>
    /// <param name="filePath">The path to the existing file to update.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="delimiter">Optional. The delimiter to use for CSV fields (default: comma).</param>
    /// <param name="hasHeader">Optional. Whether to include a header row (default: true).</param>
    /// <param name="columnMapping">Optional. Dictionary mapping property names to CSV column headers.</param>
    /// <returns>The path to the updated file.</returns>
    public async Task<string> UpdateCsvFile<T>(
        T item,
        string filePath,
        string fileSystemName,
        string delimiter = ",",
        bool hasHeader = true,
        Dictionary<string, string>? columnMapping = null)
    {
        if (item == null)
        {
            throw new ArgumentNullException(nameof(item));
        }

        return await UpdateCsvFileWithItems(new[] { item }, filePath, fileSystemName, delimiter, hasHeader, columnMapping);
    }

    /// <summary>
    /// Updates an existing CSV file in Azure Data Lake Storage with a collection of items.
    /// </summary>
    /// <typeparam name="T">The type of the items to update with.</typeparam>
    /// <param name="items">The collection of items containing the updated data.</param>
    /// <param name="filePath">The path to the existing file to update.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="delimiter">Optional. The delimiter to use for CSV fields (default: comma).</param>
    /// <param name="hasHeader">Optional. Whether to include a header row (default: true).</param>
    /// <param name="columnMapping">Optional. Dictionary mapping property names to CSV column headers.</param>
    /// <returns>The path to the updated file.</returns>
    public async Task<string> UpdateCsvFileWithItems<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName,
        string delimiter = ",",
        bool hasHeader = true,
        Dictionary<string, string>? columnMapping = null)
    {
        if (items == null)
        {
            throw new ArgumentNullException(nameof(items));
        }

        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        // Configure CSV formatter
        _csvFormatter.Delimiter = delimiter;
        _csvFormatter.HasHeader = hasHeader;

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Check if file exists
        if (!await fileClient.ExistsAsync())
        {
            throw new FileNotFoundException($"The file {filePath} was not found in the file system {fileSystemName}.");
        }

        // Serialize items to CSV
        using var stream = await _csvFormatter.SerializeItemsAsync(items, columnMapping);

        // Overwrite the existing file
        await fileClient.UploadAsync(stream, overwrite: true);

        _logger.LogInformation("Successfully updated CSV file with {Count} items at {Path} in file system {FileSystem}", 
            items.Count(), filePath, fileSystemName);

        return filePath;
    }

    #endregion

    #region NDJSON Buffer Methods

    /// <inheritdoc />
    public async Task AppendJsonLineAsync<T>(
        T item,
        string filePath,
        string fileSystemName)
    {
        await AppendJsonLinesAsync(new[] { item }, filePath, fileSystemName);
    }

    /// <inheritdoc />
    public async Task AppendJsonLinesAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName)
    {
        if (items == null)
        {
            throw new ArgumentNullException(nameof(items));
        }

        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var itemsList = items.ToList();
        if (!itemsList.Any())
        {
            _logger.LogDebug("No items to append to {FilePath}", filePath);
            return;
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Serialize items to NDJSON format
        using var memoryStream = new MemoryStream();
        // Use UTF-8 without BOM to avoid deserialization issues
        using var writer = new StreamWriter(memoryStream, new System.Text.UTF8Encoding(false), leaveOpen: true);

        foreach (var item in itemsList)
        {
            var json = System.Text.Json.JsonSerializer.Serialize(item);
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
            // File doesn't exist, create it first
            _logger.LogDebug("Creating new buffer file at {FilePath}", filePath);
            await fileClient.CreateAsync();
            offset = 0;
        }

        // Append data using native Azure append operation with retry on race condition
        int retryCount = 0;
        const int maxRetries = 2;

        while (retryCount <= maxRetries)
        {
            try
            {
                await fileClient.AppendAsync(memoryStream, offset);
                await fileClient.FlushAsync(offset + memoryStream.Length);

                _logger.LogInformation("Successfully appended {Count} items to buffer file {FilePath} at offset {Offset}",
                    itemsList.Count, filePath, offset);
                return; // Success, exit method
            }
            catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound" && retryCount < maxRetries)
            {
                // File was deleted during append/flush (race condition with compaction)
                // Retry by recreating the file
                retryCount++;
                _logger.LogWarning("Buffer file {FilePath} was deleted during append (race condition), retrying ({Retry}/{MaxRetries})",
                    filePath, retryCount, maxRetries);

                // Reset stream and offset for retry
                memoryStream.Position = 0;
                offset = 0;

                // Recreate the file
                try
                {
                    await fileClient.CreateAsync();
                }
                catch (Azure.RequestFailedException createEx) when (createEx.ErrorCode == "PathAlreadyExists")
                {
                    // File was recreated by another process, get its size
                    var properties = await fileClient.GetPropertiesAsync();
                    offset = properties.Value.ContentLength;
                    memoryStream.Position = 0; // Reset stream
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to append to buffer file {FilePath} at offset {Offset}", filePath, offset);
                throw;
            }
        }

        _logger.LogError("Failed to append to buffer file {FilePath} after {MaxRetries} retries due to race condition",
            filePath, maxRetries);
        throw new InvalidOperationException($"Failed to append to buffer file {filePath} after {maxRetries} retries due to concurrent deletion");
    }

    /// <inheritdoc />
    public async Task<IEnumerable<T>> ReadJsonLinesFile<T>(
        string filePath,
        string fileSystemName)
    {
        if (string.IsNullOrWhiteSpace(filePath))
        {
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        // Check if file exists
        if (!await fileClient.ExistsAsync())
        {
            throw new FileNotFoundException($"The file {filePath} was not found in the file system {fileSystemName}.");
        }

        // Download the file to a memory stream
        var memoryStream = new MemoryStream();
        await fileClient.ReadToAsync(memoryStream);
        memoryStream.Position = 0;

        // Read and deserialize each line
        var result = new List<T>();
        using var reader = new StreamReader(memoryStream);

        while (!reader.EndOfStream)
        {
            var line = await reader.ReadLineAsync();
            if (!string.IsNullOrWhiteSpace(line))
            {
                try
                {
                    var item = System.Text.Json.JsonSerializer.Deserialize<T>(line);
                    if (item != null)
                    {
                        result.Add(item);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to deserialize line in {FilePath}: {Line}", filePath, line);
                }
            }
        }

        _logger.LogInformation("Successfully read {Count} items from NDJSON file {FilePath}", result.Count, filePath);
        return result;
    }

    /// <inheritdoc />
    public async Task<string> CompactBufferToParquet<T>(
        string bufferFilePath,
        string parquetFilePath,
        string fileSystemName,
        bool deleteBuffer = true) where T : IParquetSerializable<T>, new()
    {
        if (string.IsNullOrWhiteSpace(bufferFilePath))
        {
            throw new ArgumentException("Buffer file path cannot be null or empty.", nameof(bufferFilePath));
        }

        if (string.IsNullOrWhiteSpace(parquetFilePath))
        {
            throw new ArgumentException("Parquet file path cannot be null or empty.", nameof(parquetFilePath));
        }

        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            throw new ArgumentException("File system name cannot be null or empty.", nameof(fileSystemName));
        }

        var fileSystemClient = GetOrCreateFileSystemClient(fileSystemName);
        var bufferFileClient = fileSystemClient.GetFileClient(bufferFilePath);

        // Check if buffer file exists
        if (!await bufferFileClient.ExistsAsync())
        {
            _logger.LogWarning("Buffer file {BufferFilePath} does not exist, skipping compaction", bufferFilePath);
            return parquetFilePath;
        }

        // Read buffer data
        _logger.LogInformation("Reading buffer file {BufferFilePath} for compaction", bufferFilePath);
        var bufferItems = await ReadJsonLinesFile<T>(bufferFilePath, fileSystemName);
        var bufferList = bufferItems.ToList();

        if (!bufferList.Any())
        {
            _logger.LogInformation("Buffer file {BufferFilePath} is empty, deleting if requested", bufferFilePath);
            if (deleteBuffer)
            {
                await bufferFileClient.DeleteAsync();
            }
            return parquetFilePath;
        }

        _logger.LogInformation("Read {Count} items from buffer, checking for existing Parquet file", bufferList.Count);

        // Check if Parquet file exists and read it
        var parquetFileClient = fileSystemClient.GetFileClient(parquetFilePath);
        List<T> allData;
        bool parquetExists = await parquetFileClient.ExistsAsync();

        if (parquetExists)
        {
            _logger.LogInformation("Existing Parquet file found, reading and merging with buffer data");
            var existingData = await ReadParquetItems<T>(parquetFilePath, fileSystemName);
            var existingList = existingData.ToList();

            _logger.LogInformation("Read {ExistingCount} existing records, merging with {BufferCount} buffer records",
                existingList.Count, bufferList.Count);

            allData = existingList.Concat(bufferList).ToList();
        }
        else
        {
            _logger.LogInformation("No existing Parquet file, creating new one with buffer data");
            allData = bufferList;
        }

        // Write combined data to Parquet
        _logger.LogInformation("Writing {TotalCount} records to Parquet file {ParquetFilePath}",
            allData.Count, parquetFilePath);

        // Use StoreItemsAsParquet for new files, UpdateParquetFileWithItems for existing files
        if (parquetExists)
        {
            await UpdateParquetFileWithItems(allData, parquetFilePath, fileSystemName);
        }
        else
        {
            // Extract directory path from parquetFilePath
            var directoryPath = System.IO.Path.GetDirectoryName(parquetFilePath)?.Replace('\\', '/') ?? string.Empty;
            var fileName = System.IO.Path.GetFileName(parquetFilePath);
            await StoreItemsAsParquet(allData, directoryPath, fileSystemName, fileName, overwrite: false);
        }

        // Delete buffer if requested
        if (deleteBuffer)
        {
            _logger.LogInformation("Deleting buffer file {BufferFilePath} after successful compaction", bufferFilePath);
            await bufferFileClient.DeleteAsync();
        }

        _logger.LogInformation("Successfully compacted {BufferCount} buffer records into Parquet file with {TotalCount} total records",
            bufferList.Count, allData.Count);

        return parquetFilePath;
    }

    #endregion
}
