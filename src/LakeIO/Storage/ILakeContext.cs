using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Configuration;
using LakeIO.Annotations;

namespace LakeIO;

/// <summary>
/// Defines the interface for interacting with Azure Data Lake Storage.
/// </summary>
public interface ILakeContext : IDisposable
{
    /// <summary>
    /// Gets or creates a DataLakeServiceClient for the specified connection string.
    /// </summary>
    /// <param name="connectionString">The connection string to the storage account.</param>
    /// <returns>A <see cref="DataLakeServiceClient" /> instance.</returns>
    DataLakeServiceClient GetOrCreateServiceClient(string connectionString);

    /// <summary>
    /// Gets or creates a DataLakeFileSystemClient for the specified file system.
    /// </summary>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="connectionString">
    /// Optional. The connection string to use. If not provided, uses the default connection
    /// string.
    /// </param>
    /// <returns>A <see cref="DataLakeFileSystemClient" /> instance.</returns>
    DataLakeFileSystemClient GetOrCreateFileSystemClient(string fileSystemName, string? connectionString = null);

    /// <summary>
    /// Stores an item as a JSON file in Azure Data Lake Storage.
    /// </summary>
    /// <typeparam name="T">The type of the item to store.</typeparam>
    /// <param name="item">The item to store.</param>
    /// <param name="directoryPath">The directory path where the file will be stored.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="fileName">Optional. The name of the file. If not provided, a GUID will be used.</param>
    /// <param name="jsonSettings">Optional. The JSON serializer options.</param>
    /// <param name="overwrite">Whether to overwrite the file if it already exists.</param>
    /// <returns>The path to the stored file.</returns>
    Task<string> StoreItemAsJson<T>(
        T item,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        JsonSerializerOptions? jsonSettings = null,
        bool overwrite = true);

    /// <summary>
    /// Updates an existing JSON file in Azure Data Lake Storage with new content.
    /// </summary>
    /// <typeparam name="T">The type of the item to update with.</typeparam>
    /// <param name="item">The item containing the updated data.</param>
    /// <param name="filePath">The path to the existing file to update.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="jsonSettings">Optional. The JSON serializer options.</param>
    /// <returns>The path to the updated file.</returns>
    Task<string> UpdateJsonFile<T>(
        T item,
        string filePath,
        string fileSystemName,
        JsonSerializerOptions? jsonSettings = null);
        
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
    Task<string> StoreItemAsParquet<T>(
        T item,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        bool overwrite = true) where T : IParquetSerializable<T>;
    
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
    Task<string> StoreItemsAsParquet<T>(
        IEnumerable<T> items,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        bool overwrite = true) where T : IParquetSerializable<T>;
    
    /// <summary>
    /// Updates an existing Parquet file in Azure Data Lake Storage with new content.
    /// </summary>
    /// <typeparam name="T">The type of the item to update with, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
    /// <param name="item">The item containing the updated data.</param>
    /// <param name="filePath">The path to the existing file to update.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>The path to the updated file.</returns>
    Task<string> UpdateParquetFile<T>(
        T item,
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>;
    
    /// <summary>
    /// Updates an existing Parquet file in Azure Data Lake Storage with a collection of items.
    /// </summary>
    /// <typeparam name="T">The type of the items to update with, which must implement IParquetSerializable&lt;T&gt;.</typeparam>
    /// <param name="items">The collection of items containing the updated data.</param>
    /// <param name="filePath">The path to the existing file to update.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>The path to the updated file.</returns>
    Task<string> UpdateParquetFileWithItems<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>;
    
    /// <summary>
    /// Reads a Parquet file from Azure Data Lake Storage and deserializes it to an object.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to, which must implement IParquetSerializable&lt;T&gt; and have a parameterless constructor.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>The deserialized object.</returns>
    Task<T> ReadParquetFile<T>(
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new();
    
    /// <summary>
    /// Reads a Parquet file from Azure Data Lake Storage and deserializes it to a collection of objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to, which must implement IParquetSerializable&lt;T&gt; and have a parameterless constructor.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>A collection of deserialized objects.</returns>
    Task<IEnumerable<T>> ReadParquetItems<T>(
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new();

    /// <summary>
    /// Reads a JSON file from Azure Data Lake Storage and deserializes it to an object.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="jsonSettings">Optional. The JSON serializer options.</param>
    /// <returns>The deserialized object.</returns>
    Task<T> ReadJsonFile<T>(
        string filePath,
        string fileSystemName,
        JsonSerializerOptions? jsonSettings = null);

    /// <summary>
    /// Reads a JSON file from Azure Data Lake Storage and deserializes it to a collection of objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="jsonSettings">Optional. The JSON serializer options.</param>
    /// <returns>A collection of deserialized objects.</returns>
    Task<IEnumerable<T>> ReadJsonItems<T>(
        string filePath,
        string fileSystemName,
        JsonSerializerOptions? jsonSettings = null);

    /// <summary>
    /// Lists all files in a directory path in Azure Data Lake Storage.
    /// </summary>
    /// <param name="directoryPath">The directory path to list files from.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="recursive">Whether to list files recursively.</param>
    /// <param name="suffix">Optional file suffix filter (e.g., "_buffer.ndjson", ".parquet").</param>
    /// <returns>A collection of file paths.</returns>
    Task<IEnumerable<string>> ListFiles(
        string directoryPath,
        string fileSystemName,
        bool recursive = false,
        string? suffix = null);

    /// <summary>
    /// Lists all directories (subdirectories) in a directory path in Azure Data Lake Storage.
    /// </summary>
    /// <param name="directoryPath">The directory path to list directories from.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="recursive">Whether to list directories recursively.</param>
    /// <returns>A collection of directory paths.</returns>
    Task<IEnumerable<string>> ListDirectories(
        string directoryPath,
        string fileSystemName,
        bool recursive = false);

    /// <summary>
    /// Checks if a file is a valid Parquet file by checking size and format.
    /// </summary>
    /// <param name="filePath">The file path to check.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>True if the file is valid, false otherwise.</returns>
    Task<bool> IsValidParquetFile(string filePath, string fileSystemName);

    /// <summary>
    /// Lists all files in a directory path in Azure Data Lake Storage that match the specified date range based on file modification time.
    /// </summary>
    /// <param name="directoryPath">The directory path to list files from.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="fromDate">The start date to filter files (inclusive).</param>
    /// <param name="toDate">The end date to filter files (inclusive).</param>
    /// <param name="recursive">Whether to list files recursively.</param>
    /// <returns>A collection of file paths that fall within the specified date range.</returns>
    Task<IEnumerable<string>> ListFilesByDateRange(
        string directoryPath,
        string fileSystemName,
        DateTime fromDate,
        DateTime toDate,
        bool recursive = false);


    /// <summary>
    /// Lists all files in a directory path in Azure Data Lake Storage with their metadata.
    /// </summary>
    /// <param name="directoryPath">The directory path to list files from.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="recursive">Whether to list files recursively.</param>
    /// <returns>A collection of file information including paths and metadata.</returns>
    Task<IEnumerable<DataLakeFileInfo>> ListFilesWithMetadata(
        string directoryPath,
        string fileSystemName,
        bool recursive = false);

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
    Task<string> StoreItemAsCsv<T>(
        T item,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        string delimiter = ",",
        bool hasHeader = true,
        Dictionary<string, string>? columnMapping = null,
        bool overwrite = true);

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
    Task<string> StoreItemsAsCsv<T>(
        IEnumerable<T> items,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        string delimiter = ",",
        bool hasHeader = true,
        Dictionary<string, string>? columnMapping = null,
        bool overwrite = true);

    /// <summary>
    /// Reads a CSV file from Azure Data Lake Storage and deserializes it to an object.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="delimiter">Optional. The delimiter used in the CSV file (default: comma).</param>
    /// <param name="hasHeader">Optional. Whether the CSV file has a header row (default: true).</param>
    /// <returns>The deserialized object.</returns>
    Task<T> ReadCsvFile<T>(
        string filePath,
        string fileSystemName,
        string delimiter = ",",
        bool hasHeader = true);

    /// <summary>
    /// Reads a CSV file from Azure Data Lake Storage and deserializes it to a collection of objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="delimiter">Optional. The delimiter used in the CSV file (default: comma).</param>
    /// <param name="hasHeader">Optional. Whether the CSV file has a header row (default: true).</param>
    /// <returns>A collection of deserialized objects.</returns>
    Task<IEnumerable<T>> ReadCsvItems<T>(
        string filePath,
        string fileSystemName,
        string delimiter = ",",
        bool hasHeader = true);

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
    Task<string> UpdateCsvFile<T>(
        T item,
        string filePath,
        string fileSystemName,
        string delimiter = ",",
        bool hasHeader = true,
        Dictionary<string, string>? columnMapping = null);

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
    Task<string> UpdateCsvFileWithItems<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName,
        string delimiter = ",",
        bool hasHeader = true,
        Dictionary<string, string>? columnMapping = null);

    /// <summary>
    /// Appends a single item as a JSON line to a buffer file using native Azure append operations.
    /// This method uses streaming and has minimal memory overhead.
    /// </summary>
    /// <typeparam name="T">The type of the item to append.</typeparam>
    /// <param name="item">The item to append.</param>
    /// <param name="filePath">The full path to the buffer file.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task AppendJsonLineAsync<T>(
        T item,
        string filePath,
        string fileSystemName);

    /// <summary>
    /// Appends multiple items as JSON lines to a buffer file using native Azure append operations.
    /// This method uses streaming and has minimal memory overhead.
    /// </summary>
    /// <typeparam name="T">The type of the items to append.</typeparam>
    /// <param name="items">The collection of items to append.</param>
    /// <param name="filePath">The full path to the buffer file.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task AppendJsonLinesAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName);

    /// <summary>
    /// Reads a newline-delimited JSON (NDJSON) file and deserializes it to a collection of objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <param name="filePath">The path to the NDJSON file.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <returns>A collection of deserialized objects.</returns>
    Task<IEnumerable<T>> ReadJsonLinesFile<T>(
        string filePath,
        string fileSystemName);

    /// <summary>
    /// Compacts a JSON lines buffer file into a Parquet file, optionally merging with existing Parquet data.
    /// </summary>
    /// <typeparam name="T">The type of items, which must implement IParquetSerializable&lt;T&gt; and have a parameterless constructor.</typeparam>
    /// <param name="bufferFilePath">The path to the JSON lines buffer file.</param>
    /// <param name="parquetFilePath">The path to the target Parquet file.</param>
    /// <param name="fileSystemName">The name of the file system.</param>
    /// <param name="deleteBuffer">Whether to delete the buffer file after successful compaction. Defaults to true.</param>
    /// <returns>The path to the compacted Parquet file.</returns>
    Task<string> CompactBufferToParquet<T>(
        string bufferFilePath,
        string parquetFilePath,
        string fileSystemName,
        bool deleteBuffer = true) where T : IParquetSerializable<T>, new();
}

/// <summary>
/// Represents information about a file in Azure Data Lake Storage.
/// </summary>
public class DataLakeFileInfo
{
    /// <summary>
    /// Gets or sets the full path of the file or directory in the data lake.
    /// </summary>
    public string Path { get; set; }
    
    /// <summary>
    /// Gets or sets the name of the file or directory.
    /// </summary>
    public string Name { get; set; }
    
    /// <summary>
    /// Gets or sets the size of the file in bytes.
    /// </summary>
    public long Size { get; set; }
    
    /// <summary>
    /// Gets or sets the last modified date and time of the file or directory.
    /// </summary>
    public DateTimeOffset? LastModified { get; set; }
    
    /// <summary>
    /// Gets or sets the creation date and time of the file or directory.
    /// </summary>
    public DateTimeOffset? CreatedOn { get; set; }
    
    /// <summary>
    /// Gets or sets a value indicating whether this represents a directory.
    /// </summary>
    public bool IsDirectory { get; set; }
}
