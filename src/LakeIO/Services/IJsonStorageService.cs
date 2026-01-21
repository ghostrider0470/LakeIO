using System.Text.Json;

namespace LakeIO.Services;

/// <summary>
/// Service for JSON file operations in Azure Data Lake Storage.
/// Handles storing, reading, and managing JSON files using System.Text.Json.
/// </summary>
public interface IJsonStorageService
{
    /// <summary>
    /// Stores a single item as a JSON file in the specified directory.
    /// </summary>
    /// <typeparam name="T">The type of the item to store.</typeparam>
    /// <param name="item">The item to store.</param>
    /// <param name="directoryPath">The directory path where the file will be stored.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="fileName">Optional. The name of the file. If not provided, a GUID-based name will be generated.</param>
    /// <param name="jsonOptions">Optional. Custom JSON serialization options.</param>
    /// <param name="overwrite">Whether to overwrite the file if it exists. Default is true.</param>
    /// <returns>The full path of the stored file.</returns>
    /// <exception cref="ArgumentNullException">Thrown when item is null.</exception>
    /// <exception cref="ArgumentException">Thrown when directoryPath or fileSystemName is invalid.</exception>
    Task<string> StoreItemAsync<T>(
        T item,
        string directoryPath,
        string fileSystemName,
        string? fileName = null,
        JsonSerializerOptions? jsonOptions = null,
        bool overwrite = true);

    /// <summary>
    /// Stores a collection of items as JSON files in the specified directory.
    /// Each item is stored as a separate file.
    /// </summary>
    /// <typeparam name="T">The type of the items to store.</typeparam>
    /// <param name="items">The collection of items to store.</param>
    /// <param name="directoryPath">The directory path where the files will be stored.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="jsonOptions">Optional. Custom JSON serialization options.</param>
    /// <param name="overwrite">Whether to overwrite files if they exist. Default is true.</param>
    /// <returns>A list of full paths of the stored files.</returns>
    /// <exception cref="ArgumentNullException">Thrown when items is null.</exception>
    /// <exception cref="ArgumentException">Thrown when directoryPath or fileSystemName is invalid.</exception>
    Task<List<string>> StoreItemsAsync<T>(
        IEnumerable<T> items,
        string directoryPath,
        string fileSystemName,
        JsonSerializerOptions? jsonOptions = null,
        bool overwrite = true);

    /// <summary>
    /// Reads a JSON file and deserializes it to the specified type.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <param name="filePath">The path to the JSON file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="jsonOptions">Optional. Custom JSON deserialization options.</param>
    /// <returns>The deserialized object, or null if the file doesn't exist.</returns>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    /// <exception cref="JsonException">Thrown when the file content is not valid JSON.</exception>
    Task<T?> ReadItemAsync<T>(
        string filePath,
        string fileSystemName,
        JsonSerializerOptions? jsonOptions = null);

    /// <summary>
    /// Reads multiple JSON files from a directory and deserializes them to the specified type.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <param name="directoryPath">The directory path to read files from.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="searchPattern">Optional. Pattern to filter files (e.g., "*.json"). Default is "*.json".</param>
    /// <param name="jsonOptions">Optional. Custom JSON deserialization options.</param>
    /// <returns>A collection of deserialized objects.</returns>
    /// <exception cref="ArgumentException">Thrown when directoryPath or fileSystemName is invalid.</exception>
    Task<IEnumerable<T>> ReadItemsAsync<T>(
        string directoryPath,
        string fileSystemName,
        string searchPattern = "*.json",
        JsonSerializerOptions? jsonOptions = null);

    /// <summary>
    /// Appends a single item as a JSON line to a newline-delimited JSON (NDJSON) file.
    /// Uses native Azure append operations for high performance.
    /// </summary>
    /// <typeparam name="T">The type of the item to append.</typeparam>
    /// <param name="item">The item to append.</param>
    /// <param name="filePath">The path to the NDJSON file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentNullException">Thrown when item is null.</exception>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task AppendJsonLineAsync<T>(
        T item,
        string filePath,
        string fileSystemName);

    /// <summary>
    /// Appends multiple items as JSON lines to a newline-delimited JSON (NDJSON) file.
    /// Uses native Azure append operations for high performance.
    /// </summary>
    /// <typeparam name="T">The type of the items to append.</typeparam>
    /// <param name="items">The items to append.</param>
    /// <param name="filePath">The path to the NDJSON file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentNullException">Thrown when items is null.</exception>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task AppendJsonLinesAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName);

    /// <summary>
    /// Reads a newline-delimited JSON (NDJSON) file and deserializes it to a collection of objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize each line to.</typeparam>
    /// <param name="filePath">The path to the NDJSON file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>A collection of deserialized objects.</returns>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<IEnumerable<T>> ReadJsonLinesAsync<T>(
        string filePath,
        string fileSystemName);
}
