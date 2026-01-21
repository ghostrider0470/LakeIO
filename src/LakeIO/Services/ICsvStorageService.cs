namespace LakeIO.Services;

/// <summary>
/// Service for CSV file operations in Azure Data Lake Storage.
/// Handles storing, reading, and managing CSV files.
/// </summary>
public interface ICsvStorageService
{
    /// <summary>
    /// Stores a collection of items as a CSV file.
    /// </summary>
    /// <typeparam name="T">The type of items to store.</typeparam>
    /// <param name="items">The collection of items to store.</param>
    /// <param name="filePath">The path where the CSV file will be stored.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="columnMapping">Optional. Dictionary mapping property names to CSV column names.</param>
    /// <param name="overwrite">Whether to overwrite the file if it exists. Default is true.</param>
    /// <returns>The full path of the stored CSV file.</returns>
    /// <exception cref="ArgumentNullException">Thrown when items is null.</exception>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<string> StoreItemsAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName,
        Dictionary<string, string>? columnMapping = null,
        bool overwrite = true);

    /// <summary>
    /// Reads a CSV file and deserializes it to a collection of objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to.</typeparam>
    /// <param name="filePath">The path to the CSV file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="columnMapping">Optional. Dictionary mapping CSV column names to property names.</param>
    /// <returns>A collection of deserialized objects, or empty collection if file doesn't exist.</returns>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<IEnumerable<T>> ReadItemsAsync<T>(
        string filePath,
        string fileSystemName,
        Dictionary<string, string>? columnMapping = null);

    /// <summary>
    /// Appends items to an existing CSV file.
    /// If the file doesn't exist, it will be created with headers.
    /// </summary>
    /// <typeparam name="T">The type of items to append.</typeparam>
    /// <param name="items">The collection of items to append.</param>
    /// <param name="filePath">The path to the CSV file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="columnMapping">Optional. Dictionary mapping property names to CSV column names.</param>
    /// <returns>The full path of the updated CSV file.</returns>
    /// <exception cref="ArgumentNullException">Thrown when items is null.</exception>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<string> AppendItemsAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName,
        Dictionary<string, string>? columnMapping = null);
}
