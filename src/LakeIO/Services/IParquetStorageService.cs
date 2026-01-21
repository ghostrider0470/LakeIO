using LakeIO.Annotations;
using LakeIO.Formatters;
using LakeIO.Formatters.Interfaces;

namespace LakeIO.Services;

/// <summary>
/// Service for Parquet file operations in Azure Data Lake Storage.
/// Handles storing, reading, and managing Parquet files with optimal performance.
/// </summary>
public interface IParquetStorageService
{
    /// <summary>
    /// Stores a collection of items as a Parquet file.
    /// </summary>
    /// <typeparam name="T">The type of items to store. Must implement IParquetSerializable.</typeparam>
    /// <param name="items">The collection of items to store.</param>
    /// <param name="filePath">The path where the Parquet file will be stored.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="overwrite">Whether to overwrite the file if it exists. Default is true.</param>
    /// <returns>The full path of the stored Parquet file.</returns>
    /// <exception cref="ArgumentNullException">Thrown when items is null.</exception>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<string> StoreItemsAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName,
        bool overwrite = true) where T : IParquetSerializable<T>, new();

    /// <summary>
    /// Reads a Parquet file and deserializes it to a collection of objects.
    /// </summary>
    /// <typeparam name="T">The type to deserialize to. Must implement IParquetSerializable.</typeparam>
    /// <param name="filePath">The path to the Parquet file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>A collection of deserialized objects, or empty collection if file doesn't exist.</returns>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<IEnumerable<T>> ReadItemsAsync<T>(
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new();

    /// <summary>
    /// Updates a Parquet file by replacing its contents with new items.
    /// If the file doesn't exist, it will be created.
    /// </summary>
    /// <typeparam name="T">The type of items to store. Must implement IParquetSerializable.</typeparam>
    /// <param name="items">The collection of items to store.</param>
    /// <param name="filePath">The path to the Parquet file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>The full path of the updated Parquet file.</returns>
    /// <exception cref="ArgumentNullException">Thrown when items is null.</exception>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<string> UpdateFileAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new();

    /// <summary>
    /// Appends items to an existing Parquet file by reading, merging, and rewriting.
    /// If the file doesn't exist, it will be created with the new items.
    /// Note: This operation reads the entire file into memory.
    /// </summary>
    /// <typeparam name="T">The type of items to append. Must implement IParquetSerializable.</typeparam>
    /// <param name="items">The collection of items to append.</param>
    /// <param name="filePath">The path to the Parquet file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>The full path of the updated Parquet file.</returns>
    /// <exception cref="ArgumentNullException">Thrown when items is null.</exception>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<string> AppendItemsAsync<T>(
        IEnumerable<T> items,
        string filePath,
        string fileSystemName) where T : IParquetSerializable<T>, new();

    /// <summary>
    /// Gets the file size of a Parquet file in bytes.
    /// </summary>
    /// <param name="filePath">The path to the Parquet file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>The file size in bytes, or 0 if the file doesn't exist.</returns>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<long> GetFileSizeAsync(
        string filePath,
        string fileSystemName);

    /// <summary>
    /// Checks if a Parquet file size exceeds the configured warning threshold.
    /// </summary>
    /// <param name="filePath">The path to the Parquet file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>True if the file size exceeds the warning threshold, otherwise false.</returns>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<bool> IsFileSizeWarningAsync(
        string filePath,
        string fileSystemName);

    /// <summary>
    /// Checks if a Parquet file exists.
    /// </summary>
    /// <param name="filePath">The path to the Parquet file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>True if the file exists, otherwise false.</returns>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<bool> FileExistsAsync(
        string filePath,
        string fileSystemName);
}
