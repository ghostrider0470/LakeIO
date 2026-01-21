using LakeIO.Annotations;
using LakeIO.Formatters;
using LakeIO.Formatters.Interfaces;

namespace LakeIO.Services;

/// <summary>
/// Service for buffer file operations in Azure Data Lake Storage.
/// Implements the Hybrid Buffer + Parquet strategy for high-throughput write operations.
/// </summary>
public interface IBufferStorageService
{
    /// <summary>
    /// Compacts a JSON lines buffer file into a Parquet file.
    /// Optionally merges with existing Parquet data and deletes the buffer after compaction.
    /// </summary>
    /// <typeparam name="T">The type of items in the buffer. Must implement IParquetSerializable.</typeparam>
    /// <param name="bufferFilePath">The path to the buffer file (.ndjson).</param>
    /// <param name="parquetFilePath">The path to the target Parquet file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="deleteBuffer">Whether to delete the buffer file after successful compaction. Default is true.</param>
    /// <returns>The full path of the resulting Parquet file.</returns>
    /// <exception cref="ArgumentException">Thrown when file paths or fileSystemName is invalid.</exception>
    Task<string> CompactBufferToParquetAsync<T>(
        string bufferFilePath,
        string parquetFilePath,
        string fileSystemName,
        bool deleteBuffer = true) where T : IParquetSerializable<T>, new();

    /// <summary>
    /// Lists all buffer files in a directory matching the configured buffer file suffix.
    /// </summary>
    /// <param name="directoryPath">The directory path to search for buffer files.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="recursive">Whether to search recursively in subdirectories. Default is true.</param>
    /// <returns>A list of buffer file paths.</returns>
    /// <exception cref="ArgumentException">Thrown when directoryPath or fileSystemName is invalid.</exception>
    Task<List<string>> ListBufferFilesAsync(
        string directoryPath,
        string fileSystemName,
        bool recursive = true);

    /// <summary>
    /// Gets the size of a buffer file in bytes.
    /// </summary>
    /// <param name="bufferFilePath">The path to the buffer file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>The file size in bytes, or 0 if the file doesn't exist.</returns>
    /// <exception cref="ArgumentException">Thrown when bufferFilePath or fileSystemName is invalid.</exception>
    Task<long> GetBufferSizeAsync(
        string bufferFilePath,
        string fileSystemName);

    /// <summary>
    /// Checks if a buffer file size exceeds the configured auto-compaction threshold.
    /// </summary>
    /// <param name="bufferFilePath">The path to the buffer file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>True if the buffer size exceeds the threshold, otherwise false.</returns>
    /// <exception cref="ArgumentException">Thrown when bufferFilePath or fileSystemName is invalid.</exception>
    Task<bool> ShouldCompactAsync(
        string bufferFilePath,
        string fileSystemName);

    /// <summary>
    /// Deletes a buffer file.
    /// </summary>
    /// <param name="bufferFilePath">The path to the buffer file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown when bufferFilePath or fileSystemName is invalid.</exception>
    Task DeleteBufferAsync(
        string bufferFilePath,
        string fileSystemName);

    /// <summary>
    /// Generates a buffer file path from a Parquet file path by replacing the extension.
    /// </summary>
    /// <param name="parquetFilePath">The Parquet file path.</param>
    /// <returns>The corresponding buffer file path.</returns>
    /// <exception cref="ArgumentException">Thrown when parquetFilePath is invalid.</exception>
    string GetBufferPathFromParquetPath(string parquetFilePath);

    /// <summary>
    /// Generates a Parquet file path from a buffer file path by replacing the suffix.
    /// </summary>
    /// <param name="bufferFilePath">The buffer file path.</param>
    /// <returns>The corresponding Parquet file path.</returns>
    /// <exception cref="ArgumentException">Thrown when bufferFilePath is invalid.</exception>
    string GetParquetPathFromBufferPath(string bufferFilePath);

    /// <summary>
    /// Checks if a file path matches the configured buffer file suffix.
    /// </summary>
    /// <param name="filePath">The file path to check.</param>
    /// <returns>True if the file path is a buffer file, otherwise false.</returns>
    bool IsBufferFile(string filePath);
}
