namespace LakeIO.Services;

/// <summary>
/// Service for general file system operations in Azure Data Lake Storage.
/// Handles listing, copying, moving, and deleting files and directories.
/// </summary>
public interface IFileOperationsService
{
    /// <summary>
    /// Lists files in a directory with optional filtering.
    /// </summary>
    /// <param name="directoryPath">The directory path to list files from.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="recursive">Whether to list files recursively in subdirectories. Default is false.</param>
    /// <param name="fileExtension">Optional. Filter by file extension (e.g., ".json", ".parquet").</param>
    /// <returns>A list of file paths.</returns>
    /// <exception cref="ArgumentException">Thrown when directoryPath or fileSystemName is invalid.</exception>
    Task<List<string>> ListFilesAsync(
        string directoryPath,
        string fileSystemName,
        bool recursive = false,
        string? fileExtension = null);

    /// <summary>
    /// Checks if a file exists in the specified location.
    /// </summary>
    /// <param name="filePath">The path to the file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>True if the file exists, otherwise false.</returns>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<bool> FileExistsAsync(
        string filePath,
        string fileSystemName);

    /// <summary>
    /// Deletes a file from the specified location.
    /// </summary>
    /// <param name="filePath">The path to the file to delete.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task DeleteFileAsync(
        string filePath,
        string fileSystemName);

    /// <summary>
    /// Deletes multiple files from the specified location.
    /// </summary>
    /// <param name="filePaths">The collection of file paths to delete.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="continueOnError">Whether to continue deleting other files if one fails. Default is true.</param>
    /// <returns>The number of files successfully deleted.</returns>
    /// <exception cref="ArgumentNullException">Thrown when filePaths is null.</exception>
    /// <exception cref="ArgumentException">Thrown when fileSystemName is invalid.</exception>
    Task<int> DeleteFilesAsync(
        IEnumerable<string> filePaths,
        string fileSystemName,
        bool continueOnError = true);

    /// <summary>
    /// Gets the size of a file in bytes.
    /// </summary>
    /// <param name="filePath">The path to the file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>The file size in bytes, or 0 if the file doesn't exist.</returns>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<long> GetFileSizeAsync(
        string filePath,
        string fileSystemName);

    /// <summary>
    /// Gets metadata properties of a file.
    /// </summary>
    /// <param name="filePath">The path to the file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>A FileMetadata object containing file properties, or null if file doesn't exist.</returns>
    /// <exception cref="ArgumentException">Thrown when filePath or fileSystemName is invalid.</exception>
    Task<FileMetadata?> GetFileMetadataAsync(
        string filePath,
        string fileSystemName);

    /// <summary>
    /// Checks if a directory exists in the specified location.
    /// </summary>
    /// <param name="directoryPath">The path to the directory.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>True if the directory exists, otherwise false.</returns>
    /// <exception cref="ArgumentException">Thrown when directoryPath or fileSystemName is invalid.</exception>
    Task<bool> DirectoryExistsAsync(
        string directoryPath,
        string fileSystemName);

    /// <summary>
    /// Creates a directory at the specified location.
    /// </summary>
    /// <param name="directoryPath">The path where the directory will be created.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown when directoryPath or fileSystemName is invalid.</exception>
    Task CreateDirectoryAsync(
        string directoryPath,
        string fileSystemName);

    /// <summary>
    /// Deletes a directory and optionally all its contents.
    /// </summary>
    /// <param name="directoryPath">The path to the directory to delete.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <param name="recursive">Whether to delete the directory and all its contents. Default is false.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown when directoryPath or fileSystemName is invalid.</exception>
    Task DeleteDirectoryAsync(
        string directoryPath,
        string fileSystemName,
        bool recursive = false);

    /// <summary>
    /// Renames (moves) a file atomically within the same file system.
    /// This operation is atomic and can be used to safely isolate files during processing.
    /// </summary>
    /// <param name="sourceFilePath">The current path to the file.</param>
    /// <param name="destinationFilePath">The new path for the file.</param>
    /// <param name="fileSystemName">The name of the file system (container).</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown when filePaths or fileSystemName is invalid.</exception>
    /// <exception cref="Azure.RequestFailedException">Thrown when the source file doesn't exist or destination already exists.</exception>
    Task RenameFileAsync(
        string sourceFilePath,
        string destinationFilePath,
        string fileSystemName);
}

/// <summary>
/// Represents metadata properties of a file in Azure Data Lake Storage.
/// </summary>
public class FileMetadata
{
    /// <summary>
    /// The full path to the file.
    /// </summary>
    public required string Path { get; init; }

    /// <summary>
    /// The file size in bytes.
    /// </summary>
    public long SizeInBytes { get; init; }

    /// <summary>
    /// The date and time when the file was created.
    /// </summary>
    public DateTimeOffset? CreatedOn { get; init; }

    /// <summary>
    /// The date and time when the file was last modified.
    /// </summary>
    public DateTimeOffset? LastModified { get; init; }

    /// <summary>
    /// The ETag of the file for concurrency control.
    /// </summary>
    public string? ETag { get; init; }

    /// <summary>
    /// Whether the path represents a directory.
    /// </summary>
    public bool IsDirectory { get; init; }
}
