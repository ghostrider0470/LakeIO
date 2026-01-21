using LakeIO.Configuration;
using LakeIO.Validation;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace LakeIO.Services;

/// <summary>
/// Implementation of general file operations for Azure Data Lake Storage.
/// Provides file and directory management capabilities.
/// </summary>
public class FileOperationsService : IFileOperationsService
{
    private readonly ILogger<FileOperationsService> _logger;
    private readonly IDataLakeClientManager _clientManager;
    private readonly LakeOptions _options;

    public FileOperationsService(
        ILogger<FileOperationsService> logger,
        IDataLakeClientManager clientManager,
        IOptions<LakeOptions> options)
    {
        _logger = logger.ThrowIfNull();
        _clientManager = clientManager.ThrowIfNull();
        _options = options?.Value.ThrowIfNull() ?? throw new ArgumentNullException(nameof(options));
    }

    public async Task<List<string>> ListFilesAsync(
        string directoryPath,
        string fileSystemName,
        bool recursive = false,
        string? fileExtension = null)
    {
        directoryPath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Listing files in {DirectoryPath}, recursive: {Recursive}, extension: {Extension}",
                directoryPath, recursive, fileExtension ?? "all");
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var files = new List<string>();

        try
        {
            await foreach (var pathItem in fileSystemClient.GetPathsAsync(directoryPath, recursive))
            {
                if (pathItem.IsDirectory == false)
                {
                    if (string.IsNullOrEmpty(fileExtension) ||
                        pathItem.Name.EndsWith(fileExtension, StringComparison.OrdinalIgnoreCase))
                    {
                        files.Add(pathItem.Name);
                    }
                }
            }
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
        {
            _logger.LogDebug("Directory does not exist, returning empty list: {DirectoryPath}", directoryPath);
            return files;
        }

        _logger.LogInformation("Found {Count} files in {DirectoryPath}", files.Count, directoryPath);

        return files;
    }

    public async Task<bool> FileExistsAsync(
        string filePath,
        string fileSystemName)
    {
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        return await fileClient.ExistsAsync();
    }

    public async Task DeleteFileAsync(
        string filePath,
        string fileSystemName)
    {
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Deleting file {FilePath} from file system {FileSystem}",
                filePath, fileSystemName);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        await fileClient.DeleteAsync();

        _logger.LogInformation("Successfully deleted file {FilePath}", filePath);
    }

    public async Task<int> DeleteFilesAsync(
        IEnumerable<string> filePaths,
        string fileSystemName,
        bool continueOnError = true)
    {
        filePaths.ThrowIfNullOrEmpty();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var filePathsList = filePaths.ToList();
        var successCount = 0;
        var failureCount = 0;

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Deleting {Count} files from file system {FileSystem}",
                filePathsList.Count, fileSystemName);
        }

        foreach (var filePath in filePathsList)
        {
            try
            {
                await DeleteFileAsync(filePath, fileSystemName);
                successCount++;
            }
            catch (Exception ex)
            {
                failureCount++;
                _logger.LogError(ex, "Failed to delete file {FilePath}", filePath);

                if (!continueOnError)
                {
                    throw;
                }
            }
        }

        _logger.LogInformation(
            "Deleted {SuccessCount} of {TotalCount} files. Failures: {FailureCount}",
            successCount, filePathsList.Count, failureCount);

        return successCount;
    }

    public async Task<long> GetFileSizeAsync(
        string filePath,
        string fileSystemName)
    {
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

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

    public async Task<FileMetadata?> GetFileMetadataAsync(
        string filePath,
        string fileSystemName)
    {
        filePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var fileClient = fileSystemClient.GetFileClient(filePath);

        try
        {
            var properties = await fileClient.GetPropertiesAsync();

            return new FileMetadata
            {
                Path = filePath,
                SizeInBytes = properties.Value.ContentLength,
                CreatedOn = properties.Value.CreatedOn,
                LastModified = properties.Value.LastModified,
                ETag = properties.Value.ETag.ToString(),
                IsDirectory = false
            };
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound" || ex.ErrorCode == "BlobNotFound")
        {
            _logger.LogWarning("File not found: {FilePath}", filePath);
            return null;
        }
    }

    public async Task<bool> DirectoryExistsAsync(
        string directoryPath,
        string fileSystemName)
    {
        directoryPath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);

        return await directoryClient.ExistsAsync();
    }

    public async Task CreateDirectoryAsync(
        string directoryPath,
        string fileSystemName)
    {
        directoryPath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Creating directory {DirectoryPath} in file system {FileSystem}",
                directoryPath, fileSystemName);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);

        await directoryClient.CreateIfNotExistsAsync();

        _logger.LogInformation("Successfully created directory {DirectoryPath}", directoryPath);
    }

    public async Task DeleteDirectoryAsync(
        string directoryPath,
        string fileSystemName,
        bool recursive = false)
    {
        directoryPath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Deleting directory {DirectoryPath}, recursive: {Recursive}",
                directoryPath, recursive);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var directoryClient = fileSystemClient.GetDirectoryClient(directoryPath);

        if (recursive)
        {
            // Delete recursively (including all contents)
            await directoryClient.DeleteAsync();
        }
        else
        {
            // Try to delete (will fail if directory is not empty)
            await directoryClient.DeleteAsync();
        }

        _logger.LogInformation("Successfully deleted directory {DirectoryPath}", directoryPath);
    }

    public async Task RenameFileAsync(
        string sourceFilePath,
        string destinationFilePath,
        string fileSystemName)
    {
        sourceFilePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        destinationFilePath.ThrowIfNullOrWhiteSpace().ValidateFilePath();
        fileSystemName.ThrowIfNullOrWhiteSpace().ValidateFileSystemName();

        if (_options.EnableDetailedLogging)
        {
            _logger.LogDebug("Renaming file from {SourcePath} to {DestinationPath}",
                sourceFilePath, destinationFilePath);
        }

        var fileSystemClient = _clientManager.GetOrCreateFileSystemClient(fileSystemName);
        var sourceFileClient = fileSystemClient.GetFileClient(sourceFilePath);

        try
        {
            // Perform atomic rename operation
            // Note: This will fail if destination already exists
            await sourceFileClient.RenameAsync(destinationFilePath);

            _logger.LogInformation("Successfully renamed file from {SourcePath} to {DestinationPath}",
                sourceFilePath, destinationFilePath);
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathNotFound")
        {
            _logger.LogWarning("Source file not found for rename: {SourcePath}", sourceFilePath);
            throw;
        }
        catch (Azure.RequestFailedException ex) when (ex.ErrorCode == "PathAlreadyExists")
        {
            _logger.LogError("Destination file already exists: {DestinationPath}", destinationFilePath);
            throw;
        }
    }
}
