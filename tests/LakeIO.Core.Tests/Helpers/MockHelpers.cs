using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using NSubstitute;

namespace LakeIO.Tests.Helpers;

/// <summary>
/// Static helper methods for creating mock Azure SDK objects used across tests.
/// Reduces boilerplate when setting up NSubstitute mocks for DataLake clients and responses.
/// </summary>
public static class MockHelpers
{
    /// <summary>
    /// Creates a mock <see cref="Azure.Response"/> with the given HTTP status code.
    /// </summary>
    public static Azure.Response CreateMockRawResponse(int status = 200)
    {
        var response = Substitute.For<Azure.Response>();
        response.Status.Returns(status);
        return response;
    }

    /// <summary>
    /// Creates a <see cref="PathInfo"/> using the Azure SDK model factory with sensible test defaults.
    /// </summary>
    public static PathInfo CreateMockPathInfo(string? etag = null, DateTimeOffset? lastModified = null)
    {
        var etagValue = new ETag(etag ?? "test-etag");
        var lastModifiedValue = lastModified ?? DateTimeOffset.UtcNow;

        return DataLakeModelFactory.PathInfo(etagValue, lastModifiedValue);
    }

    /// <summary>
    /// Creates a full <see cref="Azure.Response{T}"/> of <see cref="PathInfo"/>
    /// suitable for mocking UploadAsync return values.
    /// </summary>
    public static Azure.Response<PathInfo> CreateUploadResponse(string? etag = null)
    {
        var pathInfo = CreateMockPathInfo(etag);
        var rawResponse = CreateMockRawResponse();

        return Azure.Response.FromValue(pathInfo, rawResponse);
    }

    /// <summary>
    /// Creates a mock <see cref="DataLakeFileSystemClient"/> with the given name.
    /// </summary>
    public static DataLakeFileSystemClient CreateMockFileSystemClient(string name = "test-fs")
    {
        var mock = Substitute.For<DataLakeFileSystemClient>();
        mock.Name.Returns(name);
        return mock;
    }

    /// <summary>
    /// Creates a mock <see cref="DataLakeFileClient"/>.
    /// </summary>
    public static DataLakeFileClient CreateMockFileClient(string path = "test-path")
    {
        var mock = Substitute.For<DataLakeFileClient>();
        mock.Path.Returns(path);
        mock.Name.Returns(System.IO.Path.GetFileName(path));
        return mock;
    }

    /// <summary>
    /// Wires a mock <see cref="DataLakeFileSystemClient"/> to return the given
    /// <see cref="DataLakeFileClient"/> when <c>GetFileClient</c> is called with any path.
    /// </summary>
    public static void SetupFileClientOnFsClient(
        DataLakeFileSystemClient mockFs,
        DataLakeFileClient mockFile)
    {
        mockFs.GetFileClient(Arg.Any<string>()).Returns(mockFile);
    }

    /// <summary>
    /// Creates a <see cref="FileDownloadDetails"/> via the DataLake model factory with sensible defaults.
    /// Required for constructing streaming/content results since the class has no public constructor.
    /// </summary>
    public static FileDownloadDetails CreateFileDownloadDetails()
    {
        return DataLakeModelFactory.FileDownloadDetails(
            lastModified: DateTimeOffset.UtcNow,
            metadata: new Dictionary<string, string>(),
            contentRange: null,
            eTag: new ETag("\"test-etag\""),
            contentEncoding: null,
            cacheControl: null,
            contentDisposition: null,
            contentLanguage: null,
            copyCompletionTime: default,
            copyStatusDescription: null,
            copyId: null,
            copyProgress: null,
            copySource: null,
            copyStatus: CopyStatus.Success,
            leaseDuration: DataLakeLeaseDuration.Infinite,
            leaseState: DataLakeLeaseState.Available,
            leaseStatus: DataLakeLeaseStatus.Unlocked,
            acceptRanges: null,
            isServerEncrypted: false,
            encryptionKeySha256: null,
            contentHash: null);
    }

    /// <summary>
    /// Creates a <see cref="DataLakeFileReadStreamingResult"/> via the DataLake model factory.
    /// Use for mocking ReadStreamingAsync return values.
    /// </summary>
    public static DataLakeFileReadStreamingResult CreateStreamingResult(Stream content)
    {
        var details = CreateFileDownloadDetails();
        return DataLakeModelFactory.DataLakeFileReadStreamingResult(content, details);
    }

    /// <summary>
    /// Creates a full <see cref="Azure.Response{T}"/> of <see cref="DataLakeFileReadStreamingResult"/>
    /// suitable for mocking ReadStreamingAsync return values.
    /// </summary>
    public static Azure.Response<DataLakeFileReadStreamingResult> CreateStreamingResponse(Stream content)
    {
        var result = CreateStreamingResult(content);
        var rawResponse = CreateMockRawResponse();
        return Azure.Response.FromValue(result, rawResponse);
    }

    /// <summary>
    /// Creates a <see cref="DataLakeFileReadResult"/> via the DataLake model factory.
    /// Use for mocking ReadContentAsync return values.
    /// </summary>
    public static Azure.Response<DataLakeFileReadResult> CreateContentResponse(BinaryData content)
    {
        var details = CreateFileDownloadDetails();
        var result = DataLakeModelFactory.DataLakeFileReadResult(content, details);
        var rawResponse = CreateMockRawResponse();
        return Azure.Response.FromValue(result, rawResponse);
    }

    /// <summary>
    /// Creates a <see cref="PathProperties"/> via the DataLake model factory with essential values.
    /// </summary>
    public static PathProperties CreatePathProperties(
        long contentLength = 1024,
        string contentType = "application/octet-stream",
        string? etag = null)
    {
        return DataLakeModelFactory.PathProperties(
            lastModified: DateTimeOffset.UtcNow,
            creationTime: DateTimeOffset.UtcNow.AddDays(-1),
            metadata: new Dictionary<string, string>(),
            copyCompletionTime: default,
            copyStatusDescription: null,
            copyId: null,
            copyProgress: null,
            copySource: null,
            copyStatus: CopyStatus.Success,
            isIncrementalCopy: false,
            leaseDuration: DataLakeLeaseDuration.Infinite,
            leaseState: DataLakeLeaseState.Available,
            leaseStatus: DataLakeLeaseStatus.Unlocked,
            contentLength: contentLength,
            contentType: contentType,
            eTag: new ETag(etag ?? "\"test-etag\""),
            contentHash: null,
            contentEncoding: null,
            contentDisposition: null,
            contentLanguage: null,
            cacheControl: null,
            acceptRanges: null,
            isServerEncrypted: false,
            encryptionKeySha256: null,
            accessTier: null,
            archiveStatus: null,
            accessTierChangeTime: default,
            isDirectory: false);
    }
}
