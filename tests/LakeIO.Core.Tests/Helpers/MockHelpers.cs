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
}
