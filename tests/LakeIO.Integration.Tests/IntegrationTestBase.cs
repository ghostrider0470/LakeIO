using Xunit;

namespace LakeIO.Integration.Tests;

/// <summary>
/// Abstract base class for integration tests that require a real Azure Data Lake connection.
/// Tests skip gracefully when <c>LAKEIO_TEST_CONNECTION_STRING</c> is not set.
/// </summary>
/// <remarks>
/// <para>
/// Two environment variables control integration test execution:
/// <list type="bullet">
///   <item><c>LAKEIO_TEST_CONNECTION_STRING</c> (required) -- Azure Storage connection string.</item>
///   <item><c>LAKEIO_TEST_FILESYSTEM</c> (required) -- name of an existing file system (container).</item>
/// </list>
/// Each test class gets a unique directory prefix (<c>test-{guid}/</c>) to avoid conflicts.
/// Teardown deletes all files created under that prefix (best-effort).
/// </para>
/// </remarks>
public abstract class IntegrationTestBase : IAsyncLifetime
{
    protected const string ConnectionStringEnvVar = "LAKEIO_TEST_CONNECTION_STRING";
    protected const string FileSystemEnvVar = "LAKEIO_TEST_FILESYSTEM";

    protected LakeClient Client { get; private set; } = null!;
    protected FileSystemClient FileSystem { get; private set; } = null!;

    /// <summary>
    /// Unique directory prefix for this test class instance.
    /// All test files should be created under this path.
    /// </summary>
    protected string TestDirectory { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvVar);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            Assert.Skip($"{ConnectionStringEnvVar} not set. Skipping integration tests.");
        }

        var fileSystemName = Environment.GetEnvironmentVariable(FileSystemEnvVar);
        if (string.IsNullOrWhiteSpace(fileSystemName))
        {
            Assert.Skip($"{FileSystemEnvVar} not set. Skipping integration tests.");
        }

        Client = new LakeClient(connectionString);
        FileSystem = Client.GetFileSystemClient(fileSystemName);
        TestDirectory = $"test-{Guid.NewGuid():N}";

        await ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (FileSystem is null)
            return;

        // Best-effort cleanup: list and delete all files under the test directory
        try
        {
            var options = new GetPathsOptions
            {
                Path = TestDirectory,
                Recursive = true
            };

            var paths = new List<PathItem>();
            await foreach (var path in FileSystem.Directory().GetPathsAsync(options))
            {
                paths.Add(path);
            }

            // Delete files first (leaf nodes), then directories
            foreach (var path in paths.Where(p => !p.IsDirectory).OrderByDescending(p => p.Name))
            {
                try
                {
                    await FileSystem.Files().DeleteAsync(path.Name);
                }
                catch
                {
                    // Best-effort cleanup -- ignore individual delete failures
                }
            }

            // Delete directories (deepest first)
            foreach (var path in paths.Where(p => p.IsDirectory).OrderByDescending(p => p.Name))
            {
                try
                {
                    var dirClient = FileSystem.GetDirectoryClient(path.Name);
                    await dirClient.DeleteAsync();
                }
                catch
                {
                    // Best-effort cleanup
                }
            }
        }
        catch
        {
            // Best-effort cleanup -- ignore listing failures
        }
    }

    /// <summary>
    /// Generates a unique file path within the test directory.
    /// </summary>
    /// <param name="extension">File extension (e.g., ".json", ".csv").</param>
    protected string UniquePath(string extension = ".json")
        => $"{TestDirectory}/{Guid.NewGuid():N}{extension}";
}
