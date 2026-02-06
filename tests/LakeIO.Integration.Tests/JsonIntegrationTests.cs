using Azure;
using FluentAssertions;
using Xunit;

namespace LakeIO.Integration.Tests;

/// <summary>
/// Integration tests for JSON read/write operations against a real Azure Data Lake account.
/// Skips gracefully when <c>LAKEIO_TEST_CONNECTION_STRING</c> is not set.
/// </summary>
public class JsonIntegrationTests : IntegrationTestBase
{
    [Fact]
    public async Task WriteAndRead_RoundTrip()
    {
        // Arrange
        var path = UniquePath();
        var expected = new TestRecord { Id = 42, Name = "Integration Test" };

        // Act
        var writeResult = await FileSystem.Json().WriteAsync(path, expected);
        var readResult = await FileSystem.Json().ReadAsync<TestRecord>(path);

        // Assert
        writeResult.Value.Path.Should().Contain(path);
        readResult.Value.Should().NotBeNull();
        readResult.Value!.Id.Should().Be(42);
        readResult.Value.Name.Should().Be("Integration Test");
    }

    [Fact]
    public async Task AppendAndReadNdjson_RoundTrip()
    {
        // Arrange
        var path = UniquePath(".ndjson");
        var records = new[]
        {
            new TestRecord { Id = 1, Name = "First" },
            new TestRecord { Id = 2, Name = "Second" },
            new TestRecord { Id = 3, Name = "Third" }
        };

        // Act
        foreach (var record in records)
        {
            await FileSystem.Json().AppendNdjsonAsync(path, record);
        }

        var results = new List<TestRecord>();
        await foreach (var item in FileSystem.Json().ReadNdjsonAsync<TestRecord>(path))
        {
            results.Add(item);
        }

        // Assert
        results.Should().HaveCount(3);
        results[0].Id.Should().Be(1);
        results[0].Name.Should().Be("First");
        results[1].Id.Should().Be(2);
        results[1].Name.Should().Be("Second");
        results[2].Id.Should().Be(3);
        results[2].Name.Should().Be("Third");
    }

    [Fact]
    public async Task WriteAsync_OverwriteFalse_SecondWriteThrows()
    {
        // Arrange
        var path = UniquePath();
        var record = new TestRecord { Id = 1, Name = "Once" };

        // Act -- first write succeeds
        await FileSystem.Json().WriteAsync(path, record, overwrite: false);

        // Act & Assert -- second write with overwrite:false should throw
        var act = async () => await FileSystem.Json().WriteAsync(path, record, overwrite: false);
        await act.Should().ThrowAsync<RequestFailedException>();
    }
}

/// <summary>
/// Simple test record for JSON serialization round-trip tests.
/// </summary>
public class TestRecord
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
