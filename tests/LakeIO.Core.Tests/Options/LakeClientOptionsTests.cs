using Azure.Storage.Files.DataLake;
using FluentAssertions;
using Xunit;

namespace LakeIO.Tests.Options;

public class LakeClientOptionsTests
{
    private readonly LakeClientOptions _sut = new();

    [Fact]
    public void Defaults_JsonSerializerOptions_IsNull()
    {
        // Default is null -- System.Text.Json defaults are used when null
        _sut.JsonSerializerOptions.Should().BeNull();
    }

    [Fact]
    public void Defaults_Retry_IsNotNull()
    {
        _sut.Retry.Should().NotBeNull();
    }

    [Fact]
    public void Defaults_Csv_IsNotNull()
    {
        _sut.Csv.Should().NotBeNull();
    }

    [Fact]
    public void Defaults_DefaultParquetCompression_IsSnappy()
    {
        _sut.DefaultParquetCompression.Should().Be("Snappy");
    }

    [Fact]
    public void Defaults_DefaultParquetRowGroupSize_Is10000()
    {
        _sut.DefaultParquetRowGroupSize.Should().Be(10_000);
    }

    [Fact]
    public void Defaults_ChunkedUploadThreshold_Is4MB()
    {
        _sut.ChunkedUploadThreshold.Should().Be(4 * 1024 * 1024);
    }

    [Fact]
    public void Defaults_OperationTimeout_Is5Minutes()
    {
        _sut.OperationTimeout.Should().Be(TimeSpan.FromMinutes(5));
    }

    [Fact]
    public void Defaults_EnableDiagnostics_IsFalse()
    {
        _sut.EnableDiagnostics.Should().BeFalse();
    }

    [Fact]
    public void ToDataLakeClientOptions_ReturnsNonNull()
    {
        var result = _sut.ToDataLakeClientOptions();

        result.Should().NotBeNull();
        result.Should().BeOfType<DataLakeClientOptions>();
    }

    [Fact]
    public void ToDataLakeClientOptions_CopiesRetrySettings()
    {
        _sut.Retry.MaxRetries.Should().BeGreaterThan(0, "default should have retries configured");

        var azureOptions = _sut.ToDataLakeClientOptions();

        azureOptions.Retry.MaxRetries.Should().Be(_sut.Retry.MaxRetries);
        azureOptions.Retry.Delay.Should().Be(_sut.Retry.Delay);
        azureOptions.Retry.MaxDelay.Should().Be(_sut.Retry.MaxDelay);
    }

    [Fact]
    public void Properties_AreSettable()
    {
        _sut.DefaultParquetCompression = "Gzip";
        _sut.DefaultParquetRowGroupSize = 50_000;
        _sut.ChunkedUploadThreshold = 8 * 1024 * 1024;
        _sut.EnableDiagnostics = true;

        _sut.DefaultParquetCompression.Should().Be("Gzip");
        _sut.DefaultParquetRowGroupSize.Should().Be(50_000);
        _sut.ChunkedUploadThreshold.Should().Be(8 * 1024 * 1024);
        _sut.EnableDiagnostics.Should().BeTrue();
    }
}
