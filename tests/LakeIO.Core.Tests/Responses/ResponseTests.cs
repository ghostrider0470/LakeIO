using FluentAssertions;
using LakeIO.Tests.Helpers;
using Xunit;

namespace LakeIO.Tests.Responses;

public class ResponseTests
{
    [Fact]
    public void Constructor_SetsValueAndRawResponse()
    {
        var rawResponse = MockHelpers.CreateMockRawResponse(200);
        var value = new StorageResult { Path = "test/file.json" };

        var response = new Response<StorageResult>(value, rawResponse);

        response.Value.Should().BeSameAs(value);
        response.RawResponse.Should().BeSameAs(rawResponse);
    }

    [Fact]
    public void Constructor_WithNullRawResponse_ThrowsArgumentNullException()
    {
        var value = new StorageResult { Path = "test/file.json" };

        var act = () => new Response<StorageResult>(value, null!);

        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("rawResponse");
    }

    [Fact]
    public void Constructor_WithNullValue_DoesNotThrow()
    {
        var rawResponse = MockHelpers.CreateMockRawResponse(200);

        var response = new Response<StorageResult?>(null, rawResponse);

        response.Value.Should().BeNull();
    }

    [Fact]
    public void GetRawResponse_ReturnsSameAsRawResponseProperty()
    {
        var rawResponse = MockHelpers.CreateMockRawResponse(200);
        var value = new StorageResult { Path = "test.json" };

        var response = new Response<StorageResult>(value, rawResponse);

        response.GetRawResponse().Should().BeSameAs(response.RawResponse);
    }

    [Fact]
    public void ImplicitConversion_ReturnsValue()
    {
        var rawResponse = MockHelpers.CreateMockRawResponse(200);
        var sr = new StorageResult { Path = "test.json" };
        var response = new Response<StorageResult>(sr, rawResponse);

        StorageResult result = response;

        result.Should().BeSameAs(sr);
    }

    [Fact]
    public void ImplicitConversion_WithNullResponse_ThrowsArgumentNullException()
    {
        Response<StorageResult> response = null!;

        var act = () =>
        {
            StorageResult _ = response;
        };

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void ImplicitConversion_WithValueType_ReturnsValue()
    {
        var rawResponse = MockHelpers.CreateMockRawResponse(200);
        var response = new Response<int>(42, rawResponse);

        int result = response;

        result.Should().Be(42);
    }

    [Fact]
    public void ImplicitConversion_WithBoolValue_ReturnsValue()
    {
        var rawResponse = MockHelpers.CreateMockRawResponse(200);
        var response = new Response<bool>(true, rawResponse);

        bool result = response;

        result.Should().BeTrue();
    }
}
