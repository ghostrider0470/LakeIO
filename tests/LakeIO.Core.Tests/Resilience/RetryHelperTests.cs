using Azure;
using Azure.Core;
using FluentAssertions;
using Xunit;

namespace LakeIO.Tests.Resilience;

public class RetryHelperTests
{
    [Fact]
    public async Task ExecuteAsync_RetriesOnConfiguredStatusCode()
    {
        var options = new LakeRetryOptions
        {
            AdditionalRetryStatusCodes = [409],
            MaxRetries = 2,
            Delay = TimeSpan.FromMilliseconds(10),
            MaxDelay = TimeSpan.FromMilliseconds(100),
        };
        var helper = new RetryHelper(options);
        var callCount = 0;

        var result = await helper.ExecuteAsync<int>(async ct =>
        {
            callCount++;
            if (callCount <= 2)
                throw new RequestFailedException(409, "Conflict");
            return 42;
        });

        result.Should().Be(42);
        callCount.Should().Be(3, "1 initial + 2 retries");
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotRetry_WhenStatusCodeNotConfigured()
    {
        var options = new LakeRetryOptions
        {
            AdditionalRetryStatusCodes = [409],
            Delay = TimeSpan.FromMilliseconds(10),
        };
        var helper = new RetryHelper(options);
        var callCount = 0;

        var act = () => helper.ExecuteAsync<int>(async ct =>
        {
            callCount++;
            throw new RequestFailedException(404, "Not Found");
        });

        var ex = await act.Should().ThrowAsync<RequestFailedException>();
        ex.Which.Status.Should().Be(404);
        callCount.Should().Be(1, "non-configured status code should not be retried");
    }

    [Fact]
    public async Task ExecuteAsync_NoOpWhenNoAdditionalStatusCodes()
    {
        var options = new LakeRetryOptions(); // empty AdditionalRetryStatusCodes
        var helper = new RetryHelper(options);
        var callCount = 0;

        var act = () => helper.ExecuteAsync<int>(async ct =>
        {
            callCount++;
            throw new RequestFailedException(409, "Conflict");
        });

        await act.Should().ThrowAsync<RequestFailedException>();
        callCount.Should().Be(1, "ResiliencePipeline.Empty should not retry");
    }

    [Fact]
    public async Task ExecuteAsync_ExhaustsRetriesThenThrows()
    {
        var options = new LakeRetryOptions
        {
            AdditionalRetryStatusCodes = [409],
            MaxRetries = 2,
            Delay = TimeSpan.FromMilliseconds(10),
            MaxDelay = TimeSpan.FromMilliseconds(100),
        };
        var helper = new RetryHelper(options);
        var callCount = 0;

        var act = () => helper.ExecuteAsync<int>(async ct =>
        {
            callCount++;
            throw new RequestFailedException(409, "Conflict");
        });

        await act.Should().ThrowAsync<RequestFailedException>();
        callCount.Should().Be(3, "1 initial + 2 retries before giving up");
    }

    [Fact]
    public async Task ExecuteAsync_VoidOverload_Retries()
    {
        var options = new LakeRetryOptions
        {
            AdditionalRetryStatusCodes = [409],
            MaxRetries = 2,
            Delay = TimeSpan.FromMilliseconds(10),
            MaxDelay = TimeSpan.FromMilliseconds(100),
        };
        var helper = new RetryHelper(options);
        var callCount = 0;

        await helper.ExecuteAsync(async ct =>
        {
            callCount++;
            if (callCount <= 2)
                throw new RequestFailedException(409, "Conflict");
        });

        callCount.Should().Be(3, "1 initial + 2 retries for void overload");
    }

    [Fact]
    public async Task ExecuteAsync_PassesCancellationToken()
    {
        var options = new LakeRetryOptions
        {
            AdditionalRetryStatusCodes = [409],
            Delay = TimeSpan.FromMilliseconds(10),
        };
        var helper = new RetryHelper(options);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = () => helper.ExecuteAsync<int>(async ct =>
        {
            ct.ThrowIfCancellationRequested();
            return 42;
        }, cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task ExecuteAsync_FixedRetryMode_UsesConstantDelay()
    {
        var options = new LakeRetryOptions
        {
            AdditionalRetryStatusCodes = [409],
            MaxRetries = 1,
            Delay = TimeSpan.FromMilliseconds(50),
            MaxDelay = TimeSpan.FromMilliseconds(200),
            RetryMode = RetryMode.Fixed,
        };
        var helper = new RetryHelper(options);
        var callCount = 0;

        var result = await helper.ExecuteAsync<int>(async ct =>
        {
            callCount++;
            if (callCount <= 1)
                throw new RequestFailedException(409, "Conflict");
            return 99;
        });

        result.Should().Be(99);
        callCount.Should().Be(2, "1 initial + 1 retry in fixed mode");
    }
}
