using Azure;
using Azure.Core;
using Polly;
using Polly.Retry;

namespace LakeIO;

/// <summary>
/// Internal retry helper that wraps a Polly <see cref="ResiliencePipeline"/> built from
/// <see cref="LakeRetryOptions"/>. Used to retry operations for application-level status
/// codes (e.g. 409 Conflict) that the Azure SDK transport layer does not retry.
/// </summary>
internal class RetryHelper
{
    private readonly ResiliencePipeline _pipeline;

    /// <summary>
    /// Creates a new <see cref="RetryHelper"/> from the given retry options.
    /// When <see cref="LakeRetryOptions.AdditionalRetryStatusCodes"/> is empty,
    /// the pipeline is <see cref="ResiliencePipeline.Empty"/> (zero overhead).
    /// </summary>
    internal RetryHelper(LakeRetryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.AdditionalRetryStatusCodes.Count == 0)
        {
            _pipeline = ResiliencePipeline.Empty;
            return;
        }

        var statusCodes = new HashSet<int>(options.AdditionalRetryStatusCodes);

        _pipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder()
                    .Handle<RequestFailedException>(ex => statusCodes.Contains(ex.Status)),
                MaxRetryAttempts = options.MaxRetries,
                Delay = options.Delay,
                MaxDelay = options.MaxDelay,
                BackoffType = options.RetryMode == RetryMode.Fixed
                    ? DelayBackoffType.Constant
                    : DelayBackoffType.Exponential,
                UseJitter = true,
            })
            .Build();
    }

    /// <summary>
    /// Executes the given operation through the retry pipeline.
    /// </summary>
    internal async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, ValueTask<T>> operation,
        CancellationToken cancellationToken = default)
    {
        return await _pipeline.ExecuteAsync(operation, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the given void operation through the retry pipeline.
    /// </summary>
    internal async Task ExecuteAsync(
        Func<CancellationToken, ValueTask> operation,
        CancellationToken cancellationToken = default)
    {
        await _pipeline.ExecuteAsync(operation, cancellationToken).ConfigureAwait(false);
    }
}
