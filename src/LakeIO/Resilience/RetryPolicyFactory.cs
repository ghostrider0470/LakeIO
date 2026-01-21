using LakeIO.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace LakeIO.Resilience;

/// <summary>
/// Factory for creating resilience policies with configurable retry strategies.
/// Implements exponential backoff with jitter for Azure Storage operations.
/// </summary>
public class RetryPolicyFactory : IRetryPolicyFactory
{
    private readonly ILogger<RetryPolicyFactory> _logger;
    private readonly RetryOptions _options;

    public RetryPolicyFactory(
        ILogger<RetryPolicyFactory> logger,
        IOptions<RetryOptions> options)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
    }

    public ResiliencePipeline<T> GetStorageRetryPolicy<T>()
    {
        return new ResiliencePipelineBuilder<T>()
            .AddRetry(new RetryStrategyOptions<T>
            {
                ShouldHandle = new PredicateBuilder<T>()
                    .Handle<Azure.RequestFailedException>(ex =>
                        ex.Status == 429 || // Too Many Requests
                        ex.Status == 503 || // Service Unavailable
                        ex.Status == 500)   // Internal Server Error
                    .Handle<TimeoutException>()
                    .Handle<IOException>(),
                MaxRetryAttempts = _options.MaxRetryAttempts,
                Delay = _options.Strategy == RetryStrategy.Exponential
                    ? TimeSpan.FromMilliseconds(_options.InitialDelayMs)
                    : TimeSpan.FromMilliseconds(_options.InitialDelayMs),
                BackoffType = _options.Strategy == RetryStrategy.Exponential
                    ? DelayBackoffType.Exponential
                    : DelayBackoffType.Constant,
                UseJitter = _options.UseJitter,
                MaxDelay = TimeSpan.FromMilliseconds(_options.MaxDelayMs),
                OnRetry = args =>
                {
                    _logger.LogWarning(
                        "Retry attempt {AttemptNumber} after {Delay}ms due to: {Exception}",
                        args.AttemptNumber,
                        args.RetryDelay.TotalMilliseconds,
                        args.Outcome.Exception?.Message ?? "Unknown error");
                    return ValueTask.CompletedTask;
                }
            })
            .Build();
    }

    public ResiliencePipeline GetFileOperationPolicy()
    {
        return new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder()
                    .Handle<Azure.RequestFailedException>(ex =>
                        ex.Status == 429 ||
                        ex.Status == 503 ||
                        ex.Status == 500 ||
                        ex.ErrorCode == "OperationTimedOut")
                    .Handle<TimeoutException>()
                    .Handle<IOException>(),
                MaxRetryAttempts = _options.MaxRetryAttempts,
                Delay = TimeSpan.FromMilliseconds(_options.InitialDelayMs),
                BackoffType = _options.Strategy == RetryStrategy.Exponential
                    ? DelayBackoffType.Exponential
                    : DelayBackoffType.Constant,
                UseJitter = _options.UseJitter,
                MaxDelay = TimeSpan.FromMilliseconds(_options.MaxDelayMs),
                OnRetry = args =>
                {
                    _logger.LogWarning(
                        "File operation retry {AttemptNumber} after {Delay}ms due to: {Exception}",
                        args.AttemptNumber,
                        args.RetryDelay.TotalMilliseconds,
                        args.Outcome.Exception?.Message ?? "Unknown error");
                    return ValueTask.CompletedTask;
                }
            })
            .Build();
    }

    public ResiliencePipeline GetMetadataOperationPolicy()
    {
        return new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder()
                    .Handle<Azure.RequestFailedException>(ex =>
                        ex.Status == 429 ||
                        ex.Status == 503 ||
                        ex.Status == 500)
                    .Handle<TimeoutException>(),
                MaxRetryAttempts = Math.Min(_options.MaxRetryAttempts, 3), // Fewer retries for metadata
                Delay = TimeSpan.FromMilliseconds(_options.InitialDelayMs / 2), // Faster retry for lightweight operations
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = _options.UseJitter,
                MaxDelay = TimeSpan.FromMilliseconds(_options.MaxDelayMs / 2),
                OnRetry = args =>
                {
                    _logger.LogDebug(
                        "Metadata operation retry {AttemptNumber} after {Delay}ms",
                        args.AttemptNumber,
                        args.RetryDelay.TotalMilliseconds);
                    return ValueTask.CompletedTask;
                }
            })
            .Build();
    }
}
