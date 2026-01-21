using LakeIO.Configuration;
using LakeIO.Formatters.Interfaces;
using LakeIO.Formatters.Json;
using LakeIO.Formatters.Parquet;
using LakeIO.Formatters.Csv;
using LakeIO.Resilience;
using LakeIO.Services;
using LakeIO.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace LakeIO.Extensions;

/// <summary>
/// Extension methods for registering LakeIO services with dependency injection.
/// Simplifies service registration with default implementations.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds all LakeIO services to the service collection.
    /// Configuration should be provided via IOptions pattern in appsettings.json.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddLakeIO(this IServiceCollection services)
    {
        // Register core infrastructure
        services.TryAddSingleton<IDataLakeClientManager, DataLakeClientManager>();
        services.TryAddSingleton<IRetryPolicyFactory, RetryPolicyFactory>();
        services.TryAddSingleton<IStorageMetrics, LoggerStorageMetrics>();
        services.TryAddSingleton<IFileSizeManager, FileSizeManager>();

        // Register formatters
        services.TryAddSingleton<IFileFormatter, SystemTextJsonFormatter>();
        services.TryAddSingleton<IParquetFileFormatter, ParquetFileFormatter>();
        services.TryAddSingleton<ICsvFileFormatter, CsvFileFormatter>();

        // Register services
        services.TryAddSingleton<IJsonStorageService, JsonStorageService>();
        services.TryAddSingleton<IParquetStorageService, ParquetStorageService>();
        services.TryAddSingleton<ICsvStorageService, CsvStorageService>();
        services.TryAddSingleton<IBufferStorageService, BufferStorageService>();
        services.TryAddSingleton<IFileOperationsService, FileOperationsService>();

        return services;
    }
}
