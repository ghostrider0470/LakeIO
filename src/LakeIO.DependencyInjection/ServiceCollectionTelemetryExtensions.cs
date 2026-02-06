using LakeIO.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace LakeIO;

/// <summary>
/// Extension methods for registering LakeIO telemetry components with an
/// <see cref="IServiceCollection"/>.
/// </summary>
/// <remarks>
/// <para>
/// Use <see cref="AddLakeIOTelemetry"/> to register the <see cref="CostEstimator"/>
/// singleton and optionally configure <see cref="ObservabilityOptions"/> via the
/// options pattern. This is separate from the OTel-provider-level
/// <c>AddLakeIOInstrumentation()</c> extensions on <c>TracerProviderBuilder</c> and
/// <c>MeterProviderBuilder</c> (in the <c>LakeIO.Telemetry</c> package).
/// </para>
/// <para>
/// Example usage:
/// <code>
/// services.AddLakeIO("connection-string")
///         .AddLakeIOTelemetry(opts => opts.EnableCostEstimation = true);
/// </code>
/// </para>
/// </remarks>
public static class ServiceCollectionTelemetryExtensions
{
    /// <summary>
    /// Registers LakeIO telemetry components: a singleton <see cref="CostEstimator"/>
    /// and optionally configures <see cref="ObservabilityOptions"/> via the options pattern.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureOptions">
    /// An optional delegate to configure <see cref="ObservabilityOptions"/>. When
    /// <see langword="null"/>, default options are used (cost estimation disabled,
    /// <see cref="StorageTier.Hot"/> tier).
    /// </param>
    /// <returns>The <paramref name="services"/> instance for chaining.</returns>
    public static IServiceCollection AddLakeIOTelemetry(
        this IServiceCollection services,
        Action<ObservabilityOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configureOptions is not null)
        {
            services.Configure(configureOptions);
        }

        services.TryAddSingleton<CostEstimator>();
        return services;
    }
}
