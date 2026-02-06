using Azure.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace LakeIO;

/// <summary>
/// Extension methods for registering <see cref="LakeClient"/> with an
/// <see cref="IServiceCollection"/>.
/// </summary>
/// <remarks>
/// <para>
/// Use these methods in your application startup to register a singleton
/// <see cref="LakeClient"/> instance. All overloads use
/// <see cref="ServiceCollectionDescriptorExtensions.TryAddSingleton{TService}(IServiceCollection, TService)"/>
/// for idempotent registration -- calling <c>AddLakeIO</c> multiple times is safe
/// and the first registration wins.
/// </para>
/// <para>
/// Connection string example:
/// <code>
/// services.AddLakeIO("DefaultEndpointsProtocol=https;AccountName=...");
/// </code>
/// </para>
/// <para>
/// TokenCredential example:
/// <code>
/// services.AddLakeIO(
///     new Uri("https://myaccount.dfs.core.windows.net"),
///     new DefaultAzureCredential());
/// </code>
/// </para>
/// <para>
/// IConfiguration example:
/// <code>
/// services.AddLakeIO(configuration.GetSection("LakeIO"));
/// </code>
/// </para>
/// </remarks>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a singleton <see cref="LakeClient"/> using a connection string with
    /// default <see cref="LakeClientOptions"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">
    /// The Azure Storage connection string for the Data Lake account.
    /// </param>
    /// <returns>The <paramref name="services"/> instance for chaining.</returns>
    public static IServiceCollection AddLakeIO(this IServiceCollection services, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        services.TryAddSingleton(new LakeClient(connectionString));
        return services;
    }

    /// <summary>
    /// Registers a singleton <see cref="LakeClient"/> using a service URI and
    /// <see cref="TokenCredential"/> with default <see cref="LakeClientOptions"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="serviceUri">
    /// The Data Lake service URI (e.g., <c>https://accountname.dfs.core.windows.net</c>).
    /// </param>
    /// <param name="credential">The token credential for authentication.</param>
    /// <returns>The <paramref name="services"/> instance for chaining.</returns>
    public static IServiceCollection AddLakeIO(
        this IServiceCollection services,
        Uri serviceUri,
        TokenCredential credential)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(serviceUri);
        ArgumentNullException.ThrowIfNull(credential);

        services.TryAddSingleton(new LakeClient(serviceUri, credential));
        return services;
    }

    /// <summary>
    /// Registers a singleton <see cref="LakeClient"/> using an <see cref="IConfiguration"/>
    /// section that contains either a <c>ConnectionString</c> or <c>ServiceUri</c> value.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configuration">
    /// A configuration section containing a <c>ConnectionString</c> key (preferred) or a
    /// <c>ServiceUri</c> key. When <c>ServiceUri</c> is used, a <see cref="TokenCredential"/>
    /// must be registered in the service collection.
    /// </param>
    /// <returns>The <paramref name="services"/> instance for chaining.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when neither <c>ConnectionString</c> nor <c>ServiceUri</c> is found in the
    /// configuration section.
    /// </exception>
    public static IServiceCollection AddLakeIO(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.TryAddSingleton<LakeClient>(sp =>
        {
            var connectionString = configuration["ConnectionString"];
            if (!string.IsNullOrWhiteSpace(connectionString))
            {
                return new LakeClient(connectionString);
            }

            var serviceUri = configuration["ServiceUri"];
            if (!string.IsNullOrWhiteSpace(serviceUri))
            {
                var credential = sp.GetRequiredService<TokenCredential>();
                return new LakeClient(new Uri(serviceUri), credential);
            }

            throw new InvalidOperationException(
                "LakeIO configuration must contain a 'ConnectionString' or 'ServiceUri' value.");
        });

        return services;
    }

    /// <summary>
    /// Registers a singleton <see cref="LakeClient"/> using a connection string with
    /// custom <see cref="LakeClientOptions"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="connectionString">
    /// The Azure Storage connection string for the Data Lake account.
    /// </param>
    /// <param name="configureOptions">
    /// A delegate to configure <see cref="LakeClientOptions"/>.
    /// </param>
    /// <returns>The <paramref name="services"/> instance for chaining.</returns>
    public static IServiceCollection AddLakeIO(
        this IServiceCollection services,
        string connectionString,
        Action<LakeClientOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentNullException.ThrowIfNull(configureOptions);

        var options = new LakeClientOptions();
        configureOptions(options);
        services.TryAddSingleton(new LakeClient(connectionString, options));
        return services;
    }

    /// <summary>
    /// Registers a singleton <see cref="LakeClient"/> using a service URI and
    /// <see cref="TokenCredential"/> with custom <see cref="LakeClientOptions"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="serviceUri">
    /// The Data Lake service URI (e.g., <c>https://accountname.dfs.core.windows.net</c>).
    /// </param>
    /// <param name="credential">The token credential for authentication.</param>
    /// <param name="configureOptions">
    /// A delegate to configure <see cref="LakeClientOptions"/>.
    /// </param>
    /// <returns>The <paramref name="services"/> instance for chaining.</returns>
    public static IServiceCollection AddLakeIO(
        this IServiceCollection services,
        Uri serviceUri,
        TokenCredential credential,
        Action<LakeClientOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(serviceUri);
        ArgumentNullException.ThrowIfNull(credential);
        ArgumentNullException.ThrowIfNull(configureOptions);

        var options = new LakeClientOptions();
        configureOptions(options);
        services.TryAddSingleton(new LakeClient(serviceUri, credential, options));
        return services;
    }
}
