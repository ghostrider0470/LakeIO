using Azure.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace LakeIO;

/// <summary>
/// Fluent builder for registering multiple named <see cref="LakeClient"/> instances
/// with shared default options and per-client overrides.
/// </summary>
/// <remarks>
/// <para>
/// Use the builder API via <c>services.AddLakeIO(builder => { ... })</c> when you need
/// to configure defaults shared across clients or register multiple named clients in a
/// single block:
/// </para>
/// <code>
/// services.AddLakeIO(builder =>
/// {
///     builder.ConfigureDefaults(opts => opts.EnableDiagnostics = true);
///     builder.AddClient("account-connection-string");
///     builder.AddClient("hot", hotConnectionString);
///     builder.AddClient("cold", coldConnectionString, opts => opts.OperationTimeout = TimeSpan.FromMinutes(10));
/// });
/// </code>
/// </remarks>
public sealed class LakeIOBuilder
{
    private readonly IServiceCollection _services;
    private Action<LakeClientOptions>? _configureDefaults;
    private readonly HashSet<string> _registeredNames = new(StringComparer.Ordinal);

    /// <summary>
    /// Initializes a new instance of the <see cref="LakeIOBuilder"/> class.
    /// </summary>
    /// <param name="services">The service collection to register clients into.</param>
    internal LakeIOBuilder(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _services = services;
    }

    /// <summary>
    /// Configures default <see cref="LakeClientOptions"/> applied to all clients registered
    /// through this builder. Per-client overrides are applied on top of these defaults.
    /// </summary>
    /// <param name="configureOptions">A delegate to configure the default options.</param>
    /// <returns>This builder instance for chaining.</returns>
    /// <remarks>
    /// Multiple calls overwrite the previous delegate (last one wins).
    /// </remarks>
    public LakeIOBuilder ConfigureDefaults(Action<LakeClientOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(configureOptions);
        _configureDefaults = configureOptions;
        return this;
    }

    /// <summary>
    /// Registers the default (unnamed) <see cref="LakeClient"/> using a connection string.
    /// </summary>
    /// <param name="connectionString">Azure Storage connection string.</param>
    /// <returns>This builder instance for chaining.</returns>
    /// <remarks>
    /// <para>
    /// This registers the client both as a non-keyed singleton (for backward compatibility
    /// with constructor injection of <see cref="LakeClient"/>) and as a keyed singleton
    /// under <see cref="ILakeClientFactory.DefaultName"/> (for factory resolution).
    /// </para>
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a default client has already been registered through this builder.
    /// </exception>
    public LakeIOBuilder AddClient(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        GuardDuplicate(ILakeClientFactory.DefaultName);

        var options = BuildOptions(null);
        var client = new LakeClient(connectionString, options);

        // Non-keyed: backward compat with plain LakeClient injection
        _services.TryAddSingleton(client);

        // Keyed: allows factory.CreateClient(ILakeClientFactory.DefaultName)
        _services.TryAddKeyedSingleton<LakeClient>(
            ILakeClientFactory.DefaultName,
            (sp, key) => sp.GetRequiredService<LakeClient>());

        return this;
    }

    /// <summary>
    /// Registers a named <see cref="LakeClient"/> using a connection string with optional
    /// per-client option overrides.
    /// </summary>
    /// <param name="name">
    /// A unique name identifying this client. Used with
    /// <see cref="ILakeClientFactory.CreateClient"/> or <c>[FromKeyedServices(name)]</c>.
    /// </param>
    /// <param name="connectionString">Azure Storage connection string.</param>
    /// <param name="configureOptions">
    /// An optional delegate to configure per-client <see cref="LakeClientOptions"/>.
    /// These overrides are applied on top of any defaults set via
    /// <see cref="ConfigureDefaults"/>.
    /// </param>
    /// <returns>This builder instance for chaining.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a client with the specified <paramref name="name"/> has already been registered.
    /// </exception>
    public LakeIOBuilder AddClient(
        string name,
        string connectionString,
        Action<LakeClientOptions>? configureOptions = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        GuardDuplicate(name);

        var options = BuildOptions(configureOptions);
        _services.AddKeyedSingleton<LakeClient>(
            name,
            (sp, key) => new LakeClient(connectionString, options));

        return this;
    }

    /// <summary>
    /// Registers a named <see cref="LakeClient"/> using a service URI and
    /// <see cref="TokenCredential"/> with optional per-client option overrides.
    /// </summary>
    /// <param name="name">
    /// A unique name identifying this client. Used with
    /// <see cref="ILakeClientFactory.CreateClient"/> or <c>[FromKeyedServices(name)]</c>.
    /// </param>
    /// <param name="serviceUri">
    /// The Data Lake service URI (e.g., <c>https://accountname.dfs.core.windows.net</c>).
    /// </param>
    /// <param name="credential">The token credential for authentication.</param>
    /// <param name="configureOptions">
    /// An optional delegate to configure per-client <see cref="LakeClientOptions"/>.
    /// These overrides are applied on top of any defaults set via
    /// <see cref="ConfigureDefaults"/>.
    /// </param>
    /// <returns>This builder instance for chaining.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a client with the specified <paramref name="name"/> has already been registered.
    /// </exception>
    public LakeIOBuilder AddClient(
        string name,
        Uri serviceUri,
        TokenCredential credential,
        Action<LakeClientOptions>? configureOptions = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(serviceUri);
        ArgumentNullException.ThrowIfNull(credential);
        GuardDuplicate(name);

        var options = BuildOptions(configureOptions);
        _services.AddKeyedSingleton<LakeClient>(
            name,
            (sp, key) => new LakeClient(serviceUri, credential, options));

        return this;
    }

    /// <summary>
    /// Builds a fresh <see cref="LakeClientOptions"/> instance with defaults and optional
    /// per-client overrides applied.
    /// </summary>
    private LakeClientOptions BuildOptions(Action<LakeClientOptions>? configureOverrides)
    {
        var options = new LakeClientOptions();
        _configureDefaults?.Invoke(options);
        configureOverrides?.Invoke(options);
        return options;
    }

    /// <summary>
    /// Guards against duplicate client name registration.
    /// </summary>
    private void GuardDuplicate(string name)
    {
        if (!_registeredNames.Add(name))
        {
            throw new InvalidOperationException(
                $"A client with name '{name}' has already been registered.");
        }
    }
}
