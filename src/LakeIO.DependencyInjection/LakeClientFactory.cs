using Microsoft.Extensions.DependencyInjection;

namespace LakeIO;

/// <summary>
/// Default implementation of <see cref="ILakeClientFactory"/> that resolves named
/// <see cref="LakeClient"/> instances from the DI container using keyed services.
/// </summary>
/// <remarks>
/// This class is internal because consumers interact exclusively through the
/// <see cref="ILakeClientFactory"/> interface. The DI container wires the implementation
/// automatically when any named <c>AddLakeIO</c> overload or the builder API is used.
/// </remarks>
internal sealed class LakeClientFactory : ILakeClientFactory
{
    private readonly IServiceProvider _serviceProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="LakeClientFactory"/> class.
    /// </summary>
    /// <param name="serviceProvider">The service provider for resolving keyed services.</param>
    public LakeClientFactory(IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public LakeClient CreateClient(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return _serviceProvider.GetRequiredKeyedService<LakeClient>(name);
    }
}
