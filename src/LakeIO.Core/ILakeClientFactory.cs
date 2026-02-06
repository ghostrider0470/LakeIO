namespace LakeIO;

/// <summary>
/// Factory for resolving named <see cref="LakeClient"/> instances from the DI container.
/// </summary>
/// <remarks>
/// <para>
/// Register named clients via <c>AddLakeIO(name, connectionString)</c> or
/// <c>AddLakeIO(builder => builder.AddClient(name, ...))</c>, then resolve them
/// through this factory or via <c>[FromKeyedServices(name)]</c> attribute injection.
/// </para>
/// <para>
/// The factory is registered automatically when any named <c>AddLakeIO</c> overload is used.
/// </para>
/// </remarks>
public interface ILakeClientFactory
{
    /// <summary>
    /// The well-known key used for the default (unnamed) client registration.
    /// </summary>
    const string DefaultName = "__default__";

    /// <summary>
    /// Resolves a named <see cref="LakeClient"/> from the DI container.
    /// </summary>
    /// <param name="name">
    /// The client name used during registration. Use <see cref="DefaultName"/> for the
    /// default client.
    /// </param>
    /// <returns>The resolved <see cref="LakeClient"/> instance.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when no client with the specified <paramref name="name"/> has been registered.
    /// </exception>
    LakeClient CreateClient(string name);
}
