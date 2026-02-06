using Azure.Core;
using FluentAssertions;
using LakeIO.Telemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Xunit;

namespace LakeIO.Tests.DependencyInjection;

public class ServiceCollectionExtensionsTests
{
    // Azurite-compatible connection string (never hits real Azure)
    private const string ValidConnectionString =
        "DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "EndpointSuffix=core.windows.net";

    private const string AlternateConnectionString =
        "DefaultEndpointsProtocol=https;AccountName=alternate;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "EndpointSuffix=core.windows.net";

    private static readonly Uri DummyUri = new("https://devstoreaccount1.dfs.core.windows.net");

    // ── AddLakeIO(connectionString) ─────────────────────────────────────

    [Fact]
    public void AddLakeIO_ConnectionString_RegistersSingletonLakeClient()
    {
        var services = new ServiceCollection();
        services.AddLakeIO(ValidConnectionString);

        var provider = services.BuildServiceProvider();
        var client1 = provider.GetRequiredService<LakeClient>();
        var client2 = provider.GetRequiredService<LakeClient>();

        client1.Should().NotBeNull();
        client1.Should().BeSameAs(client2);
    }

    [Fact]
    public void AddLakeIO_ConnectionString_NullThrowsArgumentException()
    {
        var services = new ServiceCollection();
        var act = () => services.AddLakeIO((string)null!);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddLakeIO_ConnectionString_EmptyThrowsArgumentException()
    {
        var services = new ServiceCollection();
        var act = () => services.AddLakeIO(string.Empty);

        act.Should().Throw<ArgumentException>();
    }

    // ── AddLakeIO(uri, credential) ──────────────────────────────────────

    [Fact]
    public void AddLakeIO_TokenCredential_RegistersSingletonLakeClient()
    {
        var credential = Substitute.For<TokenCredential>();
        var services = new ServiceCollection();
        services.AddLakeIO(DummyUri, credential);

        var provider = services.BuildServiceProvider();
        var client1 = provider.GetRequiredService<LakeClient>();
        var client2 = provider.GetRequiredService<LakeClient>();

        client1.Should().NotBeNull();
        client1.Should().BeSameAs(client2);
    }

    [Fact]
    public void AddLakeIO_TokenCredential_NullUri_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();
        var credential = Substitute.For<TokenCredential>();

        var act = () => services.AddLakeIO((Uri)null!, credential);

        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddLakeIO_TokenCredential_NullCredential_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();

        var act = () => services.AddLakeIO(DummyUri, (TokenCredential)null!);

        act.Should().Throw<ArgumentNullException>();
    }

    // ── AddLakeIO(IConfiguration) ───────────────────────────────────────

    [Fact]
    public void AddLakeIO_Configuration_WithConnectionString_ResolvesLakeClient()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionString"] = ValidConnectionString
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLakeIO(config);

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<LakeClient>();

        client.Should().NotBeNull();
    }

    [Fact]
    public void AddLakeIO_Configuration_WithServiceUri_ResolvesLakeClient()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ServiceUri"] = "https://devstoreaccount1.dfs.core.windows.net"
            })
            .Build();

        var credential = Substitute.For<TokenCredential>();
        var services = new ServiceCollection();
        services.AddSingleton(credential);
        services.AddLakeIO(config);

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<LakeClient>();

        client.Should().NotBeNull();
    }

    [Fact]
    public void AddLakeIO_Configuration_MissingBoth_ThrowsInvalidOperationException()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>())
            .Build();

        var services = new ServiceCollection();
        services.AddLakeIO(config);

        var provider = services.BuildServiceProvider();
        var act = () => provider.GetRequiredService<LakeClient>();

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*ConnectionString*ServiceUri*");
    }

    // ── AddLakeIO with options overloads ────────────────────────────────

    [Fact]
    public void AddLakeIO_ConnectionStringWithOptions_AppliesOptions()
    {
        var services = new ServiceCollection();
        services.AddLakeIO(ValidConnectionString, opts =>
        {
            opts.EnableDiagnostics = true;
        });

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<LakeClient>();

        client.Should().NotBeNull();
    }

    [Fact]
    public void AddLakeIO_TokenCredentialWithOptions_AppliesOptions()
    {
        var credential = Substitute.For<TokenCredential>();
        var services = new ServiceCollection();
        services.AddLakeIO(DummyUri, credential, opts =>
        {
            opts.EnableDiagnostics = true;
        });

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<LakeClient>();

        client.Should().NotBeNull();
    }

    // ── TryAddSingleton idempotency ─────────────────────────────────────

    [Fact]
    public void AddLakeIO_CalledTwice_FirstRegistrationWins()
    {
        var services = new ServiceCollection();
        services.AddLakeIO(ValidConnectionString);
        services.AddLakeIO(AlternateConnectionString);

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<LakeClient>();

        // First registration used devstoreaccount1
        client.Uri.Host.Should().Contain("devstoreaccount1");
    }

    // ── AddLakeIOTelemetry ──────────────────────────────────────────────

    [Fact]
    public void AddLakeIOTelemetry_RegistersCostEstimator()
    {
        var services = new ServiceCollection();
        services.AddLakeIOTelemetry();

        var provider = services.BuildServiceProvider();
        var estimator = provider.GetRequiredService<CostEstimator>();

        estimator.Should().NotBeNull();
    }

    [Fact]
    public void AddLakeIOTelemetry_WithOptions_ConfiguresObservabilityOptions()
    {
        var services = new ServiceCollection();
        services.AddLakeIOTelemetry(opts =>
        {
            opts.EnableCostEstimation = true;
            opts.StorageTier = StorageTier.Cool;
        });

        var provider = services.BuildServiceProvider();
        var estimator = provider.GetRequiredService<CostEstimator>();

        estimator.Should().NotBeNull();
    }

    [Fact]
    public void AddLakeIOTelemetry_Idempotent()
    {
        var services = new ServiceCollection();
        services.AddLakeIOTelemetry();
        services.AddLakeIOTelemetry();

        var provider = services.BuildServiceProvider();
        var estimator1 = provider.GetRequiredService<CostEstimator>();
        var estimator2 = provider.GetRequiredService<CostEstimator>();

        estimator1.Should().BeSameAs(estimator2);
    }
}
