# LakeIO

[![NuGet Version](https://img.shields.io/nuget/v/LakeIO.Core)](https://www.nuget.org/packages/LakeIO.Core/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

.NET library for Azure Data Lake Storage Gen2 with format-specific clients, streaming I/O, and production-ready observability.

## Features

- **Multi-package architecture** -- install only what you need (Core, Parquet, Telemetry, DI)
- **Format-specific operations** -- JSON (with NDJSON streaming), Parquet (with schema evolution), CSV
- **Raw file operations** -- upload, download, delete, move, exists, get properties
- **Batch operations** -- delete, move, copy with per-item error collection (never throws on partial failure)
- **Directory listing** -- `IAsyncEnumerable<PathItem>` with `PathFilter` fluent API for client-side filtering
- **Streaming I/O** -- `IAsyncEnumerable` reads and `ChunkedUploadStream` writes handle files over 1 GB without OOM
- **BCL-native observability** -- `System.Diagnostics.ActivitySource` tracing and `System.Diagnostics.Metrics.Meter` metrics
- **Dependency injection** -- `AddLakeIO()` and `AddLakeIOTelemetry()` service collection extensions

## Installation

```bash
# Core (JSON, CSV, File, Batch, Directory operations)
dotnet add package LakeIO.Core

# Parquet support (optional)
dotnet add package LakeIO.Parquet

# Telemetry / OpenTelemetry integration (optional)
dotnet add package LakeIO.Telemetry

# Dependency injection extensions (optional)
dotnet add package LakeIO.DependencyInjection
```

## Quick Start

```csharp
using LakeIO;

var client = new LakeClient("DefaultEndpointsProtocol=https;AccountName=...");
var fs = client.GetFileSystemClient("my-filesystem");

// Write JSON
var data = new { Id = 1, Name = "Example" };
await fs.Json().WriteAsync("data/example.json", data);

// Read JSON
var result = await fs.Json().ReadAsync<MyModel>("data/example.json");
Console.WriteLine(result.Value.Name);
```

## Authentication

### Connection string

```csharp
var client = new LakeClient("DefaultEndpointsProtocol=https;AccountName=...");
```

### TokenCredential (Azure Identity)

```csharp
var client = new LakeClient(
    new Uri("https://myaccount.dfs.core.windows.net"),
    new DefaultAzureCredential());
```

### With options

```csharp
var client = new LakeClient("DefaultEndpointsProtocol=https;AccountName=...",
    new LakeClientOptions
    {
        DefaultParquetCompression = "Snappy",
        DefaultParquetRowGroupSize = 50_000,
        ChunkedUploadThreshold = 8 * 1024 * 1024 // 8 MB chunks
    });
```

## Operations

All format operations are accessed via extension methods on `FileSystemClient`:

```csharp
var fs = client.GetFileSystemClient("my-filesystem");

fs.Json()       // JSON and NDJSON operations
fs.Csv()        // CSV operations
fs.Parquet()    // Parquet operations (requires LakeIO.Parquet package)
fs.Files()      // Raw file upload/download/delete/move
fs.Batch()      // Batch delete/move/copy
fs.Directory()  // Directory listing and counting
```

### JSON Operations

```csharp
// Write a single object as JSON
await fs.Json().WriteAsync("users/user1.json", new User { Id = 1, Name = "Alice" });

// Read a JSON file
Response<User?> result = await fs.Json().ReadAsync<User>("users/user1.json");
User user = result.Value;

// Append a line to an NDJSON file (creates if not exists)
await fs.Json().AppendNdjsonAsync("events/log.ndjson", new Event { Type = "click", Ts = DateTime.UtcNow });

// Stream NDJSON records without loading entire file into memory
await foreach (Event e in fs.Json().ReadNdjsonAsync<Event>("events/log.ndjson"))
{
    Console.WriteLine(e.Type);
}
```

Per-operation `JsonOptions` can override the default `JsonSerializerOptions`:

```csharp
await fs.Json().WriteAsync("data.json", value, new JsonOptions
{
    SerializerOptions = new JsonSerializerOptions { WriteIndented = true }
});
```

### Parquet Operations

> Requires the `LakeIO.Parquet` package.

```csharp
using LakeIO.Parquet;

// Write a collection to Parquet
IReadOnlyCollection<SensorReading> readings = GetReadings();
await fs.Parquet().WriteAsync("data/readings.parquet", readings);

// Stream read -- IAsyncEnumerable, no full buffering
await foreach (SensorReading r in fs.Parquet().ReadStreamAsync<SensorReading>("data/readings.parquet"))
{
    Process(r);
}

// Streaming write from an IAsyncEnumerable source (bounded memory)
IAsyncEnumerable<SensorReading> source = ProduceReadingsAsync();
await fs.Parquet().WriteStreamAsync("data/readings.parquet", source);

// Inspect schema without loading data
ParquetSchema schema = await fs.Parquet().GetSchemaAsync("data/readings.parquet");

// Merge with automatic schema evolution (appends new columns as nullable)
await fs.Parquet().MergeAsync("data/readings.parquet", newReadings);

// Compact NDJSON to Parquet (streaming, bounded memory)
await fs.Parquet().CompactNdjsonAsync<SensorReading>("events/log.ndjson", "data/compacted.parquet");
```

Per-operation `ParquetOptions` can override compression and row group size:

```csharp
await fs.Parquet().WriteAsync("data/readings.parquet", readings, new ParquetOptions
{
    CompressionMethod = CompressionMethod.Zstd,
    RowGroupSize = 100_000
});
```

### CSV Operations

```csharp
// Write a collection to CSV
var orders = new List<Order>
{
    new() { OrderId = 1, Customer = "Alice", Total = 150.00m },
    new() { OrderId = 2, Customer = "Bob", Total = 200.00m }
};
await fs.Csv().WriteAsync("reports/orders.csv", orders);

// Read entire CSV into memory
Response<IReadOnlyList<Order>> result = await fs.Csv().ReadAsync<Order>("reports/orders.csv");

// Stream read -- IAsyncEnumerable for large CSV files
await foreach (Order order in fs.Csv().ReadStreamAsync<Order>("reports/orders.csv"))
{
    Process(order);
}
```

Per-operation `CsvOptions` can override delimiter, header, and culture:

```csharp
await fs.Csv().WriteAsync("data.tsv", items, new CsvOptions
{
    Delimiter = "\t",
    HasHeader = true,
    CultureName = "de-DE"
});
```

### File Operations

```csharp
// Upload a stream
await using var stream = File.OpenRead("local-file.bin");
await fs.Files().UploadAsync("uploads/file.bin", stream, contentType: "application/octet-stream");

// Download as BinaryData (small files)
Response<BinaryData> data = await fs.Files().DownloadAsync("uploads/file.bin");

// Download as Stream (large files, avoids memory pressure)
Response<Stream> streamResult = await fs.Files().DownloadStreamAsync("uploads/file.bin");

// Check existence
Response<bool> exists = await fs.Files().ExistsAsync("uploads/file.bin");

// Get file properties
Response<PathProperties> props = await fs.Files().GetPropertiesAsync("uploads/file.bin");

// Move / rename (server-side, no re-upload)
await fs.Files().MoveAsync("uploads/file.bin", "archive/file.bin");

// Delete
await fs.Files().DeleteAsync("archive/file.bin");
```

### Batch Operations

Batch operations iterate sequentially and collect per-item errors without throwing on partial failure.

```csharp
// Batch delete
BatchResult deleteResult = await fs.Batch().DeleteAsync(new[]
{
    "temp/file1.json",
    "temp/file2.json",
    "temp/file3.json"
});

if (!deleteResult.IsFullySuccessful)
{
    foreach (var item in deleteResult.Items.Where(i => !i.Succeeded))
    {
        Console.WriteLine($"Failed to delete {item.Path}: {item.Error}");
    }
}

// Batch move
BatchResult moveResult = await fs.Batch().MoveAsync(new[]
{
    new BatchMoveItem { SourcePath = "inbox/a.json", DestinationPath = "archive/a.json" },
    new BatchMoveItem { SourcePath = "inbox/b.json", DestinationPath = "archive/b.json" }
});

// Batch copy (download-then-upload under the hood)
BatchResult copyResult = await fs.Batch().CopyAsync(new[]
{
    new BatchCopyItem { SourcePath = "data/original.parquet", DestinationPath = "backup/original.parquet" }
});

// All batch methods accept IProgress<BatchProgress> for progress reporting
var progress = new Progress<BatchProgress>(p =>
    Console.WriteLine($"{p.Completed}/{p.Total} -- {p.CurrentPath}"));
await fs.Batch().DeleteAsync(paths, progress);
```

### Directory Operations

```csharp
// List all paths in a directory (IAsyncEnumerable, no buffering)
await foreach (PathItem item in fs.Directory().GetPathsAsync(new GetPathsOptions
{
    Path = "data",
    Recursive = true
}))
{
    Console.WriteLine($"{item.Name} ({item.ContentLength} bytes)");
}

// List with client-side filtering via PathFilter
await foreach (PathItem item in fs.Directory().GetPathsAsync(new GetPathsOptions
{
    Path = "data",
    Recursive = true,
    Filter = new PathFilter()
        .WithExtension(".json")
        .LargerThan(1024)
        .ModifiedAfter(DateTimeOffset.UtcNow.AddDays(-7))
        .FilesOnly()
}))
{
    Console.WriteLine(item.Name);
}

// Count matching paths (streaming, no List<T> allocation)
long count = await fs.Directory().CountAsync(new GetPathsOptions
{
    Path = "logs",
    Recursive = true,
    Filter = new PathFilter().WithExtension(".ndjson")
});

// Get properties for a specific path
Response<FileProperties> props = await fs.Directory().GetPropertiesAsync("data/records.parquet");
```

**PathFilter methods:** `WithExtension`, `ModifiedAfter`, `ModifiedBefore`, `LargerThan`, `SmallerThan`, `FilesOnly`, `DirectoriesOnly`, `NameContains`

## Streaming

LakeIO is designed to handle files larger than available memory.

**Reading:** All operations that return `IAsyncEnumerable<T>` use `yield return` internally -- no full buffering. This includes `ReadNdjsonAsync`, `ReadStreamAsync` (CSV), `ReadStreamAsync` (Parquet), and `GetPathsAsync`.

**Writing:** `ChunkedUploadStream` uploads data progressively via `AppendAsync` as the producer writes. Configure the chunk size via `LakeClientOptions.ChunkedUploadThreshold` (default 4 MB).

**Parquet streaming write:** `WriteStreamAsync` consumes an `IAsyncEnumerable<T>` source and batches into row groups incrementally, keeping memory bounded to roughly one row group at a time.

## Dependency Injection

> Requires the `LakeIO.DependencyInjection` package.

```csharp
using LakeIO;

// Connection string
services.AddLakeIO("DefaultEndpointsProtocol=https;AccountName=...");

// TokenCredential
services.AddLakeIO(
    new Uri("https://myaccount.dfs.core.windows.net"),
    new DefaultAzureCredential());

// IConfiguration section (reads ConnectionString or ServiceUri key)
services.AddLakeIO(configuration.GetSection("LakeIO"));

// Connection string with custom options
services.AddLakeIO("DefaultEndpointsProtocol=https;AccountName=...", options =>
{
    options.DefaultParquetCompression = "Zstd";
    options.ChunkedUploadThreshold = 8 * 1024 * 1024;
});

// TokenCredential with custom options
services.AddLakeIO(
    new Uri("https://myaccount.dfs.core.windows.net"),
    new DefaultAzureCredential(),
    options => { options.EnableDiagnostics = true; });

// Add telemetry (registers CostEstimator + ObservabilityOptions)
services.AddLakeIOTelemetry();
services.AddLakeIOTelemetry(opts => opts.EnableCostEstimation = true);
```

All `AddLakeIO` overloads register `LakeClient` as a singleton. Calling `AddLakeIO` multiple times is safe (first registration wins).

## Observability

LakeIO emits distributed traces and metrics using the BCL `System.Diagnostics` API, with no hard dependency on any telemetry SDK.

| Concept | Name | Description |
|---------|------|-------------|
| ActivitySource | `LakeIO` | Emits spans for every storage operation (json.write, parquet.read_stream, file.upload, etc.) |
| Meter | `LakeIO` | Emits counters and histograms |
| Counter | `lakeio.operations.total` | Operation count by type |
| Counter | `lakeio.bytes.transferred` | Bytes transferred by direction (read/write) |
| Histogram | `lakeio.operations.duration` | Operation duration in seconds |

### OpenTelemetry Integration

> Requires the `LakeIO.Telemetry` package.

```csharp
using LakeIO.Telemetry;

builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing.AddLakeIOInstrumentation())
    .WithMetrics(metrics => metrics.AddLakeIOInstrumentation());
```

The `LakeIO.Telemetry` package also provides `CostEstimator` for mapping operations to Azure Storage pricing tiers.

## Package Architecture

| Package | Purpose | Dependencies |
|---------|---------|-------------|
| **LakeIO.Core** | JSON, CSV, File, Batch, Directory operations, streaming infrastructure | Azure.Storage.Files.DataLake, CsvHelper |
| **LakeIO.Parquet** | Parquet read/write with schema evolution and NDJSON compaction | LakeIO.Core, Parquet.Net |
| **LakeIO.Telemetry** | OpenTelemetry builder extensions, cost estimation | LakeIO.Core, OpenTelemetry.Api |
| **LakeIO.DependencyInjection** | `AddLakeIO()` and `AddLakeIOTelemetry()` ServiceCollection extensions | LakeIO.Core, LakeIO.Telemetry |

## Requirements

- .NET 10.0 or later
- Azure Data Lake Storage Gen2 account

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
