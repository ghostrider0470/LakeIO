# LakeIO

[![NuGet Version](https://img.shields.io/nuget/v/LakeIO)](https://www.nuget.org/packages/LakeIO/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive .NET library for working with Azure Data Lake Storage Gen2, supporting JSON, Parquet, and CSV file formats with thread-safe client caching and an intuitive API.

## Features

- **Multiple File Format Support**
  - JSON files with System.Text.Json
  - Parquet files with Parquet.Net and custom attribute mapping
  - CSV files with CsvHelper and flexible column mapping
- **Thread-Safe Client Management**: Efficient caching of Azure Data Lake clients
- **Comprehensive File Operations**: Read, write, update, and validate files
- **Directory Operations**: List files with filtering and metadata
- **Async/Await Pattern**: All I/O operations are asynchronous
- **Dependency Injection Ready**: Easy integration with .NET DI containers
- **Robust Error Handling**: Detailed error messages and proper exception handling
- **Automatic Directory Creation**: Creates directories as needed
- **Flexible Configuration**: Multiple connection string resolution options

## Prerequisites

- .NET 6.0 or later
- Azure Data Lake Storage Gen2 account
- Connection string with appropriate permissions

## Installation

```bash
dotnet add package LakeIO
```

## Configuration

Add your Data Lake connection string to your `appsettings.json`:

```json
{
  "DataLakeConnectionString": "DefaultEndpointsProtocol=https;AccountName=..."

  // OR

  "DataLake": {
    "ConnectionString": "DefaultEndpointsProtocol=https;AccountName=..."
  }
}
```

## Quick Start

```csharp
using LakeIO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

// Set up configuration
var configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json")
    .AddEnvironmentVariables()
    .Build();

// Create a logger (or use DI)
using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
var logger = loggerFactory.CreateLogger<LakeContext>();

// Create a new instance of LakeContext
var lakeContext = new LakeContext(configuration, logger);

// Store an object as JSON
var user = new { Id = 1, Name = "John Doe", Email = "john@example.com" };
await lakeContext.StoreItemAsJson(user, "users", "myfilesystem", "user_1.json");

// Read it back
var retrievedUser = await lakeContext.ReadJsonFile<User>("users/user_1.json", "myfilesystem");
```

## Dependency Injection

```csharp
using LakeIO.Extensions;

// In your Program.cs or Startup.cs
services.AddLakeIO();
```

## Detailed Usage Examples

### JSON Operations

```csharp
// Store a single item as JSON
var product = new Product { Id = 1, Name = "Laptop", Price = 999.99 };
await lakeContext.StoreItemAsJson(
    product,
    "products/electronics",
    "myfilesystem",
    "laptop.json");

// Read a single JSON object
var product = await lakeContext.ReadJsonFile<Product>(
    "products/laptop.json",
    "myfilesystem");

// Read a JSON array
var products = await lakeContext.ReadJsonItems<Product>(
    "products/all-products.json",
    "myfilesystem");

// Update an existing JSON file
product.Price = 899.99;
await lakeContext.UpdateJsonFile(
    product,
    "products/laptop.json",
    "myfilesystem");
```

### Parquet Operations

To use Parquet storage, implement the `IParquetSerializable<T>` interface:

```csharp
using LakeIO.Annotations;

public class SensorData : IParquetSerializable<SensorData>
{
    [ParquetColumn("sensor_id")]
    public string SensorId { get; set; }

    [ParquetColumn("temperature")]
    public double Temperature { get; set; }

    [ParquetColumn("timestamp")]
    public DateTime Timestamp { get; set; }
}

// Store items as Parquet
var readings = new List<SensorData>
{
    new() { SensorId = "S001", Temperature = 23.5, Timestamp = DateTime.UtcNow },
    new() { SensorId = "S002", Temperature = 24.1, Timestamp = DateTime.UtcNow }
};

await lakeContext.StoreItemsAsParquet(
    readings,
    "sensor-data/2024",
    "myfilesystem",
    "readings.parquet");

// Read Parquet file
var data = await lakeContext.ReadParquetItems<SensorData>(
    "sensor-data/2024/readings.parquet",
    "myfilesystem");

// Validate Parquet file
bool isValid = await lakeContext.IsValidParquetFile(
    "sensor-data/2024/readings.parquet",
    "myfilesystem");
```

### CSV Operations

```csharp
// Store items as CSV with custom delimiter
var orders = new List<Order>
{
    new() { OrderId = 1, CustomerName = "Alice", Total = 150.00 },
    new() { OrderId = 2, CustomerName = "Bob", Total = 200.00 }
};

await lakeContext.StoreItemsAsCsv(
    orders,
    "reports/orders",
    "myfilesystem",
    "daily-orders.csv",
    delimiter: ",",
    hasHeader: true);

// Store with custom column mapping
var columnMapping = new Dictionary<string, string>
{
    ["OrderId"] = "Order ID",
    ["CustomerName"] = "Customer",
    ["Total"] = "Total Amount"
};

await lakeContext.StoreItemsAsCsv(
    orders,
    "reports/orders",
    "myfilesystem",
    columnMapping: columnMapping);

// Read CSV file
var orders = await lakeContext.ReadCsvItems<Order>(
    "reports/orders/daily-orders.csv",
    "myfilesystem",
    delimiter: ",",
    hasHeader: true);

// Update CSV file
orders.Add(new Order { OrderId = 3, CustomerName = "Charlie", Total = 175.00 });
await lakeContext.UpdateCsvFileWithItems(
    orders,
    "reports/orders/daily-orders.csv",
    "myfilesystem");
```

### Directory and File Listing

```csharp
// List all files in a directory
var files = await lakeContext.ListFiles(
    "products",
    "myfilesystem",
    recursive: true);

foreach (var file in files)
{
    Console.WriteLine($"File: {file}");
}

// List files by date range
var recentFiles = await lakeContext.ListFilesByDateRange(
    "logs",
    "myfilesystem",
    fromDate: DateTime.UtcNow.AddDays(-7),
    toDate: DateTime.UtcNow,
    recursive: true);

// List files with metadata
var filesWithInfo = await lakeContext.ListFilesWithMetadata(
    "data",
    "myfilesystem",
    recursive: true);

foreach (var fileInfo in filesWithInfo)
{
    Console.WriteLine($"Path: {fileInfo.Path}");
    Console.WriteLine($"Size: {fileInfo.Size} bytes");
    Console.WriteLine($"Modified: {fileInfo.LastModified}");
    Console.WriteLine($"Is Directory: {fileInfo.IsDirectory}");
}
```

### Client Management

```csharp
// Get or create a service client (cached)
var serviceClient = lakeContext.GetOrCreateServiceClient(connectionString);

// Get or create a file system client (cached)
var fileSystemClient = lakeContext.GetOrCreateFileSystemClient(
    "myfilesystem",
    connectionString);

// Dispose to clean up all cached clients
lakeContext.Dispose();
```

## API Reference

### LakeContext

The main class for interacting with Azure Data Lake Storage.

#### Constructor
- `LakeContext(IConfiguration configuration, ILogger<LakeContext> logger)`

#### JSON Methods
- `StoreItemAsJson<T>` - Store a single item as JSON
- `UpdateJsonFile<T>` - Update an existing JSON file
- `ReadJsonFile<T>` - Read a single JSON object
- `ReadJsonItems<T>` - Read a collection of JSON objects

#### Parquet Methods
- `StoreItemAsParquet<T>` - Store a single item as Parquet
- `StoreItemsAsParquet<T>` - Store multiple items as Parquet
- `UpdateParquetFile<T>` - Update with a single item
- `UpdateParquetFileWithItems<T>` - Update with multiple items
- `ReadParquetFile<T>` - Read a single Parquet object
- `ReadParquetItems<T>` - Read multiple Parquet objects
- `IsValidParquetFile` - Validate Parquet file format

#### CSV Methods
- `StoreItemAsCsv<T>` - Store a single item as CSV
- `StoreItemsAsCsv<T>` - Store multiple items as CSV
- `UpdateCsvFile<T>` - Update with a single item
- `UpdateCsvFileWithItems<T>` - Update with multiple items
- `ReadCsvFile<T>` - Read a single CSV object
- `ReadCsvItems<T>` - Read multiple CSV objects

#### Directory Operations
- `ListFiles` - List all files in a directory
- `ListFilesByDateRange` - List files filtered by modification date
- `ListFilesWithMetadata` - List files with detailed metadata

#### Client Management
- `GetOrCreateServiceClient` - Get cached service client
- `GetOrCreateFileSystemClient` - Get cached file system client

### DataLakeFileInfo

Metadata information for files and directories:
- `Path` - Full path to the file
- `Name` - File name only
- `Size` - File size in bytes
- `LastModified` - Last modification timestamp
- `CreatedOn` - Creation timestamp (currently null)
- `IsDirectory` - Whether the item is a directory

## Advanced Features

### Custom Parquet Attributes

Use `ParquetColumnAttribute` to control Parquet serialization:

```csharp
using LakeIO.Annotations;

public class MetricData : IParquetSerializable<MetricData>
{
    [ParquetColumn("metric_name")]
    public string Name { get; set; }

    [ParquetColumn("value", ParquetType = typeof(double))]
    public decimal Value { get; set; }

    [ParquetColumn("tags")]
    public Dictionary<string, string> Tags { get; set; }
}
```

### Error Handling

```csharp
try
{
    await lakeContext.ReadJsonFile<User>("users/user.json", "myfilesystem");
}
catch (FileNotFoundException ex)
{
    // Handle missing file
}
catch (InvalidOperationException ex)
{
    // Handle configuration issues
}
catch (Azure.RequestFailedException ex)
{
    // Handle Azure service errors
}
```

## Best Practices

1. **Use Dependency Injection**: Register `LakeContext` as a singleton via `AddLakeIO()`
2. **Handle Exceptions**: Always wrap operations in try-catch blocks
3. **Validate File Paths**: Ensure paths use forward slashes
4. **Dispose Properly**: Call `Dispose()` when done to clean up clients
5. **Use Appropriate Formats**:
   - JSON for flexible, human-readable data
   - Parquet for large datasets and columnar analytics
   - CSV for simple tabular data and Excel compatibility

## Troubleshooting

### Common Issues

1. **Connection String Not Found**
   - Verify configuration keys: `DataLakeConnectionString` or `DataLake:ConnectionString`
   - Check environment variables and user secrets

2. **File Not Found Errors**
   - Ensure file paths use forward slashes (`/`)
   - Verify the file system name is correct
   - Check permissions on the storage account

3. **Parquet Serialization Errors**
   - Ensure class implements `IParquetSerializable<T>`
   - Verify `ParquetColumnAttribute` usage
   - Check that all properties have appropriate types

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

Hamza Abdagic

## Acknowledgments

- Built on [Azure.Storage.Files.DataLake](https://github.com/Azure/azure-sdk-for-net)
- JSON support via System.Text.Json
- Parquet support via [Parquet.Net](https://github.com/aloneguid/parquet-dotnet)
- CSV support via [CsvHelper](https://joshclose.github.io/CsvHelper/)
