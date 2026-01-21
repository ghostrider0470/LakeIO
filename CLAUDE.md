# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LakeIO is a .NET library that provides a simple interface for interacting with Azure Data Lake Storage Gen2. It supports JSON, Parquet, and CSV file formats with thread-safe client caching.

## Common Development Commands

### Build Commands
```bash
# Build the solution
dotnet build

# Build in Release mode
dotnet build -c Release

# Build specific project
dotnet build src/LakeIO/LakeIO.csproj

# Pack as NuGet package
dotnet pack -c Release

# Run the sample application
dotnet run --project samples/LakeIO.Sample/LakeIO.Sample.csproj
```

### Development Setup
1. Ensure .NET 9.0 SDK is installed
2. Configure Azure Data Lake connection string in `appsettings.json`:
   ```json
   {
     "DataLakeConnectionString": "your-connection-string-here"
   }
   ```

## Architecture Overview

### Core Components

1. **LakeContext** (`src/LakeIO/Storage/LakeContext.cs`)
   - Main implementation class that orchestrates all data lake operations
   - Implements thread-safe client caching using `ConcurrentDictionary`
   - Supports dependency injection with `IConfiguration` and `ILogger`
   - All methods are async and return `Task<T>`

2. **File Format Strategy Pattern**
   - **IFileFormatter** interface defines the contract for all formatters
   - Implementations in `src/LakeIO/Formatters/`:
     - `SystemTextJsonFormatter`: Uses System.Text.Json for JSON serialization
     - `ParquetFileFormatter`: Uses Parquet.Net with custom attribute support
     - `CsvFileFormatter`: Uses CsvHelper for CSV operations
   - New formats can be added by implementing `IFileFormatter`

3. **Parquet Attribute System**
   - `ParquetColumnAttribute`: Maps C# properties to Parquet columns
   - `IParquetSerializable`: Interface for objects that can be serialized to Parquet
   - Located in `src/LakeIO/Annotations/`

### Key Design Patterns

- **Dependency Injection**: All dependencies injected via constructor
- **Strategy Pattern**: File formatters are interchangeable
- **Repository Pattern**: `LakeContext` acts as a repository for data lake operations
- **Async/Await**: All I/O operations are asynchronous
- **Thread-Safe Caching**: Clients cached per container using `ConcurrentDictionary`

### Project Structure
```
/
├── src/
│   └── LakeIO/                         # Main library project
│       ├── Storage/                     # Core data lake logic
│       ├── Formatters/                  # File format implementations
│       └── Annotations/                 # Custom attributes
├── samples/
│   └── LakeIO.Sample/                  # Example usage
└── LakeIO.sln                          # Solution file
```

## Important Implementation Details

1. **Connection String Resolution**: The library checks for connection strings in this order:
   - `DataLakeConnectionString`
   - `DataLake:ConnectionString`

2. **File Path Conventions**:
   - All file paths use forward slashes (`/`)
   - Paths are automatically normalized

3. **Error Handling**:
   - Throws `ArgumentNullException` for null parameters
   - Throws `InvalidOperationException` for configuration issues
   - Azure SDK exceptions bubble up for storage-related errors

4. **Extension Points**:
   - New file formats: Implement `IFileFormatter`
   - Custom serialization: Use `ParquetColumnAttribute` for Parquet
   - CSV mapping: Provide `CsvColumnMapping` dictionary

## Version Information
- Current Version: 1.0.0
- Target Framework: .NET 9.0
- Supports: .NET 6.0+
