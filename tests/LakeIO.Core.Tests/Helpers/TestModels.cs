namespace LakeIO.Tests.Helpers;

/// <summary>
/// Simple test record for JSON and CSV serialization tests.
/// </summary>
public class TestRecord
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

/// <summary>
/// Test DTO for batch and CSV operation tests.
/// </summary>
public class TestOrder
{
    public int OrderId { get; set; }
    public string Customer { get; set; } = string.Empty;
    public decimal Total { get; set; }
}

/// <summary>
/// Test DTO for Parquet tests with typical sensor data fields.
/// </summary>
public class TestSensorData
{
    public int Id { get; set; }
    public string SensorId { get; set; } = string.Empty;
    public double Value { get; set; }
    public DateTime Timestamp { get; set; }
}
