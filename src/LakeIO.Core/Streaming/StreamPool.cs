using Microsoft.IO;

namespace LakeIO;

/// <summary>
/// Provides a process-wide <see cref="RecyclableMemoryStreamManager"/> singleton
/// for pooled <see cref="MemoryStream"/> instances that avoid Large Object Heap allocations.
/// </summary>
/// <remarks>
/// Use <c>StreamPool.Manager.GetStream(tag)</c> instead of <c>new MemoryStream()</c>
/// for any buffer that may grow beyond 85KB (the LOH threshold).
/// The manager uses default settings: 128KB blocks, 1MB large buffers.
/// </remarks>
internal static class StreamPool
{
    /// <summary>
    /// The singleton <see cref="RecyclableMemoryStreamManager"/> instance.
    /// Thread-safe by design (RecyclableMemoryStreamManager is documented as thread-safe).
    /// </summary>
    internal static readonly RecyclableMemoryStreamManager Manager = new();
}
