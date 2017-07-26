namespace BlobCache
{
    using JetBrains.Annotations;

    /// <summary>
    ///     Statistics about a storage blob
    /// </summary>
    public struct StorageStatistics
    {
        /// <summary>
        ///     Gets the space used by chunks
        /// </summary>
        /// <remarks>Without overhead</remarks>
        [PublicAPI]
        public long UsedSpace { get; internal set; }

        /// <summary>
        ///     Gets the maximum free space available in the storage
        /// </summary>
        /// <remarks>Sum of free space chunks, available only when same chunk sizes used for new chunks as the free chunks</remarks>
        [PublicAPI]
        public long FreeSpace { get; internal set; }

        /// <summary>
        ///     Gets the overhead in the storage
        /// </summary>
        /// <remarks>Contains overhead for used and free chunks and includes storage header overhead</remarks>
        [PublicAPI]
        public long Overhead { get; internal set; }

        /// <summary>
        ///     Gets the storage blob size
        /// </summary>
        [PublicAPI]
        public long FileSize { get; internal set; }

        /// <summary>
        ///     Gets the number of used chunks
        /// </summary>
        [PublicAPI]
        public int UsedChunks { get; internal set; }

        /// <summary>
        ///     Gets the number of free chunks
        /// </summary>
        [PublicAPI]
        public int FreeChunks { get; internal set; }
    }
}