﻿namespace BlobCache
{
    public struct CacheStatistics
    {
        /// <summary>
        /// Gets the number of entries in the cache
        /// </summary>
        public int NumberOfEntries { get; internal set; }

        /// <summary>
        /// Gets the number of bytes used in the cache for internal purposes. Includes overhead by the caching information and overhead by the storage blob
        /// </summary>
        public long Overhead { get; internal set; }

        /// <summary>
        /// Gets the average compression ratio of the entries if the cache is compressed. Higher number is better.
        /// </summary>
        public double CompressionRatio { get; internal set; }

        /// <summary>
        /// Gets the average used space relative to entry lengths. Includes per entry overhead. Lower number is better.
        /// </summary>
        public double StorageRatio { get; internal set; }

        /// <summary>
        /// Gets the storage file size on the disk
        /// </summary>
        public long FileSize { get; internal set; }

        /// <summary>
        /// Gets the used space in the storage, includes overhead
        /// </summary>
        public long UsedSpace { get; internal set; }

        /// <summary>
        /// Gets the maximum usable free space without expanding the cache file
        /// </summary>
        public long FreeSpace { get; internal set; }

        /// <summary>
        /// Gets the sum of the entry sizes
        /// </summary>
        public long EntriesSize { get; internal set; }
    }
}