namespace BlobCache
{
    using System;

    /// <summary>
    ///     Info about a cache entry
    /// </summary>
    public struct CacheEntryInfo
    {
        /// <summary>
        ///     Gets the date the entry added to the cache
        /// </summary>
        /// <remarks>Time in UTC</remarks>
        public DateTime Added { get; internal set; }
    }
}