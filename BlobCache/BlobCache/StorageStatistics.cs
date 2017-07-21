namespace BlobCache
{
    using JetBrains.Annotations;

    public struct StorageStatistics
    {
        [PublicAPI]
        public long UsedSpace { get; set; }

        [PublicAPI]
        public long FreeSpace { get; set; }

        [PublicAPI]
        public long Overhead { get; set; }

        [PublicAPI]
        public long FileSize { get; set; }

        [PublicAPI]
        public int UsedChunks { get; set; }

        [PublicAPI]
        public int FreeChunks { get; set; }
    }
}