namespace BlobCache
{
    using System.Collections.Generic;

    public struct StorageInfo
    {
        public bool Initialized { get; set; }
        public int ReaderCount { get; set; }
        public int QueuedReaderCount { get; set; }
        public int WriterCount { get; set; }
        public int QueuedWriterCount { get; set; }
        public List<StorageChunk> Chunks { get; set; }
    }
}