namespace BlobCache
{
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    public struct StorageInfo
    {
        public bool Initialized { get; set; }
        public int ReaderCount { get; set; }
        public int QueuedReaderCount { get; set; }
        public int WriterCount { get; set; }
        public int QueuedWriterCount { get; set; }
        public List<StorageChunk> Chunks { get; set; }

        public static StorageInfo ReadFromStream(Stream stream)
        {
            using (var r = new BinaryReader(stream, Encoding.UTF8))
            {
                var i = r.ReadBoolean();
                var rc = r.ReadInt32();
                var qrc = r.ReadInt32();
                var wc = r.ReadInt32();
                var qwc = r.ReadInt32();
                var count = r.ReadInt32();

                var list = new List<StorageChunk>();
                for (var c = 0; c < count; c++)
                    list.Add(StorageChunk.FromStream(r));

                return new StorageInfo
                {
                    Initialized = i,
                    ReaderCount = rc,
                    QueuedReaderCount = qrc,
                    WriterCount = wc,
                    QueuedWriterCount = qwc,
                    Chunks = list
                };
            }
        }

        public void WriteToStream(Stream stream)
        {
            using (var w = new BinaryWriter(stream, Encoding.UTF8))
            {
                w.Write(Initialized);
                w.Write(ReaderCount);
                w.Write(QueuedReaderCount);
                w.Write(WriterCount);
                w.Write(QueuedWriterCount);
                w.Write(Chunks.Count);
                foreach (var c in Chunks)
                    c.ToStream(w);
            }
        }
    }
}