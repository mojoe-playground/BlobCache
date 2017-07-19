namespace BlobCache
{
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    public struct StorageInfo
    {
        public bool Initialized { get; set; }
        public ulong Version { get; set; }
        public List<StorageChunk> Chunks { get; set; }

        public static StorageInfo ReadFromStream(Stream stream)
        {
            using (var r = new BinaryReader(stream, Encoding.UTF8))
            {
                var i = r.ReadBoolean();
                var v = r.ReadUInt64();
                var count = r.ReadInt32();

                var list = new List<StorageChunk>();
                for (var c = 0; c < count; c++)
                    list.Add(StorageChunk.FromStream(r));

                return new StorageInfo
                {
                    Initialized = i,
                    Version = v,
                    Chunks = list
                };
            }
        }

        public void WriteToStream(Stream stream)
        {
            using (var w = new BinaryWriter(stream, Encoding.UTF8))
            {
                w.Write(Initialized);
                w.Write(Version);
                w.Write(Chunks.Count);
                foreach (var c in Chunks)
                    c.ToStream(w);
            }
        }
    }
}