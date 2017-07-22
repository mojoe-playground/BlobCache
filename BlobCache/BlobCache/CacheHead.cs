namespace BlobCache
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    internal struct CacheHead
    {
        public string Key { get; set; }
        public List<uint> Chunks { get; set; }
        public DateTime TimeToLive { get; set; }
        public int Length { get; set; }
        public List<StorageChunk> ValidChunks { get; set; }
        public StorageChunk HeadChunk { get; set; }

        public void ToStream(BinaryWriter writer)
        {
            writer.Write(Key);
            writer.Write(TimeToLive.Ticks);
            writer.Write(Length);
            writer.Write(Chunks.Count);
            foreach (var c in Chunks)
                writer.Write(c);
            writer.Flush();
        }

        public static CacheHead FromStream(BinaryReader reader)
        {
            var k = reader.ReadString();
            var ttl = new DateTime(reader.ReadInt64(), DateTimeKind.Utc);
            var l = reader.ReadInt32();
            var c = reader.ReadInt32();
            var list = new List<uint>();
            for (var i = 0; i < c; i++)
                list.Add(reader.ReadUInt32());

            return new CacheHead { Key = k, TimeToLive = ttl, Chunks = list, Length = l };
        }
    }
}