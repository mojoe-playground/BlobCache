namespace BlobCache
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    /// <summary>
    ///     Cache entry head record
    /// </summary>
    internal struct CacheHead
    {
        /// <summary>
        ///     Gets or sets the entry key
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        ///     Gets or sets the chunk ids in order
        /// </summary>
        public List<uint> Chunks { get; set; }

        /// <summary>
        ///     Gets or sets the entry time to live
        /// </summary>
        public DateTime TimeToLive { get; set; }

        /// <summary>
        ///     Gets the length of the data
        /// </summary>
        public int Length { get; set; }

        /// <summary>
        ///     Gets or sets the data chunks
        /// </summary>
        public List<StorageChunk> ValidChunks { get; set; }

        /// <summary>
        ///     Gets or sets the head chunk
        /// </summary>
        public StorageChunk HeadChunk { get; set; }

        /// <summary>
        ///     Writes the entry to the storage
        /// </summary>
        /// <param name="writer">Writer to use</param>
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

        /// <summary>
        ///     Reads a cache head record from the stream
        /// </summary>
        /// <param name="reader">Reader to use</param>
        /// <returns>Cache head</returns>
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

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{Key} ({TimeToLive}, {Length}) - {HeadChunk}";
        }
    }
}