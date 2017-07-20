namespace BlobCache
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;

    /// <summary>
    ///     Information about storage chunks
    /// </summary>
    public struct StorageInfo
    {
        /// <summary>
        ///     Gets or sets a value indicating whether the storage is initialized
        /// </summary>
        internal bool Initialized { get; set; }

        /// <summary>
        ///     Gets the added data version since storage initialization
        /// </summary>
        /// <remarks>Increased when a new chunk is added to the storage</remarks>
        public ulong AddedVersion { get; internal set; }

        /// <summary>
        ///     Gets the removed data version since storage initialization
        /// </summary>
        /// <remarks>Increased when a chunk is removed from the storage</remarks>
        public ulong RemovedVersion { get; internal set; }

        /// <summary>
        ///     Gets the data version since storage initialization
        /// </summary>
        /// <remarks>Increased when a chunk is added to or removed from the storage</remarks>
        public ulong Version => AddedVersion + RemovedVersion;

        /// <summary>
        ///     Gets the chunks in storage
        /// </summary>
        /// <remarks>When the structure used in selectors it is possible the list is filtered to valid chunks for the selector</remarks>
        public IReadOnlyList<StorageChunk> Chunks => ChunkList;

        /// <summary>
        ///     Gets the chunks in storage
        /// </summary>
        /// <remarks>When the structure used in selectors it is possible the list is filtered to valid chunks for the selector</remarks>
        internal List<StorageChunk> ChunkList { get; set; }

        /// <summary>
        ///     Reads the info from a stream
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <returns>Storage info in the stream</returns>
        internal static StorageInfo ReadFromStream(Stream stream)
        {
            using (var r = new BinaryReader(stream, Encoding.UTF8))
            {
                var i = r.ReadBoolean();
                var av = r.ReadUInt64();
                var rv = r.ReadUInt64();
                var count = r.ReadInt32();

                var list = new List<StorageChunk>();
                for (var c = 0; c < count; c++)
                    list.Add(StorageChunk.FromStream(r));

                return new StorageInfo
                {
                    Initialized = i,
                    AddedVersion = av,
                    RemovedVersion = rv,
                    ChunkList = list
                };
            }
        }

        /// <summary>
        ///     Write the info to a stream
        /// </summary>
        /// <param name="stream">Stream to write to</param>
        internal void WriteToStream(Stream stream)
        {
            using (var w = new BinaryWriter(stream, Encoding.UTF8))
            {
                w.Write(Initialized);
                w.Write(AddedVersion);
                w.Write(RemovedVersion);
                w.Write(Chunks.Count);
                foreach (var c in Chunks)
                    c.ToStream(w);
            }
        }

        /// <summary>
        ///     Creates a copy of the storage info and filters chunks
        /// </summary>
        /// <param name="filter">Filter to apply to the chunks</param>
        /// <returns>Copied storage info</returns>
        internal StorageInfo FilterChunks(Func<StorageChunk, bool> filter)
        {
            return new StorageInfo { Initialized = Initialized, AddedVersion = AddedVersion, RemovedVersion = RemovedVersion, ChunkList = ChunkList.Where(filter).ToList() };
        }
    }
}