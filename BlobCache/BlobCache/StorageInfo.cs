namespace BlobCache
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;

    /// <summary>
    ///     Information about storage chunks
    /// </summary>
    public struct StorageInfo
    {
        private List<StorageChunk> _chunkList;

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
        ///     Gets or sets the chunks in storage
        /// </summary>
        /// <remarks>When the structure used in selectors it is possible the list is filtered to valid chunks for the selector</remarks>
        private List<StorageChunk> ChunkList
        {
            get
            {
                if (_chunkList == null)
                {
                    _chunkList = new List<StorageChunk>();
                    ChunkDictionary = new Dictionary<uint, int>();
                }
                return _chunkList;
            }
            set
            {
                var dict = new Dictionary<uint, int>();
                for (var i = 0; i < value.Count; i++)
                    dict[value[i].Id] = i;
                _chunkList = value;
                ChunkDictionary = dict;
            }
        }

        /// <summary>
        ///     Adds a new chunk to the chunk list
        /// </summary>
        /// <param name="chunk">Chunk to add</param>
        internal void AddChunk(StorageChunk chunk)
        {
            ChunkList.Add(chunk);
            ChunkDictionary[chunk.Id] = ChunkList.Count - 1;
        }

        /// <summary>
        ///     REmoves a chunk from the chunk list
        /// </summary>
        /// <param name="chunk">Chunk to remove</param>
        internal void RemoveChunk(StorageChunk chunk)
        {
            var index = ChunkDictionary[chunk.Id];
            ChunkList.RemoveAt(index);

            // Force recreation of ChunkDictionary
            ChunkList = ChunkList;
            //ChunkDictionary.Remove(chunk.Id);
        }

        /// <summary>
        ///     Gets a chunk from the list by it's id
        /// </summary>
        /// <param name="id">Chunk id to search</param>
        /// <returns>Chunk</returns>
        internal StorageChunk GetChunkById(uint id)
        {
            var index = ChunkDictionary[id];
            FailIndex(index);
            return ChunkList[index];
        }

        /// <summary>
        ///     Updates an existing chunk
        /// </summary>
        /// <param name="chunk">Chunk to update</param>
        internal void UpdateChunk(StorageChunk chunk)
        {
            ReplaceChunk(chunk.Id, chunk);
        }

        /// <summary>
        ///     Replaces a chunk with another one
        /// </summary>
        /// <param name="id">Chunk id to replace</param>
        /// <param name="chunk">Replacement chunk</param>
        internal void ReplaceChunk(uint id, StorageChunk chunk)
        {
            var index = ChunkDictionary[id];
            FailIndex(index);
            ChunkList[index] = chunk;
            ChunkDictionary.Remove(id);
            ChunkDictionary[chunk.Id] = index;
        }

        [Conditional("DEBUG")]
        private void FailIndex(int index)
        {
            if (index < 0 || index>=ChunkList.Count && Debugger.IsAttached)
                Debugger.Break();
        }

        /// <summary>
        ///     Gets the chunks in storage
        /// </summary>
        private Dictionary<uint, int> ChunkDictionary { get; set; }

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

                var si = new StorageInfo
                {
                    Initialized = i,
                    AddedVersion = av,
                    RemovedVersion = rv
                };

                for (var c = 0; c < count; c++)
                    si.AddChunk(StorageChunk.FromStream(r));

                return si;
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