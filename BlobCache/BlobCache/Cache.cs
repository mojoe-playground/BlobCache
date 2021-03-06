﻿namespace BlobCache
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.IO.Compression;
    using System.Linq;
    using System.Security;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using ConcurrencyModes;
    using JetBrains.Annotations;

    /// <inheritdoc />
    /// <summary>
    ///     Blob cache
    /// </summary>
    public class Cache : IDisposable
    {
        /// <summary>
        ///     Heads cache
        /// </summary>
        private readonly Dictionary<string, (uint Hash, List<CacheHead> Heads)> _headCache = new Dictionary<string, (uint, List<CacheHead>)>();

        /// <summary>
        ///     Gets the reference time used for cleanup
        /// </summary>
        /// <remarks>Used in tests to simulate old data in storages</remarks>
        internal Func<DateTime> CleanupTime = () => DateTime.UtcNow;

        /// <summary>
        ///     Storage AddedVersion the heads cache belongs to
        /// </summary>
        private ulong _headCacheAddedVersion;

        /// <summary>
        ///     Storage RemovedVersion the heads cache belongs to
        /// </summary>
        private ulong _headCacheRemovedVersion;

        private TaskScheduler _scheduler = TaskScheduler.Default;

        /// <inheritdoc />
        /// <summary>
        ///     Initializes a new instance of the <see cref="T:BlobCache.Cache" /> class
        /// </summary>
        /// <param name="fileName">Storage file name</param>
        public Cache(string fileName)
            : this(new BlobStorage(fileName))
        {
        }

        /// <inheritdoc />
        /// <summary>
        ///     Initializes a new instance of the <see cref="T:BlobCache.Cache" /> class
        /// </summary>
        /// <param name="fileName">Storage file name</param>
        /// <param name="keyComparer">Key comparer to use</param>
        public Cache(string fileName, IKeyComparer keyComparer)
            : this(new BlobStorage(fileName), keyComparer)
        {
        }

        /// <inheritdoc />
        /// <summary>
        ///     Initializes a new instance of the <see cref="T:BlobCache.Cache" /> class
        /// </summary>
        /// <param name="storage">Storage to use</param>
        public Cache(BlobStorage storage)
            : this(storage, new CaseSensitiveKeyComparer())
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="Cache" /> class
        /// </summary>
        /// <param name="storage">Storage to use</param>
        /// <param name="keyComparer">Key comparer to use</param>
        public Cache(BlobStorage storage, IKeyComparer keyComparer)
        {
            Storage = storage;
            CleanupNeeded = Storage.IsInitialized;
            KeyComparer = keyComparer ?? throw new ArgumentNullException(nameof(keyComparer));
        }

        /// <summary>
        ///     Cleans up used resources
        /// </summary>
        ~Cache()
        {
            Dispose();
        }

        /// <summary>
        ///     Gets or sets a value indicating whether cache data can be compressed
        /// </summary>
        [PublicAPI]
        public bool CanCompress { get; set; }

        /// <summary>
        ///     Gets or sets a value indicating whether cache should be clean up at initialization
        /// </summary>
        [PublicAPI]
        public bool CleanupAtInitialize { get; set; } = true;

        /// <summary>
        ///     Gets or sets the ratio used when removing records from the storage beacuse maximum file size is over the limit
        /// </summary>
        [PublicAPI]
        public double CutBackRatio { get; set; } = 0.8;

        /// <summary>
        ///     Gets or sets the maximum file size
        /// </summary>
        public long MaximumSize { get; set; }

        /// <summary>
        ///     Gets or sets a value indicating whether remove invalid caches if an error occures at initialization
        /// </summary>
        [PublicAPI]
        public bool RemoveInvalidCache { get; set; }

        /// <summary>
        ///     Gets or sets the task scheduler to use for scheduling tasks
        /// </summary>
        [PublicAPI]
        public TaskScheduler Scheduler
        {
            get => _scheduler;
            set
            {
                _scheduler = value;
                Storage.Scheduler = value;
            }
        }

        /// <summary>
        ///     Gets or sets a value indicating whether initialization should fail or blob storage should be truncated if a chunk
        ///     loading fail at initialization
        /// </summary>
        [PublicAPI]
        public bool TruncateOnChunkInitializationError { get; set; }

        /// <summary>
        ///     Gets a value indicating whether cleanup needed because storage was already initialized when the constructor called
        /// </summary>
        private bool CleanupNeeded { get; }

        /// <summary>
        ///     Gets the comparer to compare keys
        /// </summary>
        private IKeyComparer KeyComparer { get; }

        /// <summary>
        ///     Gets the blob storage
        /// </summary>
        private BlobStorage Storage { get; }

        /// <summary>
        ///     Adds an entry to the cache
        /// </summary>
        /// <param name="key">Entry key</param>
        /// <param name="timeToLive">time to live of the data</param>
        /// <param name="data">Data to cache</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        public async Task Add(string key, DateTime timeToLive, byte[] data, CancellationToken token)
        {
            if (data == null)
                throw new ArgumentNullException(nameof(data));

            using (var ms = new MemoryStream(data))
            {
                await Add(key, timeToLive, ms, token);
            }
        }

        /// <summary>
        ///     Adds an entry to the cache
        /// </summary>
        /// <param name="key">Entry key</param>
        /// <param name="timeToLive">time to live of the data</param>
        /// <param name="data">Data to cache</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        public async Task Add(string key, DateTime timeToLive, Stream data, CancellationToken token)
        {
            if (data == null)
                throw new ArgumentNullException(nameof(data));
            var hash = KeyComparer.GetHash(key);

            // Get old records, will be removed at the end
            var heads = await Heads(key, token);

            var length = (int)(data.Length - data.Position);
            var remaining = (uint)length;
            var ids = new List<uint>();

            // Save data first
            var freeSize = 1u;
            while (remaining > 0)
            {
                token.ThrowIfCancellationRequested();

                // Split data to 5MB blocks
                var blockRemaining = Math.Min(remaining, 5 * 1024 * 1024);

                // If block is bigger than 1k get the free spaces from storage and try to fill them up. To minimize fragmentation use free chunks greater than 1 / 20 of the block size.
                // If block is smaller than 1k do not look for free space because storage will look for a free chunk with enough space
                if (freeSize > 0)
                    freeSize = blockRemaining > 1024 ? (await Storage.GetFreeChunkSizes(token)).FirstOrDefault(s => s > blockRemaining / 20) : 0u;

                var len = blockRemaining;
                if (freeSize > 0)
                    len = Math.Min(len, freeSize - DataHead.DataHeadSize);

                var buffer = new byte[len + DataHead.DataHeadSize];

                var read = 1;
                var readPosition = DataHead.DataHeadSize;
                while (read > 0 && readPosition < len)
                {
                    read = await data.ReadAsync(buffer, readPosition, buffer.Length - readPosition, token);
                    readPosition += read;
                }

                using (var stream = Encode(buffer, new DataHead(CanCompress ? DataCompression.Deflate : DataCompression.None)))
                {
                    var chunk = await Storage.AddChunk(ChunkTypes.Data, hash, stream, token);

                    remaining -= len;
                    ids.Add(chunk.Id);
                }
            }

            // Save header
            var header = new CacheHead { Key = key, TimeToLive = timeToLive.ToUniversalTime(), Chunks = ids, Length = length };
            using (var ms = new MemoryStream())
            using (var w = new BinaryWriter(ms, Encoding.UTF8, true))
            {
                header.ToStream(w);

                ms.Position = 0;
                await Storage.AddChunk(ChunkTypes.Head, hash, ms, token);
            }

            // Remove old heads
            foreach (var h in heads)
            {
                await Storage.RemoveChunk(sc =>
                {
                    var l = sc.Chunks.Where(c => c.Id == h.HeadChunk.Id && c.Type == ChunkTypes.Head && c.UserData == hash).ToList();
                    if (l.Count == 0)
                        return null;
                    return l.First();
                }, token);
                foreach (var ch in h.ValidChunks)
                    await Storage.RemoveChunk(sc =>
                    {
                        var l = sc.Chunks.Where(c => c.Id == ch.Id && c.Type == ChunkTypes.Data && c.UserData == hash).ToList();
                        if (l.Count == 0)
                            return null;
                        return l.First();
                    }, token);
            }
        }

        /// <summary>
        ///     Optimizes storage, removes dead data
        /// </summary>
        /// <returns>Task</returns>
        public Task Cleanup(CancellationToken token)
        {
            return Task.Factory.StartNew(async () =>
            {
                var heads = await Heads(null, token);

                var now = CleanupTime();

                // Find invalid headers and remove the record
                var badHeaders = heads.Where(h => h.TimeToLive < now || h.ValidChunks.Count != h.Chunks.Count).ToList();
                foreach (var r in badHeaders)
                    await Remove(r.Key, token);

                // Find good headers and their data chunk ids
                var goodHeaders = heads.Where(h => h.TimeToLive >= now && h.ValidChunks.Count == h.Chunks.Count).ToList();
                var goodData = goodHeaders.SelectMany(d => d.ValidChunks.Select(c => c.Id)).Distinct().ToDictionary(id => id);

                var oldDataCutoff = now.AddDays(-1);

                // Remove data chunks not belonging to good headers and added more than a day ago
                while (await Storage.RemoveChunk(si => si.Chunks.FirstOrDefault(ch => ch.Type == ChunkTypes.Data && ch.Added < oldDataCutoff && !goodData.ContainsKey(ch.Id)), token))
                {
                }

                // Cut excess space at the storage end
                await Storage.CutBackPadding(token);

                if (MaximumSize <= 0)
                    return;

                // Check storage size is over maximum
                var statistics = await Storage.Statistics(token);

                if (statistics.FileSize < MaximumSize)
                    return;

                // Calculate target size to slim down
                var targetSize = MaximumSize * CutBackRatio;

                // Order heads by remaining time of the record then oldest first.
                heads = (await Heads(null, token)).OrderBy(h => h.TimeToLive).ThenBy(h => h.HeadChunk.Added).ToList();

                // Get the size to shred from storage
                var spaceNeeded = statistics.FileSize - targetSize;
                foreach (var h in heads)
                {
                    if (spaceNeeded < 0)
                        break;

                    await Remove(h.Key, token);

                    // Shred weight of the record (overhead not calculated, a litle extra shredded weight is not a problem)
                    spaceNeeded -= h.Length;
                }

                // Cut excess space at the storage end again
                await Storage.CutBackPadding(token);
            }, token, TaskCreationOptions.DenyChildAttach, Scheduler).Unwrap();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Storage?.Dispose();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///     Checks whether a key exists in the cache
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if an entry exists with the key</returns>
        public async Task<bool> Exists(string key, CancellationToken token)
        {
            return !string.IsNullOrEmpty((await ValidHead(key, token)).Key);
        }

        /// <summary>
        ///     Gets an entry from the cache
        /// </summary>
        /// <param name="key">Entry key to get</param>
        /// <param name="target">Target stream to write the entry</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if found</returns>
        public async Task<bool> Get(string key, Stream target, CancellationToken token)
        {
            var res = await GetWithInfo(key, target, token);
            return res.success;
        }

        /// <summary>
        ///     Gets an entry from the cache
        /// </summary>
        /// <param name="key">Entry key to get</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Entry data, null if not found</returns>
        public async Task<byte[]> Get(string key, CancellationToken token)
        {
            var res = await GetWithInfo(key, token);
            return res.data;
        }

        /// <summary>
        ///     Gets an entry from the cache with extra information
        /// </summary>
        /// <param name="key">Entry key to get</param>
        /// <param name="target">Target stream to write the entry</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Indicator whether entry found and information about the entry</returns>
        public async Task<(bool success, CacheEntryInfo info)> GetWithInfo(string key, Stream target, CancellationToken token)
        {
            var success = false;
            var info = await GetWithInfo(key,
                l => { success = true; },
                d =>
                {
                    using (d)
                    {
                        d.CopyTo(target);
                    }
                },
                token);

            return (success, info);
        }

        /// <summary>
        ///     Gets an entry from the cache with extra information
        /// </summary>
        /// <param name="key">Entry key to get</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Entry data, or null if not found and information about the entry</returns>
        public async Task<(byte[] data, CacheEntryInfo info)> GetWithInfo(string key, CancellationToken token)
        {
            byte[] result = null;

            var position = 0;

            var info = await GetWithInfo(key,
                l => { result = new byte[l]; },
                d =>
                {
                    using (d)
                    {
                        var read = 1;
                        while (read != 0)
                        {
                            read = d.Read(result, position, result.Length - position);
                            position += read;
                        }
                    }
                },
                token);

            return (result, info);
        }

        /// <summary>
        ///     Initializes the cache
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if initialization successfull</returns>
        public async Task<bool> Initialize(CancellationToken token)
        {
            // If storage size exceeds two times the maximum size try to delete the cache and start over
            if (MaximumSize > 0 && Storage.Info.Exists && Storage.Info.Length > MaximumSize * 2)
                try
                {
                    Storage.Info.Delete();
                }
                catch (IOException)
                {
                }
                catch (SecurityException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }

            var res = false;
            Storage.TruncateOnChunkInitializationError = TruncateOnChunkInitializationError;

            if (!RemoveInvalidCache)
            {
                res = await Storage.Initialize<SessionConcurrencyHandler>(token);
            }
            else
            {
                try
                {
                    res = await Storage.Initialize<SessionConcurrencyHandler>(token);
                }
                catch
                {
                    // if initialize failing the storage will be removed and initialization occures again
                }

                if (!res)
                {
                    try
                    {
                        File.Delete(Storage.Info.FullName + ".invalid");
                        File.Move(Storage.Info.FullName, Storage.Info.FullName + ".invalid");
                        //Storage.Info.Delete();
                    }
                    catch (IOException)
                    {
                    }
                    catch (SecurityException)
                    {
                    }
                    catch (UnauthorizedAccessException)
                    {
                    }

                    res = await Storage.Initialize<SessionConcurrencyHandler>(token);
                }
            }

            if (res && (CleanupNeeded || Storage.FreshlyInitialized) && CleanupAtInitialize)
                await Cleanup(token);

            return res;
        }

        /// <summary>
        ///     Remove an entry from the cache
        /// </summary>
        /// <param name="key">Entry key to remove</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if remove successfull</returns>
        public async Task<bool> Remove(string key, CancellationToken token)
        {
            var hash = KeyComparer.GetHash(key);
            var heads = await Heads(key, token);

            if (heads.Count == 0)
                return false;

            foreach (var h in heads)
            {
                await Storage.RemoveChunk(sc =>
                {
                    var l = sc.Chunks.Where(c => c.Id == h.HeadChunk.Id && c.Type == ChunkTypes.Head && c.UserData == hash).ToList();
                    if (l.Count == 0)
                        return null;
                    return l.First();
                }, token);
                foreach (var ch in h.ValidChunks)
                    await Storage.RemoveChunk(sc =>
                    {
                        var l = sc.Chunks.Where(c => c.Id == ch.Id && c.Type == ChunkTypes.Data && c.UserData == hash).ToList();
                        if (l.Count == 0)
                            return null;
                        return l.First();
                    }, token);
            }

            return true;
        }

        /// <summary>
        ///     Gets statistics from the cache
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Cache statistics</returns>
        [PublicAPI]
        public async Task<CacheStatistics> Statistics(CancellationToken token)
        {
            var heads = await Heads(null, token);
            var storageStatistics = await Storage.Statistics(token);

            return new CacheStatistics { FileSize = storageStatistics.FileSize, UsedSpace = storageStatistics.UsedSpace + storageStatistics.Overhead, FreeSpace = storageStatistics.FreeSpace, Overhead = storageStatistics.Overhead + heads.Sum(h => h.HeadChunk.Size) + heads.Sum(h => h.ValidChunks.Count * DataHead.DataHeadSize), CompressionRatio = heads.Count == 0 ? 0 : 1.0 - heads.Average(h => h.ValidChunks.Sum(c => c.Size - DataHead.DataHeadSize) / (double)h.Length), EntriesSize = heads.Sum(h => h.Length), NumberOfEntries = heads.Count, StorageRatio = heads.Count == 0 ? 0 : heads.Average(h => (h.HeadChunk.Size + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize + h.ValidChunks.Sum(c => c.Size + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize)) / (double)h.Length) };
        }

        /// <summary>
        ///     Decodes a data chunk
        /// </summary>
        /// <param name="data">Data to decode</param>
        /// <returns>Decoded data</returns>
        private static Stream Decode(byte[] data)
        {
            // Returns stream instead of byte[] because its one less byte[] allocation
            // Uses byte[] parameter instead of stream because stream would be MemoryStream and it would reallocate it's buffer, so it would be slower

            var head = DataHead.ReadFromByteArray(data);

            switch (head.Compression)
            {
                case DataCompression.None:
                    return new MemoryStream(data, head.Size, data.Length - head.Size, false);

                case DataCompression.Deflate:
                    var input = new MemoryStream(data) { Position = head.Size };
                    return new DeflateStream(input, CompressionMode.Decompress);

                default:
                    throw new InvalidOperationException("Invalid cache compression method");
            }
        }

        /// <summary>
        ///     Encodes a data chunk
        /// </summary>
        /// <param name="buffer">Data to encode</param>
        /// <param name="head">Data head</param>
        /// <returns>Encoded data</returns>
        private static Stream Encode(byte[] buffer, DataHead head)
        {
            // This method returns a stream instead of byte[] so we will get one less byte[] allocation if deflating

            switch (head.Compression)
            {
                case DataCompression.None:
                    head.WriteToByteArray(buffer, null);
                    return new MemoryStream(buffer);

                case DataCompression.Deflate:
                    var output = new MemoryStream();
                    try
                    {
                        head.WriteToStream(output, null);
                        using (var dstream = new DeflateStream(output, CompressionLevel.Optimal, true))
                        {
                            dstream.Write(buffer, DataHead.DataHeadSize, buffer.Length - DataHead.DataHeadSize);
                        }

                        output.Position = 0;
                        if (output.Length < buffer.Length)
                            return output;

                        output.Close();
                    }
                    catch
                    {
                        output.Close();
                        throw;
                    }

                    head.WriteToByteArray(buffer, DataCompression.None);
                    return new MemoryStream(buffer);

                default:
                    head.WriteToByteArray(buffer, DataCompression.None);
                    return new MemoryStream(buffer);
            }
        }

        /// <summary>
        ///     Gets the public info from a cache head
        /// </summary>
        /// <param name="head">Cache head</param>
        /// <returns>Public info</returns>
        private static CacheEntryInfo InfoFromHead(CacheHead head)
        {
            return new CacheEntryInfo { Added = head.HeadChunk.Added };
        }

        /// <summary>
        ///     Gets an entry from the cache with extra information
        /// </summary>
        /// <param name="key">Entry key to get</param>
        /// <param name="initializer">Gives the entry size to the caller</param>
        /// <param name="processor">Processes a chunk of data</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Entry info</returns>
        /// <remarks>
        ///     If the data is found then the actions called in the following order: initializer with the entry size,
        ///     processor with the first chunk ... processor with the last chunk
        /// </remarks>
        private async Task<CacheEntryInfo> GetWithInfo(string key, Action<int> initializer, Action<Stream> processor, CancellationToken token)
        {
            var hash = KeyComparer.GetHash(key);
            var head = await ValidHead(key, token);

            if (string.IsNullOrEmpty(head.Key))
                return default(CacheEntryInfo);

            initializer(head.Length);
            if (head.Length == 0)
                return InfoFromHead(head);

            await Storage.ReadChunks(sc =>
            {
                var list = new List<StorageChunk>();
                foreach (var id in head.Chunks)
                {
                    var chunk = sc.Chunks.FirstOrDefault(c => c.Id == id);
                    if (chunk.UserData != hash || chunk.Type != ChunkTypes.Data)
                        return null;

                    list.Add(chunk);
                }

                return list;
            }, (c, d) => processor(Decode(d)), token);

            return InfoFromHead(head);
        }

        /// <summary>
        ///     Gets the heads belonging to a cache key
        /// </summary>
        /// <param name="key">Cache key to check or null for all the heads</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>List of cache heads found</returns>
        private async Task<List<CacheHead>> Heads([CanBeNull] string key, CancellationToken token)
        {
            if (key == null)
                key = string.Empty;

            var hash = 0u;
            if (!string.IsNullOrEmpty(key))
                hash = KeyComparer.GetHash(key);

            var res = new List<CacheHead>();
            var data = new List<StorageChunk>();

            var readFromCache = false;
            var readHeads = new HashSet<uint>();
            var headData = new List<(StorageChunk Chunk, byte[] Data)>();
            var hasMore = true;
            while (hasMore)
            {
                hasMore = false;

                // Read all head records (matching the key if given) and store all data chunk info
                var headsRead = await Storage.ReadChunks(sc =>
                {
                    lock (_headCache)
                    {
                        // Clear cache if an item removed
                        if (sc.RemovedVersion != _headCacheRemovedVersion)
                        {
                            _headCacheRemovedVersion = _headCacheAddedVersion = sc.RemovedVersion;
                            _headCache.Clear();
                            readHeads.Clear();
                            headData.Clear();
                        }

                        // Remove all head cached data if an item added and check cached non existent data added
                        // If an existing record is replaced it will trigger an item removal so cache will be cleared there
                        if (sc.AddedVersion != _headCacheAddedVersion)
                        {
                            _headCacheAddedVersion = sc.AddedVersion;
                            _headCache.Remove(string.Empty);
                            readHeads.Clear();
                            headData.Clear();

                            var ids = sc.Chunks.Where(c => c.Type == ChunkTypes.Head).Select(c => c.UserData).Distinct().ToDictionary(c => c);

                            foreach (var k in _headCache.Keys.ToList())
                                if (_headCache[k].Heads.Count == 0 && ids.ContainsKey(_headCache[k].Hash))
                                    _headCache.Remove(k);
                        }

                        // Check whether data is in the cache
                        if (_headCache.ContainsKey(key) || _headCache.ContainsKey(string.Empty))
                        {
                            readFromCache = true;
                            return null;
                        }
                    }

                    data = sc.Chunks.Where(c => c.Type == ChunkTypes.Data).ToList();
                    var heads = sc.Chunks.Where(c => c.Type == ChunkTypes.Head);
                    if (!string.IsNullOrEmpty(key))
                        heads = heads.Where(c => c.UserData == hash);
                    else
                    {
                        heads = heads.Where(c => !readHeads.Contains(c.Id));
                        var l = heads.Take(26).ToList();
                        hasMore = l.Count == 26;
                        heads = l.Take(25);
                    }

                    return heads.ToList();
                }, token);

                foreach (var h in headsRead)
                {
                    if (!readHeads.Contains(h.Chunk.Id))
                        readHeads.Add(h.Chunk.Id);
                    headData.Add(h);
                }
            }

            // If data is in the cache return it from there
            if (readFromCache)
                lock (_headCache)
                {
                    if (_headCache.ContainsKey(key))
                        // Return cached data if key is cached
                        return _headCache[key].Heads.ToList();

                    // Filter all heads list for the given key
                    return _headCache[string.Empty].Heads.Where(h => KeyComparer.SameKey(key, h.Key)).ToList();
                }

            token.ThrowIfCancellationRequested();

            // Process loaded head records
            foreach (var h in headData)
                using (var ms = new MemoryStream(h.Data))
                using (var r = new BinaryReader(ms, Encoding.UTF8))
                {
                    var head = CacheHead.FromStream(r);
                    // If key given and not maching, ignore the header
                    if (!string.IsNullOrEmpty(key) && !KeyComparer.SameKey(key, head.Key))
                        continue;
                    head.HeadChunk = h.Chunk;
                    var headHash = KeyComparer.GetHash(head.Key);
                    head.ValidChunks = head.Chunks.Where(c => data.Any(ch => ch.Id == c && ch.UserData == headHash)).Select(c => data.First(ch => ch.Id == c)).ToList();
                    res.Add(head);
                }

            lock (_headCache)
            {
                _headCache[key] = (hash, res.ToList());
            }

            return res;
        }

        /// <summary>
        ///     Gets the last added cache entry for a given key with valid data
        /// </summary>
        /// <param name="key">Key of the cache entry to get</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Cache head belonging to the key</returns>
        private async Task<CacheHead> ValidHead(string key, CancellationToken token)
        {
            var n = DateTime.UtcNow;
            return (await Heads(key, token)).Where(h => h.HeadChunk.Type == ChunkTypes.Head && h.Chunks.Count == h.ValidChunks.Count && h.TimeToLive > n).OrderByDescending(h => h.HeadChunk.Added).FirstOrDefault();
        }
    }
}