namespace BlobCache
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

    public class Cache : IDisposable
    {
        private readonly Dictionary<string, (uint Hash, List<CacheHead> Heads)> _headCache = new Dictionary<string, (uint, List<CacheHead>)>();

        /// <summary>
        ///     Gets the reference time used for cleanup
        /// </summary>
        /// <remarks>Used in tests to simulate old data in storages</remarks>
        internal Func<DateTime> CleanupTime = () => DateTime.UtcNow;

        private ulong _headCacheAddedVersion;
        private ulong _headCacheRemovedVersion;

        public Cache(string fileName)
            : this(new BlobStorage(fileName))
        {
        }

        public Cache(string fileName, IKeyComparer keyComparer)
            : this(new BlobStorage(fileName), keyComparer)
        {
        }

        public Cache(BlobStorage storage)
            : this(storage, new CaseSensitiveKeyComparer())
        {
        }

        public Cache(BlobStorage storage, IKeyComparer keyComparer)
        {
            Storage = storage;
            CleanupNeeded = Storage.IsInitialized;
            KeyComparer = keyComparer ?? throw new ArgumentNullException(nameof(keyComparer));
        }

        ~Cache()
        {
            Dispose();
        }

        [PublicAPI]
        public bool CanCompress { get; set; }

        [PublicAPI]
        public double CutBackRatio { get; set; } = 0.8;

        public long MaximumSize { get; set; }

        [PublicAPI]
        public bool RemoveInvalidCache { get; set; }

        [PublicAPI]
        public bool CleanupAtInitialize { get; set; } = true;

        private bool CleanupNeeded { get; }

        private IKeyComparer KeyComparer { get; }

        private BlobStorage Storage { get; }

        public async Task Add(string key, DateTime timeToLive, byte[] data, CancellationToken token)
        {
            if (data == null)
                throw new ArgumentNullException(nameof(data));

            using (var ms = new MemoryStream(data))
            {
                await Add(key, timeToLive, ms, token);
            }
        }

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

                buffer = Encode(buffer, new DataHead(CanCompress ? DataCompression.Deflate : DataCompression.None));

                var chunk = await Storage.AddChunk(ChunkTypes.Data, hash, buffer, token);

                remaining -= len;
                ids.Add(chunk.Id);
            }

            // Save header
            var header = new CacheHead { Key = key, TimeToLive = timeToLive.ToUniversalTime(), Chunks = ids, Length = length };
            using (var ms = new MemoryStream())
            using (var w = new BinaryWriter(ms, Encoding.UTF8, true))
            {
                header.ToStream(w);

                await Storage.AddChunk(ChunkTypes.Head, hash, ms.ToArray(), token);
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
            return Task.Run(async () =>
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
                var chunks = await Storage.GetChunks(token);

                // Remove data chunks not belonging to good headers and added more than a day ago
                foreach (var c in chunks.Where(ch => ch.Type == ChunkTypes.Data && ch.Added < oldDataCutoff && !goodData.ContainsKey(ch.Id) && !ch.Changing).ToList())
                    await Storage.RemoveChunk(sc => sc.Chunks.FirstOrDefault(ch => ch.Id == c.Id && ch.Type == c.Type && ch.Position == c.Position && ch.Size == c.Size && ch.UserData == c.UserData), token);

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
            }, token);
        }

        public void Dispose()
        {
            Storage?.Dispose();
            GC.SuppressFinalize(this);
        }

        public async Task<bool> Exists(string key, CancellationToken token)
        {
            return !string.IsNullOrEmpty((await ValidHead(key, token)).Key);
        }

        public async Task<bool> Get(string key, Stream target, CancellationToken token)
        {
            var res = await GetWithInfo(key, target, token);
            return res.success;
        }

        public async Task<byte[]> Get(string key, CancellationToken token)
        {
            var res = await GetWithInfo(key, token);
            return res.data;
        }

        public async Task<(bool success, CacheEntryInfo info)> GetWithInfo(string key, Stream target, CancellationToken token)
        {
            var success = false;
            var info = await GetWithInfo(key,
                l => { success = true; },
                d => { target.Write(d, 0, d.Length); },
                token);

            return (success, info);
        }

        public async Task<(byte[] data, CacheEntryInfo info)> GetWithInfo(string key, CancellationToken token)
        {
            byte[] result = null;

            var position = 0;

            var info = await GetWithInfo(key,
                l => { result = new byte[l]; },
                d =>
                {
                    Array.Copy(d, 0, result, position, d.Length);
                    position += d.Length;
                },
                token);

            return (result, info);
        }

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

            if (!RemoveInvalidCache)
                res = await Storage.Initialize<SessionConcurrencyHandler>(token);
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

                    res = await Storage.Initialize<SessionConcurrencyHandler>(token);
                }
            }

            if (res && (CleanupNeeded || Storage.FreshlyInitialized) && CleanupAtInitialize)
                await Cleanup(token);

            return res;
        }

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

        private static byte[] Decode(byte[] data)
        {
            var head = DataHead.ReadFromByteArray(data);

            switch (head.Compression)
            {
                case DataCompression.None:
                    var res = new byte[data.Length - head.Size];
                    Array.Copy(data, head.Size, res, 0, res.Length);
                    return res;

                case DataCompression.Deflate:
                    using (var input = new MemoryStream(data))
                    {
                        input.Position = head.Size;

                        using (var output = new MemoryStream())
                        {
                            using (var dstream = new DeflateStream(input, CompressionMode.Decompress))
                            {
                                dstream.CopyTo(output);
                            }

                            return output.ToArray();
                        }
                    }

                default:
                    throw new InvalidOperationException("Invalid cache compression method");
            }
        }

        private static byte[] Encode(byte[] buffer, DataHead head)
        {
            switch (head.Compression)
            {
                case DataCompression.None:
                    head.WriteToByteArray(buffer, null);
                    return buffer;

                case DataCompression.Deflate:
                    using (var output = new MemoryStream())
                    {
                        head.WriteToStream(output, null);
                        using (var dstream = new DeflateStream(output, CompressionLevel.Optimal, true))
                        {
                            dstream.Write(buffer, DataHead.DataHeadSize, buffer.Length - DataHead.DataHeadSize);
                        }

                        if (output.Length < buffer.Length)
                            return output.ToArray();
                    }

                    head.WriteToByteArray(buffer, DataCompression.None);
                    return buffer;

                default:
                    head.WriteToByteArray(buffer, DataCompression.None);
                    return buffer;
            }
        }

        private static CacheEntryInfo InfoFromHead(CacheHead head)
        {
            return new CacheEntryInfo { Added = head.HeadChunk.Added };
        }

        private async Task<CacheEntryInfo> GetWithInfo(string key, Action<int> initializer, Action<byte[]> processor, CancellationToken token)
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

        private async Task<List<CacheHead>> Heads(string key, CancellationToken token)
        {
            if (key == null)
                key = string.Empty;

            var hash = 0u;
            if (!string.IsNullOrEmpty(key))
                hash = KeyComparer.GetHash(key);

            var res = new List<CacheHead>();
            var data = new List<StorageChunk>();

            var readFromCache = false;
            // Read all head records (matching the key if given) and store all data chunk info
            var headData = await Storage.ReadChunks(sc =>
            {
                lock (_headCache)
                {
                    // Clear cache if an item removed
                    if (sc.RemovedVersion != _headCacheRemovedVersion)
                    {
                        _headCacheRemovedVersion = _headCacheAddedVersion = sc.RemovedVersion;
                        _headCache.Clear();
                    }

                    // Remove all head cached data if an item added and check cached non existent data added
                    // If an existing record is replaced it will trigger an item removal so cache will be cleared there
                    if (sc.AddedVersion != _headCacheAddedVersion)
                    {
                        _headCacheAddedVersion = sc.AddedVersion;
                        _headCache.Remove(string.Empty);

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
                return heads.ToList();
            }, token);

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

        private async Task<CacheHead> ValidHead(string key, CancellationToken token)
        {
            var n = DateTime.UtcNow;
            return (await Heads(key, token)).LastOrDefault(h => h.HeadChunk.Type == ChunkTypes.Head && h.Chunks.Count == h.ValidChunks.Count && h.TimeToLive > n);
        }
    }
}