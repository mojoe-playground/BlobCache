namespace BlobCache
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Security;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using CityHash;
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

        public Cache(BlobStorage storage)
        {
            Storage = storage;
            CleanupNeeded = Storage.IsInitialized;
        }

        [PublicAPI]
        public double CutBackRatio { get; set; } = 0.8;

        public long MaximumSize { get; set; }

        private bool CleanupNeeded { get; }

        private BlobStorage Storage { get; }

        public async Task Add(string key, DateTime timeToLive, byte[] data, CancellationToken token)
        {
            if (data == null)
                throw new ArgumentNullException(nameof(data));
            var hash = GetHash(key);

            // Get old records, will be removed at the end
            var heads = await Heads(key, token);

            var position = 0u;
            var remaining = (uint)data.Length;
            var ids = new List<uint>();

            // Save data first
            var freeSize = 1u;
            while (remaining > 0)
            {
                // Split data to 5MB blocks
                var blockRemaining = Math.Min(remaining, 5 * 1024 * 1024);

                // If block is bigger than 1k get the free spaces from storage and try to fill them up. To minimize fragmentation use free chunks greater than 1 / 20 of the block size.
                // If block is smaller than 1k do not look for free space because storage will look for a free chunk with enough space
                if (freeSize > 0)
                    freeSize = blockRemaining > 1024 ? (await Storage.GetFreeChunkSizes(token)).FirstOrDefault(s => s > blockRemaining / 20) : 0u;

                var len = blockRemaining;
                if (freeSize > 0)
                    len = Math.Min(len, freeSize);

                var buffer = new byte[len];

                Array.Copy(data, position, buffer, 0, len);

                var chunk = await Storage.AddChunk(ChunkTypes.Data, hash, buffer, token);

                position += len;
                remaining -= len;
                ids.Add(chunk.Id);
            }

            // Save header
            var header = new CacheHead { Key = key, TimeToLive = timeToLive.ToUniversalTime(), Chunks = ids, Length = data.Length };
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
        }

        public async Task<bool> Exists(string key, CancellationToken token)
        {
            return !string.IsNullOrEmpty((await ValidHead(key, token)).Key);
        }

        public async Task<bool> Get(string key, Stream target, CancellationToken token)
        {
            var hash = GetHash(key);
            var head = await ValidHead(key, token);

            if (string.IsNullOrEmpty(head.Key))
                return false;

            if (head.Length == 0)
                return true;

            var chunks = await Storage.ReadChunks(sc =>
            {
                var list = new List<(StorageChunk, Stream)>();
                foreach (var id in head.Chunks)
                {
                    var chunk = sc.Chunks.FirstOrDefault(c => c.Id == id);
                    if (chunk.UserData != hash || chunk.Type != ChunkTypes.Data)
                        return null;

                    list.Add((chunk, target));
                }

                return list;
            }, token);

            if (chunks.Count == 0)
                return false;

            return true;
        }

        public async Task<byte[]> Get(string key, CancellationToken token)
        {
            var hash = GetHash(key);
            var head = await ValidHead(key, token);

            if (string.IsNullOrEmpty(head.Key))
                return null;

            var result = new byte[head.Length];
            if (head.Length == 0)
                return result;

            var position = 0;

            var chunks = await Storage.ReadChunks(sc =>
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
            }, token);

            if (chunks.Count == 0)
                return null;

            foreach (var c in chunks)
            {
                Array.Copy(c.Data, 0, result, position, c.Data.Length);
                position += c.Data.Length;
            }

            return result;
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

            var res = await Storage.Initialize<SessionConcurrencyHandler>(token);

            if (res && (CleanupNeeded || Storage.FreshlyInitialized))
                await Cleanup(token);

            return res;
        }

        public async Task<bool> Remove(string key, CancellationToken token)
        {
            var hash = GetHash(key);
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

        private static uint GetHash(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
            return CityHash.CityHash32(key, Encoding.UTF8);
        }

        private static bool KeyEquals(string key1, string key2)
        {
            return string.Equals(key1, key2);
        }

        private async Task<List<CacheHead>> Heads(string key, CancellationToken token)
        {
            if (key == null)
                key = string.Empty;

            var hash = 0u;
            if (!string.IsNullOrEmpty(key))
                hash = GetHash(key);

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
                    return _headCache[string.Empty].Heads.Where(h => KeyEquals(key, h.Key)).ToList();
                }

            token.ThrowIfCancellationRequested();

            // Process loaded head records
            foreach (var h in headData)
                using (var ms = new MemoryStream(h.Data))
                using (var r = new BinaryReader(ms, Encoding.UTF8))
                {
                    var head = CacheHead.FromStream(r);
                    // If key given and not maching, ignore the header
                    if (!string.IsNullOrEmpty(key) && !KeyEquals(key, head.Key))
                        continue;
                    head.HeadChunk = h.Chunk;
                    var headHash = GetHash(head.Key);
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