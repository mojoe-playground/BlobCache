namespace BlobCache
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using CityHash;
    using ConcurrencyModes;

    public class Cache : IDisposable
    {
        private readonly Dictionary<string, List<CacheHead>> _headCache = new Dictionary<string, List<CacheHead>>();
        private ulong _headCacheVersion;

        public Cache(string fileName)
            : this(new BlobStorage(fileName))
        {
        }

        public Cache(BlobStorage storage)
        {
            Storage = storage;
        }

        private BlobStorage Storage { get; }

        public async Task Add(string key, DateTime timeToLive, byte[] data)
        {
            if (data == null)
                throw new ArgumentNullException(nameof(data));
            var hash = GetHash(key);

            // Get old records, will be removed at the end
            var heads = await Heads(key);

            var position = 0u;
            var remaining = (uint)data.Length;
            var ids = new List<uint>();

            // Save data first
            var freeSize = 1u;
            while (remaining > 0)
            {
                // If data is bigger than 1k get the free spaces from storage and try to fill them up. To minimize fragmentation use free blocks greater than 1 / 20 of the data size.
                // If data is smaller than 1k and there is a free slot with enough space storage will save there
                if (freeSize > 0)
                    freeSize = remaining > 1024 ? (await Storage.GetFreeChunkSizes()).FirstOrDefault(s => s > remaining / 20) : 0u;

                var len = remaining;
                len = Math.Min(len, freeSize > 0 ? freeSize : 5 * 1024 * 1024);

                var buffer = new byte[len];

                Array.Copy(data, position, buffer, 0, len);

                var chunk = await Storage.AddChunk(ChunkTypes.Data, hash, buffer);

                position += len;
                remaining -= len;
                ids.Add(chunk.Id);
            }

            // Save header
            var header = new CacheHead { Key = key, Added = DateTime.UtcNow, TimeToLive = timeToLive.ToUniversalTime(), Chunks = ids, Length = data.Length };
            using (var ms = new MemoryStream())
            using (var w = new BinaryWriter(ms, Encoding.UTF8, true))
            {
                header.ToStream(w);

                await Storage.AddChunk(ChunkTypes.Head, hash, ms.ToArray());
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
                });
                foreach (var ch in h.ValidChunks)
                    await Storage.RemoveChunk(sc =>
                    {
                        var l = sc.Chunks.Where(c => c.Id == ch.Id && c.Type == ChunkTypes.Data && c.UserData == hash).ToList();
                        if (l.Count == 0)
                            return null;
                        return l.First();
                    });
            }
        }

        public void Dispose()
        {
            Storage?.Dispose();
        }

        public async Task<bool> Exists(string key)
        {
            return !string.IsNullOrEmpty((await ValidHead(key)).Key);
        }

        public async Task<bool> Get(string key, Stream target)
        {
            var hash = GetHash(key);
            var head = await ValidHead(key);

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
            });

            if (chunks.Count == 0)
                return false;

            return true;
        }

        public async Task<byte[]> Get(string key)
        {
            var hash = GetHash(key);
            var head = await ValidHead(key);

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
            });

            if (chunks.Count == 0)
                return null;

            foreach (var c in chunks)
            {
                Array.Copy(c.Data, 0, result, position, c.Data.Length);
                position += c.Data.Length;
            }

            return result;
        }

        public async Task<bool> Initialize()
        {
            return await Storage.Initialize<SessionConcurrencyHandler>();
        }

        public async Task<bool> Remove(string key)
        {
            var hash = GetHash(key);
            var heads = await Heads(key);

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
                });
                foreach (var ch in h.ValidChunks)
                    await Storage.RemoveChunk(sc =>
                    {
                        var l = sc.Chunks.Where(c => c.Id == ch.Id && c.Type == ChunkTypes.Data && c.UserData == hash).ToList();
                        if (l.Count == 0)
                            return null;
                        return l.First();
                    });
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

        private async Task<List<CacheHead>> Heads(string key)
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
                    // Clear cache if cache version not current
                    if (sc.Version != _headCacheVersion)
                    {
                        _headCacheVersion = sc.Version;
                        _headCache.Clear();
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
            });

            // If data is in the cache return it from there
            if (readFromCache)
                lock (_headCache)
                {
                    if (_headCache.ContainsKey(key))
                        // Return cached data if key is cached
                        return _headCache[key].ToList();

                    // Filter all heads list for the given key
                    return _headCache[string.Empty].Where(h => KeyEquals(key, h.Key)).ToList();
                }

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
                _headCache[key] = res.ToList();
            }

            return res;
        }

        private async Task<CacheHead> ValidHead(string key)
        {
            var n = DateTime.UtcNow;
            return (await Heads(key)).LastOrDefault(h => h.HeadChunk.Type == ChunkTypes.Head && h.Chunks.Count == h.ValidChunks.Count && h.TimeToLive > n);
        }
    }
}