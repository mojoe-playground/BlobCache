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
        public Cache(string fileName)
            : this(new BlobStorage(fileName, new SessionConcurrencyHandler()))
        {
        }

        public Cache(BlobStorage storage)
        {
            Storage = storage;
        }

        private BlobStorage Storage { get; }

        private async Task< CacheHead> ValidHead(string key)
        {
            var n = DateTime.Now;
            return (await Heads(key)).LastOrDefault(h => h.HeadChunk.Type == ChunkTypes.Head && h.Chunks.Count == h.ValidChunks.Count && h.TimeToLive > n);
        }

        public async Task<bool> Exists(string key)
        {
            return !string.IsNullOrEmpty((await ValidHead(key)).Key);
        }

        public async Task<byte[]> Get(string key)
        {
            var head = await ValidHead(key);

            if (string.IsNullOrEmpty(head.Key))
                return null;

            var result = new byte[head.Length];
            var position = 0;
            foreach (var c in head.Chunks)
            {
                var data = await Storage.ReadChunk(c);
                Array.Copy(data, 0, result, position, data.Length);
                position += data.Length;
            }

            return result;
        }

        public void Dispose()
        {
            Storage?.Dispose();
        }

        public async Task<bool> Initialize()
        {
            return await Storage.Initialize();
        }

        public async Task Add(string key, DateTime timeToLive, byte[] data)
        {
            if (data == null)
                throw new ArgumentNullException(nameof(data));
            var hash = GetHash(key);

            // Get old records, will be removed at the end
            var heads = await Heads(key);

            var position = 0u;
            var remaining = (uint) data.Length;
            var ids = new List<uint>();

            // Save data first
            var freeSize = 1u;
            while (remaining > 0)
            {
                // If data is bigger than 1k get the free spaces from storage and try to fill them up. To minimize fragmentation use free blocks greater than 1 / 20 of the data size.
                // If data is smaller than 1k and there is a free slot with enough space storage will save there
                if (freeSize > 0)
                    freeSize = remaining > 1024
                        ? (await Storage.GetFreeChunkSizes()).FirstOrDefault(s => s > remaining / 20)
                        : 0u;

                var len = remaining;
                if (freeSize > 0)
                    len = Math.Min(len, freeSize);

                var buffer = new byte[len];

                Array.Copy(data, position, buffer, 0, len);

                var chunk = await Storage.AddChunk(ChunkTypes.Data, hash, buffer);

                position += len;
                remaining -= len;
                ids.Add(chunk.Id);
            }

            // Save header
            var header = new CacheHead {Key = key, TimeToLive = timeToLive, Chunks = ids, Length = data.Length};
            using (var ms = new MemoryStream())
            using (var w = new BinaryWriter(ms, Encoding.UTF8, true))
            {
                header.ToStream(w);

                await Storage.AddChunk(ChunkTypes.Head, hash, ms.ToArray());
            }

            // Remove old heads
            foreach (var h in heads)
            {
                foreach (var id in h.ValidChunks)
                    await Storage.RemoveChunk(id);
                await Storage.RemoveChunk(h.HeadChunk);
            }
        }

        public async Task<bool> Remove(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(key);

            var heads = await Heads(key);

            if (heads.Count == 0)
                return false;

            foreach (var h in heads)
            {
                foreach (var id in h.ValidChunks)
                    await Storage.RemoveChunk(id);
                await Storage.RemoveChunk(h.HeadChunk);
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
            var chunks = await Storage.GetChunks();
            var heads = chunks.Where(c => c.Type == ChunkTypes.Head).ToList();
            var data = chunks.Where(c => c.Type == ChunkTypes.Data).ToList();

            if (!string.IsNullOrEmpty(key))
            {
                var hash = GetHash(key);
                heads = heads.Where(c => c.UserData == hash).ToList();
            }

            var res = new List<CacheHead>();
            foreach (var h in heads)
                using (var ms = new MemoryStream(await Storage.ReadChunk(h.Id)))
                using (var r = new BinaryReader(ms, Encoding.UTF8))
                {
                    var head = CacheHead.FromStream(r);
                    if (!string.IsNullOrEmpty(key) && !KeyEquals(key, head.Key))
                        continue;
                    head.HeadChunk = h;
                    var hash = GetHash(head.Key);
                    head.ValidChunks = head.Chunks.Where(c => data.Any(ch => ch.Id == c && ch.UserData == hash))
                        .Select(c => data.First(ch => ch.Id == c)).ToList();
                    res.Add(head);
                }

            return res;
        }
    }
}