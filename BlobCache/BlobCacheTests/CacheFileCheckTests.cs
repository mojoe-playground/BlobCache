namespace BlobCacheTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using BlobCache;
    using BlobCache.ConcurrencyModes;
    using Xunit;
    using Xunit.Abstractions;

    public class CacheFileCheckTests
    {
        //private const string CacheToTest = @"c:\Users\mojoe.Home\AppData\Local\VideoPlayer\cache-ReferencedItem.blob";
        //private const string CacheToTest = @"c:\Users\mojoe.Home\AppData\Local\VideoPlayer\cache-StringData.blob";
        //private const string CacheToTest = @"c:\Users\mojoe.Home\AppData\Local\VideoPlayer\cache-TheMovieDBData.blob";
        private const string CacheToTest = "CacheTest.blob";
        //private const string CacheToTest = @"c:\Users\mojoe.Home\AppData\Local\VideoPlayer\cache-ImageLod.blob.invalid";

        private static readonly IKeyComparer KeyComparer = new CaseSensitiveKeyComparer();
        private readonly double CutBackRatio = 0.8;

        private readonly long MaximumSize = 0;

        public CacheFileCheckTests(ITestOutputHelper output)
        {
            Output = output;

            using (var s = new BlobStorage("CacheTest.blob"))
            {
                s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None).GetAwaiter().GetResult();
            }
        }

        private ITestOutputHelper Output { get; }

        [Fact]
        public async Task Statistics()
        {
            using (var c = new Cache(CacheToTest) { RemoveInvalidCache = false, CleanupAtInitialize = false })
            {
                Assert.True(await c.Initialize(CancellationToken.None));
                var s = await c.Statistics(CancellationToken.None);
                Output.WriteLine("CompressionRatio: {0:P}", s.CompressionRatio);
                Output.WriteLine("EntriesSize: {0}", s.EntriesSize);
                Output.WriteLine("FileSize: {0}", s.FileSize);
                Output.WriteLine("FreeSpace: {0}", s.FreeSpace);
                Output.WriteLine("NumberOfEntries: {0}", s.NumberOfEntries);
                Output.WriteLine("Overhead: {0}", s.Overhead);
                Output.WriteLine("StorageRatio: {0:P}", s.StorageRatio);
                Output.WriteLine("UsedSpace: {0}", s.UsedSpace);
            }
        }

        [Fact]
        public async Task CleanupDryRun()
        {
            var sw = new Stopwatch();
            sw.Start();
            try
            {
                using (var s = new BlobStorage(CacheToTest))
                {
                    Output.WriteLine("Opening storage");
                    Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));

                    Output.WriteLine($"Opening elapsed time: {sw.ElapsedMilliseconds} ms");
                    var delta = sw.ElapsedMilliseconds;

                    var heads = await Heads(s, null, CancellationToken.None);
                    Output.WriteLine($"Heads loaded: {sw.ElapsedMilliseconds - delta} ms");

                    var now = DateTime.UtcNow;

                    // Find invalid headers and remove the record
                    var badHeaders = heads.Where(h => h.TimeToLive < now || h.ValidChunks.Count != h.Chunks.Count).ToList();
                    foreach (var r in badHeaders)
                        Output.WriteLine($"Removing old/invalid header: {r}");
                    //await Remove(r.Key, token);

                    // Find good headers and their data chunk ids
                    var goodHeaders = heads.Where(h => h.TimeToLive >= now && h.ValidChunks.Count == h.Chunks.Count).ToList();
                    var goodData = goodHeaders.SelectMany(d => d.ValidChunks.Select(c => c.Id)).Distinct().ToDictionary(id => id);

                    var oldDataCutoff = now.AddDays(-1);
                    var chunks = await s.GetChunks(CancellationToken.None);

                    // Remove data chunks not belonging to good headers and added more than a day ago
                    foreach (var c in chunks.Where(ch => ch.Type == ChunkTypes.Data && ch.Added < oldDataCutoff && !goodData.ContainsKey(ch.Id) && !ch.Changing).ToList())
                        Output.WriteLine($"Removing data chunks not belonging to good headers: {c}");
                    //await Storage.RemoveChunk(sc => sc.Chunks.FirstOrDefault(ch => ch.Id == c.Id && ch.Type == c.Type && ch.Position == c.Position && ch.Size == c.Size && ch.UserData == c.UserData), token);

                    // Cut excess space at the storage end
                    //await s.CutBackPadding(CancellationToken.None);

                    if (MaximumSize <= 0)
                        return;

                    // Check storage size is over maximum
                    var statistics = await s.Statistics(CancellationToken.None);

                    if (statistics.FileSize < MaximumSize)
                        return;

                    // Calculate target size to slim down
                    var targetSize = MaximumSize * CutBackRatio;

                    // Order heads by remaining time of the record then oldest first.
                    heads = /*(await Heads(s, null, CancellationToken.None))*/goodHeaders.OrderBy(h => h.TimeToLive).ThenBy(h => h.HeadChunk.Added).ToList();

                    // Get the size to shred from storage
                    var spaceNeeded = statistics.FileSize - targetSize;
                    foreach (var h in heads)
                    {
                        if (spaceNeeded < 0)
                            break;

                        Output.WriteLine($"Removing old header: {h}");
                        //await Remove(h.Key, token);

                        // Shred weight of the record (overhead not calculated, a litle extra shredded weight is not a problem)
                        spaceNeeded -= h.Length;
                    }

                    // Cut excess space at the storage end again
                    //await Storage.CutBackPadding(token);
                }
            }
            finally
            {
                sw.Stop();
                Output.WriteLine($"Cleanup dry run elapsed time: {sw.ElapsedMilliseconds} ms");
            }
        }

        [Fact]
        public async Task Open()
        {
            var sw = new Stopwatch();
            sw.Start();
            using (var s = new BlobStorage(CacheToTest))
            {
                Output.WriteLine("Opening storage");
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));
            }
            sw.Stop();
            Output.WriteLine($"Opening elapsed time: {sw.ElapsedMilliseconds} ms");
        }


        private async Task<List<CacheHead>> Heads(BlobStorage storage, string key, CancellationToken token)
        {
            if (key == null)
                key = string.Empty;

            var hash = 0u;
            if (!string.IsNullOrEmpty(key))
                hash = KeyComparer.GetHash(key);

            var res = new List<CacheHead>();
            var data = new List<StorageChunk>();

            // Read all head records (matching the key if given) and store all data chunk info
            var headData = await storage.ReadChunks(sc =>
            {
                data = sc.Chunks.Where(c => c.Type == ChunkTypes.Data).ToList();
                var heads = sc.Chunks.Where(c => c.Type == ChunkTypes.Head);
                if (!string.IsNullOrEmpty(key))
                    heads = heads.Where(c => c.UserData == hash);
                return heads.ToList();
            }, token);

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

            return res;
        }
    }
}