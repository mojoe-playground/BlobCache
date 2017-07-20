namespace BlobCacheTests
{
    using System.IO;
    using System.Linq;
    using System.Threading;
    using BlobCache;
    using BlobCache.ConcurrencyModes;
    using Xunit;

    public class GlobalBlobStorageTests
    {
        [Fact]
        public async void AddChunk()
        {
            File.Delete("globaltest.blob");
            using (var s = new BlobStorage("globaltest.blob"))
            {
                Assert.True(await s.Initialize<SessionConcurrencyHandler>(CancellationToken.None));

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data, CancellationToken.None);
                Assert.Equal(1u, c1.Id);
                Assert.Equal(11u, c1.UserData);
                Assert.Equal((uint)data.Length, c1.Size);

                var res = await s.ReadChunks(sc => sc.Chunks.Where(c => c.Id == 1), CancellationToken.None);
                Assert.Equal(data, res.First().Data);
            }
        }
    }
}