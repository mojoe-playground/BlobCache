namespace BlobCacheTests
{
    using System.IO;
    using System.Linq;
    using BlobCache;
    using BlobCache.ConcurrencyModes;
    using Xunit;

    public class GlobalBlobStorageTests
    {
        [Fact]
        public async void AddChunk()
        {
            File.Delete("globaltest.blob");
            using (var s = new BlobStorage("globaltest.blob", new SessionConcurrencyHandler()))
            {
                Assert.True(await s.Initialize());

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data);
                Assert.Equal(1u, c1.Id);
                Assert.Equal(11u, c1.UserData);
                Assert.Equal((uint)data.Length, c1.Size);

                var res = await s.ReadChunks((sc, v) => sc.Where(c => c.Id == 1));
                Assert.Equal(data, res.First().Data);
            }
        }
    }
}