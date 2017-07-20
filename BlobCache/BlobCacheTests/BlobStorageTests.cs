namespace BlobCacheTests
{
    using System.IO;
    using System.Linq;
    using System.Threading;
    using BlobCache;
    using BlobCache.ConcurrencyModes;
    using Xunit;

    public class BlobStorageTests
    {
        [Fact]
        public async void AddChunk()
        {
            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data, CancellationToken.None);
                Assert.Equal(1u, c1.Id);
                Assert.Equal(11u, c1.UserData);
                Assert.Equal((uint)data.Length, c1.Size);

                var res = await s.ReadChunks(c => c.Id == 1, CancellationToken.None);
                Assert.Equal(data, res.First().Data);
            }
        }

        [Fact]
        public async void ChunkIdReuse()
        {
            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data, CancellationToken.None);
                var c2 = await s.AddChunk(ChunkTypes.Test, 12, data, CancellationToken.None);
                await s.AddChunk(ChunkTypes.Test, 13, data, CancellationToken.None);

                Assert.Equal(3, (await s.GetChunks(CancellationToken.None)).Count);

                await s.RemoveChunk(sc => sc.Chunks.FirstOrDefault(c => c.Id == c1.Id), CancellationToken.None);
                await s.RemoveChunk(sc => sc.Chunks.FirstOrDefault(c => c.Id == c2.Id), CancellationToken.None);

                var c4 = await s.AddChunk(ChunkTypes.Test, 14, data, CancellationToken.None);
                Assert.Equal(c1.Id, c4.Id);
            }
        }

        [Fact]
        public async void FreeSpaceCombination()
        {
            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data, CancellationToken.None);
                var c2 = await s.AddChunk(ChunkTypes.Test, 12, data, CancellationToken.None);
                await s.AddChunk(ChunkTypes.Test, 13, data, CancellationToken.None);

                Assert.Equal(3, (await s.GetChunks(CancellationToken.None)).Count);

                await s.RemoveChunk(sc => sc.Chunks.FirstOrDefault(c => c.Id == c2.Id), CancellationToken.None);

                var chunks = await s.GetChunks(CancellationToken.None);
                Assert.Equal(3, chunks.Count);
                Assert.Equal(1, chunks.Count(c => c.Type == ChunkTypes.Free));

                await s.RemoveChunk(sc => sc.Chunks.FirstOrDefault(c => c.Id == c1.Id), CancellationToken.None);

                chunks = await s.GetChunks(CancellationToken.None);
                Assert.Equal(2, chunks.Count);
                Assert.Equal(1, chunks.Count(c => c.Type == ChunkTypes.Free));
            }

            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data, CancellationToken.None);
                var c2 = await s.AddChunk(ChunkTypes.Test, 12, data, CancellationToken.None);
                await s.AddChunk(ChunkTypes.Test, 13, data, CancellationToken.None);

                Assert.Equal(3, (await s.GetChunks(CancellationToken.None)).Count);

                await s.RemoveChunk(sc => sc.Chunks.FirstOrDefault(c => c.Id == c1.Id), CancellationToken.None);

                var chunks = await s.GetChunks(CancellationToken.None);
                Assert.Equal(3, chunks.Count);
                Assert.Equal(1, chunks.Count(c => c.Type == ChunkTypes.Free));

                await s.RemoveChunk(sc => sc.Chunks.FirstOrDefault(c => c.Id == c2.Id), CancellationToken.None);

                chunks = await s.GetChunks(CancellationToken.None);
                Assert.Equal(2, chunks.Count);
                Assert.Equal(1, chunks.Count(c => c.Type == ChunkTypes.Free));
            }
        }

        [Fact]
        public async void GetChunks()
        {
            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));

                Assert.Empty(await s.GetChunks(CancellationToken.None));

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data, CancellationToken.None);

                Assert.Equal(c1, (await s.GetChunks(CancellationToken.None)).Single());

                await s.RemoveChunk(sc => sc.Chunks.FirstOrDefault(c => c.Id == c1.Id), CancellationToken.None);

                Assert.Equal(c1, (await s.GetChunks(CancellationToken.None)).Single());

                Assert.Equal(ChunkTypes.Free, (await s.GetChunks(CancellationToken.None)).Single().Type);
            }
        }

        [Fact]
        public async void Initialization()
        {
            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));
                Assert.True(await s.Initialize<SessionConcurrencyHandler>(CancellationToken.None));
            }

            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));

                using (var s1 = new BlobStorage("test.blob"))
                {
                    Assert.True(await s1.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));
                }
            }

            File.Delete("test2.blob");
            File.WriteAllText("test2.blob", "ab");
            using (var s = new BlobStorage("test2.blob"))
            {
                Assert.False(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));
            }

            File.Delete("test2.blob");
            File.WriteAllText("test2.blob",
                "ABCD234                                                                    ");
            using (var s = new BlobStorage("test2.blob"))
            {
                Assert.False(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));
            }

            File.Delete("test2.blob");
            File.WriteAllText("test2.blob",
                "BLOB234                                                                    ");
            using (var s = new BlobStorage("test2.blob"))
            {
                Assert.False(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));
            }
        }

        [Fact]
        public async void RemoveChunk()
        {
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data, CancellationToken.None);
                Assert.Equal(11u, c1.UserData);
                Assert.Equal((uint)data.Length, c1.Size);

                var res = await s.ReadChunks(c => c.Id == c1.Id, CancellationToken.None);
                Assert.Equal(data, res.First().Data);

                await s.RemoveChunk(sc => sc.Chunks.FirstOrDefault(c => c.Id == c1.Id), CancellationToken.None);

                var chunks = await s.GetChunks(CancellationToken.None);
                Assert.Equal(ChunkTypes.Free, chunks.Single(c => c.Id == c1.Id).Type);
            }
        }

        [Fact]
        public async void SpaceReuse()
        {
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data, CancellationToken.None);
                Assert.Equal(11u, c1.UserData);
                Assert.Equal((uint)data.Length, c1.Size);

                var res = await s.ReadChunks(c => c.Id == c1.Id, CancellationToken.None);
                Assert.Equal(data, res.First().Data);

                data = Enumerable.Range(0, 256).Select(r => (byte)2).ToArray();
                var c2 = await s.AddChunk(ChunkTypes.Test, 12, data, CancellationToken.None);
                Assert.Equal(12u, c2.UserData);
                Assert.Equal((uint)data.Length, c2.Size);

                res = await s.ReadChunks(c => c.Id == c2.Id, CancellationToken.None);
                Assert.Equal(data, res.First().Data);

                var size = s.Info.Length;

                await s.RemoveChunk(sc => sc.Chunks.FirstOrDefault(c => c.Id == c1.Id), CancellationToken.None);

                data = Enumerable.Range(0, 128).Select(r => (byte)3).ToArray();
                var c3 = await s.AddChunk(ChunkTypes.Test, 13, data, CancellationToken.None);
                Assert.Equal(13u, c3.UserData);
                Assert.Equal((uint)data.Length, c3.Size);

                res = await s.ReadChunks(c => c.Id == c3.Id, CancellationToken.None);
                Assert.Equal(data, res.First().Data);

                Assert.Equal(size, s.Info.Length);
            }
        }

        [Fact]
        public async void StreamRead()
        {
            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data, CancellationToken.None);

                using (var ms = new MemoryStream())
                {
                    // ReSharper disable once AccessToDisposedClosure
                    var res = await s.ReadChunks(c => c.Id == c1.Id, c => ms, CancellationToken.None);
                    Assert.Equal(data, ms.ToArray());
                    Assert.Equal(ms, res.First().Data);
                }

                using (var ms = new MemoryStream())
                {
                    // ReSharper disable once AccessToDisposedClosure
                    var res = await s.ReadChunks(sc => sc.Chunks.Where(c => c.Id == c1.Id), c => ms, CancellationToken.None);
                    Assert.Equal(data, ms.ToArray());
                    Assert.Equal(ms, res.First().Data);
                }

                using (var ms = new MemoryStream())
                {
                    // ReSharper disable once AccessToDisposedClosure
                    var res = await s.ReadChunks(c => c.Id == c1.Id ? ms : null, CancellationToken.None);
                    Assert.Equal(data, ms.ToArray());
                    Assert.Equal(ms, res.First().Data);
                }

                using (var ms = new MemoryStream())
                {
                    // ReSharper disable once AccessToDisposedClosure
                    var res = await s.ReadChunks(sc => sc.Chunks.Where(c => c.Id == c1.Id).Select(c => (c, (Stream)ms)), CancellationToken.None);
                    Assert.Equal(data, ms.ToArray());
                    Assert.Equal(ms, res.First().Data);
                }
            }
        }
    }
}