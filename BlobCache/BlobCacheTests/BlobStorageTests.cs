namespace BlobCacheTests
{
    using System.IO;
    using System.Linq;
    using BlobCache;
    using Xunit;

    public class BlobStorageTests
    {
        [Fact]
        public async void AddChunk()
        {
            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize());

                var data = Enumerable.Range(0, 256).Select(r => (byte) 1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data);
                Assert.Equal(1, c1.Id);
                Assert.Equal(11, c1.UserData);
                Assert.Equal((uint) data.Length, c1.Size);

                var res = await s.ReadChunk(1);
                Assert.Equal(data, res);
            }
        }

        [Fact]
        public async void GetChunks()
        {
            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize());

                Assert.Empty(await s.GetChunks());

                var data = Enumerable.Range(0, 256).Select(r => (byte) 1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data);

                Assert.Equal(c1, (await s.GetChunks()).Single());

                await s.RemoveChunk(c1);

                Assert.Equal(c1, (await s.GetChunks()).Single());

                Assert.Equal(ChunkTypes.Free, (await s.GetChunks()).Single().Type);
            }
        }

        [Fact]
        public async void Initialization()
        {
            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize());
                Assert.True(await s.Initialize());
            }

            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize());

                using (var s1 = new BlobStorage("test.blob"))
                {
                    Assert.True(await s1.Initialize());
                }
            }

            File.Delete("test2.blob");
            File.WriteAllText("test2.blob", "ab");
            using (var s = new BlobStorage("test2.blob"))
            {
                Assert.False(await s.Initialize());
            }

            File.Delete("test2.blob");
            File.WriteAllText("test2.blob", "ABCD234                                                                    ");
            using (var s = new BlobStorage("test2.blob"))
            {
                Assert.False(await s.Initialize());
            }

            File.Delete("test2.blob");
            File.WriteAllText("test2.blob", "BLOB234                                                                    ");
            using (var s = new BlobStorage("test2.blob"))
            {
                Assert.False(await s.Initialize());
            }
        }

        [Fact]
        public async void RemoveChunk()
        {
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize());

                var data = Enumerable.Range(0, 256).Select(r => (byte) 1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data);
                Assert.Equal(11, c1.UserData);
                Assert.Equal((uint) data.Length, c1.Size);

                var res = await s.ReadChunk(c1.Id);
                Assert.Equal(data, res);

                await s.RemoveChunk(c1);

                var chunks = await s.GetChunks();
                Assert.Equal(ChunkTypes.Free, chunks.Single(c => c.Id == c1.Id).Type);
            }
        }

        [Fact]
        public async void SpaceReuse()
        {
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize());

                var data = Enumerable.Range(0, 256).Select(r => (byte) 1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data);
                Assert.Equal(11, c1.UserData);
                Assert.Equal((uint) data.Length, c1.Size);

                var res = await s.ReadChunk(c1.Id);
                Assert.Equal(data, res);

                data = Enumerable.Range(0, 256).Select(r => (byte) 2).ToArray();
                var c2 = await s.AddChunk(ChunkTypes.Test, 12, data);
                Assert.Equal(12, c2.UserData);
                Assert.Equal((uint) data.Length, c2.Size);

                res = await s.ReadChunk(c2.Id);
                Assert.Equal(data, res);

                var size = s.Info.Length;

                await s.RemoveChunk(c1);

                data = Enumerable.Range(0, 128).Select(r => (byte) 3).ToArray();
                var c3 = await s.AddChunk(ChunkTypes.Test, 13, data);
                Assert.Equal(13, c3.UserData);
                Assert.Equal((uint) data.Length, c3.Size);

                res = await s.ReadChunk(c3.Id);
                Assert.Equal(data, res);

                Assert.Equal(size, s.Info.Length);
            }
        }

        [Fact]
        public async void FreeSpaceCombination()
        {
            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize());

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data);
                var c2 = await s.AddChunk(ChunkTypes.Test, 12, data);
                await s.AddChunk(ChunkTypes.Test, 13, data);

                Assert.Equal(3, (await s.GetChunks()).Count);

                await s.RemoveChunk(c2);

                var chunks = await s.GetChunks();
                Assert.Equal(3, chunks.Count);
                Assert.Equal(1, chunks.Count(c => c.Type == ChunkTypes.Free));

                await s.RemoveChunk(c1);

                chunks = await s.GetChunks();
                Assert.Equal(2, chunks.Count);
                Assert.Equal(1, chunks.Count(c => c.Type == ChunkTypes.Free));
            }

            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize());

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data);
                var c2 = await s.AddChunk(ChunkTypes.Test, 12, data);
                await s.AddChunk(ChunkTypes.Test, 13, data);

                Assert.Equal(3, (await s.GetChunks()).Count);

                await s.RemoveChunk(c1);

                var chunks = await s.GetChunks();
                Assert.Equal(3, chunks.Count);
                Assert.Equal(1, chunks.Count(c => c.Type == ChunkTypes.Free));

                await s.RemoveChunk(c2);

                chunks = await s.GetChunks();
                Assert.Equal(2, chunks.Count);
                Assert.Equal(1, chunks.Count(c => c.Type == ChunkTypes.Free));
            }
        }

        [Fact]
        public async void ChunkIdReuse()
        {
            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize());

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                var c1 = await s.AddChunk(ChunkTypes.Test, 11, data);
                var c2 = await s.AddChunk(ChunkTypes.Test, 12, data);
                await s.AddChunk(ChunkTypes.Test, 13, data);

                Assert.Equal(3, (await s.GetChunks()).Count);

                await s.RemoveChunk(c1);
                await s.RemoveChunk(c2);

                var c4 = await s.AddChunk(ChunkTypes.Test, 14, data);
                Assert.Equal(c1.Id, c4.Id);
            }
        }
    }
}