namespace BlobCacheTests
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
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
        public async void AddChunkStream()
        {
            File.Delete("test.blob");
            using (var s = new BlobStorage("test.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                using (var ms = new MemoryStream(data))
                {
                    ms.Position = 4;
                    var c1 = await s.AddChunk(ChunkTypes.Test, 11, ms, CancellationToken.None);

                    Assert.Equal(1u, c1.Id);
                    Assert.Equal(11u, c1.UserData);
                    Assert.Equal((uint)data.Length - 4, c1.Size);
                }

                var res = await s.ReadChunks(c => c.Id == 1, CancellationToken.None);
                Assert.Equal(data.Skip(4), res.First().Data);
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
        public async void ConcurrencyAddAppDomain()
        {
            await ConcurrencyAdd<AppDomainConcurrencyHandler>();
        }

        [Fact]
        public async void ConcurrencyAddSession()
        {
            await ConcurrencyAdd<SessionConcurrencyHandler>();
        }

        [Fact]
        public async void ConcurrencyAppDomain()
        {
            await Concurrency<AppDomainConcurrencyHandler>();
        }

        [Fact]
        public async void ConcurrencyRemoveAppDomain()
        {
            await ConcurrencyRemove<AppDomainConcurrencyHandler>();
        }

        [Fact]
        public async void ConcurrencyRemoveSession()
        {
            await ConcurrencyRemove<SessionConcurrencyHandler>();
        }

        [Fact]
        public async void ConcurrencySession()
        {
            await Concurrency<SessionConcurrencyHandler>();
        }

        [Fact]
        public async void CrcData()
        {
            File.Delete("testcrc.blob");
            using (var s = new BlobStorage("testcrc.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                await s.AddChunk(ChunkTypes.Test, 11, data, CancellationToken.None);
            }

            using (var f = File.OpenWrite("testcrc.blob"))
            {
                f.Position = 150;
                f.WriteByte(5);
            }

            using (var s = new BlobStorage("testcrc.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));
                await Assert.ThrowsAsync<InvalidDataException>(() => s.ReadChunks(sc => true, CancellationToken.None));
            }
        }

        [Fact]
        public async void CrcHead()
        {
            File.Delete("testcrc.blob");
            using (var s = new BlobStorage("testcrc.blob"))
            {
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));

                var data = Enumerable.Range(0, 256).Select(r => (byte)1).ToArray();
                await s.AddChunk(ChunkTypes.Test, 11, data, CancellationToken.None);
            }

            using (var f = File.OpenWrite("testcrc.blob"))
            {
                f.Position = 32;
                f.WriteByte(5);
            }

            using (var s = new BlobStorage("testcrc.blob"))
            {
                await Assert.ThrowsAsync<InvalidDataException>(() => s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));
                s.TruncateOnChunkInitializationError = true;
                Assert.True(await s.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None));
                s.Info.Refresh();
                Assert.Equal(BlobStorage.HeaderSize, s.Info.Length);
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

                Assert.Equal(c1.Id, (await s.GetChunks(CancellationToken.None)).Single().Id);

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
        public async void RepeatedAppDomain()
        {
            await Repeated<AppDomainConcurrencyHandler>();
        }

        [Fact]
        public async void RepeatedSession()
        {
            await Repeated<SessionConcurrencyHandler>();
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

                s.Info.Refresh();
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

        private static async Task Concurrency<T>()
            where T : ConcurrencyHandler, new()
        {
            File.Delete("concurrency.blob");

            var rn = new Random();
            using (var s = new BlobStorage("concurrency.blob"))
            {
                await s.Initialize<T>(CancellationToken.None);

                for (uint j = 0; j < 50; j++)
                    await s.AddChunk(ChunkTypes.Test, j, Enumerable.Range(0, rn.Next(500) + 500).Select(_ => (byte)rn.Next(256)).ToArray(), CancellationToken.None);

                for (uint j = 0; j < 25; j++)
                    await s.RemoveChunk(si => si.Chunks.Count == 0 ? (StorageChunk?)null : si.Chunks[rn.Next(si.Chunks.Count)], CancellationToken.None);
            }

            var counter = 0;
            const int max = 100;

            for (var i = 0; i < max; i++)
            {
                var t = new Thread(() =>
                {
                    try
                    {
                        var r = new Random();
                        using (var s = new BlobStorage("concurrency.blob"))
                        {
                            s.Initialize<T>(CancellationToken.None).Wait();

                            for (uint j = 0; j < 10; j++)
                            {
                                var op = r.Next(2);
                                if (op == 0)
                                    s.AddChunk(ChunkTypes.Test, j, Enumerable.Range(0, r.Next(500) + 500).Select(_ => (byte)r.Next(256)).ToArray(), CancellationToken.None).Wait();
                                if (op == 1)
                                    s.RemoveChunk(si => si.Chunks.Count == 0 ? (StorageChunk?)null : si.Chunks[r.Next(si.Chunks.Count)], CancellationToken.None).Wait();
                            }
                        }
                    }
                    finally
                    {
                        Interlocked.Increment(ref counter);
                    }
                });
                t.Start();
                Thread.Sleep(100);
            }

            // ReSharper disable once LoopVariableIsNeverChangedInsideLoop
            while (counter < max)
                Thread.Sleep(100);

            using (var s = new BlobStorage("concurrency.blob"))
            {
                Assert.True(await s.Initialize<T>(CancellationToken.None));
            }
        }


        private static async Task ConcurrencyAdd<T>()
            where T : ConcurrencyHandler, new()
        {
            File.Delete("concurrencyAdd.blob");

            var rn = new Random();
            using (var s = new BlobStorage("concurrencyAdd.blob"))
            {
                await s.Initialize<T>(CancellationToken.None);

                for (uint j = 0; j < 50; j++)
                    await s.AddChunk(ChunkTypes.Test, j, Enumerable.Range(0, rn.Next(500) + 500).Select(_ => (byte)rn.Next(256)).ToArray(), CancellationToken.None);

                for (uint j = 0; j < 25; j++)
                    await s.RemoveChunk(si => si.Chunks.Count == 0 ? (StorageChunk?)null : si.Chunks[rn.Next(si.Chunks.Count)], CancellationToken.None);
            }

            var counter = 0;
            const int max = 100;

            for (var i = 0; i < max; i++)
            {
                var t = new Thread(() =>
                {
                    try
                    {
                        var r = new Random();
                        using (var s = new BlobStorage("concurrencyAdd.blob"))
                        {
                            s.Initialize<T>(CancellationToken.None).Wait();

                            for (uint j = 0; j < 10; j++)
                                s.AddChunk(ChunkTypes.Test, j, Enumerable.Range(0, r.Next(500) + 500).Select(_ => (byte)r.Next(256)).ToArray(), CancellationToken.None).Wait();
                        }
                    }
                    finally
                    {
                        Interlocked.Increment(ref counter);
                    }
                });
                t.Start();
                Thread.Sleep(100);
            }

            // ReSharper disable once LoopVariableIsNeverChangedInsideLoop
            while (counter < max)
                Thread.Sleep(100);

            using (var s = new BlobStorage("concurrencyAdd.blob"))
            {
                Assert.True(await s.Initialize<T>(CancellationToken.None));
            }
        }


        private static async Task ConcurrencyRemove<T>()
            where T : ConcurrencyHandler, new()
        {
            File.Delete("concurrencyRemove.blob");

            var rn = new Random();
            using (var s = new BlobStorage("concurrencyRemove.blob"))
            {
                await s.Initialize<T>(CancellationToken.None);

                for (uint j = 0; j < 050; j++)
                    await s.AddChunk(ChunkTypes.Test, j, Enumerable.Range(0, rn.Next(500) + 500).Select(_ => (byte)rn.Next(256)).ToArray(), CancellationToken.None);
            }

            var counter = 0;
            const int max = 100;

            for (var i = 0; i < max; i++)
            {
                var t = new Thread(() =>
                {
                    try
                    {
                        var r = new Random();
                        using (var s = new BlobStorage("concurrencyRemove.blob"))
                        {
                            s.Initialize<T>(CancellationToken.None).Wait();

                            for (uint j = 0; j < 10; j++)
                                s.RemoveChunk(si => si.Chunks.Count == 0 ? (StorageChunk?)null : si.Chunks[r.Next(si.Chunks.Count)], CancellationToken.None).Wait();
                        }
                    }
                    finally
                    {
                        Interlocked.Increment(ref counter);
                    }
                });
                t.Start();
                Thread.Sleep(100);
            }

            // ReSharper disable once LoopVariableIsNeverChangedInsideLoop
            while (counter < max)
                Thread.Sleep(100);

            using (var s = new BlobStorage("concurrencyRemove.blob"))
            {
                Assert.True(await s.Initialize<T>(CancellationToken.None));
            }
        }

        private static async Task Repeated<T>()
            where T : ConcurrencyHandler, new()
        {
            File.Delete("repeated.blob");

            var rn = new Random();
            using (var s = new BlobStorage("repeated.blob"))
            {
                await s.Initialize<T>(CancellationToken.None);

                for (uint j = 0; j < 50; j++)
                    await s.AddChunk(ChunkTypes.Test, j, Enumerable.Range(0, rn.Next(500) + 500).Select(_ => (byte)rn.Next(256)).ToArray(), CancellationToken.None);

                for (uint j = 0; j < 25; j++)
                    await s.RemoveChunk(si => si.Chunks.Count == 0 ? (StorageChunk?)null : si.Chunks[rn.Next(si.Chunks.Count)], CancellationToken.None);
            }

            var counter = 0;
            const int max = 100;

            for (var i = 0; i < max; i++)
                try
                {
                    var r = new Random();
                    using (var s = new BlobStorage("repeated.blob"))
                    {
                        s.Initialize<T>(CancellationToken.None).Wait();

                        for (uint j = 0; j < 10; j++)
                        {
                            var op = r.Next(2);
                            if (op == 0)
                                await s.AddChunk(ChunkTypes.Test, j, Enumerable.Range(0, r.Next(500) + 500).Select(_ => (byte)r.Next(256)).ToArray(), CancellationToken.None);
                            if (op == 1)
                                await s.RemoveChunk(si => si.Chunks.Count == 0 ? (StorageChunk?)null : si.Chunks[r.Next(si.Chunks.Count)], CancellationToken.None);
                        }
                    }
                }
                finally
                {
                    Interlocked.Increment(ref counter);
                }

            // ReSharper disable once LoopVariableIsNeverChangedInsideLoop
            while (counter < max)
                Thread.Sleep(100);

            using (var s = new BlobStorage("repeated.blob"))
            {
                Assert.True(await s.Initialize<T>(CancellationToken.None));
            }
        }
    }
}