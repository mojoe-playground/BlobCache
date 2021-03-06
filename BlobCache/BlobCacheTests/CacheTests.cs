﻿namespace BlobCacheTests
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using BlobCache;
    using BlobCache.ConcurrencyModes;
    using Xunit;

    public class CacheTests
    {
        [Fact]
        public async void KeyComparers()
        {
            Assert.Throws<ArgumentNullException>(() => new Cache("aa", null));

            File.Delete("cache.blob");
            using (var c = new Cache("cache.blob"))
            {
                Assert.True(await c.Initialize(CancellationToken.None));

                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                Assert.Equal(File.ReadAllBytes("xunit.core.xml"), await c.Get("xunit.core.xml", CancellationToken.None));
                Assert.Null(await c.Get("XUNIT.CORE.xml", CancellationToken.None));
            }

            File.Delete("cache.blob");
            using (var c = new Cache("cache.blob", new CaseInsensitiveKeyComparer()))
            {
                Assert.True(await c.Initialize(CancellationToken.None));

                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                Assert.Equal(File.ReadAllBytes("xunit.core.xml"), await c.Get("xunit.core.xml", CancellationToken.None));
                Assert.Equal(File.ReadAllBytes("xunit.core.xml"), await c.Get("XUNIT.CORE.xml", CancellationToken.None));
            }
        }

        [Fact]
        public async void Add()
        {
            File.Delete("cache.blob");
            using (var c = new Cache("cache.blob"))
            {
                Assert.True(await c.Initialize(CancellationToken.None));

                await Assert.ThrowsAsync<ArgumentNullException>(() => c.Add("xunit.core.xml", DateTime.MaxValue, (byte[])null, CancellationToken.None));

                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                Assert.Equal(File.ReadAllBytes("xunit.core.xml"), await c.Get("xunit.core.xml", CancellationToken.None));
            }
        }

        [Fact]
        public async void AddTtl()
        {
            File.Delete("cache.blob");
            using (var c = new Cache("cache.blob"))
            {
                Assert.True(await c.Initialize(CancellationToken.None));

                await c.Add("xunit.core.xml", DateTime.UtcNow.Add(TimeSpan.FromSeconds(3)), File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                Assert.Equal(File.ReadAllBytes("xunit.core.xml"), await c.Get("xunit.core.xml", CancellationToken.None));
                Thread.Sleep(5000);
                Assert.Null(await c.Get("xunit.core.xml", CancellationToken.None));
            }
        }

        [Fact]
        public async void Compress()
        {
            File.Delete("cache.blob");
            var s = new BlobStorage("cache.blob");
            using (var c = new Cache(s))
            {
                Assert.True(await c.Initialize(CancellationToken.None));

                var data = File.ReadAllBytes("xunit.core.xml");
                await c.Add("xunit.core.xml", DateTime.MaxValue, data, CancellationToken.None);
                Assert.Equal(data, await c.Get("xunit.core.xml", CancellationToken.None));

                s.Info.Refresh();
                Assert.False(data.Length > s.Info.Length);
            }

            File.Delete("cache.blob");
            s = new BlobStorage("cache.blob");
            using (var c = new Cache(s))
            {
                c.CanCompress = true;
                Assert.True(await c.Initialize(CancellationToken.None));

                var data = File.ReadAllBytes("xunit.core.xml");
                await c.Add("xunit.core.xml", DateTime.MaxValue, data, CancellationToken.None);
                Assert.Equal(data, await c.Get("xunit.core.xml", CancellationToken.None));

                s.Info.Refresh();
                Assert.True(data.Length > s.Info.Length);
            }
        }

        [Fact]
        public async void GetWithInfo()
        {
            File.Delete("cache.blob");
            using (var c = new Cache("cache.blob"))
            {
                Assert.True(await c.Initialize(CancellationToken.None));
                var r = await c.GetWithInfo("xunit.core.xml", CancellationToken.None);
                Assert.Null(r.data);
                Assert.Equal(default(CacheEntryInfo), r.info);

                var now = DateTime.UtcNow;
                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                r = await c.GetWithInfo("xunit.core.xml", CancellationToken.None);
                Assert.Equal(File.ReadAllBytes("xunit.core.xml"), r.data);
                Assert.True(now <= r.info.Added);
                Assert.True(r.info.Added <= DateTime.UtcNow);

                now = DateTime.UtcNow;
                var data = new byte[0];
                await c.Add("null.xml", DateTime.MaxValue, data, CancellationToken.None);
                r = await c.GetWithInfo("null.xml", CancellationToken.None);
                Assert.Equal(data, r.data);
                Assert.True(now <= r.info.Added);
                Assert.True(r.info.Added <= DateTime.UtcNow);
            }
        }

        [Fact]
        public async void GetWithInfoStream()
        {
            File.Delete("cache.blob");
            using (var c = new Cache("cache.blob"))
            {
                Assert.True(await c.Initialize(CancellationToken.None));
                using (var ms = new MemoryStream())
                {
                    var r = await c.GetWithInfo("xunit.core.xml", ms, CancellationToken.None);
                    Assert.False(r.success);
                    Assert.Equal(default(CacheEntryInfo), r.info);
                }

                using (var ms = new MemoryStream())
                {
                    var now = DateTime.UtcNow;
                    await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                    var r = await c.GetWithInfo("xunit.core.xml", ms, CancellationToken.None);
                    Assert.True(r.success);
                    Assert.Equal(File.ReadAllBytes("xunit.core.xml"), ms.ToArray());
                    Assert.True(now <= r.info.Added);
                    Assert.True(r.info.Added <= DateTime.UtcNow);
                }

                using (var ms = new MemoryStream())
                {
                    var now = DateTime.UtcNow;
                    var data = new byte[0];
                    await c.Add("null.xml", DateTime.MaxValue, data, CancellationToken.None);
                    var r = await c.GetWithInfo("null.xml", ms, CancellationToken.None);
                    Assert.True(r.success);
                    Assert.Equal(data, ms.ToArray());
                    Assert.True(now <= r.info.Added);
                    Assert.True(r.info.Added <= DateTime.UtcNow);
                }
            }
        }

        [Fact]
        public async void AddStream()
        {
            File.Delete("cache.blob");
            using (var c = new Cache("cache.blob"))
            {
                Assert.True(await c.Initialize(CancellationToken.None));

                await Assert.ThrowsAsync<ArgumentNullException>(() => c.Add("xunit.core.xml", DateTime.MaxValue, (Stream)null, CancellationToken.None));

                using (var ms = new MemoryStream(File.ReadAllBytes("xunit.core.xml")))
                {
                    ms.Position = 4;
                    await c.Add("xunit.core.xml", DateTime.MaxValue, ms, CancellationToken.None);
                }
                Assert.Equal(File.ReadAllBytes("xunit.core.xml").Skip(4), await c.Get("xunit.core.xml", CancellationToken.None));
            }
        }

        [Fact]
        public async void Cleanup()
        {
            File.Delete("cache.blob");
            var storage = new BlobStorage("cache.blob");
            await storage.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None);
            using (var c = new Cache(storage))
            {
                c.CleanupTime = () => DateTime.UtcNow.AddDays(2);
                Assert.True(await c.Initialize(CancellationToken.None));

                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                await c.Add("xunit.assert.xml", DateTime.UtcNow.AddMinutes(2), File.ReadAllBytes("xunit.assert.xml"), CancellationToken.None);
                await storage.AddChunk(ChunkTypes.Data, 8, new byte[] { 1, 2, 3 }, CancellationToken.None);

                storage.Info.Refresh();
                var length = storage.Info.Length;
                await c.Cleanup(CancellationToken.None);

                var chunks = await storage.GetChunks(CancellationToken.None);
                Assert.Equal(1, chunks.Count(ch => ch.Type == ChunkTypes.Head));
                Assert.Equal(1, chunks.Count(ch => ch.Type == ChunkTypes.Data));
                storage.Info.Refresh();
                Assert.True(length > storage.Info.Length);
            }
        }

        [Fact]
        public async void Exists()
        {
            File.Delete("cache.blob");
            var storage = new BlobStorage("cache.blob");
            await storage.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None);
            using (var c = new Cache(storage))
            {
                Assert.True(await c.Initialize(CancellationToken.None));

                Assert.False(await c.Exists("xunit.core.xml", CancellationToken.None));
                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                Assert.True(await c.Exists("xunit.core.xml", CancellationToken.None));
                Assert.True(await c.Remove("xunit.core.xml", CancellationToken.None));
                Assert.False(await c.Exists("xunit.core.xml", CancellationToken.None));
                await c.Add("xunit.core.xml", DateTime.MinValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                Assert.False(await c.Exists("xunit.core.xml", CancellationToken.None));
            }
        }

        [Fact]
        public async void GetFile()
        {
            File.Delete("cache.blob");
            var storage = new BlobStorage("cache.blob");
            await storage.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None);
            using (var c = new Cache(storage))
            {
                Assert.True(await c.Initialize(CancellationToken.None));

                using (var ms = new MemoryStream())
                {
                    Assert.Null(await c.Get("xunit.core.xml", CancellationToken.None));
                    Assert.False(await c.Get("xunit.core.xml", ms, CancellationToken.None));

                    await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                    Assert.Equal(File.ReadAllBytes("xunit.core.xml"), await c.Get("xunit.core.xml", CancellationToken.None));
                    Assert.True(await c.Get("xunit.core.xml", ms, CancellationToken.None));
                    Assert.Equal(File.ReadAllBytes("xunit.core.xml"), ms.ToArray());

                    Assert.True(await c.Remove("xunit.core.xml", CancellationToken.None));

                    Assert.Null(await c.Get("xunit.core.xml", CancellationToken.None));
                    Assert.False(await c.Get("xunit.core.xml", ms, CancellationToken.None));
                    await c.Add("xunit.core.xml", DateTime.MinValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                    Assert.Null(await c.Get("xunit.core.xml", CancellationToken.None));
                    Assert.False(await c.Get("xunit.core.xml", ms, CancellationToken.None));
                }
            }
        }

        [Fact]
        public async void GetMultiPartFile()
        {
            File.Delete("cache.blob");
            var storage = new BlobStorage("cache.blob");
            await storage.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None);
            using (var c = new Cache(storage))
            {
                Assert.True(await c.Initialize(CancellationToken.None));

                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                await c.Add("xunit.assert.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.assert.xml"), CancellationToken.None);
                await c.Remove("xunit.core.xml", CancellationToken.None);
                await c.Add("xunit.execution.desktop.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.execution.desktop.xml"), CancellationToken.None);

                Assert.Equal(File.ReadAllBytes("xunit.execution.desktop.xml"), await c.Get("xunit.execution.desktop.xml", CancellationToken.None));
                using (var ms = new MemoryStream())
                {
                    Assert.True(await c.Get("xunit.execution.desktop.xml", ms, CancellationToken.None));
                    Assert.Equal(File.ReadAllBytes("xunit.execution.desktop.xml"), ms.ToArray());
                }
            }

            Assert.True(new FileInfo("cache.blob").Length < new FileInfo("xunit.core.xml").Length + new FileInfo("xunit.assert.xml").Length + new FileInfo("xunit.execution.desktop.xml").Length);
        }

        [Fact]
        public async void MaximumSizeCleanup()
        {
            File.Delete("cache.blob");
            var storage = new BlobStorage("cache.blob");
            await storage.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None);
            using (var c = new Cache(storage))
            {
                c.CleanupTime = () => DateTime.UtcNow.AddDays(2);
                Assert.True(await c.Initialize(CancellationToken.None));

                var data = File.ReadAllBytes("xunit.core.xml");
                await c.Add("xunit.core.xml", DateTime.MaxValue, data, CancellationToken.None);
                c.MaximumSize = (int)(data.Length * 2.2);
                c.CutBackRatio = 0.75;

                await c.Cleanup(CancellationToken.None);
                Assert.True(await c.Exists("xunit.core.xml", CancellationToken.None));

                await c.Add("xunit.core2.xml", DateTime.MaxValue.AddDays(-1), data, CancellationToken.None);
                await c.Add("xunit.assert.xml", DateTime.MaxValue.AddDays(-5), File.ReadAllBytes("xunit.assert.xml"), CancellationToken.None);

                await c.Cleanup(CancellationToken.None);

                var chunks = await storage.GetChunks(CancellationToken.None);
                Assert.Equal(1, chunks.Count(ch => ch.Type == ChunkTypes.Head));
                Assert.Equal(1, chunks.Count(ch => ch.Type == ChunkTypes.Data));
                Assert.True(await c.Exists("xunit.core.xml", CancellationToken.None));
                storage.Info.Refresh();
                Assert.True(storage.Info.Length < c.MaximumSize);
            }
        }

        [Fact]
        public async void MultiCacheUse()
        {
            File.Delete("cache.blob");
            using (var c1 = new Cache(new BlobStorage("cache.blob")))
            using (var c2 = new Cache(new BlobStorage("cache.blob")))
            {
                Assert.True(await c1.Initialize(CancellationToken.None));
                Assert.True(await c2.Initialize(CancellationToken.None));

                await c1.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                Assert.True(await c2.Exists("xunit.core.xml", CancellationToken.None));
                Assert.True(await c2.Remove("xunit.core.xml", CancellationToken.None));
                Assert.Null(await c1.Get("xunit.core.xml", CancellationToken.None));
            }
        }

        [Fact]
        public async void OverwriteFile()
        {
            File.Delete("cache.blob");
            var storage = new BlobStorage("cache.blob");
            await storage.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None);
            using (var c = new Cache(storage))
            {
                Assert.True(await c.Initialize(CancellationToken.None));

                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.assert.xml"), CancellationToken.None);
                Assert.Equal(File.ReadAllBytes("xunit.assert.xml"), await c.Get("xunit.core.xml", CancellationToken.None));
                await c.Add("xunit.execution.desktop.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.execution.desktop.xml"), CancellationToken.None);
            }

            Assert.True(new FileInfo("cache.blob").Length < new FileInfo("xunit.core.xml").Length + new FileInfo("xunit.assert.xml").Length + new FileInfo("xunit.execution.desktop.xml").Length);
        }

        [Fact]
        public async void RemoveFile()
        {
            File.Delete("cache.blob");
            var storage = new BlobStorage("cache.blob");
            await storage.Initialize<AppDomainConcurrencyHandler>(CancellationToken.None);
            using (var c = new Cache(storage))
            {
                Assert.True(await c.Initialize(CancellationToken.None));

                Assert.False(await c.Remove("xunit.core.xml", CancellationToken.None));
                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"), CancellationToken.None);
                await c.Add("xunit.assert.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.assert.xml"), CancellationToken.None);
                Assert.True(await c.Remove("xunit.core.xml", CancellationToken.None));
                Assert.False(await c.Remove("xunit.core.xml", CancellationToken.None));
                await c.Add("xunit.execution.desktop.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.execution.desktop.xml"), CancellationToken.None);
            }

            Assert.True(new FileInfo("cache.blob").Length < new FileInfo("xunit.core.xml").Length + new FileInfo("xunit.assert.xml").Length + new FileInfo("xunit.execution.desktop.xml").Length);
        }
    }
}