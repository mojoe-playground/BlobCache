namespace BlobCacheTests
{
    using System;
    using System.IO;
    using BlobCache;
    using BlobCache.ConcurrencyModes;
    using Xunit;

    public class CacheTests
    {
        [Fact]
        public async void Add()
        {
            File.Delete("cache.blob");
            using (var c = new Cache("cache.blob"))
            {
                Assert.True(await c.Initialize());

                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"));
                Assert.Equal(File.ReadAllBytes("xunit.core.xml"), await c.Get("xunit.core.xml"));
            }
        }

        [Fact]
        public async void Exists()
        {
            File.Delete("cache.blob");
            var storage = new BlobStorage("cache.blob");
            await storage.Initialize<AppDomainConcurrencyHandler>();
            using (var c = new Cache(storage))
            {
                Assert.True(await c.Initialize());

                Assert.False(await c.Exists("xunit.core.xml"));
                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"));
                Assert.True(await c.Exists("xunit.core.xml"));
                Assert.True(await c.Remove("xunit.core.xml"));
                Assert.False(await c.Exists("xunit.core.xml"));
                await c.Add("xunit.core.xml", DateTime.MinValue, File.ReadAllBytes("xunit.core.xml"));
                Assert.False(await c.Exists("xunit.core.xml"));
            }
        }

        [Fact]
        public async void GetFile()
        {
            File.Delete("cache.blob");
            var storage = new BlobStorage("cache.blob");
            await storage.Initialize<AppDomainConcurrencyHandler>();
            using (var c = new Cache(storage))
            {
                Assert.True(await c.Initialize());

                using (var ms = new MemoryStream())
                {
                    Assert.Null(await c.Get("xunit.core.xml"));
                    Assert.False(await c.Get("xunit.core.xml", ms));

                    await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"));
                    Assert.Equal(File.ReadAllBytes("xunit.core.xml"), await c.Get("xunit.core.xml"));
                    Assert.True(await c.Get("xunit.core.xml", ms));
                    Assert.Equal(File.ReadAllBytes("xunit.core.xml"), ms.ToArray());

                    Assert.True(await c.Remove("xunit.core.xml"));

                    Assert.Null(await c.Get("xunit.core.xml"));
                    Assert.False(await c.Get("xunit.core.xml", ms));
                    await c.Add("xunit.core.xml", DateTime.MinValue, File.ReadAllBytes("xunit.core.xml"));
                    Assert.Null(await c.Get("xunit.core.xml"));
                    Assert.False(await c.Get("xunit.core.xml", ms));
                }
            }
        }

        [Fact]
        public async void GetMultiPartFile()
        {
            File.Delete("cache.blob");
            var storage = new BlobStorage("cache.blob");
            await storage.Initialize<AppDomainConcurrencyHandler>();
            using (var c = new Cache(storage))
            {
                Assert.True(await c.Initialize());

                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"));
                await c.Add("xunit.assert.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.assert.xml"));
                await c.Remove("xunit.core.xml");
                await c.Add("xunit.execution.desktop.xml", DateTime.MaxValue,
                    File.ReadAllBytes("xunit.execution.desktop.xml"));

                Assert.Equal(File.ReadAllBytes("xunit.execution.desktop.xml"), await c.Get("xunit.execution.desktop.xml"));
                using (var ms = new MemoryStream())
                {
                    Assert.True(await c.Get("xunit.execution.desktop.xml", ms));
                    Assert.Equal(File.ReadAllBytes("xunit.execution.desktop.xml"), ms.ToArray());
                }
            }

            Assert.True(new FileInfo("cache.blob").Length < new FileInfo("xunit.core.xml").Length + new FileInfo("xunit.assert.xml").Length + new FileInfo("xunit.execution.desktop.xml").Length);
        }

        [Fact]
        public async void MultiCacheUse()
        {
            File.Delete("cache.blob");
            using (var c1 = new Cache(new BlobStorage("cache.blob")))
            using (var c2 = new Cache(new BlobStorage("cache.blob")))
            {
                Assert.True(await c1.Initialize());
                Assert.True(await c2.Initialize());

                await c1.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"));
                Assert.True(await c2.Exists("xunit.core.xml"));
                Assert.True(await c2.Remove("xunit.core.xml"));
                Assert.Null(await c1.Get("xunit.core.xml"));
            }
        }

        [Fact]
        public async void OverwriteFile()
        {
            File.Delete("cache.blob");
            var storage = new BlobStorage("cache.blob");
            await storage.Initialize<AppDomainConcurrencyHandler>();
            using (var c = new Cache(storage))
            {
                Assert.True(await c.Initialize());

                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"));
                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.assert.xml"));
                Assert.Equal(File.ReadAllBytes("xunit.assert.xml"), await c.Get("xunit.core.xml"));
                await c.Add("xunit.execution.desktop.xml", DateTime.MaxValue,
                    File.ReadAllBytes("xunit.execution.desktop.xml"));
            }

            Assert.True(new FileInfo("cache.blob").Length < new FileInfo("xunit.core.xml").Length + new FileInfo("xunit.assert.xml").Length + new FileInfo("xunit.execution.desktop.xml").Length);
        }

        [Fact]
        public async void RemoveFile()
        {
            File.Delete("cache.blob");
            var storage = new BlobStorage("cache.blob");
            await storage.Initialize<AppDomainConcurrencyHandler>();
            using (var c = new Cache(storage))
            {
                Assert.True(await c.Initialize());

                Assert.False(await c.Remove("xunit.core.xml"));
                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"));
                await c.Add("xunit.assert.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.assert.xml"));
                Assert.True(await c.Remove("xunit.core.xml"));
                Assert.False(await c.Remove("xunit.core.xml"));
                await c.Add("xunit.execution.desktop.xml", DateTime.MaxValue,
                    File.ReadAllBytes("xunit.execution.desktop.xml"));
            }

            Assert.True(new FileInfo("cache.blob").Length < new FileInfo("xunit.core.xml").Length + new FileInfo("xunit.assert.xml").Length + new FileInfo("xunit.execution.desktop.xml").Length);
        }
    }
}