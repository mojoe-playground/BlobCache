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
            using (var c = new Cache(new BlobStorage("cache.blob", new AppDomainConcurrencyHandler())))
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
            using (var c = new Cache(new BlobStorage("cache.blob", new AppDomainConcurrencyHandler())))
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
        public async void Get()
        {
            File.Delete("cache.blob");
            using (var c = new Cache(new BlobStorage("cache.blob", new AppDomainConcurrencyHandler())))
            {
                Assert.True(await c.Initialize());

                Assert.Null(await c.Get("xunit.core.xml"));
                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"));
                Assert.Equal(File.ReadAllBytes("xunit.core.xml"), await c.Get("xunit.core.xml"));
                Assert.True(await c.Remove("xunit.core.xml"));
                Assert.Null(await c.Get("xunit.core.xml"));
                await c.Add("xunit.core.xml", DateTime.MinValue, File.ReadAllBytes("xunit.core.xml"));
                Assert.Null(await c.Get("xunit.core.xml"));
            }
        }

        [Fact]
        public async void Overwrite()
        {
            File.Delete("cache.blob");
            using (var c = new Cache(new BlobStorage("cache.blob", new AppDomainConcurrencyHandler())))
            {
                Assert.True(await c.Initialize());

                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"));
                await c.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.assert.xml"));
                Assert.Equal(File.ReadAllBytes("xunit.assert.xml"), await c.Get("xunit.core.xml"));
                await c.Add("xunit.execution.desktop.xml", DateTime.MaxValue,
                    File.ReadAllBytes("xunit.execution.desktop.xml"));
            }

            Assert.True(new FileInfo("cache.blob").Length < new FileInfo("xunit.core.xml").Length +
                        new FileInfo("xunit.assert.xml").Length + new FileInfo("xunit.execution.desktop.xml").Length);
        }

        [Fact]
        public async void Remove()
        {
            File.Delete("cache.blob");
            using (var c = new Cache(new BlobStorage("cache.blob", new AppDomainConcurrencyHandler())))
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

            Assert.True(new FileInfo("cache.blob").Length < new FileInfo("xunit.core.xml").Length +
                        new FileInfo("xunit.assert.xml").Length + new FileInfo("xunit.execution.desktop.xml").Length);
        }

        [Fact]
        public async void Multi()
        {
            File.Delete("cache.blob");
            using (var c1 = new Cache(new BlobStorage("cache.blob", new AppDomainConcurrencyHandler())))
            using (var c2 = new Cache(new BlobStorage("cache.blob", new AppDomainConcurrencyHandler())))
            {
                Assert.True(await c1.Initialize());
                Assert.True(await c2.Initialize());

                await c1.Add("xunit.core.xml", DateTime.MaxValue, File.ReadAllBytes("xunit.core.xml"));
                Assert.True(await c2.Exists("xunit.core.xml"));
                Assert.True(await c2.Remove("xunit.core.xml"));
                Assert.Null(await c1.Get("xunit.core.xml"));
            }
        }
    }
}