namespace BlobCache.ConcurrencyModes
{
    using System;
    using JetBrains.Annotations;

    public abstract class ConcurrencyHandler : IDisposable
    {
        [PublicAPI]
        public Guid Id { get; set; }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public abstract IDisposable ReadLock(int timeout);

        public abstract IDisposable WriteLock(int timeout);

        public abstract StorageInfo ReadInfo();

        public abstract void WriteInfo(StorageInfo info);

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
            }
        }
    }
}