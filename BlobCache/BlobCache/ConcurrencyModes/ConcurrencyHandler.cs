namespace BlobCache.ConcurrencyModes
{
    using System;
    using JetBrains.Annotations;

    public abstract class ConcurrencyHandler : IDisposable
    {
        [PublicAPI]
        public Guid Id { get; set; }

        public int Timeout { get; protected set; } = 1000;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public abstract IDisposable ReadLock(int timeout);

        public abstract IDisposable WriteLock(int timeout);

        public abstract StorageInfo ReadInfo();

        public abstract void WriteInfo(StorageInfo info);

        // Should wait for manual signal
        public abstract void WaitForReadFinish();

        // Should set manual signal
        public abstract void SignalReadFinish();

        // Should reset manual signal
        public abstract void SignalWaitRequired();

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
            }
        }
    }
}