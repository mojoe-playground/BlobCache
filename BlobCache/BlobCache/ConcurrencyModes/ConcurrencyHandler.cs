namespace BlobCache.ConcurrencyModes
{
    using System;
    using System.Threading;
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

        public abstract StorageInfo ReadInfo();

        public abstract IDisposable ReadLock(int timeout, CancellationToken token);

        // Should set manual signal
        public abstract void SignalReadFinish();

        // Should reset manual signal
        public abstract void SignalWaitRequired();

        // Should wait for manual signal
        public abstract void WaitForReadFinish(CancellationToken token);

        public abstract void WriteInfo(StorageInfo info);

        public abstract IDisposable WriteLock(int timeout, CancellationToken token);

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
            }
        }
    }
}