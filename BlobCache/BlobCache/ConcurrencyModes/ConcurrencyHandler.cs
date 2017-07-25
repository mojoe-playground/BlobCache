namespace BlobCache.ConcurrencyModes
{
    using System;
    using System.Threading;
    using JetBrains.Annotations;

    public abstract class ConcurrencyHandler : IDisposable
    {
        [PublicAPI]
        public Guid Id { get; private set; }

        public int Timeout { get; protected set; } = 30000;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public abstract IDisposable Lock(int timeout, CancellationToken token);

        public abstract StorageInfo ReadInfo();

        public virtual void SetId(Guid id)
        {
            Id = id;
        }

        // Should set manual signal
        public abstract void SignalReadFinish();

        // Should reset manual signal
        public abstract void SignalWaitRequired();

        // Should wait for manual signal
        public abstract void WaitForReadFinish(CancellationToken token);

        public abstract void WriteInfo(StorageInfo info);

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
            }
        }
    }
}