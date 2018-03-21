namespace BlobCache.ConcurrencyModes
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using JetBrains.Annotations;

    /// <summary>
    ///     Base class for handling concurrency in storages
    /// </summary>
    public abstract class ConcurrencyHandler : IDisposable
    {
        /// <summary>
        ///     Gets the storage Id
        /// </summary>
        [PublicAPI]
        public Guid Id { get; private set; }

        /// <summary>
        ///     Gets the preferred timeout used for locks
        /// </summary>
        public int Timeout { get; protected set; } = 30000;

        /// <summary>
        ///     Cleans up resources used
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Try to enter the lock
        /// </summary>
        /// <returns>True if locked, otherwise false</returns>
        public abstract bool TryEnterLock();

        /// <summary>
        /// Releases entered lock
        /// </summary>
        public abstract void ReleaseLock();

        /// <summary>
        ///     Reads the current storage info data
        /// </summary>
        /// <remarks>Call only when lock is held</remarks>
        /// <returns>Storage info</returns>
        public abstract StorageInfo ReadInfo();

        /// <summary>
        ///     Sets the id belonging to the storage
        /// </summary>
        /// <param name="id">Storage id</param>
        public virtual void SetId(Guid id)
        {
            Id = id;
        }

        // Should set manual signal
        /// <summary>
        ///     Signals a chunk read is finished
        /// </summary>
        public abstract void SignalReadFinish();

        // Should reset manual signal
        /// <summary>
        ///     Signals wait is needed for a chunk to became available
        /// </summary>
        public abstract void SignalWaitRequired();

        // Should wait for manual signal
        /// <summary>
        ///     Waits for a chunk read to finish
        /// </summary>
        /// <param name="token">Cancellation token</param>
        public abstract void WaitForReadFinish(CancellationToken token);

        /// <summary>
        ///     Write the current storage info data
        /// </summary>
        /// <param name="info">Storage info to write</param>
        /// <param name="stableChunkChanged">Indicating whether stable chunks list should be refreshed</param>
        /// <remarks>Call only when lock is held</remarks>
        public abstract void WriteInfo(StorageInfo info, bool stableChunkChanged);

        /// <summary>
        ///     Cleans up used resources
        /// </summary>
        /// <param name="disposing">Value indicating whether it is called from Dispose or from Destructor</param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
            }
        }
    }
}