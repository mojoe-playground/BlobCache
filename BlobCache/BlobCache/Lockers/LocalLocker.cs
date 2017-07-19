namespace BlobCache.Lockers
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    internal class LocalLocker : IDisposable
    {
        private readonly bool _hasHandle;
        private readonly Guid _id;
        private object _locker;

        public LocalLocker(Guid id, int timeOut)
        {
            _id = id;
            _locker = LocalSyncData.AcquireLock(_id);

            if (timeOut < 0)
                Monitor.TryEnter(_locker, Timeout.Infinite, ref _hasHandle);
            else
                Monitor.TryEnter(_locker, timeOut, ref _hasHandle);

            if (_hasHandle == false)
                throw new TimeoutException("Timeout waiting for exclusive access on StorageLocker");
        }

        public void Dispose()
        {
            if (_locker != null)
            {
                LocalSyncData.ReleaseLock(_id);

                if (_hasHandle)
                    Monitor.Exit(_locker);
                _locker = null;
            }
        }
    }
}