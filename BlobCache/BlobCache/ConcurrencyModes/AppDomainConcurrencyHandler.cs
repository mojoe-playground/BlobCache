namespace BlobCache.ConcurrencyModes
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    public class AppDomainConcurrencyHandler : ConcurrencyHandler
    {
        public override IDisposable ReadLock(int timeout)
        {
            var l = LocalSyncData.ReadWriteLock(Id);

            if (timeout < 0)
                l.EnterReadLock();
            else if (!l.TryEnterReadLock(TimeSpan.FromMilliseconds(timeout)))
                throw new TimeoutException();

            return new LockRelease(l, true);
        }

        public override IDisposable WriteLock(int timeout)
        {
            var l = LocalSyncData.ReadWriteLock(Id);

            if (timeout < 0)
                l.EnterWriteLock();
            else if (!l.TryEnterWriteLock(TimeSpan.FromMilliseconds(timeout)))
                throw new TimeoutException();

            return new LockRelease(l, false);
        }

        public override StorageInfo ReadInfo()
        {
            return LocalSyncData.ReadInfo(Id);
        }

        public override void WriteInfo(StorageInfo info)
        {
            LocalSyncData.WriteInfo(Id, info);
        }

        protected override void Dispose(bool disposing)
        {
            LocalSyncData.ReleaseData(Id);
        }

        private class LockRelease : IDisposable
        {
            private readonly ReaderWriterLockSlim _lock;
            private readonly bool _read;

            public LockRelease(ReaderWriterLockSlim locker, bool read)
            {
                _lock = locker;
                _read = read;
            }

            public void Dispose()
            {
                if (_read)
                    _lock.ExitReadLock();
                else
                    _lock.ExitWriteLock();
            }
        }

        private static class LocalSyncData
        {
            private static readonly
                Dictionary<Guid, (int UsedLockCount, StorageInfo Info, ReaderWriterLockSlim ReadWriteLock)> Data =
                    new Dictionary<Guid, (int, StorageInfo, ReaderWriterLockSlim)>();

            public static void ReleaseData(Guid id)
            {
                lock (Data)
                {
                    Data.Remove(id);
                }
            }

            public static StorageInfo ReadInfo(Guid id)
            {
                lock (Data)
                {
                    if (Data.ContainsKey(id))
                        return Data[id].Info;
                }
                return default(StorageInfo);
            }

            public static void WriteInfo(Guid id, StorageInfo info)
            {
                lock (Data)
                {
                    if (!Data.ContainsKey(id))
                    {
                        Data[id] = (0, info, new ReaderWriterLockSlim());
                    }
                    else
                    {
                        var r = Data[id];
                        Data[id] = (r.UsedLockCount, info, r.ReadWriteLock);
                    }
                }
            }

            public static ReaderWriterLockSlim ReadWriteLock(Guid id)
            {
                ReaderWriterLockSlim locker;
                lock (Data)
                {
                    if (!Data.ContainsKey(id))
                        Data[id] = (0, default(StorageInfo), new ReaderWriterLockSlim());

                    locker = Data[id].ReadWriteLock;
                }

                return locker;
            }
        }
    }
}