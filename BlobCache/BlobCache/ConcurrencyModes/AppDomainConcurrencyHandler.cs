namespace BlobCache.ConcurrencyModes
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    public class AppDomainConcurrencyHandler : ConcurrencyHandler
    {
        public override StorageInfo ReadInfo()
        {
            return LocalSyncData.ReadInfo(Id);
        }

        public override IDisposable ReadLock(int timeout, CancellationToken token)
        {
            var l = LocalSyncData.ReadWriteLock(Id);

            if (timeout < 0)
                l.EnterReadLock();
            else if (!l.TryEnterReadLock(TimeSpan.FromMilliseconds(timeout)))
                throw new TimeoutException();

            return new LockRelease(l, true);
        }

        public override void SignalReadFinish()
        {
            LocalSyncData.Signal(Id).Set();
        }

        public override void SignalWaitRequired()
        {
            LocalSyncData.Signal(Id).Reset();
        }

        public override void WaitForReadFinish(CancellationToken token)
        {
            LocalSyncData.Signal(Id).Wait(token);
        }

        public override void WriteInfo(StorageInfo info)
        {
            LocalSyncData.WriteInfo(Id, info);
        }

        public override IDisposable WriteLock(int timeout, CancellationToken token)
        {
            var l = LocalSyncData.ReadWriteLock(Id);

            if (timeout < 0)
                l.EnterWriteLock();
            else if (!l.TryEnterWriteLock(TimeSpan.FromMilliseconds(timeout)))
                throw new TimeoutException();

            return new LockRelease(l, false);
        }

        protected override void Dispose(bool disposing)
        {
            LocalSyncData.ReleaseData(Id);
        }

        private static class LocalSyncData
        {
            private static readonly Dictionary<Guid, (int UsedLockCount, StorageInfo Info, ReaderWriterLockSlim ReadWriteLock, ManualResetEventSlim ManualReset)> Data = new Dictionary<Guid, (int, StorageInfo, ReaderWriterLockSlim, ManualResetEventSlim)>();

            public static StorageInfo ReadInfo(Guid id)
            {
                lock (Data)
                {
                    if (Data.ContainsKey(id))
                        return Data[id].Info;
                }
                return default(StorageInfo);
            }

            public static ReaderWriterLockSlim ReadWriteLock(Guid id)
            {
                ReaderWriterLockSlim locker;
                lock (Data)
                {
                    if (!Data.ContainsKey(id))
                        Data[id] = (0, default(StorageInfo), new ReaderWriterLockSlim(), new ManualResetEventSlim());

                    locker = Data[id].ReadWriteLock;
                }

                return locker;
            }

            public static void ReleaseData(Guid id)
            {
                lock (Data)
                {
                    Data.Remove(id);
                }
            }

            public static ManualResetEventSlim Signal(Guid id)
            {
                ManualResetEventSlim locker;
                lock (Data)
                {
                    if (!Data.ContainsKey(id))
                        Data[id] = (0, default(StorageInfo), new ReaderWriterLockSlim(), new ManualResetEventSlim());

                    locker = Data[id].ManualReset;
                }

                return locker;
            }

            public static void WriteInfo(Guid id, StorageInfo info)
            {
                lock (Data)
                {
                    if (!Data.ContainsKey(id))
                    {
                        Data[id] = (0, info, new ReaderWriterLockSlim(), new ManualResetEventSlim());
                    }
                    else
                    {
                        var r = Data[id];
                        Data[id] = (r.UsedLockCount, info, r.ReadWriteLock, r.ManualReset);
                    }
                }
            }
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
    }
}