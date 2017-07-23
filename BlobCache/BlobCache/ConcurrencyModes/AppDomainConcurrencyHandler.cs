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

        public override IDisposable Lock(int timeout, CancellationToken token)
        {
            var l = LocalSyncData.LockObject(Id);

            if (timeout < 0)
                Monitor.Enter(l);
            else
            {
                bool lockTaken = false;
                Monitor.TryEnter(l, timeout, ref lockTaken);
            if (!lockTaken)
                    throw new TimeoutException();
            }

            return new LockRelease(l);
        }

        protected override void Dispose(bool disposing)
        {
            LocalSyncData.ReleaseData(Id);
        }

        private static class LocalSyncData
        {
            private static readonly Dictionary<Guid, (int UsedLockCount, StorageInfo Info, object Lock, ManualResetEventSlim ManualReset)> Data = new Dictionary<Guid, (int, StorageInfo, object, ManualResetEventSlim)>();

            public static StorageInfo ReadInfo(Guid id)
            {
                lock (Data)
                {
                    if (Data.ContainsKey(id))
                        return Data[id].Info;
                }
                return default(StorageInfo);
            }

            public static object LockObject(Guid id)
            {
                object locker;
                lock (Data)
                {
                    if (!Data.ContainsKey(id))
                        Data[id] = (0, default(StorageInfo), new object(), new ManualResetEventSlim());

                    locker = Data[id].Lock;
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
                        Data[id] = (0, default(StorageInfo), new object(), new ManualResetEventSlim());

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
                        Data[id] = (0, info, new object(), new ManualResetEventSlim());
                    }
                    else
                    {
                        var r = Data[id];
                        Data[id] = (r.UsedLockCount, info, r.Lock, r.ManualReset);
                    }
                }
            }
        }

        private class LockRelease : IDisposable
        {
            private readonly object _lock;

            public LockRelease(object locker)
            {
                _lock = locker;
            }

            public void Dispose()
            {
                Monitor.Exit(_lock);
            }
        }
    }
}