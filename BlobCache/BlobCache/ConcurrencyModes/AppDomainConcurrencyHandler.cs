namespace BlobCache.ConcurrencyModes
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    public class AppDomainConcurrencyHandler : ConcurrencyHandler
    {
        public override IDisposable Lock(int timeout, CancellationToken token)
        {
            var l = LocalSyncData.LockObject(Id);

            if (timeout < 0)
            {
                Monitor.Enter(l);
            }
            else
            {
                var lockTaken = false;
                Monitor.TryEnter(l, timeout, ref lockTaken);
                if (!lockTaken)
                    throw new TimeoutException();
            }

            return new LockRelease(l);
        }

        public override StorageInfo ReadInfo()
        {
            return LocalSyncData.ReadInfo(Id);
        }

        public override void SetId(Guid id)
        {
            base.SetId(id);

            LocalSyncData.AcquireData(Id);
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

        protected override void Dispose(bool disposing)
        {
            LocalSyncData.ReleaseData(Id);
        }

        private static class LocalSyncData
        {
            private static readonly Dictionary<Guid, (int UsedLockCount, StorageInfo Info, object Lock, ManualResetEventSlim ManualReset, int Counter)> Data = new Dictionary<Guid, (int, StorageInfo, object, ManualResetEventSlim, int)>();

            public static void AcquireData(Guid id)
            {
                lock (Data)
                {
                    if (!Data.ContainsKey(id))
                    {
                        Data[id] = (0, new StorageInfo(), new object(), new ManualResetEventSlim(), 1);
                    }
                    else
                    {
                        var r = Data[id];
                        Data[id] = (r.UsedLockCount, r.Info, r.Lock, r.ManualReset, r.Counter + 1);
                    }
                }
            }

            public static object LockObject(Guid id)
            {
                lock (Data)
                {
                    return Data[id].Lock;
                }
            }

            public static StorageInfo ReadInfo(Guid id)
            {
                lock (Data)
                {
                    return Data[id].Info;
                }
            }

            public static void ReleaseData(Guid id)
            {
                lock (Data)
                {
                    if (id != Guid.Empty)
                    {
                        var r = Data[id];
                        if (r.Counter == 1)
                            Data.Remove(id);
                        else
                            Data[id] = (r.UsedLockCount, r.Info, r.Lock, r.ManualReset, r.Counter - 1);
                    }
                }
            }

            public static ManualResetEventSlim Signal(Guid id)
            {
                lock (Data)
                {
                    return Data[id].ManualReset;
                }
            }

            public static void WriteInfo(Guid id, StorageInfo info)
            {
                lock (Data)
                {
                    var r = Data[id];
                    Data[id] = (r.UsedLockCount, info, r.Lock, r.ManualReset, r.Counter);
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