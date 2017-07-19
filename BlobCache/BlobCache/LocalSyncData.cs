namespace BlobCache
{
    using System;
    using System.Collections.Generic;
    using Nito.AsyncEx;

    internal static class LocalSyncData
    {
        private static readonly Dictionary<Guid, (object Lock, int UsedLockCount, StorageInfo Info, AsyncReaderWriterLock ReadWriteLock)> Data =
            new Dictionary<Guid, (object, int, StorageInfo, AsyncReaderWriterLock)>();

        public static void ReleaseLock(Guid id)
        {
            lock (Data)
            {
                var r = Data[id];
                Data[id] = (r.Lock, r.UsedLockCount - 1, r.Info, r.ReadWriteLock);
            }
        }

        public static object AcquireLock(Guid id)
        {
            object res;
            lock (Data)
            {
                if (!Data.ContainsKey(id))
                {
                    res = new object();
                    Data[id] = (res, 1, default(StorageInfo), new AsyncReaderWriterLock());
                }
                else
                {
                    var r = Data[id];
                    Data[id] = (r.Lock, r.UsedLockCount + 1, r.Info, r.ReadWriteLock);
                    res = r.Item1;
                }
            }

            return res;
        }

        public static void ReleaseData(Guid id)
        {
            lock (Data)
                Data.Remove(id);
        }

        public static StorageInfo ReadInfo(Guid id)
        {
            lock (Data)
                if (Data.ContainsKey(id))
                    return Data[id].Info;
            return default(StorageInfo);
        }

        public static void WriteInfo(Guid id, StorageInfo info)
        {
            lock (Data)
            {
                if (!Data.ContainsKey(id))
                {
                    Data[id] = (new object(), 0, info, new AsyncReaderWriterLock());
                }
                else
                {
                    var r = Data[id];
                    Data[id] = (r.Lock, r.UsedLockCount, info, r.ReadWriteLock);
                }
            }
        }

        public static AsyncReaderWriterLock ReadWriteLock(Guid id)
        {
            AsyncReaderWriterLock locker;
            lock (Data)
            {
                if (!Data.ContainsKey(id))
                    Data[id] = (new object(), 0, default(StorageInfo), new AsyncReaderWriterLock());

                locker = Data[id].ReadWriteLock;
            }

            return locker;
        }
    }
}