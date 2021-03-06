﻿namespace BlobCache.ConcurrencyModes
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>
    ///     Concurrency handler in an app domain
    /// </summary>
    public class AppDomainConcurrencyHandler : ConcurrencyHandler
    {
        /// <inheritdoc />
        public override bool TryEnterLock()
        {
            var l = LocalSyncData.LockObject(Id);
            var lockTaken = false;
            Monitor.TryEnter(l, 0, ref lockTaken);

            return lockTaken;
        }

        /// <inheritdoc />
        public override void ReleaseLock()
        {
            var l = LocalSyncData.LockObject(Id);
            Monitor.Exit(l);
        }

        /// <inheritdoc />
        public override StorageInfo ReadInfo()
        {
            return LocalSyncData.ReadInfo(Id);
        }

        /// <inheritdoc />
        public override void SetId(Guid id)
        {
            base.SetId(id);

            LocalSyncData.AcquireData(Id);
        }

        /// <inheritdoc />
        public override void SignalReadFinish()
        {
            LocalSyncData.Signal(Id).Set();
        }

        /// <inheritdoc />
        public override void SignalWaitRequired()
        {
            LocalSyncData.Signal(Id).Reset();
        }

        /// <inheritdoc />
        public override void WaitForReadFinish(CancellationToken token)
        {
            LocalSyncData.Signal(Id).Wait(token);
        }

        /// <inheritdoc />
        public override void WriteInfo(StorageInfo info, bool stableChunkChanged)
        {
            if (stableChunkChanged)
                info.RefreshStableChunks();
            LocalSyncData.WriteInfo(Id, info);
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            LocalSyncData.ReleaseData(Id);
        }

        /// <summary>
        ///     Synchronization data container
        /// </summary>
        private static class LocalSyncData
        {
            private static readonly Dictionary<Guid, (int UsedLockCount, StorageInfo Info, object Lock, ManualResetEventSlim ManualReset, int Counter)> Data = new Dictionary<Guid, (int, StorageInfo, object, ManualResetEventSlim, int)>();

            /// <summary>
            ///     Initializes data for a storage id
            /// </summary>
            /// <param name="id">Storage id</param>
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

            /// <summary>
            ///     Returns the lock fro a storage id
            /// </summary>
            /// <param name="id">Storage id</param>
            /// <returns>Lock</returns>
            public static object LockObject(Guid id)
            {
                lock (Data)
                {
                    return Data[id].Lock;
                }
            }

            /// <summary>
            ///     Reads the storage info for s storage id
            /// </summary>
            /// <param name="id">Storage id</param>
            /// <returns>Storage info</returns>
            public static StorageInfo ReadInfo(Guid id)
            {
                lock (Data)
                {
                    return Data[id].Info;
                }
            }

            /// <summary>
            ///     Releases the data for a storage id
            /// </summary>
            /// <param name="id">Storage id</param>
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

            /// <summary>
            ///     Gets the read signal for a storage id
            /// </summary>
            /// <param name="id">Storage id</param>
            /// <returns>Signal</returns>
            public static ManualResetEventSlim Signal(Guid id)
            {
                lock (Data)
                {
                    return Data[id].ManualReset;
                }
            }

            /// <summary>
            ///     Writes storage info for a storage id
            /// </summary>
            /// <param name="id">Storage id</param>
            /// <param name="info">Storage info</param>
            public static void WriteInfo(Guid id, StorageInfo info)
            {
                lock (Data)
                {
                    var r = Data[id];
                    Data[id] = (r.UsedLockCount, info, r.Lock, r.ManualReset, r.Counter);
                }
            }
        }
    }
}