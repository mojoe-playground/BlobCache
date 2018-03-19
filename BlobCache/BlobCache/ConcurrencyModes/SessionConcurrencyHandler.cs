//#define DebugLogging

namespace BlobCache.ConcurrencyModes
{
    using System;
    using System.IO;
    using System.IO.MemoryMappedFiles;
    using System.Security.AccessControl;
    using System.Security.Principal;
    using System.Threading;
    using JetBrains.Annotations;

    /// <inheritdoc />
    /// <summary>
    ///     Concurrency handler in a single terminal session
    /// </summary>
    public class SessionConcurrencyHandler : ConcurrencyHandler
    {
        private readonly object _locker = new object();
        private MemoryMappedFile _mmf;
        private StorageInfo _cachedInfo;

        private GlobalLockData _rwl;

        /// <summary>
        ///     Initializes a new instance of the <see cref="SessionConcurrencyHandler" /> class
        /// </summary>
        public SessionConcurrencyHandler()
        {
            Timeout = 30000;
        }

        /// <summary>
        ///     Gets or sets a value indicating whether the locks are global or only for the current terminal session
        /// </summary>
        protected bool IsGlobal { get; set; } = false;

        /// <summary>
        ///     Gets the lock data used for manage the storage
        /// </summary>
        private GlobalLockData LockData
        {
            get
            {
                if (_rwl == null)
                    lock (_locker)
                    {
                        if (_rwl != null)
                            return _rwl;

                        _rwl = new GlobalLockData(Id, IsGlobal);
                    }

                return _rwl;
            }
        }

        /// <summary>
        ///     Gets the memory mapped file used for storage info synchronization
        /// </summary>
        private MemoryMappedFile Memory
        {
            get
            {
                if (_mmf == null)
                    lock (_locker)
                    {
                        if (_mmf != null)
                            return _mmf;

                        _mmf = CreateMemoryMappedFile();
                    }

                return _mmf;
            }
        }

        /// <inheritdoc />
        public override IDisposable Lock(int timeout, CancellationToken token)
        {
            return LockData.Lock(timeout, token);
        }

        /// <inheritdoc />
        public override StorageInfo ReadInfo()
        {
            using (var s = Memory.CreateViewStream())
                return StorageInfo.ReadFromStream(s, _cachedInfo);
        }

        /// <inheritdoc />
        public override void SignalReadFinish()
        {
            LockData.ReadEvent.Set();
        }

        /// <inheritdoc />
        public override void SignalWaitRequired()
        {
            LockData.ReadEvent.Reset();
        }

        /// <inheritdoc />
        public override void WaitForReadFinish(CancellationToken token)
        {
            while (true)
            {
                if (LockData.ReadEvent.WaitOne(500))
                    break;
                token.ThrowIfCancellationRequested();
            }
        }

        /// <inheritdoc />
        public override void WriteInfo(StorageInfo info)
        {
            using (var s = Memory.CreateViewStream())
            {
                info.WriteToStream(s);
                info.RefreshStableChunks();
                _cachedInfo = info;
            }
        }

        /// <summary>
        ///     Creates the memory mapped file used for storage info synchronization
        /// </summary>
        /// <returns>Memory mapped file</returns>
        protected virtual MemoryMappedFile CreateMemoryMappedFile()
        {
            var security = new MemoryMappedFileSecurity();
            var rule = new AccessRule<MemoryMappedFileRights>(new SecurityIdentifier(WellKnownSidType.WorldSid, null), MemoryMappedFileRights.FullControl, AccessControlType.Allow);
            security.AddAccessRule(rule);

            return MemoryMappedFile.CreateOrOpen($"{(IsGlobal ? "Global\\" : "")}BlobStorage-{Id}-Info", 25 * 1024 * 1024, MemoryMappedFileAccess.ReadWrite, MemoryMappedFileOptions.None, security, HandleInheritability.None);
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            _mmf?.Dispose();
            _mmf = null;
            _rwl?.Dispose();
            _rwl = null;
        }

        /// <inheritdoc />
        /// <summary>
        ///     Contains data used for locking a storage
        /// </summary>
        private class GlobalLockData : IDisposable
        {
            private readonly Mutex _mutex;
            // ReSharper disable once NotAccessedField.Local
            private readonly Guid _id;
#if DebugLogging
            private int _lockedThreadId;
#endif

            /// <summary>
            ///     Initializes a new instance of the <see cref="GlobalLockData" /> class
            /// </summary>
            /// <param name="id">Storage id</param>
            /// <param name="global">Indicating whether to use global or terminal session specific names</param>
            public GlobalLockData(Guid id, bool global)
            {
                _id = id;
                var allowEveryoneRule = new MutexAccessRule(new SecurityIdentifier(WellKnownSidType.WorldSid, null), MutexRights.FullControl, AccessControlType.Allow);
                var securitySettings = new MutexSecurity();
                securitySettings.AddAccessRule(allowEveryoneRule);

                _mutex = new Mutex(false, $"{(global ? "Global\\" : "")}BlobStorage-{id}-Lock", out _, securitySettings);

                var eventSecurity = new EventWaitHandleSecurity();
                var eventRule = new EventWaitHandleAccessRule(new SecurityIdentifier(WellKnownSidType.WorldSid, null), EventWaitHandleRights.FullControl, AccessControlType.Allow);
                eventSecurity.AddAccessRule(eventRule);
                ReadEvent = new EventWaitHandle(true, EventResetMode.ManualReset, $"{(global ? "Global\\" : "")}BlobStorage-{id}-ReadSignal", out _, eventSecurity);
            }

            /// <summary>
            ///     Gets the read event synchronization handle
            /// </summary>
            public EventWaitHandle ReadEvent { get; }

            /// <inheritdoc />
            public void Dispose()
            {
                _mutex.Dispose();
                ReadEvent.Dispose();
            }

            /// <summary>
            ///     Locks the storage info
            /// </summary>
            /// <param name="timeout">Timeout</param>
            /// <param name="token">Cancellation token</param>
            /// <returns>Lock</returns>
            [PublicAPI]
            public IDisposable Lock(int timeout, CancellationToken token)
            {
                var to = 0;
                var internalTimeout = Math.Min(timeout, 1000);
                try
                {
                    while (!_mutex.WaitOne(internalTimeout))
                    {
                        to += internalTimeout;
#if DebugLogging
                        System.Diagnostics.Debug.WriteLine($"BlobStorage {_id} waiting for lock: thread: -{Thread.CurrentThread.ManagedThreadId} locking: {_lockedThreadId} {DateTime.Now.ToLongTimeString()} {to} {timeout}");
#endif
                        if (to >= timeout)
                        {
#if DebugLogging
                            System.Diagnostics.Debug.WriteLine($"BlobStorage {_id} timeout: thread: -{Thread.CurrentThread.ManagedThreadId} locking: {_lockedThreadId}");
#endif
                            throw new TimeoutException();
                        }
                    }
                }
                catch (AbandonedMutexException)
                {
                }

#if DebugLogging
                _lockedThreadId = Thread.CurrentThread.ManagedThreadId;
#endif
                return new LockRelease(_mutex, this);
            }

            /// <inheritdoc />
            /// <summary>
            ///     Disposable for lock release
            /// </summary>
            private sealed class LockRelease : IDisposable
            {
                private readonly Mutex _mutex;
                // ReSharper disable once NotAccessedField.Local
                private readonly GlobalLockData _data;

                /// <summary>
                ///     Initializes a new instance of the <see cref="LockRelease" /> clss
                /// </summary>
                /// <param name="mutex">Mutex to release</param>
                /// <param name="data">Lock data</param>
                public LockRelease(Mutex mutex, GlobalLockData data)
                {
                    _mutex = mutex;
                    _data = data;
                }

                /// <inheritdoc />
                public void Dispose()
                {
#if DebugLogging
                    if (_data._lockedThreadId != Thread.CurrentThread.ManagedThreadId && System.Diagnostics.Debugger.IsAttached)
                        System.Diagnostics.Debugger.Break();
                    System.Diagnostics.Debug.WriteLine($"BlobStorage {_data._id} exit lock: -" + _data._lockedThreadId + " ");
                    _data._lockedThreadId = 0;
#endif
                    _mutex.ReleaseMutex();
                }
            }
        }
    }
}