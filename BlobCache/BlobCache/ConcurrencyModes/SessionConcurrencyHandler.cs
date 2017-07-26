﻿namespace BlobCache.ConcurrencyModes
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.IO.MemoryMappedFiles;
    using System.Security.AccessControl;
    using System.Security.Principal;
    using System.Threading;
    using JetBrains.Annotations;

    /// <summary>
    ///     Concurrency handler in a single terminal session
    /// </summary>
    public class SessionConcurrencyHandler : ConcurrencyHandler
    {
        private readonly object _locker = new object();
        private MemoryMappedFile _mmf;

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
            {
                return StorageInfo.ReadFromStream(s);
            }
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

        /// <summary>
        ///     Contains data used for locking a storage
        /// </summary>
        private class GlobalLockData : IDisposable
        {
            private readonly Mutex _mutex;

            /// <summary>
            ///     Initializes a new instance of the <see cref="GlobalLockData" /> class
            /// </summary>
            /// <param name="id">Storage id</param>
            /// <param name="global">Indicating whether to use global or terminal session specific names</param>
            public GlobalLockData(Guid id, bool global)
            {
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

            /// <summary>
            ///     Releases created handlers
            /// </summary>
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
                var sw = new Stopwatch();
                sw.Start();

                try
                {
                    if (!_mutex.WaitOne(timeout))
                        throw new TimeoutException();
                }
                catch (AbandonedMutexException)
                {
                }

                return new LockRelease(_mutex);
            }

            /// <summary>
            ///     Disposable for lock release
            /// </summary>
            private class LockRelease : IDisposable
            {
                private readonly Mutex _mutex;

                /// <summary>
                ///     Initializes a new instance of the <see cref="LockRelease" /> clss
                /// </summary>
                /// <param name="mutex">Mutex to release</param>
                public LockRelease(Mutex mutex)
                {
                    _mutex = mutex;
                }

                /// <summary>
                ///     Releases the lock held
                /// </summary>
                public void Dispose()
                {
                    _mutex?.ReleaseMutex();
                }
            }
        }
    }
}