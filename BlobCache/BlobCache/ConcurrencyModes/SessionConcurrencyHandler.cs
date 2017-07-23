namespace BlobCache.ConcurrencyModes
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.IO.MemoryMappedFiles;
    using System.Security.AccessControl;
    using System.Security.Principal;
    using System.Threading;
    using JetBrains.Annotations;

    public class SessionConcurrencyHandler : ConcurrencyHandler
    {
        private readonly object _locker = new object();
        private MemoryMappedFile _mmf;

        private GlobalLockData _rwl;

        public SessionConcurrencyHandler()
        {
            Timeout = 15000;
        }

        protected bool IsGlobal { get; set; } = false;

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

        public override IDisposable Lock(int timeout, CancellationToken token)
        {
            return LockData.WriteLock(timeout, token);
        }

        public override StorageInfo ReadInfo()
        {
            using (var s = Memory.CreateViewStream())
            {
                return StorageInfo.ReadFromStream(s);
            }
        }

        public override void SignalReadFinish()
        {
            LockData.ReadEvent.Set();
        }

        public override void SignalWaitRequired()
        {
            LockData.ReadEvent.Reset();
        }

        public override void WaitForReadFinish(CancellationToken token)
        {
            while (true)
            {
                if (LockData.ReadEvent.WaitOne(500))
                    break;
                token.ThrowIfCancellationRequested();
            }
        }

        public override void WriteInfo(StorageInfo info)
        {
            using (var s = Memory.CreateViewStream())
            {
                info.WriteToStream(s);
            }
        }

        protected virtual MemoryMappedFile CreateMemoryMappedFile()
        {
            var security = new MemoryMappedFileSecurity();
            var rule = new AccessRule<MemoryMappedFileRights>(new SecurityIdentifier(WellKnownSidType.WorldSid, null), MemoryMappedFileRights.FullControl, AccessControlType.Allow);
            security.AddAccessRule(rule);

            return MemoryMappedFile.CreateOrOpen($"{(IsGlobal ? "Global\\" : "")}BlobStorage-{Id}-Info", 25 * 1024 * 1024, MemoryMappedFileAccess.ReadWrite, MemoryMappedFileOptions.None, security, HandleInheritability.None);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            _mmf?.Dispose();
            _mmf = null;
            _rwl?.Dispose();
            _rwl = null;
        }

        private class GlobalLockData : IDisposable
        {
            private readonly Mutex _mutex;

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

            public EventWaitHandle ReadEvent { get; }

            public void Dispose()
            {
                _mutex.Dispose();
                ReadEvent.Dispose();
            }

            [PublicAPI]
            public IDisposable WriteLock(int timeout, CancellationToken token)
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

            private class LockRelease : IDisposable
            {
                private readonly Mutex _mutex;

                public LockRelease(Mutex mutex)
                {
                    _mutex = mutex;
                }

                public void Dispose()
                {
                    _mutex?.ReleaseMutex();
                }
            }
        }
    }
}