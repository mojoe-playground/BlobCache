namespace BlobCache.ConcurrencyModes
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.IO.MemoryMappedFiles;
    using System.Security.AccessControl;
    using System.Security.Principal;
    using System.Threading;

    public class SessionConcurrencyHandler : ConcurrencyHandler
    {
        private readonly object _locker = new object();
        private MemoryMappedFile _mmf;

        private GlobalReaderWriterLocker _rwl;

        public SessionConcurrencyHandler()
        {
            Timeout = 5000;
        }

        protected bool IsGlobal { get; set; } = false;

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

        private GlobalReaderWriterLocker ReaderWriterLock
        {
            get
            {
                if (_rwl == null)
                    lock (_locker)
                    {
                        if (_rwl != null)
                            return _rwl;

                        _rwl = new GlobalReaderWriterLocker(Id, IsGlobal);
                    }

                return _rwl;
            }
        }

        public override StorageInfo ReadInfo()
        {
            using (var s = Memory.CreateViewStream())
            {
                return StorageInfo.ReadFromStream(s);
            }
        }

        public override IDisposable ReadLock(int timeout, CancellationToken token)
        {
            return ReaderWriterLock.ReadLock(timeout);
        }

        public override void SignalReadFinish()
        {
            ReaderWriterLock.ReadEvent.Set();
        }

        public override void SignalWaitRequired()
        {
            ReaderWriterLock.ReadEvent.Reset();
        }

        public override void WaitForReadFinish(CancellationToken token)
        {
            while (true)
            {
                if (ReaderWriterLock.ReadEvent.WaitOne(500))
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

        public override IDisposable WriteLock(int timeout, CancellationToken token)
        {
            return ReaderWriterLock.WriteLock(timeout, token);
        }

        protected virtual MemoryMappedFile CreateMemoryMappedFile()
        {
            var security = new MemoryMappedFileSecurity();
            var rule = new AccessRule<MemoryMappedFileRights>(new SecurityIdentifier(WellKnownSidType.WorldSid, null), MemoryMappedFileRights.FullControl, AccessControlType.Allow);
            security.AddAccessRule(rule);

            return MemoryMappedFile.CreateOrOpen($"{(IsGlobal ? "Global\\" : "")}BlobStorage-{Id}-Info", 1024 * 1024, MemoryMappedFileAccess.ReadWrite, MemoryMappedFileOptions.None, security, HandleInheritability.None);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            _mmf?.Dispose();
            _mmf = null;
            _rwl?.Dispose();
            _rwl = null;
        }

        private class GlobalReaderWriterLocker : IDisposable
        {
            private const int SemaphoreCount = 25;

            private readonly Mutex _mutex;
            private readonly Semaphore _semaphore;

            public GlobalReaderWriterLocker(Guid id, bool global)
            {
                var allowEveryoneRule = new MutexAccessRule(new SecurityIdentifier(WellKnownSidType.WorldSid, null), MutexRights.FullControl, AccessControlType.Allow);
                var securitySettings = new MutexSecurity();
                securitySettings.AddAccessRule(allowEveryoneRule);

                _mutex = new Mutex(false, $"{(global ? "Global\\" : "")}BlobStorage-{id}-WriteLock", out _, securitySettings);

                var semaphoreSecurity = new SemaphoreSecurity();
                var semaphoreRule = new SemaphoreAccessRule(new SecurityIdentifier(WellKnownSidType.WorldSid, null), SemaphoreRights.FullControl, AccessControlType.Allow);
                semaphoreSecurity.AddAccessRule(semaphoreRule);
                _semaphore = new Semaphore(SemaphoreCount, SemaphoreCount, $"{(global ? "Global\\" : "")}BlobStorage-{id}-ReadLock", out _, semaphoreSecurity);

                var eventSecurity = new EventWaitHandleSecurity();
                var eventRule = new EventWaitHandleAccessRule(new SecurityIdentifier(WellKnownSidType.WorldSid, null), EventWaitHandleRights.FullControl, AccessControlType.Allow);
                eventSecurity.AddAccessRule(eventRule);
                ReadEvent = new EventWaitHandle(true, EventResetMode.ManualReset, $"{(global ? "Global\\" : "")}BlobStorage-{id}-ReadSignal", out _, eventSecurity);
            }

            public EventWaitHandle ReadEvent { get; }

            public void Dispose()
            {
                _mutex.Dispose();
                _semaphore.Dispose();
            }

            public IDisposable ReadLock(int timeout)
            {
                var sw = new Stopwatch();
                sw.Start();
                var lockTaken = false;
                try
                {
                    try
                    {
                        if (!_mutex.WaitOne(timeout))
                            throw new TimeoutException();
                        lockTaken = true;
                    }
                    catch (AbandonedMutexException)
                    {
                        lockTaken = true;
                    }

                    // Wait for a semaphore slot.
                    if (!_semaphore.WaitOne(RealTimeout(timeout, sw)))
                        throw new TimeoutException();

                    return new LockRelease(null, _semaphore);
                }
                finally
                {
                    // Release mutex so others can access the semaphore.
                    if (lockTaken)
                        _mutex.ReleaseMutex();
                }
            }

            public IDisposable WriteLock(int timeout, CancellationToken token)
            {
                var sw = new Stopwatch();
                sw.Start();
                var lockTaken = false;
                try
                {
                    try
                    {
                        if (!_mutex.WaitOne(timeout))
                            throw new TimeoutException();
                        lockTaken = true;
                    }
                    catch (AbandonedMutexException)
                    {
                        lockTaken = true;
                    }

                    // Here we're waiting for the semaphore to get full,
                    // meaning that there aren't any more readers accessing.
                    // The only way to get the count is to call Release.
                    // So we wait, then immediately release.
                    // Release returns the previous count.
                    // Since we know that access to the semaphore is locked
                    // (i.e. nobody can get a slot), we know that when count
                    // goes to one less than the total possible, all the readers
                    // are done.
                    if (!_semaphore.WaitOne(RealTimeout(timeout, sw)))
                        throw new TimeoutException();

                    var count = _semaphore.Release();
                    while (count != SemaphoreCount - 1)
                    {
                        token.ThrowIfCancellationRequested();
                        // sleep briefly so other processes get a chance.
                        // You might want to tweak this value.  Sleep(1) might be okay.
                        Thread.Sleep(10);
                        if (!_semaphore.WaitOne(RealTimeout(timeout, sw)))
                            throw new TimeoutException();
                        count = _semaphore.Release();
                    }

                    // At this point, there are no more readers.
                    lockTaken = false;
                    return new LockRelease(_mutex, null);
                }
                finally
                {
                    // Release mutex so others can access the semaphore.
                    if (lockTaken)
                        _mutex.ReleaseMutex();
                }
            }

            private int RealTimeout(int timeout, Stopwatch sw)
            {
                if (timeout <= 0)
                    return timeout;

                return (int)Math.Max(1, timeout - sw.ElapsedMilliseconds);
            }

            private class LockRelease : IDisposable
            {
                private readonly Mutex _mutex;
                private readonly Semaphore _semaphore;

                public LockRelease(Mutex mutex, Semaphore semaphore)
                {
                    _mutex = mutex;
                    _semaphore = semaphore;
                }

                public void Dispose()
                {
                    _mutex?.ReleaseMutex();
                    _semaphore?.Release();
                }
            }
        }
    }
}