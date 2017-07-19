namespace BlobCache.Lockers
{
    using System;
    using System.Security.AccessControl;
    using System.Security.Principal;
    using System.Threading;

    internal class GlobalLocker : IDisposable
    {
        private readonly bool _hasHandle;
        private Mutex _mutex;

        public GlobalLocker(Guid id, int timeOut)
        {
            InitMutex(id);
            try
            {
                if (timeOut < 0)
                    _hasHandle = _mutex.WaitOne(Timeout.Infinite, false);
                else
                    _hasHandle = _mutex.WaitOne(timeOut, false);

                if (_hasHandle == false)
                    throw new TimeoutException("Timeout waiting for exclusive access on StorageLocker");
            }
            catch (AbandonedMutexException)
            {
                _hasHandle = true;
            }
        }

        public void Dispose()
        {
            if (_mutex != null)
            {
                if (_hasHandle)
                    _mutex.ReleaseMutex();
                _mutex.Dispose();
                _mutex = null;
            }
        }

        private void InitMutex(Guid id)
        {
            var allowEveryoneRule = new MutexAccessRule(new SecurityIdentifier(WellKnownSidType.WorldSid, null),
                MutexRights.FullControl, AccessControlType.Allow);
            var securitySettings = new MutexSecurity();
            securitySettings.AddAccessRule(allowEveryoneRule);

            _mutex = new Mutex(false, $"Global\\BlobStorage-{id}", out _, securitySettings);
        }
    }
}