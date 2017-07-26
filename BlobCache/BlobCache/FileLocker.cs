namespace BlobCache
{
    using System;
    using System.IO;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Threading;
    using JetBrains.Annotations;
    using Microsoft.Win32.SafeHandles;

    /// <summary>
    ///     Lock modes
    /// </summary>
    public enum LockMode
    {
        /// <summary>
        ///     Shared (read) lock
        /// </summary>
        Shared = 0,

        /// <summary>
        ///     Exclusive (write) lock
        /// </summary>
        Exclusive = 2
    }

    /// <summary>
    ///     File range locker
    /// </summary>
    public static class FileLocker
    {
        private static readonly Action WinIoError;

        static FileLocker()
        {
            var bindingAttr = BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;
            var winIoErrorMethod = typeof(string).Assembly.GetType("System.IO.__Error").GetMethod("WinIOError", bindingAttr, null, Type.EmptyTypes, null);
            WinIoError = (Action)Delegate.CreateDelegate(typeof(Action), winIoErrorMethod);
        }

        /// <summary>
        ///     Locks a part of the stream
        /// </summary>
        /// <param name="stream">Stream to lock</param>
        /// <param name="position">Region starting point</param>
        /// <param name="length">Region length</param>
        /// <param name="mode">Locking mode</param>
        /// <returns>Lock</returns>
        public static unsafe IDisposable Lock(this FileStream stream, long position, long length, LockMode mode)
        {
            var overlapped = new Overlapped
            {
                OffsetHigh = (int)(position >> 32),
                OffsetLow = (int)position
            };

            var native = overlapped.Pack(null, null);
            try
            {
                if (!LockFileEx(stream.SafeFileHandle, (uint)mode, 0, (uint)length, (uint)(length >> 32), native))
                    WinIoError();
                return new Unlocker(stream.SafeFileHandle, position, length);
            }
            finally
            {
                Overlapped.Free(native);
            }
        }

        /// <summary>
        ///     Locks a part of the stream and gives back sub stream for the range
        /// </summary>
        /// <param name="stream">Stream to lock</param>
        /// <param name="position">Region starting point</param>
        /// <param name="length">Region length</param>
        /// <param name="mode">Locking mode</param>
        /// <returns>Stream containing the locked range</returns>
        public static RangeStream Range(this FileStream stream, long position, long length, LockMode mode)
        {
            var locker = stream.Lock(position, length, mode);
            return new RangeStream(locker, stream, position, length, mode == LockMode.Exclusive);
        }


        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern unsafe bool LockFileEx(SafeFileHandle handle, uint flags, uint mustBeZero, uint countLow, uint countHigh, NativeOverlapped* overlapped);

        /// <summary>
        ///     Region unlocker
        /// </summary>
        internal unsafe class Unlocker : IDisposable
        {
            private readonly long _length;

            private readonly long _position;
            private SafeFileHandle _handle;

            /// <summary>
            ///     Initializes a new instance of the <see cref="Unlocker" /> class
            /// </summary>
            /// <param name="handle">Handle for the locked stream</param>
            /// <param name="position">Locked range starting position</param>
            /// <param name="length">Locked range length</param>
            internal Unlocker(SafeFileHandle handle, long position, long length)
            {
                _position = position;
                _length = length;
                _handle = handle;
            }

            /// <summary>
            ///     Releases the lock
            /// </summary>
            ~Unlocker()
            {
                Dispose(false);
            }

            /// <summary>
            ///     Releases the lock
            /// </summary>
            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            [DllImport("kernel32.dll", SetLastError = true)]
            private static extern bool UnlockFileEx(SafeFileHandle handle, uint mustBeZero, uint countLow, uint countHigh, NativeOverlapped* overlapped);

            [PublicAPI]
            private void Dispose(bool disposing)
            {
                if (_handle == null || _handle.IsClosed)
                    return;

                var overlapped = new Overlapped
                {
                    OffsetHigh = (int)(_position >> 32),
                    OffsetLow = (int)_position
                };

                var native = overlapped.Pack(null, null);
                try
                {
                    if (!UnlockFileEx(_handle, 0, (uint)_length, (uint)(_length >> 32), native))
                        WinIoError();
                }
                finally
                {
                    Overlapped.Free(native);
                    _handle = null;
                }
            }
        }
    }
}