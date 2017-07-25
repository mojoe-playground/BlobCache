namespace BlobCache
{
    using System;
    using System.IO;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using System.Threading;
    using JetBrains.Annotations;
    using Microsoft.Win32.SafeHandles;

    public enum LockMode
    {
        Shared = 0,
        Exclusive = 2
    }

    public static class FileLocker
    {
        private static readonly Action WinIoError;

        static FileLocker()
        {
            var bindingAttr = BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;
            var winIoErrorMethod = typeof(string).Assembly.GetType("System.IO.__Error").GetMethod("WinIOError", bindingAttr, null, Type.EmptyTypes, null);
            WinIoError = (Action)Delegate.CreateDelegate(typeof(Action), winIoErrorMethod);
        }


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

        public static Stream Range(this FileStream stream, long position, long length, LockMode mode)
        {
            var locker = stream.Lock(position, length, mode);
            return /*new BufferedStream(*/new RangeStream(locker, stream, position, length, mode == LockMode.Exclusive) /*)*/;
        }


        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern unsafe bool LockFileEx(SafeFileHandle handle, uint flags, uint mustBeZero, uint countLow, uint countHigh, NativeOverlapped* overlapped);

        private class RangeStream : Stream
        {
            private readonly IDisposable _locker;
            private readonly long _position;
            private readonly FileStream _stream;
            private long _internalPosition;

            public RangeStream(IDisposable locker, FileStream stream, long position, long length, bool canWrite)
            {
                _locker = locker;
                _stream = stream;
                _position = position;
                Length = length;
                _stream.Position = position;
                CanRead = _stream.CanRead;
                CanSeek = _stream.CanSeek;
                CanWrite = _stream.CanWrite && canWrite;
            }

            public override bool CanRead { get; }
            public override bool CanSeek { get; }
            public override bool CanWrite { get; }
            public override long Length { get; }

            public override long Position
            {
                get => _internalPosition;
                set => Seek(value, SeekOrigin.Begin);
            }

            public override void Flush()
            {
                _stream.Flush();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                var maxCount = (int)Math.Min(count, Length - _internalPosition);
                try
                {
                    var read = _stream.Read(buffer, offset, maxCount);
                    return read;
                }
                finally
                {
                    _internalPosition = _stream.Position - _position;
                }
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                long targetPosition = 0;
                switch (origin)
                {
                    case SeekOrigin.Begin:
                        targetPosition = offset;
                        break;
                    case SeekOrigin.Current:
                        targetPosition = _internalPosition + offset;
                        break;
                    case SeekOrigin.End:
                        targetPosition = Length + offset;
                        break;
                }

                if (targetPosition < 0)
                    throw new ArgumentOutOfRangeException(nameof(offset));
                if (targetPosition > Length)
                    throw new ArgumentOutOfRangeException(nameof(offset));

                try
                {
                    _stream.Position = _position + targetPosition;
                }
                catch (ObjectDisposedException)
                {
                }

                _internalPosition = targetPosition;
                return _internalPosition;
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                var maxCount = (int)Math.Min(count, Length - _internalPosition);
                try
                {
                    _stream.Write(buffer, offset, maxCount);
                }
                finally
                {
                    _internalPosition = _stream.Position - _position;
                }
            }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
                _locker.Dispose();
            }
        }

        private unsafe class Unlocker : IDisposable
        {
            private readonly long _length;

            private readonly long _position;
            private SafeFileHandle _handle;

            public Unlocker(SafeFileHandle handle, long position, long length)
            {
                _position = position;
                _length = length;
                _handle = handle;
            }

            ~Unlocker()
            {
                Dispose(false);
            }

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