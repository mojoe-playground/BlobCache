namespace BlobCache
{
    using System;
    using System.IO;

    public class RangeStream : Stream
    {
        private readonly IDisposable _locker;
        private byte[] _buffer = new byte[4096];
        private long _internalPosition;
        private int _readLength;
        private long _readPos = -1;
        private int _writeLength;
        private long _writePos = -1;

        public RangeStream(Stream stream, long position, long length)
        {
            Stream = stream;
            Start = position;
            Length = length;
            Stream.Position = position;
            CanRead = Stream.CanRead;
            CanSeek = Stream.CanSeek;
            CanWrite = Stream.CanWrite;
        }

        internal RangeStream(IDisposable locker, Stream stream, long position, long length, bool canWrite)
        {
            _locker = locker;
            Stream = stream;
            Start = position;
            Length = length;
            Stream.Position = position;
            CanRead = Stream.CanRead;
            CanSeek = Stream.CanSeek;
            CanWrite = Stream.CanWrite && canWrite;
        }

        public override bool CanRead { get; }
        public override bool CanSeek { get; }
        public override bool CanWrite { get; }
        public override long Length { get; }
        public long OriginalStreamPosition => Start + _internalPosition;

        public override long Position
        {
            get => _internalPosition;
            set => Seek(value, SeekOrigin.Begin);
        }

        public long Start { get; }
        public Stream Stream { get; }

        public override void Flush()
        {
            FlushWrite();
            if (Stream.CanSeek)
                Stream.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            FlushWrite();

            Stream.Position = _internalPosition + Start;
            var maxCount = (int)Math.Min(count, Length - _internalPosition);
            if (maxCount > _buffer.Length)
            {
                var read = Stream.Read(buffer, offset, maxCount);
                _internalPosition += read;
                return read;
            }

            if (_readPos < 0 || _readPos > _internalPosition || _readPos + _readLength < _internalPosition + maxCount)
            {
                _readPos = _internalPosition;
                _readLength = Stream.Read(_buffer, 0, (int)Math.Min(_buffer.Length, Length - _internalPosition));
            }

            Array.Copy(_buffer, _internalPosition - _readPos, buffer, offset, maxCount);
            _internalPosition += maxCount;
            Stream.Position = _internalPosition + Start;
            return maxCount;
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

            _internalPosition = targetPosition;
            Stream.Position = _internalPosition + Start;
            return _internalPosition;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            FlushRead();

            var maxCount = (int)Math.Min(count, Length - _internalPosition);

            if (maxCount > _buffer.Length)
            {
                FlushWrite();

                Stream.Position = _internalPosition + Start;
                Stream.Write(buffer, offset, maxCount);
                _internalPosition += maxCount;
                return;
            }

            if (_writePos >= 0 && _writePos + _writeLength == _internalPosition && _writeLength + maxCount < _buffer.Length)
            {
                Array.Copy(buffer, offset, _buffer, _writeLength, maxCount);
                _writeLength += maxCount;
                _internalPosition += maxCount;
                return;
            }

            FlushWrite();

            Array.Copy(buffer, offset, _buffer, 0, maxCount);
            _writePos = _internalPosition;
            _writeLength = maxCount;
            _internalPosition += maxCount;
        }

        protected override void Dispose(bool disposing)
        {
            Flush();
            base.Dispose(disposing);
            _locker?.Dispose();
            _buffer = null;
        }

        private void FlushRead()
        {
            _readPos = -1;
            _readLength = 0;
        }

        private void FlushWrite()
        {
            if (_writePos < 0)
                return;

            Stream.Position = _writePos + Start;
            Stream.Write(_buffer, 0, _writeLength);
            _writePos = -1;
            _writeLength = 0;

            Stream.Position = _internalPosition + Start;
        }
    }
}