namespace BlobCache
{
    using System;
    using System.IO;

    internal enum DataCompression
    {
        None = 0,
        Deflate = 1
    }

    internal struct DataHead
    {
        public const int DataHeadSize = 1;

        public DataCompression Compression { get; private set; }
        public int Size { get; private set; }

        public DataHead(DataCompression compression)
        {
            Compression = compression;
            Size = DataHeadSize;
        }

        public static DataHead ReadFromByteArray(byte[] data)
        {
            return new DataHead { Compression = (DataCompression)data[0], Size = 1 };
        }

        private byte[] Write(DataCompression? replacementCompression)
        {
            var res = new byte[] { DataHeadSize };
            res[0] = replacementCompression.HasValue ? (byte)replacementCompression.Value : (byte)Compression;
            return res;
        }

        public void WriteToByteArray(byte[] data, DataCompression? replacementCompression)
        {
            var head = Write(replacementCompression);
            Array.Copy(head, data, head.Length);
        }

        public void WriteToStream(Stream data, DataCompression? replacementCompression)
        {
            var head = Write(replacementCompression);
            data.Write(head, 0, head.Length);
        }
    }
}