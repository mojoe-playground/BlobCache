namespace BlobCache
{
    internal enum DataCompression
    {
        None = 0
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

        public void WriteToByteArray(byte[] data, DataCompression? replacementCompression)
        {
            data[0] = replacementCompression.HasValue ? (byte)replacementCompression.Value : (byte)Compression;
        }
    }
}