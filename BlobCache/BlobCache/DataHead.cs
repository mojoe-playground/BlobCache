﻿namespace BlobCache
{
    using System.IO;

    /// <summary>
    ///     Data compressions
    /// </summary>
    internal enum DataCompression
    {
        /// <summary>
        ///     No compression
        /// </summary>
        None = 0,

        /// <summary>
        ///     Deflate compression
        /// </summary>
        Deflate = 1
    }

    /// <summary>
    ///     Data chunk header
    /// </summary>
    internal struct DataHead
    {
        /// <summary>
        ///     Current header version header size
        /// </summary>
        public const int DataHeadSize = 1;

        /// <summary>
        ///     Gets the compression used
        /// </summary>
        public DataCompression Compression { get; private set; }

        /// <summary>
        ///     Gets the data header size of this header
        /// </summary>
        public int Size { get; private set; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="DataHead" /> class
        /// </summary>
        /// <param name="compression">Preferred compression</param>
        public DataHead(DataCompression compression)
        {
            Compression = compression;
            Size = DataHeadSize;
        }

        /// <summary>
        ///     Reads the header from chunk data
        /// </summary>
        /// <param name="data">Chunk data</param>
        /// <returns>Data header</returns>
        public static DataHead ReadFromByteArray(byte[] data)
        {
            return new DataHead { Compression = (DataCompression)data[0], Size = 1 };
        }

        /// <summary>
        ///     Writes the header to a byte array
        /// </summary>
        /// <param name="data">Buffer where the header should be written</param>
        /// <param name="replacementCompression">Indicates whether use a different compression than specified</param>
        public void WriteToByteArray(byte[] data, DataCompression? replacementCompression)
        {
            data[0] = replacementCompression.HasValue ? (byte)replacementCompression.Value : (byte)Compression;
        }

        /// <summary>
        ///     Writes the header to a stream
        /// </summary>
        /// <param name="data">Stream where the header should be written</param>
        /// <param name="replacementCompression">Indicates whether use a different compression than specified</param>
        public void WriteToStream(Stream data, DataCompression? replacementCompression)
        {
            data.WriteByte(replacementCompression.HasValue ? (byte)replacementCompression.Value : (byte)Compression);
        }
    }
}