namespace BlobCache
{
    /// <summary>
    ///     Crc16 calculation
    /// </summary>
    /// <remarks>
    ///     Based on http://www.sanity-free.com/134/standard_crc_16_in_csharp.html
    /// </remarks>
    internal static class Crc16
    {
        private const ushort Polynomial = 0xA001;
        private static readonly ushort[] Table = new ushort[256];

        static Crc16()
        {
            for (ushort i = 0; i < Table.Length; ++i)
            {
                ushort value = 0;
                var temp = i;
                for (byte j = 0; j < 8; ++j)
                {
                    if (((value ^ temp) & 0x0001) != 0)
                        value = (ushort)((value >> 1) ^ Polynomial);
                    else
                        value >>= 1;
                    temp >>= 1;
                }
                Table[i] = value;
            }
        }

        /// <summary>
        ///     Calculates 16bit CRC for the given bytes
        /// </summary>
        /// <param name="bytes">Bytes to check</param>
        /// <returns>16bit CRC</returns>
        public static ushort ComputeChecksum(byte[] bytes)
        {
            return ComputeChecksum(bytes, 0, bytes.Length, 0);
        }

        /// <summary>
        ///     Calculates 16bit CRC for the given bytes
        /// </summary>
        /// <param name="bytes">Bytes to check</param>
        /// <param name="offset">Offset in the array</param>
        /// <param name="length">Number of bytes to check</param>
        /// <param name="crc">Starting CRC</param>
        /// <returns>16bit CRC</returns>
        /// <remarks>Call with a CRC from a previous ComputeChecksum call to continue calculating CRC if data is buffered</remarks>
        public static ushort ComputeChecksum(byte[] bytes, int offset, int length, ushort crc)
        {
            for (var i = offset; i < offset + length; i++)
            {
                var index = (byte)(crc ^ bytes[i]);
                crc = (ushort)((crc >> 8) ^ Table[index]);
            }

            return crc;
        }
    }
}