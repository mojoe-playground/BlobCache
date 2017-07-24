namespace BlobCache
{
    using System;
    /// <summary>
    /// Crc16 calculation
    /// </summary>
    /// <remarks>Based on http://www.sanity-free.com/134/standard_crc_16_in_csharp.html
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

        public static ushort ComputeChecksum(byte[] bytes) => ComputeChecksum(bytes, bytes.Length, 0);

        public static ushort ComputeChecksum(byte[] bytes, int length, ushort crc)
        {
            for (var i = 0; i < length; i++)
            {
                var index = (byte)(crc ^ bytes[i]);
                crc = (ushort)((crc >> 8) ^ Table[index]);
            }

            return crc;
        }

        public static byte[] ComputeChecksumBytes(byte[] bytes)
        {
            var crc = ComputeChecksum(bytes);
            return BitConverter.GetBytes(crc);
        }
    }
}