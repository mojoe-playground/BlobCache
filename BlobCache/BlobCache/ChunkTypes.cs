namespace BlobCache
{
    using System;
    using System.Text;
    using JetBrains.Annotations;

    /// <summary>
    ///     Known chunk types
    /// </summary>
    public static class ChunkTypes
    {
        /// <summary>
        ///     Free chunk
        /// </summary>
        public static readonly int Free = FourCC("FREE");

        /// <summary>
        ///     Test chunk
        /// </summary>
        public static readonly int Test = FourCC("TEST");

        /// <summary>
        ///     Blob chunk
        /// </summary>
        public static readonly int Blob = FourCC("BLOB");

        /// <summary>
        ///     Data chunk
        /// </summary>
        public static readonly int Data = FourCC("DATA");


        /// <summary>
        ///     Head chunk
        /// </summary>
        public static readonly int Head = FourCC("HEAD");


        /// <summary>
        ///     Converts a FourCC string to number
        /// </summary>
        /// <param name="fourCC">FourCC string to convert to number</param>
        /// <returns>FourCC number</returns>
        // ReSharper disable InconsistentNaming
        [PublicAPI]
        public static int FourCC(string fourCC)
        {
            if (fourCC.Length != 4)
                throw new FormatException("Must be 4 characters long");
            var c = fourCC.ToCharArray();
            return (c[3] << 24) | (c[2] << 16) | (c[1] << 8) | c[0];
        }

        /// <summary>
        ///     Converts a FourCC number to string
        /// </summary>
        /// <param name="fourCC">FourCC number to convert to string</param>
        /// <returns>FourCC string</returns>
        public static string FourCC(int fourCC)
        {
            var characters = BitConverter.GetBytes(fourCC);
            var sb = new StringBuilder();
            foreach (var ch in characters)
                sb.Append(Convert.ToChar(ch));
            return sb.ToString();
        }
        // ReSharper restore InconsistentNaming
    }
}