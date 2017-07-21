namespace BlobCache
{
    using System;
    using System.Text;
    using JetBrains.Annotations;

    public static class ChunkTypes
    {
        public static readonly int Free = FourCC("FREE");
        public static readonly int Test = FourCC("TEST");
        public static readonly int Blob = FourCC("BLOB");

        public static readonly int Data = FourCC("DATA");


        public static readonly int Head = FourCC("HEAD");


        // ReSharper disable InconsistentNaming
        [PublicAPI]
        public static int FourCC(string fourCC)
        {
            if (fourCC.Length != 4)
                throw new FormatException("Must be 4 characters long");
            var c = fourCC.ToCharArray();
            return (c[3] << 24) | (c[2] << 16) | (c[1] << 8) | c[0];
        }

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