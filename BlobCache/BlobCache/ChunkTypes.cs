namespace BlobCache
{
    using System;

    public static class ChunkTypes
    {
        public static readonly int Free = FourCC("FREE");

        public static readonly int Test = FourCC("TEST");

        public static readonly int Blob = FourCC("BLOB");

        public static readonly int Data = FourCC("DATA");

        public static readonly int Head = FourCC("HEAD");


        // ReSharper disable InconsistentNaming
        private static int FourCC(string fourCC)
        {
            if (fourCC.Length != 4)
                throw new FormatException("Must be 4 characters long");
            var c = fourCC.ToCharArray();
            return (c[3] << 24) | (c[2] << 16) | (c[1] << 8) | c[0];
        }
        // ReSharper restore InconsistentNaming
    }
}