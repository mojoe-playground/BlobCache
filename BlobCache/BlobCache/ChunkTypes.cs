namespace BlobCache
{
    using System;

    public static class ChunkTypes
    {
        public static readonly int Free = FourCC("FREE");
        public static readonly int Test = FourCC("TEST");
        public static readonly int Blob = FourCC("BLOB");

        // ReSharper disable InconsistentNaming
        private static int FourCC(string fourCC)
        {
            if (fourCC.Length != 4)
                throw new FormatException("Must be 4 characters long");
            var c = fourCC.ToCharArray();
            return (c[3] << 24) | (c[2] << 16) | (c[1] << 8) | c[0];
            /*            if (!String.IsNullOrWhiteSpace(fourCC))
                        {
                            if (fourCC.Length != 4)
                            {
                                throw new FormatException("FOURCC length must be four characters");
                            }
                            else
                            {
                                char[] c = fourCC.ToCharArray();

                                if (toBigEndian)
                                {
                                    return String.Format("{0:X}", (c[0] << 24 | c[1] << 16 | c[2] << 8 | c[3]));
                                }
                                else if (toGuid)
                                {
                                    return String.Format("{0:X}", (c[3] << 24) | (c[2] << 16) | (c[1] << 8) | c[0]) + "-0000-0010-8000-00AA00389B71";
                                }
                                else
                                {
                                    return String.Format("{0:X}", (c[3] << 24) | (c[2] << 16) | (c[1] << 8) | c[0]);
                                }
                            }
                        }
                        return null;*/
        }
        // ReSharper restore InconsistentNaming
    }
}