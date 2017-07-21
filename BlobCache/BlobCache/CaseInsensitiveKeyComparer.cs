namespace BlobCache
{
    using System;
    using System.Text;
    using CityHash;

    public class CaseInsensitiveKeyComparer : IKeyComparer
    {
        public bool SameKey(string key1, string key2)
        {
            return string.Equals(key1?.ToUpperInvariant(), key2?.ToUpperInvariant());
        }

        public uint GetHash(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
            return CityHash.CityHash32(key.ToUpperInvariant(), Encoding.UTF8);
        }
    }
}