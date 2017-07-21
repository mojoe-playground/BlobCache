namespace BlobCache
{
    using System;
    using System.Text;
    using CityHash;

    public class CaseSensitiveKeyComparer : IKeyComparer
    {
        public bool SameKey(string key1, string key2)
        {
            return string.Equals(key1, key2);
        }

        public uint GetHash(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
            return CityHash.CityHash32(key, Encoding.UTF8);
        }
    }
}