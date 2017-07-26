namespace BlobCache
{
    using System;
    using System.Text;
    using CityHash;

    /// <summary>
    ///     Case sensitive key comparer
    /// </summary>
    public class CaseSensitiveKeyComparer : IKeyComparer
    {
        /// <inheritdoc cref="IKeyComparer.GetHash" />
        public uint GetHash(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
            return CityHash.CityHash32(key, Encoding.UTF8);
        }

        /// <inheritdoc cref="IKeyComparer.SameKey" />
        public bool SameKey(string key1, string key2)
        {
            return string.Equals(key1, key2);
        }
    }
}