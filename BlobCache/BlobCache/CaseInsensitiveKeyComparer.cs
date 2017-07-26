namespace BlobCache
{
    using System;
    using System.Text;
    using CityHash;

    /// <summary>
    ///     Case insensitive key comparer
    /// </summary>
    public class CaseInsensitiveKeyComparer : IKeyComparer
    {
        /// <inheritdoc cref="IKeyComparer.GetHash" />
        public uint GetHash(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
            return CityHash.CityHash32(key.ToUpperInvariant(), Encoding.UTF8);
        }

        /// <inheritdoc cref="IKeyComparer.SameKey" />
        public bool SameKey(string key1, string key2)
        {
            return string.Equals(key1?.ToUpperInvariant(), key2?.ToUpperInvariant());
        }
    }
}