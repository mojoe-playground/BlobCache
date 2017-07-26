namespace BlobCache
{
    /// <summary>
    ///     Cache entry key comparer
    /// </summary>
    public interface IKeyComparer
    {
        /// <summary>
        ///     Gets the hash of a key
        /// </summary>
        /// <param name="key">Key to hash</param>
        /// <returns>Hash of the key</returns>
        uint GetHash(string key);

        /// <summary>
        ///     Check whether two keys are the same
        /// </summary>
        /// <param name="key1">First key to check</param>
        /// <param name="key2">Second key to check</param>
        /// <returns>True if the keys are the same, otherwise false</returns>
        bool SameKey(string key1, string key2);
    }
}