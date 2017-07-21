namespace BlobCache
{
    public interface IKeyComparer
    {
        bool SameKey(string key1, string key2);
        uint GetHash(string key);
    }
}