namespace BlobCache
{
    using System;
    using Lockers;

    public class GlobalBobStorage : BlobStorage
    {
        public GlobalBobStorage(string fileName)
            : base(fileName)
        {
        }
    }
}