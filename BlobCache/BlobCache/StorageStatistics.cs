using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobCache
{
    public struct StorageStatistics
    {
        public long UsedSpace { get; set; }
        public long FreeSpace { get; set; }
        public long Overhead { get; set; }
        public long FileSize { get; set; }
        public int UsedChunks { get; set; }
        public int FreeChunks { get; set; }
    }
}
