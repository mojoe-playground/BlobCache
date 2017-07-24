
#define InvalidChunkDebug
namespace BlobCache
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using ConcurrencyModes;
    using JetBrains.Annotations;

    public class BlobStorage : IDisposable
    {
        internal const int HeaderSize = 24;
        private const int LastVersion = 1;

        private Stream _mainLock;

        /// <summary>
        ///     Initializes a new instance of the <see cref="BlobStorage" /> class
        /// </summary>
        /// <param name="fileName">Blob storage file name</param>
        public BlobStorage(string fileName)
        {
            Info = new FileInfo(fileName);
        }

        ~BlobStorage()
        {
            Dispose();
        }

        /// <summary>
        ///     Gets a value indicating whether the storage is initialized by this instance
        /// </summary>
        public bool FreshlyInitialized { get; private set; }

        /// <summary>
        ///     Gets a value indicating whether the cache is initialized
        /// </summary>
        public bool IsInitialized { get; private set; }

        /// <summary>
        /// Gets or sets a value indicating whether initialization should fail or blob storage should be truncated if a chunk loading fail at initialization
        /// </summary>
        [PublicAPI]
        public bool TruncateOnChunkInitializationError { get; set; }

        private ConcurrencyHandler ConcurrencyHandler { get; set; }

        private Guid Id { get; set; }

        /// <summary>
        ///     Gets the blob storage file info
        /// </summary>
        protected internal FileInfo Info { get; }

        /// <summary>
        ///     Adds a chunk to the blob
        /// </summary>
        /// <param name="chunkType">Chunk type</param>
        /// <param name="userData">Chunk user data</param>
        /// <param name="data">bytes to add</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>StorageChunk of the added chunk</returns>
        public async Task<StorageChunk> AddChunk(int chunkType, uint userData, byte[] data, CancellationToken token)
        {
            using (var ms = new MemoryStream(data))
            {
                return await AddChunk(chunkType, userData, ms, token);
            }
        }

        /// <summary>
        ///     Adds a chunk to the blob
        /// </summary>
        /// <param name="chunkType">Chunk type</param>
        /// <param name="userData">Chunk user data</param>
        /// <param name="data">Stream to add</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>StorageChunk of the added chunk</returns>
        public Task<StorageChunk> AddChunk(int chunkType, uint userData, Stream data, CancellationToken token)
        {
            return Task.Run(async () =>
            {
                var l = data.Length - data.Position;

                if (l > uint.MaxValue)
                    throw new InvalidDataException("Chunk length greater than uint.MaxValue");

                var size = (uint)l;

                StorageChunk chunk;

                using (var f = await Open(token))
                using (var w = new BinaryWriter(f, Encoding.UTF8))
                {
                    using (Lock(ConcurrencyHandler.Timeout, token))
                    {
                        var info = ReadInfo();

                        // Check for exact size free chunk
                        var free = info.Chunks.FirstOrDefault(fc => !fc.Changing && fc.Size == size && fc.Type == ChunkTypes.Free);
                        if (free.Type != ChunkTypes.Free)
                            // Check for free chunk bigger than required
                            free = info.Chunks.FirstOrDefault(fc => !fc.Changing && fc.Size > size + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize && fc.Type == ChunkTypes.Free);

                        if (free.Type == ChunkTypes.Free)
                        {
                            // if free space found in blob
                            if (free.Size == size)
                            {
                                // if chunk size equals with the free space size, replace free space with chunk
                                chunk = new StorageChunk(free.Id, userData, chunkType, free.Position,
                                        size, DateTime.UtcNow)
                                    { Changing = true };
                                info.ReplaceChunk(free.Id, chunk);
                            }
                            else
                            {
                                // chunk size < free space size, remove chunk sized portion of the free space
                                free = new StorageChunk(free.Id, 0, ChunkTypes.Free, free.Position + size + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize, free.Size - size - StorageChunk.ChunkHeaderSize - StorageChunk.ChunkFooterSize, DateTime.UtcNow);
                                info.UpdateChunk(free);
                                chunk = new StorageChunk(GetId(info.Chunks), userData, chunkType, free.Position, size, DateTime.UtcNow) { Changing = true };
                                info.AddChunk(chunk);

                                // write out new free chunk header
                                f.Position = free.Position;
                                free.ToStorage(w);

#if InvalidChunkDebug
                                if (free.Size > 0)
                                {
                                    f.Position = free.Position + StorageChunk.ChunkHeaderSize + free.Size - 1;
                                    f.WriteByte(83); // 'S' character
                                }
#endif

                                f.Flush();
                            }
                        }
                        else
                        {
                            // no space found, add chunk at the end of the file
                            var last = info.Chunks.OrderByDescending(ch => ch.Position).FirstOrDefault();
                            var position = last.Position == 0 ? HeaderSize : last.Position + last.Size + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize;
                            chunk = new StorageChunk(GetId(info.Chunks), userData, chunkType, position, size, DateTime.UtcNow) { Changing = true };
                            info.AddChunk(chunk);
                            f.SetLength(position + StorageChunk.ChunkHeaderSize + size + StorageChunk.ChunkFooterSize);
                        }

                        WriteInfo(info);

                        // write chunk data to blob with FREE chunk type
                        f.Position = chunk.Position;
                        chunk.ToStorage(w, true);
                        f.Flush();
                    }

                    var ok = false;

                    try
                    {
                        // write chunk data to stream
                        var buffer = new byte[81920];
                        long remaining = size;
                        ushort crc = 0;
                        while (remaining > 0)
                        {
                            var bytesRead = await data.ReadAsync(buffer, 0, (int)Math.Min(remaining, buffer.Length), token).ConfigureAwait(false);
                            crc = Crc16.ComputeChecksum(buffer, bytesRead, crc);
                            if (bytesRead != 0)
                                await f.WriteAsync(buffer, 0, bytesRead, token).ConfigureAwait(false);
                            else
                                break;
                            remaining -= bytesRead;
                        }
                        w.Write(crc);
                        f.Flush();

                        // write correct chunk type
                        f.Position = chunk.Position;
                        chunk.ToStorage(w);
                        f.Flush();

                        ok = true;
                    }
                    finally
                    {
                        using (Lock(ConcurrencyHandler.Timeout, CancellationToken.None))
                        {
                            var info = ReadInfo();

                            // Exception occured, chunk should stay free
                            if (!ok)
                                chunk = info.GetChunkById(chunk.Id);

                            chunk.Changing = false;
                            info.UpdateChunk(chunk);

                            info.AddedVersion++;
                            WriteInfo(info);
                        }
                    }
                }
                return chunk;
            }, token);
        }

        /// <summary>
        ///     Cuts the excess free space from the end of the storage
        /// </summary>
        /// <returns>Task</returns>
        public Task CutBackPadding(CancellationToken token)
        {
            return Task.Run(async () =>
            {
                using (var f = await Open(token))
                {
                    using (Lock(ConcurrencyHandler.Timeout, token))
                    {
                        var info = ReadInfo();

                        var position = f.Length;
                        while (info.Chunks.Count > 0)
                        {
                            var chunk = info.Chunks.Last();

                            if (chunk.Type != ChunkTypes.Free || chunk.Changing)
                                break;

                            info.RemoveChunk(chunk);
                            position = chunk.Position;
                        }

                        if (position == f.Length)
                            return;

                        f.SetLength(position);
                        WriteInfo(info);
                    }
                }
            }, token);
        }

        /// <summary>
        ///     Disposes the storage
        /// </summary>
        public void Dispose()
        {
            _mainLock?.Close();
            _mainLock = null;
            ConcurrencyHandler?.Dispose();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///     Gets the free chunk sizes
        /// </summary>
        /// <returns>Free chunk sizes in the blob</returns>
        public async Task<IReadOnlyList<uint>> GetFreeChunkSizes(CancellationToken token)
        {
            var chunks = await GetChunks(token);
            return chunks.Where(c => c.Type == ChunkTypes.Free).Select(c => c.Size).ToList();
        }

        /// <summary>
        ///     Initialize the storage
        /// </summary>
        /// <returns>True if initialization successful, otherwise false</returns>
        public async Task<bool> Initialize<T>(CancellationToken token)
            where T : ConcurrencyHandler, new()
        {
            try
            {
                try
                {
                    if (ConcurrencyHandler == null)
                        ConcurrencyHandler = new T();

                    _mainLock?.Close();
                    _mainLock = null;

                    Info.Refresh();
                    if (!Info.Exists)
                        CreateEmptyBlobStorage();

                    CheckBlobStorageHeader();

                    await CheckInitialization(token);
                    IsInitialized = true;
                }
                catch when (CloseLock())
                {
                }
            }
            catch (NotSupportedException)
            {
                return false;
            }
            catch (TimeoutException)
            {
                return false;
            }
            catch (OperationCanceledException)
            {
                return false;
            }

            return true;
        }

        /// <summary>
        ///     Reads chunks from the blob
        /// </summary>
        /// <param name="selector">
        ///     Selector to choose which chunks to read, input: available storage info, output:
        ///     chunks to read
        /// </param>
        /// <param name="streamCreator">
        ///     Stream creator to create the output streams for the data, input: chunk read, output: stream
        ///     to use
        /// </param>
        /// <param name="token">Cancellation token</param>
        /// <returns>List of chunk, Stream pairs in the order of the selector's result</returns>
        /// <remarks>
        ///     Chunks written to streams in the order of the selector's result, if multiple chunks are using the same stream
        ///     the data is written in the selector's result order
        /// </remarks>
        public async Task<IReadOnlyList<(StorageChunk Chunk, Stream Data)>> ReadChunks(Func<StorageInfo, IEnumerable<StorageChunk>> selector, Func<StorageChunk, Stream> streamCreator, CancellationToken token)
        {
            var result = new List<(StorageChunk, Stream)>();
            await ReadChunksInternal(selector, streamCreator, (c, d, s) => result.Add((c, s)), token);
            return result;
        }

        /// <summary>
        ///     Reads chunks from the blob
        /// </summary>
        /// <param name="selector">
        ///     Selector to select which chunks need to be read to which stream, input: available storage info, output:
        ///     chunks to read with streams to read to
        /// </param>
        /// <param name="token">Cancellation token</param>
        /// <returns>List of chunk, Stream pairs in the order of the selector's result</returns>
        /// <remarks>
        ///     Chunks written to streams in the order of the selector's result, if multiple chunks are using the same stream
        ///     the data is written in the selector's result order
        /// </remarks>
        public async Task<IReadOnlyList<(StorageChunk Chunk, Stream Data)>> ReadChunks(Func<StorageInfo, IEnumerable<(StorageChunk, Stream)>> selector, CancellationToken token)
        {
            var result = new List<(StorageChunk, Stream)>();
            var streamList = new Dictionary<StorageChunk, Stream>();
            await ReadChunksInternal(sc =>
            {
                var list = selector.Invoke(sc).ToList();
                foreach (var c in list)
                    streamList[c.Item1] = c.Item2;
                return list.Select(p => p.Item1);
            }, c => streamList[c], (c, d, s) => result.Add((c, s)), token);
            return result;
        }

        /// <summary>
        ///     Reads chunks from the blob
        /// </summary>
        /// <param name="condition">
        ///     Condition to choose which chunks to read
        /// </param>
        /// <param name="token">Cancellation token</param>
        /// <param name="streamCreator">
        ///     Stream creator to create the output streams for the data, input: chunk read, output: stream to use
        /// </param>
        /// <returns>List of chunk, Stream pairs</returns>
        public async Task<IReadOnlyList<(StorageChunk Chunk, Stream Data)>> ReadChunks(Func<StorageChunk, bool> condition, Func<StorageChunk, Stream> streamCreator, CancellationToken token)
        {
            var result = new List<(StorageChunk, Stream)>();
            await ReadChunksInternal(sc => sc.Chunks.Where(condition), streamCreator, (c, d, s) => result.Add((c, s)), token);
            return result;
        }

        /// <summary>
        ///     Reads chunks from the blob
        /// </summary>
        /// <param name="selector">
        ///     Selector to select which chunks need to be read to which stream, input: chunk read, output: stream to use (null
        ///     stream indicates not to read)
        /// </param>
        /// <param name="token">Cancellation token</param>
        /// <returns>List of chunk, Stream pairs</returns>
        public async Task<IReadOnlyList<(StorageChunk Chunk, Stream Data)>> ReadChunks(Func<StorageChunk, Stream> selector, CancellationToken token)
        {
            var result = new List<(StorageChunk, Stream)>();
            var streamList = new Dictionary<StorageChunk, Stream>();
            await ReadChunksInternal(sc =>
            {
                var list = new List<StorageChunk>();
                foreach (var c in sc.Chunks)
                {
                    var stream = selector.Invoke(c);
                    if (stream != null)
                    {
                        list.Add(c);
                        streamList[c] = stream;
                    }
                }
                return list;
            }, c => streamList[c], (c, d, s) => result.Add((c, s)), token);
            return result;
        }

        /// <summary>
        ///     Reads chunks from the blob
        /// </summary>
        /// <param name="selector">
        ///     Selector to choose which chunks to read, input: storage info, chunk data version, output:
        ///     chunks to read
        /// </param>
        /// <param name="token">Cancellation token</param>
        /// <returns>List of chunk, byte[] pairs in the order of the selector's result</returns>
        public async Task<IReadOnlyList<(StorageChunk Chunk, byte[] Data)>> ReadChunks(Func<StorageInfo, IEnumerable<StorageChunk>> selector, CancellationToken token)
        {
            var result = new List<(StorageChunk, byte[])>();
            await ReadChunksInternal(selector, null, (c, d, s) => result.Add((c, d)), token);
            return result;
        }

        /// <summary>
        ///     Reads chunks from the blob
        /// </summary>
        /// <param name="selector">
        ///     Selector to choose which chunks to read, input: storage info, chunk data version, output:
        ///     chunks to read
        /// </param>
        /// <param name="processor">Chunk data processor, called in the order of selector's result</param>
        /// <param name="token">Cancellation token</param>
        public async Task ReadChunks(Func<StorageInfo, IEnumerable<StorageChunk>> selector, Action<StorageChunk, byte[]> processor, CancellationToken token)
        {
            if (processor == null)
                throw new ArgumentNullException(nameof(processor));

            await ReadChunksInternal(selector, null, (c, d, s) => processor(c, d), token);
        }

        /// <summary>
        ///     Reads chunks from the blob
        /// </summary>
        /// <param name="condition">
        ///     Condition to choose which chunks to read
        /// </param>
        /// <param name="token">Cancellation token</param>
        /// <returns>List of chunk, byte[] pairs</returns>
        public async Task<IReadOnlyList<(StorageChunk Chunk, byte[] Data)>> ReadChunks(Func<StorageChunk, bool> condition, CancellationToken token)
        {
            var result = new List<(StorageChunk, byte[])>();
            await ReadChunksInternal(sc => sc.Chunks.Where(condition), null, (c, d, s) => result.Add((c, d)), token);
            return result;
        }

        /// <summary>
        ///     Removes a chunk from the blob
        /// </summary>
        /// <param name="selector">
        ///     Selector to choose which chunk to remove, input: storage info, output:
        ///     chunk to remove (null to cancel)
        /// </param>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if a chunk removed</returns>
        public Task<bool> RemoveChunk(Func<StorageInfo, StorageChunk?> selector, CancellationToken token)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return Task.Run(async () =>
            {
                var wait = false;

                while (true)
                {
                    // If there are readers for the selected chunk wait here while a read is finished and try again
                    if (wait)
                        ConcurrencyHandler.WaitForReadFinish(token);

                    using (var f = await Open(token))
                    using (var w = new BinaryWriter(f, Encoding.UTF8))
                    using (Lock(ConcurrencyHandler.Timeout, token))
                    {
                        var info = ReadInfo();

                        // Get the chunk to delete
                        var item = selector.Invoke(info.FilterChunks(c => c.Type != ChunkTypes.Free && !c.Changing));
                        if (item == null || item.Value == default(StorageChunk))
                            return false;

                        var chunk = item.Value;

                        // if there are readers for the chunk, wait for finish and try again
                        if (chunk.ReadCount > 0)
                        {
                            wait = true;
                            ConcurrencyHandler.SignalWaitRequired();
                            continue;
                        }

                        var freeSize = chunk.Size;
                        var freePosition = chunk.Position;

                        // Check next chunk is free, combine free space
                        var nextPos = chunk.Position + StorageChunk.ChunkHeaderSize + chunk.Size + StorageChunk.ChunkFooterSize;
                        var nextChunk = info.Chunks.FirstOrDefault(c => c.Position == nextPos);

                        if (nextChunk.Type == ChunkTypes.Free && !nextChunk.Changing)
                        {
                            freeSize += StorageChunk.ChunkHeaderSize + nextChunk.Size + StorageChunk.ChunkFooterSize;
                            info.RemoveChunk(nextChunk);
                        }

                        // Check previous chunk is free, combine free space
                        var pos = chunk.Position;
                        var previousChunk = info.Chunks.FirstOrDefault(c => c.Position + c.Size + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize == pos);

                        if (previousChunk.Type == ChunkTypes.Free && !previousChunk.Changing)
                        {
                            freeSize += StorageChunk.ChunkHeaderSize + previousChunk.Size + StorageChunk.ChunkFooterSize;
                            freePosition = previousChunk.Position;
                            info.RemoveChunk(previousChunk);
                        }

                        // Mark the chunk free
                        f.Position = chunk.Position;
                        chunk.ToStorage(w);

#if InvalidChunkDebug
                        if (chunk.Size > 0)
                        {
                            f.Position = chunk.Position + StorageChunk.ChunkHeaderSize + chunk.Size - 1;
                            f.WriteByte(82); // 'R' character
                        }
#endif

                        f.Flush();

                        // Mark the chunk changing while updating the file
                        chunk = new StorageChunk(chunk.Id, 0, ChunkTypes.Free, freePosition, freeSize, DateTime.UtcNow);
                        info.UpdateChunk(chunk);

                        info.RemovedVersion++;
                        WriteInfo(info);
                    }

                    break;
                }

                return true;
            }, token);
        }

        public Task<StorageStatistics> Statistics(CancellationToken token)
        {
            return Task.Run(async () =>
            {
                var chunks = await GetChunks(token);
                Info.Refresh();

                var used = chunks.Where(c => c.Type != ChunkTypes.Free).ToList();
                var free = chunks.Where(c => c.Type == ChunkTypes.Free).ToList();

                return new StorageStatistics
                {
                    Overhead = chunks.Count * (StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize) + HeaderSize,
                    UsedChunks = used.Count,
                    UsedSpace = used.Sum(c => c.Size),
                    FreeChunks = free.Count,
                    FreeSpace = free.Sum(c => c.Size),
                    FileSize = Info.Length
                };
            }, token);
        }

        /// <summary>
        ///     Gets the chunks in the storage
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>List of chunks in the storage</returns>
        /// <remarks>Information purposes only, chunks can change in another thread / process</remarks>
        internal Task<IReadOnlyList<StorageChunk>> GetChunks(CancellationToken token)
        {
            return Task.Run(() =>
            {
                using (Lock(ConcurrencyHandler.Timeout, token))
                {
                    return ReadInfo().Chunks;
                }
            }, token);
        }

        private void CheckBlobStorageHeader()
        {
            _mainLock?.Close();
            _mainLock = OpenFile();

            if (_mainLock.Length < HeaderSize)
                throw new NotSupportedException("Unknown file format (file too short)");

            using (var r = new BinaryReader(_mainLock, Encoding.UTF8, true))
            {
                var blob = r.ReadInt32();
                if (blob != ChunkTypes.Blob)
                    throw new NotSupportedException("Unknown file format");
                var version = r.ReadInt32();
                if (version > LastVersion)
                    throw new NotSupportedException("Unknown file version");
                Id = new Guid(r.ReadBytes(16));
                ConcurrencyHandler.Id = Id;
            }
        }

        private Task CheckInitialization(CancellationToken token)
        {
            return Task.Run(() =>
            {
                using (Lock(ConcurrencyHandler.Timeout, token))
                {
                    var info = ReadInfo();

                    if (info.Initialized)
                        return;

                    using (var f = OpenFile())
                    using (var br = new BinaryReader(f, Encoding.UTF8))
                    {
                        f.Position = HeaderSize;
                        var position = f.Position;
                        while (f.Position != f.Length)
                            try
                            {
                                token.ThrowIfCancellationRequested();
                                position = f.Position;
                                info.AddChunk(StorageChunk.FromStorage(br, true));
                            }
                            catch (InvalidDataException) when (TruncateOnChunkInitializationError)
                            {
                                f.SetLength(position);
                            }
                    }

                    info.Initialized = true;
                    WriteInfo(info);
                }

                FreshlyInitialized = true;
            }, token);
        }

        private bool CloseLock()
        {
            _mainLock?.Close();
            _mainLock = null;
            return false;
        }

        private void CreateEmptyBlobStorage()
        {
            using (var f = Info.Create())
            using (var w = new BinaryWriter(f, Encoding.UTF8))
            {
                w.Write(ChunkTypes.Blob);
                w.Write(1);
                w.Write(Guid.NewGuid().ToByteArray());
                w.Flush();
            }
        }

        private uint GetId(IReadOnlyList<StorageChunk> chunks)
        {
            var id = 1u;
            foreach (var c in chunks.OrderBy(c => c.Id))
                if (c.Id > id)
                    break;
                else
                    id++;

            return id;
        }

        private IDisposable Lock(int timeout, CancellationToken token)
        {
            return ConcurrencyHandler.Lock(timeout, token);
        }

        private async Task<FileStream> Open(CancellationToken token)
        {
            Info.Refresh();
            if (!Info.Exists)
                await Initialize<SessionConcurrencyHandler>(token);
            return OpenFile();
        }

        private FileStream OpenFile()
        {
            return new FileStream(Info.FullName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        }

        private async Task<(StorageChunk, byte[], Stream)> ReadChunk(StorageChunk chunk, Stream target, CancellationToken token)
        {
            using (var f = await Open(token))
            using (var br = new BinaryReader(f, Encoding.UTF8, true))
            {
                ushort crc = 0;
                var res = new byte[target == null ? chunk.Size : Math.Min(chunk.Size, 64 * 1024)];
                f.Position = chunk.Position;

                var diskChunk = StorageChunk.FromStorage(br, false);
                if (diskChunk.Position != chunk.Position || diskChunk.Id != chunk.Id || diskChunk.Added != chunk.Added || diskChunk.Size != chunk.Size || diskChunk.Type != chunk.Type || diskChunk.UserData != chunk.UserData)
                    throw new InvalidDataException("Blob and concurrency data mismatch");

                var position = 0;
                var read = 1;

                while (position < chunk.Size && read != 0)
                {
                    read = await f.ReadAsync(res, 0, (int)Math.Min(res.Length, chunk.Size - position), token);
                    position += read;
                    crc = Crc16.ComputeChecksum(res, read, crc);

                    if (target != null)
                        await target.WriteAsync(res, 0, read, token);
                }

                var diskCrc = br.ReadUInt16();

                if (diskCrc != crc)
                    throw new InvalidDataException("Invalid data read from blob storage");

                return (chunk, res, target);
            }
        }

        private Task ReadChunksInternal(Func<StorageInfo, IEnumerable<StorageChunk>> selector, Func<StorageChunk, Stream> streamCreator, Action<StorageChunk, byte[], Stream> resultProcessor, CancellationToken token)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return Task.Run(async () =>
            {
                List<StorageChunk> chunksToRead;

                using (Lock(ConcurrencyHandler.Timeout, token))
                {
                    var info = ReadInfo();

                    chunksToRead = selector.Invoke(info.FilterChunks(c => !c.Changing && c.Type != ChunkTypes.Free))?.ToList();
                    if (chunksToRead == null)
                        return;

                    foreach (var r in chunksToRead)
                    {
                        var chunk = r;
                        chunk.ReadCount++;
                        info.UpdateChunk(chunk);
                    }

                    WriteInfo(info);
                }

                var finishedOne = false;
                try
                {
                    foreach (var r in chunksToRead)
                    {
                        token.ThrowIfCancellationRequested();

                        Stream stream = null;

                        if (streamCreator != null)
                            stream = streamCreator.Invoke(r);

                        var res = await ReadChunk(r, stream, token);
                        resultProcessor(res.Item1, res.Item2, res.Item3);
                    }
                }
                finally
                {
                    using (Lock(ConcurrencyHandler.Timeout, CancellationToken.None))
                    {
                        var info = ReadInfo();

                        foreach (var r in chunksToRead)
                        {
                            var chunk = r;
                            chunk.ReadCount--;
                            info.UpdateChunk(chunk);
                            finishedOne = finishedOne || chunk.ReadCount == 0;
                        }

                        WriteInfo(info);
                    }
                }

                if (finishedOne)
                    ConcurrencyHandler.SignalReadFinish();
            }, token);
        }

        private StorageInfo ReadInfo()
        {
            return ConcurrencyHandler.ReadInfo();
        }

        private void WriteInfo(StorageInfo info)
        {
            ConcurrencyHandler.WriteInfo(info);
        }
    }
}