//#define DebugLogging

namespace BlobCache
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using ConcurrencyModes;
    using JetBrains.Annotations;

    /// <inheritdoc />
    /// <summary>
    ///     Blob storage class, storage data chunks in a blob
    /// </summary>
    public class BlobStorage : IDisposable
    {
        /// <summary>
        ///     Blob header size
        /// </summary>
        internal const int HeaderSize = 24;

        /// <summary>
        ///     Header version number
        /// </summary>
        private const int LastVersion = 1;

        private FileStream _mainLock;

        /// <summary>
        ///     Initializes a new instance of the <see cref="BlobStorage" /> class
        /// </summary>
        /// <param name="fileName">Blob storage file name</param>
        public BlobStorage(string fileName)
        {
            Info = new FileInfo(fileName);
        }

        /// <summary>
        ///     Frees allocated resources for the blob storage
        /// </summary>
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
        ///     Gets or sets the task scheduler to use for scheduling tasks
        /// </summary>
        [PublicAPI]
        public TaskScheduler Scheduler { get; set; } = TaskScheduler.Default;

        /// <summary>
        ///     Gets or sets a value indicating whether initialization should fail or blob storage should be truncated if a chunk
        ///     loading fail at initialization
        /// </summary>
        [PublicAPI]
        public bool TruncateOnChunkInitializationError { get; set; }

        /// <summary>
        ///     Gets or sets the concurrency handler to use
        /// </summary>
        private ConcurrencyHandler ConcurrencyHandler { get; set; }

        /// <summary>
        ///     Gets or sets the blob storage id
        /// </summary>
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
            return Task.Factory.StartNew(async () =>
            {
                var l = data.Length - data.Position;

                if (l > uint.MaxValue)
                    throw new InvalidDataException("Chunk length greater than uint.MaxValue");

                var size = (uint)l;

                var chunk = default(StorageChunk);

                using (var f = await Open(token))
                {
                    long lockSize = 0;
                    var ff = f;

                    await Lock(ConcurrencyHandler.Timeout, token, false, () =>
                    {
                        var info = ReadInfo();

                        CheckInitialized(info);

                        // Check for exact size free chunk
                        var free = info.Chunks.FirstOrDefault(fc => !fc.Changing && fc.Size == size && fc.Type == ChunkTypes.Free);
                        if (free.Type != ChunkTypes.Free)
                            // Check for free chunk bigger than required
                            free = info.Chunks.FirstOrDefault(fc => !fc.Changing && fc.Size > size + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize && fc.Type == ChunkTypes.Free);
                        StorageChunk? newFree = null;

                        if (free.Type == ChunkTypes.Free)
                        {
                            // if free space found in blob
                            if (free.Size == size)
                            {
                                // if chunk size equals with the free space size, replace free space with chunk
                                chunk = new StorageChunk(free.Id, userData, chunkType, free.Position, size, DateTime.UtcNow) { Changing = true };
                                info.ReplaceChunk(free.Id, chunk);
                                lockSize = size + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize;
                                Log($"Replacing free chunk {free} with chunk {chunk}");
                            }
                            else
                            {
                                // chunk size < free space size, remove chunk sized portion of the free space
                                newFree = new StorageChunk(free.Id, 0, ChunkTypes.Free, free.Position + size + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize, free.Size - size - StorageChunk.ChunkHeaderSize - StorageChunk.ChunkFooterSize, DateTime.UtcNow);
                                info.UpdateChunk(newFree.Value);
                                chunk = new StorageChunk(GetId(info.Chunks), userData, chunkType, free.Position, size, DateTime.UtcNow) { Changing = true };
                                info.AddChunk(chunk);
                                lockSize = free.Size + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize;
                                Log($"Split free chunk {free} to chunk {chunk} and free {newFree}");
                            }
                        }
                        else
                        {
                            // no space found, add chunk at the end of the file
                            var last = info.Chunks.OrderByDescending(ch => ch.Position).FirstOrDefault();
                            var position = last.Position == 0 ? HeaderSize : last.Position + last.Size + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize;
                            chunk = new StorageChunk(GetId(info.Chunks), userData, chunkType, position, size, DateTime.UtcNow) { Changing = true };
                            info.AddChunk(chunk);
                            lockSize = chunk.Size + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize;
                            Log($"Add chunk {chunk}");
                        }

                        using (var fr = ff.Range(chunk.Position, lockSize, LockMode.Exclusive))
                        using (var w = new BinaryWriter(fr, Encoding.UTF8))
                        {
                            if (fr.Stream.Length < chunk.Position + lockSize)
                                fr.Stream.SetLength(chunk.Position + lockSize);

                            if (newFree.HasValue)
                            {
                                // write out new free chunk header
                                fr.Position = newFree.Value.Position - chunk.Position;
                                newFree.Value.ToStorage(w);
                                fr.Flush();
                            }

                            // write chunk data to blob with FREE chunk type
                            fr.Position = 0;
                            chunk.ToStorage(w, true);
                            fr.Flush();
                        }

                        WriteInfo(info, true);
                    });

                    var ok = false;

                    try
                    {
                        using (var fr = f.Range(chunk.Position, lockSize, LockMode.Exclusive))
                        using (var w = new BinaryWriter(fr, Encoding.UTF8))
                        {
                            fr.Position = StorageChunk.ChunkHeaderSize;

                            // write chunk data to stream
                            var buffer = new byte[81920];
                            long remaining = size;
                            ushort crc = 0;
                            while (remaining > 0)
                            {
                                var bytesRead = await data.ReadAsync(buffer, 0, (int)Math.Min(remaining, buffer.Length), token).ConfigureAwait(false);
                                crc = Crc16.ComputeChecksum(buffer, 0, bytesRead, crc);
                                if (bytesRead != 0)
                                    await fr.WriteAsync(buffer, 0, bytesRead, token).ConfigureAwait(false);
                                else
                                    break;
                                remaining -= bytesRead;
                            }
                            w.Write(crc);
                            fr.Flush();

                            // write correct chunk type
                            fr.Position = 0;
                            chunk.ToStorage(w);
                            fr.Flush();

                            ok = true;
                        }
                    }
                    finally
                    {
                        await Lock(ConcurrencyHandler.Timeout * 4, CancellationToken.None, true, () =>
                        {
                            var info = ReadInfo();

                            CheckInitialized(info);

                            // Exception occured, chunk should stay free
                            if (!ok)
                                chunk = info.GetChunkById(chunk.Id);

                            chunk.Changing = false;
                            info.UpdateChunk(chunk);

                            info.AddedVersion++;
                            WriteInfo(info, true);
                        });
                    }
                }
                return chunk;
            }, token, TaskCreationOptions.DenyChildAttach, Scheduler).Unwrap();
        }

        /// <summary>
        ///     Cuts the excess free space from the end of the storage
        /// </summary>
        /// <returns>Task</returns>
        public Task CutBackPadding(CancellationToken token)
        {
            return Task.Factory.StartNew(async () =>
            {
                using (var f = await Open(token))
                {
                    var ff = f;
                    await Lock(ConcurrencyHandler.Timeout, token, false, () =>
                    {
                        var info = ReadInfo();

                        CheckInitialized(info);

                        var position = ff.Length;
                        while (info.Chunks.Count > 0)
                        {
                            var chunk = info.Chunks.Last();

                            if (chunk.Type != ChunkTypes.Free || chunk.Changing)
                                break;

                            info.RemoveChunk(chunk);
                            position = chunk.Position;
                        }

                        if (position == ff.Length)
                            return;

                        using (ff.Lock(position, ff.Length - position, LockMode.Exclusive))
                        {
                            ff.SetLength(position);
                        }
                        WriteInfo(info, true);
                    });
                }
            }, token, TaskCreationOptions.DenyChildAttach, Scheduler).Unwrap();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Log("Dispose");
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

                    Log("Initialize");

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

            return Task.Factory.StartNew(async () =>
            {
                var wait = false;

                while (true)
                {
                    // If there are readers for the selected chunk wait here while a read is finished and try again
                    if (wait)
                        ConcurrencyHandler.WaitForReadFinish(token);

                    using (var f = await Open(token))
                    {
                        var ff = f;

                        var res = await Lock(ConcurrencyHandler.Timeout, token, false, () =>
                        {
                            var info = ReadInfo();

                            CheckInitialized(info);

                            // Get the chunk to delete
                            var item = selector.Invoke(info.StableChunks());
                            if (item == null || item.Value == default(StorageChunk))
                                return 0;

                            var chunk = item.Value;

                            // if there are readers for the chunk, wait for finish and try again
                            if (chunk.ReadCount > 0)
                            {
                                wait = true;
                                ConcurrencyHandler.SignalWaitRequired();
                                return -1;
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

                            // Mark the chunk changing while updating the file
                            var freeChunk = new StorageChunk(chunk.Id, 0, ChunkTypes.Free, freePosition, freeSize, DateTime.UtcNow);
                            info.UpdateChunk(freeChunk);

                            using (var fr = ff.Range(freeChunk.Position, freeSize + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize, LockMode.Exclusive))
                            using (var w = new BinaryWriter(fr, Encoding.UTF8))
                            {
                                // Mark the chunk free
                                freeChunk.ToStorage(w);
                                fr.Flush();
                            }

                            Log($"Remove chunk {chunk} (previous: {previousChunk}, next: {nextChunk}), result: {freeChunk}");

                            info.RemovedVersion++;
                            WriteInfo(info, true);

                            return 1;
                        });

                        if (res == -1)
                            continue;

                        return res == 1;
                    }
                }
            }, token, TaskCreationOptions.DenyChildAttach, Scheduler).Unwrap();
        }

        /// <summary>
        ///     Gathers statistics about the storage
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Blob statistics</returns>
        public Task<StorageStatistics> Statistics(CancellationToken token)
        {
            return Task.Factory.StartNew(async () =>
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
            }, token, TaskCreationOptions.DenyChildAttach, Scheduler).Unwrap();
        }

        /// <summary>
        ///     Gets the chunks in the storage
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>List of chunks in the storage</returns>
        /// <remarks>Information purposes only, chunks can change in another thread / process</remarks>
        internal Task<IReadOnlyList<StorageChunk>> GetChunks(CancellationToken token)
        {
            return Task.Factory.StartNew(async () =>
            {
                return await Lock(ConcurrencyHandler.Timeout, token, false, () =>
                {
                    var info = ReadInfo();
                    CheckInitialized(info);
                    return info.Chunks;
                });
            }, token, TaskCreationOptions.DenyChildAttach, Scheduler).Unwrap();
        }

        /// <summary>
        ///     Validates the header of a blob storage file
        /// </summary>
        private void CheckBlobStorageHeader()
        {
            _mainLock?.Close();
            _mainLock = OpenFile();

            if (_mainLock.Length < HeaderSize)
                throw new NotSupportedException("Unknown file format (file too short)");

            using (var fr = _mainLock.Range(0, HeaderSize, LockMode.Shared))
            using (var r = new BinaryReader(fr, Encoding.UTF8))
            {
                var blob = r.ReadInt32();
                if (blob != ChunkTypes.Blob)
                    throw new NotSupportedException("Unknown file format");
                var version = r.ReadInt32();
                if (version > LastVersion)
                    throw new NotSupportedException("Unknown file version");
                Id = new Guid(r.ReadBytes(16));
                ConcurrencyHandler.SetId(Id);
            }
        }

        /// <summary>
        ///     Checks whether the storage info belonging to the blob storage is initialized and initializes it if it is not
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private Task CheckInitialization(CancellationToken token)
        {
            return Task.Factory.StartNew(async () =>
            {
                await Lock(ConcurrencyHandler.Timeout, token, false, () =>
                {
                    var info = ReadInfo();

                    if (info.Initialized)
                        return;

                    Log("Initialize info");

                    using (var f = OpenFile())
                    using (var fr = f.Range(0, f.Length, LockMode.Shared))
                    using (var br = new BinaryReader(fr, Encoding.UTF8))
                    {
                        fr.Position = HeaderSize;
                        var position = fr.Position;
                        while (fr.Position != fr.Length)
                            try
                            {
                                token.ThrowIfCancellationRequested();
                                position = fr.Position;
                                info.AddChunk(StorageChunk.FromStorage(br, true, fr.OriginalStreamPosition));
                            }
                            catch (InvalidDataException) when (TruncateOnChunkInitializationError)
                            {
                                f.SetLength(fr.Start + position);
                                break;
                            }
                    }

                    info.Initialized = true;
                    WriteInfo(info, true);
                });

                FreshlyInitialized = true;
            }, token, TaskCreationOptions.DenyChildAttach, Scheduler).Unwrap();
        }

        // ReSharper disable once UnusedParameter.Local
        /// <summary>
        ///     Checks whether the storage info is initialized
        /// </summary>
        /// <param name="info">Storage info to check</param>
        /// <exception cref="InvalidOperationException">Storage not initialized</exception>
        // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Local
        private static void CheckInitialized(StorageInfo info)
        {
            if (!info.Initialized)
                throw new InvalidOperationException("Storage not initialized!");
        }

        /// <summary>
        ///     Closes the lock on the storage file
        /// </summary>
        /// <returns>False</returns>
        private bool CloseLock()
        {
            _mainLock?.Close();
            _mainLock = null;
            return false;
        }

        /// <summary>
        ///     Creates a new empty blob storage file
        /// </summary>
        private void CreateEmptyBlobStorage()
        {
            using (var f = Info.Create())
            using (var fr = f.Range(0, HeaderSize, LockMode.Exclusive))
            using (var w = new BinaryWriter(fr, Encoding.UTF8))
            {
                Log("New file created");
                w.Write(ChunkTypes.Blob);
                w.Write(1);
                w.Write(Guid.NewGuid().ToByteArray());
                w.Flush();
            }
        }

        /// <summary>
        ///     Gets the next unused id by chunks for a new chunk
        /// </summary>
        /// <param name="chunks">List of chunks to check</param>
        /// <returns>Free id</returns>
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

        /// <summary>
        ///     Locks the blob storage info
        /// </summary>
        /// <param name="timeout">Timeout</param>
        /// <param name="token">Cancellation token</param>
        /// <param name="priority">Indicates whether this lock should have priority over other locks</param>
        /// <param name="actionToRun">Action to run while locking</param>
        private async Task Lock(int timeout, CancellationToken token, bool priority, Action actionToRun)
        {
            await Lock(timeout, token, priority, () => { actionToRun(); return 0; });
        }

        /// <summary>
        ///     Locks the blob storage info
        /// </summary>
        /// <param name="timeout">Timeout</param>
        /// <param name="token">Cancellation token</param>
        /// <param name="priority">Indicates whether this lock should have priority over other locks</param>
        /// <param name="funcToRun">Action to run while locking</param>
        private async Task<T> Lock<T>(int timeout, CancellationToken token, bool priority, Func<T> funcToRun)
        {
            IDisposable res = null;
            Log("Waiting for lock");
            try
            {
                res = await ConcurrencyHandler.Lock(timeout, token, priority);
            }
            catch (TimeoutException) when (((Func<bool>)(() =>
            {
                Log("Lock timeout");
                return false;
            })).Invoke())
            { }

            Log("Lock entered");
            using (res)
                return funcToRun();
        }

        /// <summary>
        ///     Logs debug info
        /// </summary>
        /// <param name="log">Info to log</param>
        [Conditional("DebugLogging")]
        private void Log(string log)
        {
            Debug.WriteLine($"BlobStorage: {Id}-{GetHashCode():x8}-{Thread.CurrentThread.ManagedThreadId} {log}");
        }

        /// <summary>
        ///     Opens the blob storege file stream
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Opened file stream</returns>
        /// <remarks>Try to reinitialize storage if storage file not exists</remarks>
        private async Task<FileStream> Open(CancellationToken token)
        {
            Info.Refresh();
            if (!Info.Exists)
                await Initialize<SessionConcurrencyHandler>(token);
            return OpenFile();
        }

        /// <summary>
        ///     Opens the blob storage file stream
        /// </summary>
        /// <returns>Opened file stream</returns>
        /// <remarks>
        ///     Use <see cref="Open" /> instead when not open for the <see cref="_mainLock" /> or in
        ///     <see cref="Initialize{T}" />
        /// </remarks>
        private FileStream OpenFile()
        {
            return new FileStream(Info.FullName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, 1);
        }

        /// <summary>
        ///     Reads a chunk from the storage
        /// </summary>
        /// <param name="chunk">Chunk to read</param>
        /// <param name="target">Target stream</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Data in the chunk</returns>
        /// <remarks>
        ///     If <paramref name="target" /> is given it returns null instead of the read data, because the read data went to
        ///     the <paramref name="target" />
        /// </remarks>
        private async Task<byte[]> ReadChunk(StorageChunk chunk, [CanBeNull] Stream target, CancellationToken token)
        {
            using (var f = await Open(token))
            using (var fr = f.Range(chunk.Position, chunk.Size + StorageChunk.ChunkHeaderSize + StorageChunk.ChunkFooterSize, LockMode.Shared))
            using (var br = new BinaryReader(fr, Encoding.UTF8, true))
            {
                Log($"Reading chunk: {chunk}");
                ushort crc = 0;
                var res = new byte[target == null ? chunk.Size : Math.Min(chunk.Size, 64 * 1024)];

                var diskChunk = StorageChunk.FromStorage(br, false, fr.OriginalStreamPosition);
                if (diskChunk != chunk)
                    throw new InvalidDataException("Blob and concurrency data mismatch");

                var position = 0;
                var read = 1;
                var bufferPosition = 0;

                while (position < chunk.Size && read != 0)
                {
                    read = await fr.ReadAsync(res, bufferPosition, (int)Math.Min(res.Length, chunk.Size - position), token);
                    position += read;
                    crc = Crc16.ComputeChecksum(res, bufferPosition, read, crc);

                    if (target != null)
                        await target.WriteAsync(res, 0, read, token);
                    else
                        bufferPosition += read;
                }

                var diskCrc = br.ReadUInt16();

                if (diskCrc != crc)
                    throw new InvalidDataException("Invalid data read from blob storage");

                return target == null ? res : null;
            }
        }

        /// <summary>
        ///     Reads chunks from the storage
        /// </summary>
        /// <param name="selector">Select which chunks to read</param>
        /// <param name="streamCreator">Gets the stream belonging to a read chunk if read to streams</param>
        /// <param name="resultProcessor">Activated when a chunk is read</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        private Task ReadChunksInternal([NotNull] Func<StorageInfo, IEnumerable<StorageChunk>> selector, [CanBeNull] Func<StorageChunk, Stream> streamCreator, [NotNull] Action<StorageChunk, byte[], Stream> resultProcessor, CancellationToken token)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));
            if (resultProcessor == null)
                throw new ArgumentNullException(nameof(resultProcessor));

            return Task.Factory.StartNew(async () =>
            {
                List<StorageChunk> chunksToRead = null;

                if (!await Lock(ConcurrencyHandler.Timeout, token, false, () =>
                 {
                     var info = ReadInfo();

                     CheckInitialized(info);

                     chunksToRead = selector.Invoke(info.StableChunks())?.ToList();
                     if (chunksToRead == null || chunksToRead.Count == 0)
                         return false;

                     foreach (var r in chunksToRead)
                     {
                         var chunk = r;
                         chunk.ReadCount++;
                         info.UpdateChunk(chunk);
                         Log($"Mark chunk reading: {chunk}, {chunk.ReadCount}");
                     }

                     WriteInfo(info, false);
                     return true;
                 }))
                    return;

                var finishedOne = false;
                try
                {
                    foreach (var r in chunksToRead)
                    {
                        token.ThrowIfCancellationRequested();

                        var stream = streamCreator?.Invoke(r);

                        var res = await ReadChunk(r, stream, token);
                        resultProcessor(r, res, stream);
                    }
                }
                finally
                {
                    await Lock(ConcurrencyHandler.Timeout * 4, CancellationToken.None, true, () =>
                    {
                        var info = ReadInfo();

                        CheckInitialized(info);

                        foreach (var r in chunksToRead)
                        {
                            var chunk = info.GetChunkById(r.Id);
                            chunk.ReadCount--;
                            info.UpdateChunk(chunk);
                            finishedOne = finishedOne || chunk.ReadCount == 0;
                            Log($"Unmark chunk reading: {chunk}, {chunk.ReadCount}");
                        }

                        WriteInfo(info, false);
                    });
                }

                if (finishedOne)
                    ConcurrencyHandler.SignalReadFinish();
            }, token, TaskCreationOptions.DenyChildAttach, Scheduler).Unwrap();
        }

        /// <summary>
        ///     Reads the storage info
        /// </summary>
        /// <returns>Storage info</returns>
        private StorageInfo ReadInfo()
        {
            return ConcurrencyHandler.ReadInfo();
        }

        /// <summary>
        ///     Writes the storage info
        /// </summary>
        /// <param name="info">Storage info</param>
        /// <param name="stableChunkChanged">Indicates whether stable chunk list changed</param>
        private void WriteInfo(StorageInfo info, bool stableChunkChanged)
        {
            ConcurrencyHandler.WriteInfo(info, stableChunkChanged);
        }
    }
}