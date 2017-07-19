﻿namespace BlobCache
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using ConcurrencyModes;

    public class BlobStorage : IDisposable
    {
        private const int LastVersion = 1;

        private const int HeaderSize = 24;

        private Stream _mainLock;

        public BlobStorage(string fileName, ConcurrencyHandler handler)
        {
            Info = new FileInfo(fileName);
            ConcurrencyHandler = handler;
        }

        private ConcurrencyHandler ConcurrencyHandler { get; }

        protected internal FileInfo Info { get; }
        private Guid Id { get; set; }

        public void Dispose()
        {
            _mainLock?.Close();
            _mainLock = null;
            ConcurrencyHandler?.Dispose();
            GC.SuppressFinalize(this);
        }

        public async Task<bool> Initialize()
        {
            try
            {
                _mainLock?.Close();
                _mainLock = null;

                if (!Info.Exists)
                    CreateEmptyBlobStorage();

                CheckBlobStorageHeader();

                await CheckInitialization();
            }
            catch (NotSupportedException)
            {
                return false;
            }
            catch (TimeoutException)
            {
                return false;
            }

            return true;
        }

        private Task CheckInitialization()
        {
            return Task.Run(() =>
            {
                using (WriteLock(ConcurrencyHandler.Timeout))
                {
                    var info = ReadInfo();

                    if (info.Initialized)
                        return;

                    info.Chunks = new List<StorageChunk>();
                    using (var f = Open())
                    using (var br = new BinaryReader(f, Encoding.UTF8))
                    {
                        f.Position = 24;
                        var position = f.Position;
                        while (f.Position != f.Length)
                            try
                            {
                                position = f.Position;
                                info.Chunks.Add(StorageChunk.FromStorage(br));
                            }
                            catch (InvalidDataException)
                            {
                                f.SetLength(position);
                            }
                    }

                    info.Initialized = true;
                    WriteInfo(info);
                }
            });
        }

        public async Task<StorageChunk> AddChunk(int chunkType, uint userData, byte[] data)
        {
            using (var ms = new MemoryStream(data))
            {
                return await AddChunk(chunkType, userData, ms);
            }
        }

        public Task<StorageChunk> AddChunk(int chunkType, uint userData, Stream data)
        {
            return Task.Run(async () =>
            {
                var l = data.Length;

                if (l > uint.MaxValue)
                    throw new InvalidDataException("Chunk length greater than uint.MaxValue");

                var size = (uint) l;

                StorageChunk free;
                StorageChunk chunk;

                using (var f = Open())
                using (var w = new BinaryWriter(f, Encoding.UTF8))
                {
                    using (WriteLock(ConcurrencyHandler.Timeout))
                    {
                        var info = ReadInfo();

                        // Check for exact size free chunk
                        free = info.Chunks.FirstOrDefault(
                            fc => !fc.Changing && fc.Size == size && fc.Type == ChunkTypes.Free);
                        if (free.Type != ChunkTypes.Free)
                            // Check for free chunk bigger than required
                            free = info.Chunks.FirstOrDefault(
                                fc => !fc.Changing && fc.Size > size + StorageChunk.ChunkHeaderSize &&
                                      fc.Type == ChunkTypes.Free);

                        if (free.Type == ChunkTypes.Free)
                        {
                            var position = free.Position;
                            // if free space found in blob
                            if (free.Size == size)
                            {
                                // if chunk size equals with the free space size, replace free space with chunk
                                chunk = new StorageChunk(free.Id, userData, chunkType, position,
                                    size) {Changing = true};
                                info.Chunks[info.Chunks.IndexOf(free)] = chunk;
                                free = default(StorageChunk);
                            }
                            else
                            {
                                // chunk size < free space size, remove chunk sized portion of the free space
                                var index = info.Chunks.IndexOf(free);
                                var remaining = free.Size - size - StorageChunk.ChunkHeaderSize;
                                free = new StorageChunk(free.Id, 0, ChunkTypes.Free,
                                        position + size + StorageChunk.ChunkHeaderSize, remaining)
                                    {Changing = true};
                                info.Chunks[index] = free;
                                chunk = new StorageChunk(GetId(info.Chunks), userData, chunkType, position, size)
                                    {Changing = true};
                                info.Chunks.Add(chunk);
                            }
                        }
                        else
                        {
                            // no space found, add chunk at the end of the file
                            var last = info.Chunks.OrderByDescending(ch => ch.Position).FirstOrDefault();
                            var position = last.Position == 0
                                ? HeaderSize
                                : last.Position + last.Size + StorageChunk.ChunkHeaderSize;
                            chunk = new StorageChunk(GetId(info.Chunks), userData, chunkType, position, size)
                                {Changing = true};
                            info.Chunks.Add(chunk);
                            f.SetLength(position + StorageChunk.ChunkHeaderSize + size);
                        }

                        WriteInfo(info);
                    }

                    // write chunk data to blob
                    f.Position = chunk.Position;
                    chunk.ToStorage(w, true);
                    await data.CopyToAsync(f);

                    if (free.Changing)
                    {
                        f.Position = free.Position;
                        free.ToStorage(w);
                    }

                    await f.FlushAsync();

                    f.Position = chunk.Position;
                    w.Write(chunk.Type);
                    await f.FlushAsync();
                }

                using (WriteLock(ConcurrencyHandler.Timeout))
                {
                    var info = ReadInfo();

                    var index = info.Chunks.IndexOf(chunk);
                    chunk.Changing = false;
                    info.Chunks[index] = chunk;

                    if (free.Changing)
                    {
                        index = info.Chunks.IndexOf(free);
                        free.Changing = false;
                        info.Chunks[index] = free;
                    }

                    info.Version++;
                    WriteInfo(info);
                }

                return chunk;
            });
        }

        private uint GetId(List<StorageChunk> chunks)
        {
            var id = 1u;
            foreach (var c in chunks.OrderBy(c => c.Id))
                if (c.Id > id)
                    break;
                else
                    id++;

            return id;
        }

        public Task RemoveChunk(Func<IReadOnlyList<StorageChunk>, ulong, StorageChunk?> selector)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return Task.Run(async () =>
            {
                var wait = false;

                while (true)
                {
                    StorageChunk chunk;

                    // If there are readers for the selected chunk wait here while a read is finished and try again
                    if (wait)
                        ConcurrencyHandler.WaitForReadFinish();

                    using (var f = Open())
                    using (var w = new BinaryWriter(f, Encoding.UTF8))
                    {
                        using (WriteLock(ConcurrencyHandler.Timeout))
                        {
                            var info = ReadInfo();

                            // Get the chunk to delete
                            var item = selector.Invoke(
                                info.Chunks.Where(c => c.Type != ChunkTypes.Free && !c.Changing).ToList(),
                                info.Version);
                            if (item == null || item.Value == default(StorageChunk))
                                return;

                            chunk = item.Value;

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
                            var nextPos = chunk.Position + StorageChunk.ChunkHeaderSize + chunk.Size;
                            var nextChunk = info.Chunks.FirstOrDefault(c => c.Position == nextPos);

                            if (nextChunk.Type == ChunkTypes.Free && !nextChunk.Changing)
                            {
                                freeSize += StorageChunk.ChunkHeaderSize + nextChunk.Size;
                                info.Chunks.Remove(nextChunk);
                            }

                            // Check previous chunk is free, combine free space
                            var pos = chunk.Position;
                            var previousChunk = info.Chunks.Where(c => c.Position < pos)
                                .OrderByDescending(c => c.Position).FirstOrDefault();

                            if (previousChunk.Type == ChunkTypes.Free && !previousChunk.Changing)
                            {
                                freeSize += StorageChunk.ChunkHeaderSize + previousChunk.Size;
                                freePosition = previousChunk.Position;
                                info.Chunks.Remove(previousChunk);
                            }

                            // Mark the chunk changing while updating the file
                            var index = info.Chunks.IndexOf(chunk);
                            chunk = new StorageChunk(chunk.Id, 0, ChunkTypes.Free, freePosition,
                                    freeSize)
                                {Changing = true};
                            info.Chunks[index] = chunk;

                            info.Version++;
                            WriteInfo(info);
                        }

                        // Mark the chunk free
                        f.Position = chunk.Position;
                        chunk.ToStorage(w);
                        await f.FlushAsync();
                    }

                    using (WriteLock(ConcurrencyHandler.Timeout))
                    {
                        var info = ReadInfo();

                        var index = info.Chunks.IndexOf(chunk);
                        chunk.Changing = false;
                        info.Chunks[index] = chunk;

                        WriteInfo(info);
                    }

                    break;
                }
            });
        }

        public Task<IReadOnlyList<(StorageChunk Chunk, byte[] Data)>> ReadChunks(
            Func<IReadOnlyList<StorageChunk>, ulong, IEnumerable<StorageChunk>> selector)
        {
            if (selector == null)
                throw new ArgumentNullException(nameof(selector));

            return Task.Run(async () =>
            {
                List<StorageChunk> chunksToRead;
                var res = new List<(StorageChunk, byte[])>();

                using (WriteLock(ConcurrencyHandler.Timeout))
                {
                    var info = ReadInfo();
                    var chunks = info.Chunks.Where(c => !c.Changing && c.Type != ChunkTypes.Free).ToList();

                    chunksToRead = selector.Invoke(chunks, info.Version)?.ToList();
                    if (chunksToRead == null)
                        return res;

                    foreach (var r in chunksToRead)
                    {
                        var chunk = r;
                        var index = info.Chunks.IndexOf(chunk);
                        chunk.ReadCount++;
                        info.Chunks[index] = chunk;
                    }

                    WriteInfo(info);
                }

                var finishedOne = false;
                try
                {
                    foreach (var r in chunksToRead)
                        res.Add((r, await ReadChunk(r)));
                }
                finally
                {
                    using (WriteLock(ConcurrencyHandler.Timeout))
                    {
                        var info = ReadInfo();

                        foreach (var r in chunksToRead)
                        {
                            var chunk = r;
                            var index = info.Chunks.IndexOf(chunk);
                            chunk.ReadCount--;
                            info.Chunks[index] = chunk;
                            finishedOne = finishedOne || chunk.ReadCount == 0;
                        }

                        WriteInfo(info);
                    }
                }

                if (finishedOne)
                    ConcurrencyHandler.SignalReadFinish();

                return (IReadOnlyList<(StorageChunk, byte[])>) res;
            });
        }

        private async Task<byte[]> ReadChunk(StorageChunk chunk)
        {
            using (var f = Open())
            {
                var res = new byte[chunk.Size];
                f.Position = chunk.Position + StorageChunk.ChunkHeaderSize;

                var position = 0;
                var read = 1;

                while (position < res.Length && read != 0)
                {
                    read = await f.ReadAsync(res, position, res.Length - position);
                    position += read;
                }

                return res;
            }
        }

        private FileStream Open()
        {
            return new FileStream(Info.FullName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        }

        private StorageInfo ReadInfo()
        {
            return ConcurrencyHandler.ReadInfo();
        }

        private void WriteInfo(StorageInfo info)
        {
            ConcurrencyHandler.WriteInfo(info);
        }

        private void CheckBlobStorageHeader()
        {
            _mainLock?.Close();
            _mainLock = Open();

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

        private void CreateEmptyBlobStorage()
        {
            using (var f = Info.Create())
            using (var w = new BinaryWriter(f, Encoding.UTF8))
            {
                w.Write(ChunkTypes.Blob);
                w.Write(1);
                w.Write(Guid.NewGuid().ToByteArray());
            }
        }

        private IDisposable ReadLock(int timeout)
        {
            return ConcurrencyHandler.ReadLock(timeout);
        }

        private IDisposable WriteLock(int timeout)
        {
            return ConcurrencyHandler.WriteLock(timeout);
        }

        ~BlobStorage()
        {
            Dispose();
        }

        internal Task<IReadOnlyList<StorageChunk>> GetChunks()
        {
            return Task.Run(() =>
            {
                using (ReadLock(ConcurrencyHandler.Timeout))
                {
                    return (IReadOnlyList<StorageChunk>) ReadInfo().Chunks;
                }
            });
        }

        public async Task<IReadOnlyList<uint>> GetFreeChunkSizes()
        {
            var chunks = await GetChunks();
            return chunks.Where(c => c.Type == ChunkTypes.Free).Select(c => c.Size).ToList();
        }
    }
}