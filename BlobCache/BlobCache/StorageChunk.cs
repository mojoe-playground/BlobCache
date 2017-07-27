namespace BlobCache
{
    using System;
    using System.IO;
    using System.Text;

    /// <summary>
    ///     Information about a storage chunk
    /// </summary>
    public struct StorageChunk : IEquatable<StorageChunk>
    {
        /// <summary>
        ///     Gets the chunk position
        /// </summary>
        public long Position { get; }

        /// <summary>
        ///     Gets the chunk data size
        /// </summary>
        public uint Size { get; }

        /// <summary>
        ///     Gets the chunk type
        /// </summary>
        public int Type { get; }

        /// <summary>
        ///     Gets whether the chunk is changing
        /// </summary>
        public bool Changing { get; internal set; }

        /// <summary>
        ///     Gets the chunk id
        /// </summary>
        public uint Id { get; }

        /// <summary>
        ///     Gets the user data associated with the chunk
        /// </summary>
        public uint UserData { get; }

        /// <summary>
        ///     Gets the concurrent read count
        /// </summary>
        public int ReadCount { get; internal set; }

        /// <summary>
        ///     Gets the date the chunk was added
        /// </summary>
        /// <remarks>In UTC</remarks>
        public DateTime Added { get; private set; }

        /// <summary>
        ///     Gets the date the chunk was added in ticks
        /// </summary>
        /// <remarks>>In UTC</remarks>
        private long AddedTicks { get; }

        /// <summary>
        ///     Gets the chunk header CRC
        /// </summary>
        private ushort Crc { get; }

        /// <summary>
        ///     Initializes a new instance of the <see cref="StorageChunk" /> struct
        /// </summary>
        /// <param name="id">Chunk id</param>
        /// <param name="userData">User data</param>
        /// <param name="chunkType">Chunk type</param>
        /// <param name="position">Position in the storage</param>
        /// <param name="size">Chunk data size</param>
        /// <param name="added">Date the chunk was added</param>
        internal StorageChunk(uint id, uint userData, int chunkType, long position, uint size, DateTime added)
            : this(id, userData, chunkType, position, size, added.Ticks)
        {
            Added = added;
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="StorageChunk" /> struct
        /// </summary>
        /// <param name="id">Chunk id</param>
        /// <param name="userData">User data</param>
        /// <param name="chunkType">Chunk type</param>
        /// <param name="position">Position in the storage</param>
        /// <param name="size">Chunk data size</param>
        /// <param name="added">Date the chunk was added</param>
        private StorageChunk(uint id, uint userData, int chunkType, long position, uint size, long added)
        {
            Id = id;
            Type = chunkType;
            Position = position;
            Size = size;
            UserData = userData;
            AddedTicks = added;

            Changing = false;
            ReadCount = 0;
            Crc = 0;
            Added = DateTime.MinValue;

            // Use fixed size buffer so we don't need to call ToArray() on the stream, it will be one less byte[] allocation
            using (var ms = new MemoryStream(new byte[ChunkHeaderSize - 2], 0, ChunkHeaderSize - 2, true, true))
            using (var bw = new BinaryWriter(ms))
            {
                ToStorage(bw, false, true);
                Crc = Crc16.ComputeChecksum(ms.GetBuffer());
            }
        }

        /// <summary>
        ///     Current chunk header size
        /// </summary>
        internal const int ChunkHeaderSize = 26;

        /// <summary>
        ///     Current chunk footer size
        /// </summary>
        internal const int ChunkFooterSize = 2;

        /// <summary>
        ///     Creates a new <see cref="StorageChunk" /> from blob storage
        /// </summary>
        /// <param name="reader">Reader to use</param>
        /// <param name="seekToNext">Indicating whether seek to the next chunk after the chunk read</param>
        /// <param name="position">Chunk position in the blob storage</param>
        /// <returns>StorageChunk read from blob storage</returns>
        /// <exception cref="InvalidDataException">Invalid chunk data read</exception>
        internal static StorageChunk FromStorage(BinaryReader reader, bool seekToNext, long position)
        {
            var bp = reader.BaseStream.Position;
            if (bp + ChunkHeaderSize > reader.BaseStream.Length)
                throw new InvalidDataException("No room in stream for chunk header");

            var t = reader.ReadInt32();
            var i = reader.ReadUInt32();
            var d = reader.ReadUInt32();
            var s = reader.ReadUInt32();
            var a = reader.ReadInt64();
            var crc = reader.ReadUInt16();

            if (bp + ChunkHeaderSize + s + ChunkFooterSize > reader.BaseStream.Length)
                throw new InvalidDataException("Chunk size points outside of stream");

            var chunk = new StorageChunk(i, d, t, position, s, a);

            if (chunk.Crc != crc)
                throw new InvalidDataException("Chunk header crc error");

            chunk.Added = new DateTime(chunk.AddedTicks, DateTimeKind.Utc);

            if (seekToNext)
                reader.BaseStream.Seek(s + ChunkFooterSize, SeekOrigin.Current);
            return chunk;
        }

        /// <summary>
        ///     Creates a new <see cref="StorageChunk" /> from storage info stream
        /// </summary>
        /// <param name="reader">Reader to use</param>
        /// <returns>StorageChunk read from storage info stream</returns>
        /// <exception cref="InvalidDataException">Invalid chunk data read</exception>
        internal static StorageChunk FromStream(BinaryReader reader)
        {
            var p = reader.ReadInt64();
            var t = reader.ReadInt32();
            var i = reader.ReadUInt32();
            var d = reader.ReadUInt32();
            var s = reader.ReadUInt32();
            var a = reader.ReadInt64();
            var crc = reader.ReadUInt16();
            var c = reader.ReadBoolean();
            var rc = reader.ReadInt32();

            var chunk = new StorageChunk(i, d, t, p, s, a) { Changing = c, ReadCount = rc };
            if (chunk.Crc != crc)
                throw new InvalidDataException("Chunk header crc error");
            chunk.Added = new DateTime(chunk.AddedTicks, DateTimeKind.Utc);

            return chunk;
        }

        /// <summary>
        ///     Writes chunk data to storage blob
        /// </summary>
        /// <param name="writer">Writer to use</param>
        /// <param name="forceFree">
        ///     Indicating whether a <see cref="ChunkTypes.Free" /> chunk should be written intead of the
        ///     current <see cref="Type" />
        /// </param>
        internal void ToStorage(BinaryWriter writer, bool forceFree = false)
        {
            ToStorage(writer, forceFree, false);
        }

        /// <summary>
        ///     Writes chunk data to storage blob
        /// </summary>
        /// <param name="writer">Writer to use</param>
        /// <param name="forceFree">
        ///     Indicating whether a <see cref="ChunkTypes.Free" /> chunk should be written intead of the
        ///     current <see cref="Type" />
        /// </param>
        /// <param name="skipCrc">Indicating whether CRC data should be skipped when writing to the storage</param>
        private void ToStorage(BinaryWriter writer, bool forceFree, bool skipCrc)
        {
            if (forceFree)
            {
                new StorageChunk(Id, UserData, ChunkTypes.Free, Position, Size, Added).ToStorage(writer);
                return;
            }

            writer.Write(Type);
            writer.Write(Id);
            writer.Write(UserData);
            writer.Write(Size);
            writer.Write(AddedTicks);
            if (!skipCrc)
                writer.Write(Crc);
            writer.Flush();
        }

        /// <summary>
        ///     Writes chunk data to storage info stream
        /// </summary>
        /// <param name="writer">Writer to use</param>
        internal void ToStream(BinaryWriter writer)
        {
            writer.Write(Position);
            writer.Write(Type);
            writer.Write(Id);
            writer.Write(UserData);
            writer.Write(Size);
            writer.Write(AddedTicks);
            writer.Write(Crc);
            writer.Write(Changing);
            writer.Write(ReadCount);
            writer.Flush();
        }

        /// <inheritdoc />
        public bool Equals(StorageChunk other)
        {
            return other.Position == Position && other.Id == Id && other.Added == Added && other.Size == Size && other.Type == Type && other.UserData == UserData && other.Crc == Crc;
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is StorageChunk && Equals((StorageChunk)obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return Crc;
        }

        /// <inheritdoc />
        public static bool operator ==(StorageChunk left, StorageChunk right)
        {
            return left.Equals(right);
        }

        /// <inheritdoc />
        public static bool operator !=(StorageChunk left, StorageChunk right)
        {
            return !left.Equals(right);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(Id);
            sb.Append(" - ");
            sb.Append(ChunkTypes.FourCC(Type));
            sb.Append(" @");
            sb.Append(Position);
            sb.Append(", ");
            sb.Append(Size);
            return sb.ToString();
        }
    }
}