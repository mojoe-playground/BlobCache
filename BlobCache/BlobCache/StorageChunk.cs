namespace BlobCache
{
    using System;
    using System.IO;
    using System.Text;

    public struct StorageChunk : IEquatable<StorageChunk>
    {
        public long Position { get; }
        public uint Size { get; }
        public int Type { get; }
        public bool Changing { get; internal set; }
        public uint Id { get; }
        public uint UserData { get; }
        public int ReadCount { get; internal set; }
        public DateTime Added { get; private set; }
        private long AddedTicks { get; }
        private ushort Crc { get; }

        internal StorageChunk(uint id, uint userData, int chunkType, long position, uint size, DateTime added)
            : this(id, userData, chunkType, position, size, added.Ticks)
        {
            Added = added;
        }

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

            using (var ms = new MemoryStream())
            using (var bw = new BinaryWriter(ms))
            {
                ToStorage(bw, false, true);
                Crc = Crc16.ComputeChecksum(ms.ToArray());
            }
        }

        internal const int ChunkHeaderSize = 26;
        internal const int ChunkFooterSize = 2;

        internal static StorageChunk FromStorage(BinaryReader reader, bool seekToNext, long p)
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

            var chunk = new StorageChunk(i, d, t, p, s, a);

            if (chunk.Crc != crc)
                throw new InvalidDataException("Chunk header crc error");

            chunk.Added = new DateTime(chunk.AddedTicks, DateTimeKind.Utc);

            if (seekToNext)
                reader.BaseStream.Seek(s + ChunkFooterSize, SeekOrigin.Current);
            return chunk;
        }

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

        internal void ToStorage(BinaryWriter writer, bool forceFree = false, bool skipCrc = false)
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

        public bool Equals(StorageChunk other)
        {
            return other.Position == Position && other.Id == Id && other.Added == Added && other.Size == Size && other.Type == Type && other.UserData == UserData && other.Crc == Crc;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is StorageChunk && Equals((StorageChunk)obj);
        }

        public override int GetHashCode()
        {
            return Crc;
        }

        public static bool operator ==(StorageChunk left, StorageChunk right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(StorageChunk left, StorageChunk right)
        {
            return !left.Equals(right);
        }

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