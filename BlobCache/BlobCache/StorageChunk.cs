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
        public DateTime Added { get; }

        internal StorageChunk(uint id, uint userData, int chunkType, long position, uint size, DateTime added)
        {
            Id = id;
            Type = chunkType;
            Position = position;
            Size = size;
            UserData = userData;
            Changing = false;
            ReadCount = 0;
            Added = added;
        }

        internal const int ChunkHeaderSize = 24;

        internal static StorageChunk FromStorage(BinaryReader reader)
        {
            var p = reader.BaseStream.Position;

            if (p + ChunkHeaderSize > reader.BaseStream.Length)
                throw new InvalidDataException("No room in stream for chunk header");

            var t = reader.ReadInt32();
            var i = reader.ReadUInt32();
            var d = reader.ReadUInt32();
            var s = reader.ReadUInt32();
            var a = new DateTime(reader.ReadInt64(), DateTimeKind.Utc);

            if (p + s > reader.BaseStream.Length)
                throw new InvalidDataException("Chunk size points outside of stream");

            reader.BaseStream.Seek(s, SeekOrigin.Current);
            return new StorageChunk(i, d, t, p, s, a);
        }

        internal static StorageChunk FromStream(BinaryReader reader)
        {
            var p = reader.ReadInt64();
            var t = reader.ReadInt32();
            var i = reader.ReadUInt32();
            var d = reader.ReadUInt32();
            var s = reader.ReadUInt32();
            var a = new DateTime(reader.ReadInt64(), DateTimeKind.Utc);

            return new StorageChunk(i, d, t, p, s, a);
        }

        internal void ToStorage(BinaryWriter writer, bool forceFree = false)
        {
            writer.Write(forceFree ? ChunkTypes.Free : Type);
            writer.Write(Id);
            writer.Write(UserData);
            writer.Write(Size);
            writer.Write(Added.Ticks);
            writer.Flush();
        }

        internal void ToStream(BinaryWriter writer)
        {
            writer.Write(Position);
            writer.Write(Type);
            writer.Write(Id);
            writer.Write(UserData);
            writer.Write(Size);
            writer.Write(Added.Ticks);
            writer.Flush();
        }

        public bool Equals(StorageChunk other)
        {
            return Id == other.Id;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is StorageChunk && Equals((StorageChunk)obj);
        }

        public override int GetHashCode()
        {
            return (int)Id;
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