namespace BlobCache
{
    using System;
    using System.IO;

    public struct StorageChunk: IEquatable<StorageChunk>
    {
        public long Position { get; internal set; }
        public uint Size { get; }
        public int Type { get; }
        public bool Changing { get; internal set; }
        public int Id { get; }
        public int UserData { get; }

        public StorageChunk(int id, int userData, int chunkType, long position, uint size)
        {
            Id = id;
            Type = chunkType;
            Position = position;
            Size = size;
            UserData = userData;
            Changing = false;
        }

        public const int ChunkHeaderSize = 16;

        internal static StorageChunk FromStorage(BinaryReader reader)
        {
            var p = reader.BaseStream.Position;

            if (p + ChunkHeaderSize > reader.BaseStream.Length)
                throw new InvalidDataException("No room in stream for chunk header");

            var t = reader.ReadInt32();
            var i = reader.ReadInt32();
            var d = reader.ReadInt32();
            var s = reader.ReadUInt32();

            if (p + s > reader.BaseStream.Length)
                throw new InvalidDataException("Chunk size points outside of stream");

            reader.BaseStream.Seek(s, SeekOrigin.Current);
            return new StorageChunk(i,d, t, p, s);
        }

        public bool Equals(StorageChunk other)
        {
            return Id == other.Id;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is StorageChunk && Equals((StorageChunk) obj);
        }

        public override int GetHashCode()
        {
            return Id;
        }

        public static bool operator ==(StorageChunk left, StorageChunk right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(StorageChunk left, StorageChunk right)
        {
            return !left.Equals(right);
        }
    }
}