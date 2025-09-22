using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using Org.BouncyCastle.Crypto.Engines;
using Org.BouncyCastle.Crypto.Parameters;

namespace Server.Common
{
    public static class LogFmt
    {
        public const string AUTH = "<color=#8ef>[Auth]</color>";
        public const string GAME = "<color=#9f9>[Game]</color>";
        public const string SEND = "<color=#6f6>[SEND]</color>";
        public const string RECV = "<color=#ff9>[RECV]</color>";
        public const string WARN = "<color=#fb3>[WARN]</color>";
        public const string ERR = "<color=#f66>[ERR]</color>";

        public static string Hex(byte[] data, int max = 96)
        {
            if (data == null) return "(null)";
            int n = Math.Min(max, data.Length);
            var sb = new StringBuilder(n * 3);
            for (int i = 0; i < n; i++) sb.Append(data[i].ToString("X2")).Append(' ');
            if (n < data.Length) sb.Append("…(+").Append(data.Length - n).Append(")");
            return sb.ToString().TrimEnd();
        }
    }

    // ---------------- LE Reader/Writer (UInt24 support) ----------------
    public sealed class LEReader
    {
        private readonly byte[] _buf;
        private int _pos;
        public LEReader(byte[] buffer) { _buf = buffer ?? Array.Empty<byte>(); _pos = 0; }
        public int Position => _pos;
        public int Remaining => _buf.Length - _pos;

        public byte ReadByte() => _buf[_pos++];

        public byte[] ReadBytes(int n)
        {
            var outb = new byte[n];
            Buffer.BlockCopy(_buf, _pos, outb, 0, n);
            _pos += n; return outb;
        }

        public ushort ReadUInt16()
        {
            int v = _buf[_pos] | (_buf[_pos + 1] << 8);
            _pos += 2; return (ushort)v;
        }

        public uint ReadUInt32()
        {
            uint v = (uint)(_buf[_pos] | (_buf[_pos + 1] << 8) | (_buf[_pos + 2] << 16) | (_buf[_pos + 3] << 24));
            _pos += 4; return v;
        }

        public uint ReadUInt24()
        {
            uint v = (uint)(_buf[_pos] | (_buf[_pos + 1] << 8) | (_buf[_pos + 2] << 16));
            _pos += 3; return v;
        }

        public byte[] ReadToEnd()
        {
            var n = Remaining;
            var outb = new byte[n];
            Buffer.BlockCopy(_buf, _pos, outb, 0, n);
            _pos += n; return outb;
        }
    }

    public sealed class LEWriter : IDisposable
    {
        private readonly MemoryStream _ms;
        public LEWriter() { _ms = new MemoryStream(); }
        public LEWriter(MemoryStream existing) { _ms = existing ?? new MemoryStream(); }
        public void Dispose() => _ms?.Dispose();

        public void WriteByte(byte b) => _ms.WriteByte(b);

        public void WriteBytes(byte[] b)
        {
            if (b is { Length: > 0 }) _ms.Write(b, 0, b.Length);
        }

        public void WriteUInt16(ushort v)
        {
            _ms.WriteByte((byte)(v & 0xFF));
            _ms.WriteByte((byte)((v >> 8) & 0xFF));
        }

        public void WriteUInt24(int v)
        {
            _ms.WriteByte((byte)(v & 0xFF));
            _ms.WriteByte((byte)((v >> 8) & 0xFF));
            _ms.WriteByte((byte)((v >> 16) & 0xFF));
        }

        public void WriteUInt32(uint v)
        {
            _ms.WriteByte((byte)(v & 0xFF));
            _ms.WriteByte((byte)((v >> 8) & 0xFF));
            _ms.WriteByte((byte)((v >> 16) & 0xFF));
            _ms.WriteByte((byte)((v >> 24) & 0xFF));
        }

        public byte[] ToArray() => _ms.ToArray();
    }

    // ---------------- zlib helpers ----------------
    public static class ZlibUtil
    {
        public static byte[] Inflate(byte[] input, uint expectedUncompressedLen)
        {
            if (input == null || input.Length == 0) return Array.Empty<byte>();

            int offset = 0;
            if (input.Length >= 2 && input[0] == 0x78)
                offset = 2; // strip zlib header

            using var src = new MemoryStream(input, offset, input.Length - offset);
            using var ds = new DeflateStream(src, CompressionMode.Decompress);
            using var dst = new MemoryStream((int)expectedUncompressedLen);
            ds.CopyTo(dst);
            return dst.ToArray();
        }

        public static byte[] Deflate(byte[] uncompressed)
        {
            if (uncompressed == null || uncompressed.Length == 0) return Array.Empty<byte>();

            byte[] raw;
            using (var ms = new MemoryStream())
            {
                using (var ds = new DeflateStream(ms, CompressionLevel.Fastest, leaveOpen: true))
                {
                    ds.Write(uncompressed, 0, uncompressed.Length);
                }
                raw = ms.ToArray();
            }

            uint adler = Adler32(uncompressed);

            using var z = new MemoryStream();
            z.WriteByte(0x78);
            z.WriteByte(0x9C);
            z.Write(raw, 0, raw.Length);
            z.WriteByte((byte)((adler >> 24) & 0xFF));
            z.WriteByte((byte)((adler >> 16) & 0xFF));
            z.WriteByte((byte)((adler >> 8) & 0xFF));
            z.WriteByte((byte)(adler & 0xFF));
            return z.ToArray();
        }

        private static uint Adler32(byte[] data)
        {
            const uint MOD = 65521;
            uint a = 1, b = 0;
            for (int i = 0; i < data.Length; i++)
            {
                a = (a + data[i]) % MOD;
                b = (b + a) % MOD;
            }
            return (b << 16) | a;
        }
    }

    // ---------------- Blowfish + Auth wire ----------------
    public static class AuthWire
    {
        public static readonly byte[] Key = Encoding.ASCII.GetBytes("[;'.]94-31==-%&@!^+]\x00");

        // PLAIN frame (ONLY for ProtocolVer): [u16 totalLen][type][payload][u16 0]
        public static byte[] PlainFrame(byte type, byte[] payload)
        {
            int bodyLen = 1 + (payload?.Length ?? 0);
            ushort totalLen = (ushort)(bodyLen + 4);
            var framed = new byte[totalLen];
            framed[0] = (byte)(totalLen & 0xFF);
            framed[1] = (byte)((totalLen >> 8) & 0xFF);
            framed[2] = type;
            if (payload is { Length: > 0 }) Buffer.BlockCopy(payload, 0, framed, 3, payload.Length);
            return framed; // last two bytes 0
        }

        // ENCRYPTED frame we SEND: [u16 totalLen][enc]   (no trailer)
        public static byte[] EncryptFrame(byte type, byte[] payload)
        {
            int bodyLen = 1 + (payload?.Length ?? 0);
            var body = new byte[bodyLen];
            body[0] = type;
            if (payload is { Length: > 0 }) Buffer.BlockCopy(payload, 0, body, 1, payload.Length);

            int pad = (-body.Length) & 7;
            if (pad > 0) Array.Resize(ref body, body.Length + pad);

            uint checksum = 0;
            for (int i = 0; i < body.Length; i += 4)
                checksum ^= BitConverter.ToUInt32(body, i);

            var withTail = new byte[body.Length + 8];
            Buffer.BlockCopy(body, 0, withTail, 0, body.Length);
            Buffer.BlockCopy(BitConverter.GetBytes(checksum), 0, withTail, body.Length, 4);
            // last 4 bytes remain zero

            byte[] enc = BfEncryptSwap(withTail, Key);

            ushort totalLen = (ushort)(2 + enc.Length);
            var framed = new byte[totalLen];
            framed[0] = (byte)(totalLen & 0xFF);
            framed[1] = (byte)((totalLen >> 8) & 0xFF);
            Buffer.BlockCopy(enc, 0, framed, 2, enc.Length);
            return framed;
        }

        // Flexible decrypt: accept either [u16][enc][u16 0] OR [u16][enc]
        public static byte[] DecryptFrameFlexible(byte[] hdr2, byte[] rest)
        {
            if (hdr2 == null || hdr2.Length != 2) throw new InvalidDataException("short header");
            ushort totalLen = (ushort)(hdr2[0] | (hdr2[1] << 8));
            if (rest == null || rest.Length < totalLen - 2) throw new InvalidDataException("incomplete frame");

            // Case A: repo style with trailer
            int encLenA = totalLen - 4;
            if (encLenA > 0 && rest.Length >= encLenA)
            {
                var encA = new byte[encLenA];
                Buffer.BlockCopy(rest, 0, encA, 0, encLenA);
                if (encLenA % 8 == 0)
                    return StripTail(BfDecryptSwap(encA, Key));
            }

            // Case B: variant without trailer
            int encLenB = totalLen - 2;
            if (encLenB > 0)
            {
                var encB = new byte[encLenB];
                Buffer.BlockCopy(rest, 0, encB, 0, encLenB);
                if (encLenB % 8 != 0) throw new InvalidDataException("decrypt input not 8 byte aligned");
                return StripTail(BfDecryptSwap(encB, Key));
            }

            throw new InvalidDataException("bad frame length");
        }

        private static byte[] StripTail(byte[] dec)
        {
            if (dec.Length < 8) throw new InvalidDataException("dec too short");
            int bodyLen = dec.Length - 8; // strip [u32 checksum][u32 zeros]
            var body = new byte[bodyLen];
            Buffer.BlockCopy(dec, 0, body, 0, bodyLen);
            return body; // [type][payload]
        }

        private static byte[] BfEncryptSwap(byte[] input, byte[] key)
        {
            if (input.Length % 8 != 0) throw new InvalidDataException("encrypt input not 8-byte aligned");
            var engine = new BlowfishEngine();
            engine.Init(true, new KeyParameter(key));
            var outb = new byte[input.Length];

            for (int i = 0; i < input.Length; i += 8)
            {
                var blk = ToBE8FromLEu32Pair(input, i);
                engine.ProcessBlock(blk, 0, blk, 0);
                var le = ToLEu32PairFromBE8(blk, 0);
                Buffer.BlockCopy(le, 0, outb, i, 8);
            }
            return outb;
        }

        private static byte[] BfDecryptSwap(byte[] input, byte[] key)
        {
            if (input.Length % 8 != 0) throw new InvalidDataException("decrypt input not 8-byte aligned");
            var engine = new BlowfishEngine();
            engine.Init(false, new KeyParameter(key));
            var outb = new byte[input.Length];

            for (int i = 0; i < input.Length; i += 8)
            {
                var blk = ToBE8FromLEu32Pair(input, i);
                engine.ProcessBlock(blk, 0, blk, 0);
                var le = ToLEu32PairFromBE8(blk, 0);
                Buffer.BlockCopy(le, 0, outb, i, 8);
            }
            return outb;
        }

        private static byte[] ToBE8FromLEu32Pair(byte[] input, int offset)
        {
            uint v1 = BitConverter.ToUInt32(input, offset);
            uint v2 = BitConverter.ToUInt32(input, offset + 4);
            return new byte[]
            {
                (byte)((v1 >> 24) & 0xFF),(byte)((v1 >> 16) & 0xFF),(byte)((v1 >> 8) & 0xFF),(byte)(v1 & 0xFF),
                (byte)((v2 >> 24) & 0xFF),(byte)((v2 >> 16) & 0xFF),(byte)((v2 >> 8) & 0xFF),(byte)(v2 & 0xFF),
            };
        }

        private static byte[] ToLEu32PairFromBE8(byte[] be8, int offset)
        {
            uint v1 = (uint)((be8[offset + 0] << 24) | (be8[offset + 1] << 16) | (be8[offset + 2] << 8) | be8[offset + 3]);
            uint v2 = (uint)((be8[offset + 4] << 24) | (be8[offset + 5] << 16) | (be8[offset + 6] << 8) | be8[offset + 7]);
            var out8 = new byte[8];
            Buffer.BlockCopy(BitConverter.GetBytes(v1), 0, out8, 0, 4);
            Buffer.BlockCopy(BitConverter.GetBytes(v2), 0, out8, 4, 4);
            return out8;
        }
    }
}
