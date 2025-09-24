// Server/GameServer/LegacyWriters.cs
using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using Server.Common;       // ZlibUtil lives here
using UnityEngine;

namespace Server.Game
{
    public static class LegacyWriters
    {
        // ---- Outer legacy frame ----
        // [0x10][lenLo lenHi][chanLo chanHi][opcode][payload...]
        // len = 2 (chan) + 1 (opcode) + N (payload)  -- DOES NOT include the leading 0x10
        static void WriteGameFrame(Stream s, byte opcode, ReadOnlySpan<byte> payload, ushort channel = 1, bool dump = true)
        {
            ushort len = (ushort)(2 + 1 + payload.Length);
            Span<byte> hdr = stackalloc byte[5];
            hdr[0] = 0x10;
            hdr[1] = (byte)(len & 0xFF);
            hdr[2] = (byte)(len >> 8);
            hdr[3] = (byte)(channel & 0xFF);
            hdr[4] = (byte)(channel >> 8);

            byte[] buf = new byte[hdr.Length + 1 + payload.Length];
            hdr.CopyTo(buf);
            buf[5] = opcode;
            if (!payload.IsEmpty) payload.CopyTo(buf.AsSpan(6));

            if (dump) Debug.Log($"[Game][SEND] op=0x{opcode:X2} len={len} hex={BitConverter.ToString(buf)}");

            s.Write(buf, 0, buf.Length);
            s.Flush();
        }

        // ---- Inner "Compressed A" (opcode 0x0A) ----
        // Body layout (from Go's writers.go):
        //   [0x0A]
        //   [connId:UInt24]
        //   [dest:byte]
        //   [msgType:byte]
        //   [uncompressedLen:UInt32]
        //   [zlib(compressedBody)]
        //
        // NOTE: The *whole* thing above (starting at 0x0A) is then wrapped by WriteGameFrame(...).
        public static void WriteCompressedA(NetworkStream stream, int connIdUInt24, byte dest, byte msgType, byte[] uncompressedBody, ushort channel = 1)
        {
            // zlib-wrap the inner body bytes using your existing helper
            // (Utilities.ZlibUtil.Deflate produces 0x78 0x9C ... + Adler32 footer)
            byte[] z = ZlibUtil.Deflate(uncompressedBody ?? Array.Empty<byte>());

            using var ms = new MemoryStream(7 + 4 + z.Length);
            ms.WriteByte(0x0A);

            // UInt24 little-endian (match Go byter.NewLEByter)
            uint id = (uint)connIdUInt24 & 0xFFFFFFu;
            ms.WriteByte((byte)(id & 0xFF));
            ms.WriteByte((byte)((id >> 8) & 0xFF));
            ms.WriteByte((byte)((id >> 16) & 0xFF));

            ms.WriteByte(dest);
            ms.WriteByte(msgType);

            // uncompressed length (UInt32 LE)
            uint ulen = (uint)(uncompressedBody?.Length ?? 0);
            ms.WriteByte((byte)(ulen & 0xFF));
            ms.WriteByte((byte)((ulen >> 8) & 0xFF));
            ms.WriteByte((byte)((ulen >> 16) & 0xFF));
            ms.WriteByte((byte)((ulen >> 24) & 0xFF));

            // compressed bytes
            ms.Write(z, 0, z.Length);

            // Now wrap in the legacy 0x10 frame and send
            WriteGameFrame(stream, 0x0A, ms.ToArray(), channel, dump: true);
        }
    }
}

