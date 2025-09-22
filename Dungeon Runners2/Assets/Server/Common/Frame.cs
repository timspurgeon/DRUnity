
using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace Server.Common {
  public enum Op : ushort {
    Ping = 1,
    LoginReq = 10, LoginOk = 11, LoginFail = 12,
    CharListReq = 20, CharList = 21,
    CharCreateReq = 22, CharCreateOk = 23, CharCreateFail = 24,
    EnterWorldReq = 30, EnterWorldOk = 31, EnterWorldFail = 32,
    Move = 100, MoveBroadcast = 101,
    ChatSay = 110, ChatBroadcast = 111,
  }

  public struct Frame { public ushort Op; public byte[] Payload; }

  public static class FrameIO {
    public static Frame ReadFrame(Stream s) {
      Span<byte> hdr = stackalloc byte[4];
      int got = s.Read(hdr);
      if (got == 0) throw new EndOfStreamException();
      if (got < 4) throw new IOException("short header");
      ushort len = BinaryPrimitives.ReadUInt16LittleEndian(hdr[..2]);
      ushort op  = BinaryPrimitives.ReadUInt16LittleEndian(hdr[2..4]);
      byte[] payload = new byte[len];
      int read = 0;
      while (read < len) {
        int r = s.Read(payload, read, len - read);
        if (r <= 0) throw new EndOfStreamException();
        read += r;
      }
      return new Frame { Op = op, Payload = payload };
    }

    public static void WriteFrame(Stream s, Op op, ReadOnlySpan<byte> payload) {
      Span<byte> hdr = stackalloc byte[4];
      BinaryPrimitives.WriteUInt16LittleEndian(hdr[..2], (ushort)payload.Length);
      BinaryPrimitives.WriteUInt16LittleEndian(hdr[2..4], (ushort)op);
      s.Write(hdr);
      s.Write(payload);
      s.Flush();
    }

    public static string ReadStr(BinaryReader br) {
      ushort n = br.ReadUInt16();
      var bytes = br.ReadBytes(n);
      return Encoding.UTF8.GetString(bytes);
    }
    public static void WriteStr(BinaryWriter bw, string s) {
      var bytes = Encoding.UTF8.GetBytes(s ?? "");
      bw.Write((ushort)bytes.Length);
      bw.Write(bytes);
    }
  }
}
