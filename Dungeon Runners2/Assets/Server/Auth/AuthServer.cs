using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using UnityEngine;
using Server.Common;
using Server.Net;
using Server.Store;
using Org.BouncyCastle.Crypto.Engines;
using Org.BouncyCastle.Crypto.Parameters;

namespace Server.Auth
{
    public class AuthServer
    {
        readonly NetServer net;
        readonly IStore store;

        public AuthServer(string ip, int port, IStore s)
        {
            store = s;
            net = new NetServer(ip, port, HandleClient);
        }

        public Task RunAsync() => net.RunAsync();

        private async Task HandleClient(TcpClient c)
        {
            var ep = c.Client.RemoteEndPoint?.ToString() ?? "unknown";
            Debug.Log($"{LogFmt.AUTH} Connection from {ep}");
            c.NoDelay = true;

            using var s = c.GetStream();

            string currentUser = null;
            int currentAccountId = 0;
            uint playToken = 0;

            try
            {
                // 1) ProtocolVer MUST be plaintext (repo frame)
                await SendProtocolVerHandshake_PlainRepoFrame(s);

                while (true)
                {
                    // 2+) all client packets are encrypted repo frames
                    byte[] hdr = await ReadExactAsync(s, 2);
                    if (hdr == null) { Debug.LogWarning($"{LogFmt.AUTH} {LogFmt.WARN} closed (hdr)"); break; }

                    ushort totalLen = (ushort)(hdr[0] | (hdr[1] << 8));
                    if (totalLen < 4) { Debug.LogWarning($"{LogFmt.AUTH} {LogFmt.WARN} bad frame len"); break; }

                    byte[] rest = await ReadExactAsync(s, totalLen - 2);
                    if (rest == null) { Debug.LogWarning($"{LogFmt.AUTH} {LogFmt.WARN} closed mid-frame"); break; }

                    byte[] plainBody = AuthWire.DecryptFrameFlexible(hdr, rest);
                    var br = new LEReader(plainBody);
                    byte msg = br.ReadByte();
                    Debug.Log($"{LogFmt.AUTH} {LogFmt.RECV} type=0x{msg:X2} bodyLen={plainBody.Length - 1}");

                    switch (msg)
                    {
                        case C_Login: // 0x00
                            {
                                byte[] loginBlock = br.ReadBytes(24);
                                byte[] tail6 = br.ReadBytes(6);
                                (string u, string p) = DecodeLogin(loginBlock, tail6);

                                currentUser = u;
                                currentAccountId = Math.Abs(u.GetHashCode() % 1000000);

                                var w = new LEWriter();
                                w.WriteUInt32((uint)currentAccountId);
                                await WriteAuthMessageEncrypted(s, S_LoginOk, w.ToArray());
                                Debug.Log($"{LogFmt.AUTH} {LogFmt.SEND} LoginOk accountId={currentAccountId}");

                                // Push ServerListEx immediately (pointing to localhost)
                                await SendServerListEx_Encrypted(s, IPToUInt32("127.0.0.1"));
                                Debug.Log($"{LogFmt.AUTH} {LogFmt.SEND} ServerListEx 127.0.0.1:{GamePort}");
                                break;
                            }

                        case C_ServerListExt: // 0x05
                            {
                                await SendServerListEx_Encrypted(s, IPToUInt32("127.0.0.1"));
                                break;
                            }

                        case C_AboutToPlay: // 0x02
                            {
                                _ = br.ReadUInt32(); // lo
                                _ = br.ReadUInt32(); // hi
                                byte serverId = br.ReadByte();

                                if (playToken == 0)
                                    playToken = TokenBroker.Issue(currentAccountId);

                                GlobalSessions.Set(playToken, currentUser ?? "guest");

                                // Go's path: send S_PlayOk (5→8)
                                var w = new LEWriter();
                                w.WriteUInt32(playToken);    // OneTimeKey
                                w.WriteUInt32(0x5678DEFA);   // UID (opaque)
                                w.WriteByte(serverId);       // Server ID
                                await WriteAuthMessageEncrypted(s, S_PlayOk, w.ToArray());
                                Debug.Log($"{LogFmt.AUTH} {LogFmt.SEND} PlayOk sid={serverId} token=0x{playToken:X8}");
                                break;
                            }

                        default:
                            {
                                var skip = br.ReadToEnd();
                                Debug.LogWarning($"{LogFmt.AUTH} {LogFmt.WARN} Unhandled client msg 0x{msg:X2}, rest={skip.Length}");
                                break;
                            }
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.LogWarning($"{LogFmt.AUTH} {LogFmt.ERR} exception: {ex.Message}");
            }
        }

        // ---------------- helpers ----------------
        private async Task SendProtocolVerHandshake_PlainRepoFrame(NetworkStream s)
        {
            const uint Version = 666;
            var w = new LEWriter();
            w.WriteUInt32(Version);
            w.WriteByte((byte)AuthWire.Key.Length);
            w.WriteBytes(AuthWire.Key);

            byte[] frame = AuthWire.PlainFrame(S_ProtocolVer, w.ToArray());
            Debug.Log($"{LogFmt.AUTH} {LogFmt.SEND} ProtocolVer v={Version} keyLen={AuthWire.Key.Length} hex={LogFmt.Hex(frame)}");
            await s.WriteAsync(frame, 0, frame.Length);
        }

        private async Task WriteAuthMessageEncrypted(NetworkStream s, byte serverMsgType, byte[] payload)
        {
            byte[] frame = AuthWire.EncryptFrame(serverMsgType, payload);
            Debug.Log($"{LogFmt.AUTH} {LogFmt.SEND} 0x{serverMsgType:X2} encHex={LogFmt.Hex(frame)}");
            await s.WriteAsync(frame, 0, frame.Length);
        }

        private async Task SendServerListEx_Encrypted(NetworkStream s, uint ipU32)
        {
            var w = new LEWriter();
            w.WriteByte(1);                      // count
            w.WriteByte((byte)ServerID);         // lastServerId

            // [id:byte][ip:u32][port:u32][age:bool][pk:bool][current:u16][max:u16][status:byte]
            w.WriteByte((byte)ServerID);
            w.WriteUInt32(ipU32);
            w.WriteUInt32(GamePort);
            w.WriteByte(0);
            w.WriteByte(0);
            w.WriteUInt16(0);
            w.WriteUInt16(0xFFFF);
            w.WriteByte(0x01);

            await WriteAuthMessageEncrypted(s, S_ServerListEx, w.ToArray());
            Debug.Log($"{LogFmt.AUTH} {LogFmt.SEND} ServerListEx id={ServerID} ip={UInt32ToIP(ipU32)} port={GamePort}");
        }

        private static (string user, string pass) DecodeLogin(byte[] block24, byte[] tail6)
        {
            var des = new DesEngine();
            var key = Encoding.ASCII.GetBytes("TEST\0\0\0\0");
            des.Init(false, new KeyParameter(key));
            byte[] outbuf = new byte[24];

            for (int i = 0; i < 24; i += 8)
                des.ProcessBlock(block24, i, outbuf, i);

            byte[] all = new byte[24 + 6];
            Buffer.BlockCopy(outbuf, 0, all, 0, 24);
            Buffer.BlockCopy(tail6, 0, all, 24, 6);

            string user = Encoding.ASCII.GetString(all, 0, 14).TrimEnd('\0');
            string pass = Encoding.ASCII.GetString(all, 14, 16).TrimEnd('\0');
            return (user, pass);
        }

        private static async Task<byte[]> ReadExactAsync(NetworkStream s, int len)
        {
            byte[] buf = new byte[len];
            int off = 0;
            while (off < len)
            {
                int n = await s.ReadAsync(buf, off, len - off);
                if (n <= 0) return null;
                off += n;
            }
            return buf;
        }

        private static uint IPToUInt32(string ip)
        {
            var p = ip.Split('.');
            return (uint)(byte.Parse(p[0]) | (byte.Parse(p[1]) << 8) | (byte.Parse(p[2]) << 16) | (byte.Parse(p[3]) << 24));
        }

        private static string UInt32ToIP(uint u)
        {
            var b0 = (byte)(u & 0xFF);
            var b1 = (byte)((u >> 8) & 0xFF);
            var b2 = (byte)((u >> 16) & 0xFF);
            var b3 = (byte)((u >> 24) & 0xFF);
            return $"{b0}.{b1}.{b2}.{b3}";
        }

        // --------- opcodes ---------
        private const byte C_Login = 0x00;
        private const byte C_AboutToPlay = 0x02;
        private const byte C_ServerListExt = 0x05;

        private const byte S_ProtocolVer = 0x00;
        private const byte S_LoginOk = 0x03;
        private const byte S_ServerListEx = 0x04;
        private const byte S_PlayOk = 0x07;

        private const uint GamePort = 2603;
        private const uint ServerID = 0;
    }
}
