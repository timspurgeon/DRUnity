using System;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using Server.Common;
using Server.Net;
using System.IO;

namespace Server.Game
{
    public class GameServer
    {
        private readonly NetServer net;
        private readonly string bindIp;
        private readonly int port;

        private static int NextConnId = 1;
        private readonly ConcurrentDictionary<int, RRConnection> _connections = new();
        private readonly ConcurrentDictionary<int, string> _users = new();
        private readonly ConcurrentDictionary<int, uint> _peerId24 = new();
        private readonly ConcurrentDictionary<int, List<Server.Game.GCObject>> _playerCharacters = new();

        private readonly ConcurrentDictionary<int, bool> _charListSent = new();
        private readonly ConcurrentDictionary<string, List<Server.Game.GCObject>> _persistentCharacters = new();

        private bool _gameLoopRunning = false;
        private readonly object _gameLoopLock = new object();

        // MUST be false for the retail client
        private const bool DUPLICATE_AVATAR_RECORD = false;

        // === Python gateway constants (mirror gatewayserver.py) ===
        // In python: msgDest = b'\x01' + b'\x003'[:: -1] => 01 32 00  (LE u24 = 0x003201)
        //            msgSource = b'\xdd' + b'\x00{'[::-1] => dd 7b 00 (LE u24 = 0x007BDD)
        private const uint MSG_DEST = 0x003201; // bytes LE => 01 32 00
        private const uint MSG_SOURCE = 0x007BDD; // bytes LE => DD 7B 00

        // ===== Dump helper =====
        static class DumpUtil
        {
            static readonly uint[] _crcTable = InitCrc();
            static uint[] InitCrc()
            {
                const uint poly = 0xEDB88320u;
                var t = new uint[256];
                for (uint i = 0; i < 256; i++)
                {
                    uint c = i;
                    for (int k = 0; k < 8; k++) c = ((c & 1) != 0) ? (poly ^ (c >> 1)) : (c >> 1);
                    t[i] = c;
                }
                return t;
            }
            public static uint Crc32(ReadOnlySpan<byte> data)
            {
                uint crc = 0xFFFFFFFFu;
                foreach (var b in data) crc = _crcTable[(crc ^ b) & 0xFF] ^ (crc >> 8);
                return ~crc;
            }
            public static string DumpDir
            {
                get
                {
                    var d = Path.Combine(AppContext.BaseDirectory, "dump");
                    Directory.CreateDirectory(d);
                    return d;
                }
            }

            public static void WriteBytes(string path, byte[] bytes) => File.WriteAllBytes(path, bytes);
            public static void WriteText(string path, string text) => File.WriteAllText(path, text, new UTF8Encoding(false));
            public static void DumpBlob(string tag, string suffix, byte[] bytes)
            {
                string baseName = $"{DateTime.UtcNow:yyyyMMdd_HHmmssfff}_{tag}.{suffix}";
                string full = Path.Combine(DumpDir, baseName);
                WriteBytes(full, bytes);
                Debug.Log($"[DUMP] Wrote {suffix} -> {full} ({bytes.Length} bytes)");
            }
            public static void DumpCrc(string tag, string label, byte[] bytes)
            {
                uint crc = Crc32(bytes);
                string name = $"{DateTime.UtcNow:yyyyMMdd_HHmmssfff}_{tag}.{label}.crc32.txt";
                string path = Path.Combine(DumpDir, name);
                WriteText(path, $"0x{crc:X8}\nlen={bytes.Length}\n");
                Debug.Log($"[DUMP] CRC {label} 0x{crc:X8} (len={bytes.Length}) -> {path}");
            }
            public static void DumpFullFrame(string tag, byte[] payload)
            {
                try
                {
                    DumpBlob(tag, "unity.fullframe.bin", payload);
                    DumpCrc(tag, "fullframe", payload);
                    int head = Math.Min(32, payload.Length);
                    var sb = new StringBuilder(head * 3);
                    for (int i = 0; i < head; i++) sb.Append(payload[i].ToString("X2")).Append(' ');
                    Debug.Log($"[DUMP] {tag} fullframe head({head}): {sb.ToString().TrimEnd()}");
                }
                catch (Exception ex)
                {
                    Debug.LogWarning($"[DUMP] DumpFullFrame failed for '{tag}': {ex.Message}");
                }
            }
        }
        // ============================================================================

        public GameServer(string ip, int port)
        {
            bindIp = ip;
            this.port = port;
            net = new NetServer(ip, port, HandleClient);

            Debug.Log($"[INIT] DFC Active Version set to 0x{GCObject.DFC_VERSION:X2} ({GCObject.DFC_VERSION})");
        }

        public Task RunAsync()
        {
            Debug.Log($"<color=#9f9>[Game]</color> Listening on {bindIp}:{port}");
            StartGameLoop();
            return net.RunAsync();
        }

        private void StartGameLoop()
        {
            lock (_gameLoopLock)
            {
                if (_gameLoopRunning) return;
                _gameLoopRunning = true;
            }

            Task.Run(async () =>
            {
                Debug.Log("[Game] Game loop started");
                while (_gameLoopRunning)
                {
                    try
                    {
                        await Task.Delay(16);
                    }
                    catch (Exception ex)
                    {
                        Debug.LogError($"[Game] Game loop error: {ex}");
                    }
                }
                Debug.Log("[Game] Game loop stopped");
            });
        }

        private async Task HandleClient(TcpClient c)
        {
            var ep = c.Client.RemoteEndPoint?.ToString() ?? "unknown";
            int connId = Interlocked.Increment(ref NextConnId);
            Debug.Log($"<color=#9f9>[Game]</color> Connection from {ep} (ID={connId})");
            c.NoDelay = true;
            using var s = c.GetStream();
            var rrConn = new RRConnection(connId, c, s);
            _connections[connId] = rrConn;

            try
            {
                Debug.Log($"[Game] Client {connId} connected to gameserver");
                Debug.Log($"[Game] Client {connId} - Using improved stream protocol");
                byte[] buffer = new byte[10240];
                while (rrConn.IsConnected)
                {
                    Debug.Log($"[Game] Client {connId} - Reading data...");
                    int bytesRead = await s.ReadAsync(buffer, 0, buffer.Length);
                    if (bytesRead == 0)
                    {
                        Debug.LogWarning($"[Game] Client {connId} closed connection.");
                        break;
                    }
                    Debug.Log($"[Game] Client {connId} - Read {bytesRead} bytes");
                    Debug.Log($"[Game] Client {connId} - Data: {BitConverter.ToString(buffer, 0, bytesRead)}");
                    int probe = Math.Min(8, bytesRead);
                    if (probe > 0)
                        Debug.Log($"[Game] Client {connId} - First {probe} bytes: {BitConverter.ToString(buffer, 0, probe)}");
                    byte[] receivedData = new byte[bytesRead];
                    Buffer.BlockCopy(buffer, 0, receivedData, 0, bytesRead);
                    await ProcessReceivedData(rrConn, receivedData);
                }
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] Exception from {ep} (ID={connId}): {ex.Message}");
            }
            finally
            {
                rrConn.IsConnected = false;
                _connections.TryRemove(connId, out _);
                _users.TryRemove(connId, out _);
                _peerId24.TryRemove(connId, out _);
                _playerCharacters.TryRemove(connId, out _);
                Debug.Log($"[Game] Client {connId} disconnected");
            }
        }

        private async Task ProcessReceivedData(RRConnection conn, byte[] data)
        {
            Debug.Log($"[Game] ProcessReceivedData: Processing {data.Length} bytes for client {conn.ConnId}");

            try
            {
                await ReadPacket(conn, data);
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] ProcessReceivedData: Error processing data for client {conn.ConnId}: {ex.Message}");

                if (!string.IsNullOrEmpty(conn.LoginName))
                {
                    Debug.Log($"[Game] ProcessReceivedData: Sending keep-alive for authenticated client {conn.ConnId}");
                    try
                    {
                        await SendKeepAlive(conn);
                    }
                    catch (Exception keepAliveEx)
                    {
                        Debug.LogError($"[Game] ProcessReceivedData: Keep-alive failed for client {conn.ConnId}: {keepAliveEx.Message}");
                    }
                }
            }
        }

        private async Task SendKeepAlive(RRConnection conn)
        {
            Debug.Log($"[Game] SendKeepAlive: Sending keep-alive to client {conn.ConnId}");
            var keepAlive = new LEWriter();
            keepAlive.WriteByte(0);
            try
            {
                await SendMessage0x10(conn, 0xFF, keepAlive.ToArray(), "keepalive");
                Debug.Log($"[Game] SendKeepAlive: Keep-alive sent to client {conn.ConnId}");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] SendKeepAlive: Failed to send keep-alive to client {conn.ConnId}: {ex.Message}");
                throw;
            }
        }

        private async Task ReadPacket(RRConnection conn, byte[] data)
        {
            Debug.Log($"[Game] ReadPacket: Processing {data.Length} bytes for client {conn.ConnId}");

            if (data.Length == 0)
            {
                Debug.LogWarning($"[Game] ReadPacket: Empty data for client {conn.ConnId}");
                return;
            }

            var reader = new LEReader(data);
            byte msgType = reader.ReadByte();

            Debug.Log($"[Game] ReadPacket: Message type 0x{msgType:X2} for client {conn.ConnId}");
            Debug.Log($"[Game] ReadPacket: Login name = '{conn.LoginName}' (authenticated: {!string.IsNullOrEmpty(conn.LoginName)})");

            if (msgType != 0x0A && msgType != 0x0E && string.IsNullOrEmpty(conn.LoginName))
            {
                Debug.LogError($"[Game] ReadPacket: Received invalid message type 0x{msgType:X2} before login for client {conn.ConnId}");
                Debug.LogError($"[Game] ReadPacket: Only 0x0A/0x0E messages allowed before authentication!");
                return;
            }

            switch (msgType)
            {
                case 0x0A:
                    Debug.Log($"[Game] ReadPacket: Handling Compressed A (zlib3) message for client {conn.ConnId}");
                    await HandleCompressedA(conn, reader);
                    break;
                case 0x0E:
                    Debug.Log($"[Game] ReadPacket: Handling Compressed E (zlib1) message for client {conn.ConnId}");
                    await HandleCompressedE(conn, reader);
                    break;
                case 0x06:
                    Debug.Log($"[Game] ReadPacket: Handling Type 06 message for client {conn.ConnId}");
                    await HandleType06(conn, reader);
                    break;
                case 0x31:
                    Debug.Log($"[Game] ReadPacket: Handling Type 31 message for client {conn.ConnId}");
                    await HandleType31(conn, reader);
                    break;
                default:
                    Debug.LogWarning($"[Game] ReadPacket: Unhandled message type 0x{msgType:X2} for client {conn.ConnId}");
                    Debug.LogWarning($"[Game] ReadPacket: Full message hex: {BitConverter.ToString(data)}");
                    Debug.LogWarning($"[Game] ReadPacket: First 32 bytes: {BitConverter.ToString(data, 0, Math.Min(32, data.Length))}");
                    break;
            }
        }

        private async Task HandleCompressedA(RRConnection conn, LEReader reader)
        {
            Debug.Log($"[Game] HandleCompressedA: Starting for client {conn.ConnId}");
            Debug.Log($"[Game] HandleCompressedA: Remaining bytes: {reader.Remaining}");

            // Python zlib3 format:
            // [0x0A][msgDest:u24][compLen:u32][(if 0x0A) 00 03 00 else msgSource:u24][unclen:u32][zlib...]
            const int MIN_HDR = 3 + 4 + 3 + 4; // rough min once we know branch
            if (reader.Remaining < MIN_HDR)
            {
                Debug.LogError($"[Game] HandleCompressedA: Insufficient data, have {reader.Remaining}");
            }

            // We still keep peer24 for downstream since client will send it on 0x0E too
            // but for A(0x0A) we don't strictly need to parse all subfields here for routing;
            // just decompress and forward to the inner dispatcher (same as before).
            // For brevity we reuse previous parsing path that expected:
            // [peer:u24][packetLen:u32][dest:u8][sub:u8][zero:u8][unclen:u32][zlib...]
            // but we only support the branch we generate (00 03 00).
            // If your client actually sends A-frames, keep existing inflate path:
            if (reader.Remaining < (3 + 4)) return;

            uint peer = reader.ReadUInt24();
            _peerId24[conn.ConnId] = peer;

            uint compPlus7 = reader.ReadUInt32();
            int compLen = (int)compPlus7 - 7;
            if (compLen < 0) { Debug.LogError("[Game] HandleCompressedA: bad compLen"); return; }

            if (reader.Remaining < 3 + 4 + compLen) { /* minimal check */ }

            byte dest = reader.ReadByte(); // expected 0x00
            byte sub = reader.ReadByte(); // expected 0x03
            byte zero = reader.ReadByte(); // expected 0x00
            uint unclen = reader.ReadUInt32();
            byte[] comp = reader.ReadBytes(compLen);
            byte[] inner;
            try
            {
                inner = (compLen == 0 || unclen == 0) ? Array.Empty<byte>() : ZlibUtil.Inflate(comp, unclen);
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] HandleCompressedA: Decompression failed: {ex.Message}");
                return;
            }

            // In our use, A/0x03 is just small advancement signals; route to same handler:
            await ProcessUncompressedMessage(conn, dest, sub, inner);
        }

        // ===================== zlib1 E-lane (the IMPORTANT fix) ====================
        // Python send_zlib1 format:
        // [0x0E]
        // [msgDest:u24]
        // [compressedLen:u24]
        // [0x00]
        // [msgSource:u24]
        // [0x01 0x00 0x01 0x00 0x00]
        // [uncompressedLen:u32]
        // [zlib(inner)]
        private (byte[] payload, byte[] compressed) BuildCompressedEPayload_Zlib1(byte[] innerData)
        {
            byte[] z = ZlibUtil.Deflate(innerData);
            int compressedLen = z.Length + 12; // python: len(zlibMsg) + 12

            var w = new LEWriter();
            w.WriteByte(0x0E);
            w.WriteUInt24((int)MSG_DEST);              // msgDest
            w.WriteUInt24(compressedLen);              // 3-byte comp len
            w.WriteByte(0x00);
            w.WriteUInt24((int)MSG_SOURCE);            // msgSource
            // python literal: b'\x01\x00\x01\x00\x00'
            w.WriteByte(0x01);
            w.WriteByte(0x00);
            w.WriteByte(0x01);
            w.WriteByte(0x00);
            w.WriteByte(0x00);
            w.WriteUInt32((uint)innerData.Length);     // uncompressed size
            w.WriteBytes(z);

            return (w.ToArray(), z);
        }

        // We keep an A-lane helper too, but match python's zlib3 packing when WE send:
        // Python send_zlib3 (for pktType==0x0A):
        // [0x0A][msgDest:u24][compLen:u32][00 03 00][unclen:u32][zlib...]
        private (byte[] payload, byte[] compressed) BuildCompressedAPayload_Zlib3(byte[] innerData)
        {
            byte[] z = ZlibUtil.Deflate(innerData);
            var w = new LEWriter();
            w.WriteByte(0x0A);
            w.WriteUInt24((int)MSG_DEST);              // msgDest
            w.WriteUInt32((uint)(z.Length + 7));       // comp len (+7)
            w.WriteByte(0x00);                         // 00 03 00  (python fixed)
            w.WriteByte(0x03);
            w.WriteByte(0x00);
            w.WriteUInt32((uint)innerData.Length);     // uncompressed size
            w.WriteBytes(z);
            return (w.ToArray(), z);
        }

        // --------------- SEND helpers (now split: A=zlib3, E=zlib1) ----------------
        private async Task SendCompressedEResponse(RRConnection conn, byte[] innerData)
        {
            try
            {
                var (payload, z) = BuildCompressedEPayload_Zlib1(innerData);
                Debug.Log($"[SEND][E/zlib1] comp={z.Length} unclen={innerData.Length}");
                await conn.Stream.WriteAsync(payload, 0, payload.Length);
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Wire][E] send failed: {ex.Message}");
            }
        }

        private async Task SendCompressedEResponseWithDump(RRConnection conn, byte[] innerData, string tag)
        {
            try
            {
                DumpUtil.DumpBlob(tag, "unity.uncompressed.bin", innerData);
                DumpUtil.DumpCrc(tag, "uncompressed", innerData);
                var (payload, z) = BuildCompressedEPayload_Zlib1(innerData);
                DumpUtil.DumpBlob(tag, "unity.compressed.bin", z);
                DumpUtil.DumpCrc(tag, "compressed", z);
                DumpUtil.DumpBlob(tag, "unity.fullframe.bin", payload);
                DumpUtil.DumpCrc(tag, "fullframe", payload);
                await conn.Stream.WriteAsync(payload, 0, payload.Length);
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Wire][E] dump/send failed: {ex.Message}");
            }
        }

        private async Task SendCompressedAResponse(RRConnection conn, byte[] innerData)
        {
            try
            {
                var (payload, z) = BuildCompressedAPayload_Zlib3(innerData);
                Debug.Log($"[SEND][A/zlib3] comp={z.Length} unclen={innerData.Length}");
                await conn.Stream.WriteAsync(payload, 0, payload.Length);
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Wire][A] send failed: {ex.Message}");
            }
        }

        private async Task SendCompressedAResponseWithDump(RRConnection conn, byte[] innerData, string tag)
        {
            try
            {
                DumpUtil.DumpBlob(tag, "unity.uncompressed.bin", innerData);
                DumpUtil.DumpCrc(tag, "uncompressed", innerData);
                var (payload, z) = BuildCompressedAPayload_Zlib3(innerData);
                DumpUtil.DumpBlob(tag, "unity.compressed.bin", z);
                DumpUtil.DumpCrc(tag, "compressed", z);
                DumpUtil.DumpBlob(tag, "unity.fullframe.bin", payload);
                DumpUtil.DumpCrc(tag, "fullframe", payload);
                await conn.Stream.WriteAsync(payload, 0, payload.Length);
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Wire][A] dump/send failed: {ex.Message}");
            }
        }
        // ---------------------------------------------------------------------------

        private async Task ProcessUncompressedMessage(RRConnection conn, byte dest, byte msgTypeA, byte[] uncompressed)
        {
            Debug.Log($"[Game] ProcessUncompressedMessage: A-lane dest=0x{dest:X2} sub=0x{msgTypeA:X2}");

            switch (msgTypeA)
            {
                case 0x00: // initial login blob
                    await HandleInitialLogin(conn, uncompressed);
                    break;

                case 0x02: // ticks
                    // echo empty 0x02 on A using zlib3 python layout
                    await SendCompressedAResponseWithDump(conn, Array.Empty<byte>(), "a02_empty");
                    break;

                case 0x03: // session token style
                    if (uncompressed.Length >= 4)
                    {
                        var reader = new LEReader(uncompressed);
                        uint sessionToken = reader.ReadUInt32();
                        if (GlobalSessions.TryConsume(sessionToken, out var user) && !string.IsNullOrEmpty(user))
                        {
                            conn.LoginName = user;
                            _users[conn.ConnId] = user;

                            var ack = new LEWriter();
                            ack.WriteByte(0x03);
                            await SendMessage0x10(conn, 0x0A, ack.ToArray(), "msg10_auth_ack");

                            // Immediately tick E so client advances like python does
                            await SendCompressedEResponseWithDump(conn, Array.Empty<byte>(), "e_hello_tick");

                            await Task.Delay(50);
                            await StartCharacterFlow(conn);
                        }
                        else
                        {
                            Debug.LogError($"[Game] A/0x03 invalid session token 0x{sessionToken:X8}");
                        }
                    }
                    break;

                case 0x0F:
                    await HandleChannelMessage(conn, uncompressed);
                    break;

                default:
                    Debug.LogWarning($"[Game] Unhandled A sub=0x{msgTypeA:X2}");
                    break;
            }
        }

        private async Task HandleInitialLogin(RRConnection conn, byte[] data)
        {
            Debug.Log($"[Game] HandleInitialLogin: ENTRY client {conn.ConnId}");
            if (data.Length < 5)
            {
                Debug.LogError($"[Game] HandleInitialLogin: need 5 bytes, have {data.Length}");
                return;
            }

            var reader = new LEReader(data);
            byte subtype = reader.ReadByte();
            uint oneTimeKey = reader.ReadUInt32();

            if (!GlobalSessions.TryConsume(oneTimeKey, out var user) || string.IsNullOrEmpty(user))
            {
                Debug.LogError($"[Game] HandleInitialLogin: Invalid OneTimeKey 0x{oneTimeKey:X8}");
                return;
            }

            conn.LoginName = user;
            _users[conn.ConnId] = user;
            Debug.Log($"[Game] HandleInitialLogin: SUCCESS user '{user}'");

            var ack = new LEWriter();
            ack.WriteByte(0x03);
            await SendMessage0x10(conn, 0x0A, ack.ToArray(), "msg10_auth_ack_initial");

            // prime E-lane per gateway
            await SendCompressedEResponseWithDump(conn, Array.Empty<byte>(), "e_hello_tick");

            // small A/0x03 advance (compatible with our zlib3 builder)
            var advance = new LEWriter();
            advance.WriteUInt24(0x00B2B3B4);
            advance.WriteByte(0x00);
            await SendCompressedAResponseWithDump(conn, advance.ToArray(), "advance_a03");

            // A/0x02 nudge
            await SendCompressedAResponseWithDump(conn, Array.Empty<byte>(), "nudge_a02");
            await Task.Delay(75);

            await StartCharacterFlow(conn);
        }

        private async Task HandleChannelMessage(RRConnection conn, byte[] data)
        {
            if (data.Length < 2) return;
            byte channel = data[0];
            byte messageType = data[1];

            switch (channel)
            {
                case 4:
                    switch (messageType)
                    {
                        case 0: // CharacterConnected request from client
                            await SendCharacterConnectedResponse(conn);
                            break;

                        case 1: // UI nudge 4/1 -> send tiny ack on E
                            {
                                var ack = new LEWriter();
                                ack.WriteByte(4);
                                ack.WriteByte(1);
                                ack.WriteUInt32(0);
                                await SendCompressedEResponseWithDump(conn, ack.ToArray(), "char_ui_nudge_4_1_ack");
                                break;
                            }

                        case 3: // Get list
                            await SendCharacterList(conn);
                            break;

                        case 5: // Play
                            await HandleCharacterPlay(conn, data);
                            break;

                        case 2: // Create
                            await HandleCharacterCreate(conn, data);
                            break;

                        default:
                            Debug.LogWarning($"[Game] Unhandled char msg 0x{messageType:X2}");
                            break;
                    }
                    break;

                case 9:
                    await HandleGroupChannelMessages(conn, messageType);
                    break;

                case 13:
                    await HandleZoneChannelMessages(conn, messageType, data);
                    break;

                default:
                    Debug.LogWarning($"[Game] Unhandled channel {channel}");
                    break;
            }
        }

        // Character flow now **sends on E-lane/zlib1**
        private async Task StartCharacterFlow(RRConnection conn)
        {
            Debug.Log($"[Game] StartCharacterFlow: client {conn.ConnId} ({conn.LoginName})");

            await Task.Delay(50);

            var sent = await EnsurePeerThenSendCharConnected(conn);
            if (!sent)
            {
                Debug.LogWarning("[Game] StartCharacterFlow: 4/0 deferred; nudging...");
            }

            // keep one gentle tick on A per python gateway behavior
            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(500);
                    await SendCompressedAResponseWithDump(conn, Array.Empty<byte>(), "tick_a02_500ms");
                    await Task.Delay(500);
                    if (!_charListSent.TryGetValue(conn.ConnId, out var flag) || !flag)
                        await SendCompressedAResponseWithDump(conn, Array.Empty<byte>(), "tick_a02_1000ms");
                }
                catch (Exception ex) { Debug.LogWarning($"[Game] A/0x02 tick failed: {ex.Message}"); }
            });
        }

        private async Task SendCharacterConnectedResponse(RRConnection conn)
        {
            Debug.Log($"[Game] SendCharacterConnectedResponse: *** ENTRY (DFC-style) *** For client {conn.ConnId}");

            try
            {
                const int count = 2;
                if (!_persistentCharacters.ContainsKey(conn.LoginName))
                {
                    _persistentCharacters[conn.LoginName] = new List<Server.Game.GCObject>(capacity: count);
                    Debug.Log($"[Game] SendCharacterConnectedResponse: Created character list for {conn.LoginName}");
                }

                var list = _persistentCharacters[conn.LoginName];
                while (list.Count < count)
                {
                    try
                    {
                        var p = Server.Game.Objects.NewPlayer(conn.LoginName);
                        p.ID = (uint)Server.Game.Objects.NewID();
                        list.Add(p);
                        Debug.Log($"[Game] SendCharacterConnectedResponse: Added DFC player stub ID=0x{p.ID:X8}");
                    }
                    catch (Exception ex)
                    {
                        Debug.LogError($"[Game] SendCharacterConnectedResponse: ERROR creating player stub: {ex.Message}");
                        Debug.LogError(ex.StackTrace);
                        break;
                    }
                }

                var body = new LEWriter();
                body.WriteByte(4);
                body.WriteByte(0);
                var inner = body.ToArray();

                Debug.Log($"[SEND][inner][4/0] {BitConverter.ToString(inner)} (len={inner.Length})");
                Debug.Log($"[SEND][E][prep] 4/0 using peer=0x{GetClientId24(conn.ConnId):X6} dest=0x01 sub=0x0F innerLen={inner.Length}");

                await SendCompressedEResponseWithDump(conn, inner, "char_connected");
                Debug.Log("[Game] SendCharacterConnectedResponse: *** SUCCESS *** Sent DFC-compatible 4/0 (E-lane)");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] SendCharacterConnectedResponse: *** CRITICAL EXCEPTION *** {ex.Message}");
                Debug.LogError(ex.StackTrace);
            }
        }

        private void WriteGoSendPlayer(LEWriter body, Server.Game.GCObject character)
        {
            try
            {
                long startPos = body.ToArray().Length;

                // CRITICAL FIX: First create the Avatar
                var avatar = Server.Game.Objects.LoadAvatar();

                // CRITICAL FIX: Add the Avatar to the Player first
                character.AddChild(avatar);

                // CRITICAL FIX: Then add the ProcModifier to the Player
                var procMod = Server.Game.Objects.NewProcModifier();
                character.AddChild(procMod);

                // Log the UnitContainer children before serialization
                var unitContainer = avatar.Children?.FirstOrDefault(c => c.NativeClass == "UnitContainer");
                if (unitContainer != null)
                {
                    Debug.Log($"[DFC][UnitContainer] ChildCount(before)={unitContainer.Children?.Count ?? 0}");
                    if (unitContainer.Children != null)
                    {
                        for (int i = 0; i < unitContainer.Children.Count; i++)
                        {
                            var child = unitContainer.Children[i];
                            Debug.Log($"[DFC][UnitContainer] Child[{i}] native='{child.NativeClass}' gc='{child.GCClass}'");
                        }
                    }
                }

                Debug.Log($"[Game] WriteGoSendPlayer: Writing character ID={character.ID} with DFC format");
                character.WriteFullGCObject(body);

                long afterPlayer = body.ToArray().Length;
                Debug.Log($"[Game] WriteGoSendPlayer: Player DFC write bytes={afterPlayer - startPos}");

                if (DUPLICATE_AVATAR_RECORD)
                {
                    Debug.Log("[Game] WriteGoSendPlayer: DUPLICATE_AVATAR_RECORD=true, adding standalone avatar");

                    long startAv = body.ToArray().Length;
                    avatar.WriteFullGCObject(body);
                    long afterAv = body.ToArray().Length;
                    Debug.Log($"[Game] WriteGoSendPlayer: Standalone Avatar DFC write bytes={afterAv - startAv}");

                    body.WriteByte(0x01);
                    body.WriteByte(0x01);
                    body.WriteBytes(Encoding.UTF8.GetBytes("Normal"));
                    body.WriteByte(0x00);
                    body.WriteByte(0x01);
                    body.WriteByte(0x01);
                    body.WriteUInt32(0x01);
                }
                else
                {
                    Debug.Log("[Game] WriteGoSendPlayer: Sending only Player with Avatar child (DFC format), no tail");
                }
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] WriteGoSendPlayer: EXCEPTION {ex.Message}");
                Debug.LogError($"[Game] WriteGoSendPlayer: Stack trace: {ex.StackTrace}");
            }
        }

        private async Task SendCharacterList(RRConnection conn)
        {
            Debug.Log($"[Game] SendCharacterList: *** ENTRY *** DFC format with djb2 hashes");

            try
            {
                if (!_persistentCharacters.TryGetValue(conn.LoginName, out var characters))
                {
                    Debug.LogError($"[Game] SendCharacterList: *** ERROR *** No characters found for {conn.LoginName}");
                    return;
                }

                Debug.Log($"[Game] SendCharacterList: *** FOUND CHARACTERS *** Count: {characters.Count} for {conn.LoginName}");

                int count = characters.Count;
                if (count > 255)
                {
                    Debug.LogWarning($"[Game] SendCharacterList: Character count {count} exceeds 255; clamping to 255 for wire format");
                    count = 255;
                }

                var body = new LEWriter();
                body.WriteByte(4);
                body.WriteByte(3);
                body.WriteByte((byte)count);

                Debug.Log($"[Game] SendCharacterList: *** WRITING DFC CHARACTERS *** Processing {count} characters");

                for (int i = 0; i < count; i++)
                {
                    var character = characters[i];
                    Debug.Log($"[Game] SendCharacterList: *** CHARACTER {i + 1} *** ID: {character.ID}, Writing DFC character data");

                    try
                    {
                        body.WriteUInt32(character.ID);
                        Debug.Log($"[Game] SendCharacterList: *** CHARACTER {i + 1} *** wrote ID={character.ID}");

                        WriteGoSendPlayer(body, character);
                        Debug.Log($"[Game] SendCharacterList: *** CHARACTER {i + 1} *** DFC WriteGoSendPlayer complete; current bodyLen={body.ToArray().Length}");
                    }
                    catch (Exception charEx)
                    {
                        Debug.LogError($"[Game] SendCharacterList: *** ERROR CHARACTER {i + 1} *** {charEx.Message}");
                        Debug.LogError($"[Game] SendCharacterList: *** CHARACTER {i + 1} STACK TRACE *** {charEx.StackTrace}");
                    }
                }

                var inner = body.ToArray();
                Debug.Log($"[Game] SendCharacterList: *** SENDING DFC MESSAGE *** Total body length: {inner.Length} bytes");
                Debug.Log($"[SEND][inner] CH=4,TYPE=3 DFC: {BitConverter.ToString(inner)} (len={inner.Length})");

                if (!(inner.Length >= 3 && inner[0] == 0x04 && inner[1] == 0x03))
                {
                    Debug.LogError($"[Game][FATAL] SendCharacterList header wrong: {BitConverter.ToString(inner, 0, Math.Min(inner.Length, 8))}");
                }
                else
                {
                    Debug.Log($"[Game] SendCharacterList: Header OK -> 04-03 count={inner[2]} (DFC format)");
                }

                int head = Math.Min(32, inner.Length);
                Debug.Log($"[Game] SendCharacterList: First {head} bytes: {BitConverter.ToString(inner, 0, head)}");

                Debug.Log($"[SEND][E][prep] 4/3 DFC using peer=0x{GetClientId24(conn.ConnId):X6} dest=0x01 sub=0x0F innerLen={inner.Length}");
                await SendCompressedEResponseWithDump(conn, inner, "charlist");
                Debug.Log($"[Game] SendCharacterList: *** SUCCESS *** Sent DFC format with djb2 hashes, {count} characters");

                _charListSent[conn.ConnId] = true;
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] SendCharacterList: *** CRITICAL EXCEPTION *** {ex.Message}");
                Debug.LogError($"[Game] SendCharacterList: *** STACK TRACE *** {ex.StackTrace}");
            }
        }


        private async Task SendToCharacterCreation(RRConnection conn)
        {
            var create = new LEWriter();
            create.WriteByte(4);
            create.WriteByte(4);
            await SendCompressedEResponseWithDump(conn, create.ToArray(), "char_creation_4_4");
        }

        private async Task HandleCharacterPlay(RRConnection conn, byte[] data)
        {
            var r = new LEReader(data);
            if (r.Remaining < 3) { await SendPlayFallback(); return; }
            byte ch = r.ReadByte();
            byte mt = r.ReadByte();
            if (ch != 0x04 || mt != 0x05) { await SendPlayFallback(); return; }
            if (r.Remaining < 1) { await SendPlayFallback(); return; }

            byte slot = r.ReadByte();
            if (!_persistentCharacters.TryGetValue(conn.LoginName, out var chars) || slot >= chars.Count)
            {
                await SendPlayFallback();
                return;
            }

            var w = new LEWriter();
            w.WriteByte(4);
            w.WriteByte(5);
            await SendCompressedEResponseWithDump(conn, w.ToArray(), "char_play_ack_4_5");

            await Task.Delay(100);
            await SendGroupConnectedResponse(conn);
            return;

            async Task SendPlayFallback()
            {
                var fb = new LEWriter();
                fb.WriteByte(4);
                fb.WriteByte(5);
                fb.WriteByte(1);
                await SendCompressedEResponseWithDump(conn, fb.ToArray(), "char_play_fallback");
            }
        }

        private async Task<bool> WaitForPeer24(RRConnection conn, int msTimeout = 1500, int pollMs = 10)
        {
            int waited = 0;
            while (waited < msTimeout)
            {
                if (_peerId24.TryGetValue(conn.ConnId, out var pid) && pid != 0u)
                {
                    Debug.Log($"[Wire] WaitForPeer24: got peer=0x{pid:X6} after {waited}ms");
                    return true;
                }
                await Task.Delay(pollMs);
                waited += pollMs;
            }
            Debug.LogWarning($"[Wire] WaitForPeer24: timed out after {msTimeout}ms; peer unknown");
            return false;
        }

        private async Task<bool> EnsurePeerThenSendCharConnected(RRConnection conn)
        {
            await WaitForPeer24(conn);
            // Even if peer isn't known yet, E-lane uses MSG_SOURCE/DEST constants (gateway semantics),
            // so we go ahead and send 4/0 to wake the Character UI.
            await SendCharacterConnectedResponse(conn);
            return true;
        }

        private async Task InitiateWorldEntry(RRConnection conn)
        {
            await SendGoToZone(conn, "Town");
        }

        private async Task HandleCharacterCreate(RRConnection conn, byte[] data)
        {
            Debug.Log($"[Game] HandleCharacterCreate: Character creation request from client {conn.ConnId}");
            Debug.Log($"[Game] HandleCharacterCreate: Data ({data.Length} bytes): {BitConverter.ToString(data)}");

            string characterName = $"{conn.LoginName}_NewHero";
            uint newCharId = (uint)(conn.ConnId * 100 + 1);

            try
            {
                var newCharacter = Server.Game.Objects.NewPlayer(characterName);
                newCharacter.ID = newCharId;
                Debug.Log($"[Game] HandleCharacterCreate: Created DFC character with ID={newCharId}");

                if (!_persistentCharacters.TryGetValue(conn.LoginName, out var existing))
                {
                    existing = new List<Server.Game.GCObject>();
                    _persistentCharacters[conn.LoginName] = existing;
                    Debug.Log($"[Game] HandleCharacterCreate: No existing list for {conn.LoginName}; created new list");
                }

                existing.Add(newCharacter);
                Debug.Log($"[Game] HandleCharacterCreate: Persisted new DFC character (ID: {newCharId}) for {conn.LoginName}. Total now: {existing.Count}");
            }
            catch (Exception persistEx)
            {
                Debug.LogError($"[Game] HandleCharacterCreate: *** ERROR persisting DFC character *** {persistEx.Message}");
                Debug.LogError($"[Game] HandleCharacterCreate: *** STACK TRACE *** {persistEx.StackTrace}");
            }

            var response = new LEWriter();
            response.WriteByte(4);
            response.WriteByte(2);
            response.WriteByte(1);
            response.WriteUInt32(newCharId);

            await SendCompressedEResponseWithDump(conn, response.ToArray(), "char_create_4_2");
            Debug.Log($"[Game] HandleCharacterCreate: Sent DFC character creation success for {characterName} (ID: {newCharId})");

            await Task.Delay(100);
            await SendUpdatedCharacterList(conn, newCharId, characterName);
        }

        private async Task SendUpdatedCharacterList(RRConnection conn, uint charId, string charName)
        {
            Debug.Log($"[Game] SendUpdatedCharacterList: Sending DFC list with newly created character");

            try
            {
                if (!_persistentCharacters.TryGetValue(conn.LoginName, out var chars))
                {
                    Debug.LogWarning($"[Game] SendUpdatedCharacterList: No persistent list found after create; falling back to single DFC entry build");
                    var w = new LEWriter();
                    w.WriteByte(4);
                    w.WriteByte(3);
                    w.WriteByte(1);

                    var newCharacter = Server.Game.Objects.NewPlayer(charName);
                    newCharacter.ID = charId;
                    w.WriteUInt32(charId);
                    WriteGoSendPlayer(w, newCharacter);

                    var innerSingle = w.ToArray();
                    Debug.Log($"[SEND][inner] CH=4,TYPE=3 (updated single DFC) : {BitConverter.ToString(innerSingle)} (len={innerSingle.Length})");
                    Debug.Log($"[SEND][E][prep] 4/3(DFC SINGLE) peer=0x{GetClientId24(conn.ConnId):X6} dest=0x01 sub=0x0F innerLen={innerSingle.Length}");

                    await SendCompressedEResponseWithDump(conn, innerSingle, "charlist_single");
                    Debug.Log($"[Game] SendUpdatedCharacterList: Sent updated DFC character list (SINGLE fallback) with new character (ID {charId})");
                    return;
                }
                else
                {
                    Debug.Log($"[Game] SendUpdatedCharacterList: Found persistent list (count={chars.Count}); delegating to SendCharacterList() for DFC format");
                }
            }
            catch (Exception ex)
            {
                Debug.LogWarning($"[Game] SendUpdatedCharacterList: Pre-flight check warning: {ex.Message}");
            }

            await SendCharacterList(conn);
        }

        private async Task SendGroupConnectedResponse(RRConnection conn)
        {
            var w = new LEWriter();
            w.WriteByte(9);
            w.WriteByte(0);
            await SendCompressedEResponseWithDump(conn, w.ToArray(), "group_connected_9_0");

            await Task.Delay(50);
            await SendGoToZone(conn, "Town");
        }

        private async Task HandleGroupChannelMessages(RRConnection conn, byte messageType)
        {
            switch (messageType)
            {
                case 0:
                    await SendGoToZone(conn, "Town");
                    break;
                default:
                    Debug.LogWarning($"[Game] Unhandled group msg 0x{messageType:X2}");
                    break;
            }
        }


        private async Task SendGoToZone(RRConnection conn, string zoneName)
        {
            Debug.Log($"[Game] SendGoToZone: Sending player to zone '{zoneName}'");

            try
            {
                // Step 1: Send group server connect message (similar to GO repo)
                var groupWriter = new LEWriter();
                groupWriter.WriteByte(9);  // Group server channel
                groupWriter.WriteByte(48); // Connect message type
                groupWriter.WriteUInt32(33752069); // CHAR_ID from GO repo
                groupWriter.WriteByte(1);  // Additional data from GO repo
                groupWriter.WriteByte(1);  // Additional data from GO repo
                await SendCompressedEResponseWithDump(conn, groupWriter.ToArray(), "group_connect_9_48");

                // Wait for client to process
                await Task.Delay(300);

                // Step 2: Send the goto_zone message with additional data like GO repo
                var w = new LEWriter();
                w.WriteByte(13);  // Zone server channel
                w.WriteByte(0);   // Connect message type
                w.WriteCString(zoneName); // Use WriteCString for proper null-terminated string
                w.WriteUInt32(30);  // Additional data from GO repo (0x1E)
                w.WriteByte(0);     // Number of something
                w.WriteUInt32(1);   // Townston (2)
                await SendCompressedEResponseWithDump(conn, w.ToArray(), "zone_connect_13_0");

                // Wait for client to process the zone connect message
                await Task.Delay(1000); // Increased delay to give client time to load the zone

                // Step 3: Send entity manager interval message
                await SendEntityManagerInterval(conn);

                // Wait for client to process
                await Task.Delay(300);

                // Step 4: Send entity manager random seed message
                await SendEntityManagerRandomSeed(conn);

                // Wait for client to process
                await Task.Delay(300);

                // Step 5: Send entity manager entity create init message
                await SendEntityManagerEntityCreateInit(conn);

                // Wait for client to process
                await Task.Delay(300);

                // Step 6: Send entity manager component create message
                await SendEntityManagerComponentCreate(conn);

                // Wait for client to process
                await Task.Delay(300);

                // Step 7: Send entity manager connect message
                await SendEntityManagerConnect(conn);

                // Wait for client to process
                await Task.Delay(300);

                // Step 8: Send zone ready message
                var zoneReadyWriter = new LEWriter();
                zoneReadyWriter.WriteByte(13);  // Zone channel
                zoneReadyWriter.WriteByte(1);   // Ready message type (FUNC_ZONESERVER_READY)
                zoneReadyWriter.WriteUInt32(33752069); // CHAR_ID from GO repo
                await SendCompressedEResponseWithDump(conn, zoneReadyWriter.ToArray(), "zone_ready_13_1");

                // Wait for client to process
                await Task.Delay(300);

                // Step 9: Send zone instance count message
                var instanceCountWriter = new LEWriter();
                instanceCountWriter.WriteByte(13);  // Zone channel
                instanceCountWriter.WriteByte(5);   // Instance count message type
                instanceCountWriter.WriteUInt32(1);  // Number of instances
                instanceCountWriter.WriteUInt32(1);  // Additional data from GO repo
                await SendCompressedEResponseWithDump(conn, instanceCountWriter.ToArray(), "zone_instance_count_13_5");

                Debug.Log($"[Game] SendGoToZone: Completed zone entry sequence for '{zoneName}'");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] SendGoToZone: Error during zone entry sequence: {ex.Message}");
            }
        }

        private async Task HandleZoneChannelMessages(RRConnection conn, byte messageType, byte[] data)
        {
            switch (messageType)
            {
                case 6: await HandleZoneJoin(conn); break;
                case 8: await HandleZoneReady(conn); break;
                case 0: await HandleZoneConnected(conn); break;
                case 1: await HandleZoneReadyResponse(conn); break;
                case 5: await HandleZoneInstanceCount(conn); break;
                default:
                    Debug.LogWarning($"[Game] Unhandled zone msg 0x{messageType:X2}");
                    break;
            }
        }

        private async Task HandleZoneJoin(RRConnection conn)
        {
            Debug.Log($"[Game] HandleZoneJoin: Sending zone join message to client {conn.ConnId}");

            var w = new LEWriter();
            w.WriteByte(13);
            w.WriteByte(1);
            w.WriteUInt32(1);
            w.WriteUInt16(0x12);
            for (int i = 0; i < 0x12; i++) w.WriteUInt32(0xFFFFFFFF);

            // Send the message
            await SendCompressedEResponseWithDump(conn, w.ToArray(), "zone_join_13_1");
        }

        private async Task HandleZoneConnected(RRConnection conn)
        {
            Debug.Log($"[Game] HandleZoneConnected: Sending zone connected message to client {conn.ConnId}");

            // Keep this message simple - the client expects just the channel and message type
            var w = new LEWriter();
            w.WriteByte(13);  // Zone channel
            w.WriteByte(0);   // Connected message type

            // No additional data - the client is expecting a simple message

            await SendCompressedEResponseWithDump(conn, w.ToArray(), "zone_connected_13_0");

            Debug.Log($"[Game] HandleZoneConnected: Zone connected message sent to client {conn.ConnId}");
        }

        private async Task HandleZoneReady(RRConnection conn)
        {
            Debug.Log($"[Game] HandleZoneReady: Sending zone ready message to client {conn.ConnId}");

            var w = new LEWriter();
            w.WriteByte(13);  // Zone channel
            w.WriteByte(8);   // Ready message type

            await SendCompressedEResponseWithDump(conn, w.ToArray(), "zone_ready_13_8");

            Debug.Log($"[Game] HandleZoneReady: Zone ready message sent to client {conn.ConnId}");
        }

        private async Task HandleZoneReadyResponse(RRConnection conn)
        {
            var w = new LEWriter();
            w.WriteByte(13);
            w.WriteByte(1);
            await SendCompressedEResponseWithDump(conn, w.ToArray(), "zone_ready_resp_13_1");
        }

        private async Task HandleZoneInstanceCount(RRConnection conn)
        {
            Debug.Log($"[Game] HandleZoneInstanceCount: Sending zone instance count message to client {conn.ConnId}");

            var w = new LEWriter();
            w.WriteByte(13);  // Zone channel
            w.WriteByte(5);   // Instance count message type
            w.WriteUInt32(1);  // Number of instances

            await SendCompressedEResponseWithDump(conn, w.ToArray(), "zone_instance_count_13_5");

            Debug.Log($"[Game] HandleZoneInstanceCount: Zone instance count message sent to client {conn.ConnId}");
        }

        private async Task HandleType31(RRConnection conn, LEReader reader)
        {
            // unchanged  logs + ack
            Debug.Log($"[Game] HandleType31: remaining {reader.Remaining}");
            await SendType31Ack(conn);
        }

        private async Task SendType31Ack(RRConnection conn)
        {
            try
            {
                var response = new LEWriter();
                response.WriteByte(4);
                response.WriteByte(1);
                response.WriteUInt32(0);
                await SendCompressedEResponseWithDump(conn, response.ToArray(), "type31_ack");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] SendType31Ack: {ex.Message}");
            }
        }

        // ===================== E-lane receive/dispatch ==============================
        private async Task HandleCompressedE(RRConnection conn, LEReader reader)
        {
            Debug.Log($"[Game] HandleCompressedE: (zlib1) remaining={reader.Remaining}");

            // parse zlib1 header that client sends back (mirror our BuildCompressedEPayload_Zlib1)
            const int MIN_HDR = 3 + 3 + 1 + 3 + 5 + 4;
            if (reader.Remaining < MIN_HDR)
            {
                Debug.LogError($"[Game] HandleCompressedE: insufficient {reader.Remaining}");
                return;
            }

            uint msgDest = reader.ReadUInt24();
            uint compLen = reader.ReadUInt24();
            byte zero = reader.ReadByte();
            uint msgSource = reader.ReadUInt24();
            byte b1 = reader.ReadByte(); // 01
            byte b2 = reader.ReadByte(); // 00
            byte b3 = reader.ReadByte(); // 01
            byte b4 = reader.ReadByte(); // 00
            byte b5 = reader.ReadByte(); // 00
            uint unclen = reader.ReadUInt32();

            int zLen = (int)compLen - 12;
            if (zLen < 0 || reader.Remaining < zLen)
            {
                Debug.LogError($"[Game] HandleCompressedE: bad zLen={zLen} remaining={reader.Remaining}");
                return;
            }

            byte[] comp = reader.ReadBytes(zLen);
            byte[] inner;
            try
            {
                inner = (zLen == 0 || unclen == 0) ? Array.Empty<byte>() : ZlibUtil.Inflate(comp, unclen);
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] HandleCompressedE: inflate failed {ex.Message}");
                return;
            }

            // inner begins with [channel][type]...
            await ProcessUncompressedEMessage(conn, inner);
        }

        private async Task ProcessUncompressedEMessage(RRConnection conn, byte[] inner)
        {
            if (inner.Length < 2) { await SendCompressedEResponseWithDump(conn, inner, "e_echo"); return; }
            byte channel = inner[0];
            byte type = inner[1];

            // Echo-acks that the retail client expects while preparing UI
            if (inner.Length == 0 || (inner.Length == 2 && channel == 0 && type == 2))
            {
                await SendCompressedEResponseWithDump(conn, Array.Empty<byte>(), "e_ack_empty");
                return;
            }

            // Pass through to channel handler if it's one we know
            await HandleChannelMessage(conn, inner);
        }
        // ===========================================================================

        private async Task HandleType06(RRConnection conn, LEReader reader)
        {
            Debug.Log($"[Game] HandleType06: For client {conn.ConnId}");
        }

        private async Task<byte[]> SendMessage0x10(RRConnection conn, byte channel, byte[] body, string fullDumpTag = null)
        {
            uint clientId = GetClientId24(conn.ConnId);
            uint bodyLen = (uint)(body?.Length ?? 0);

            var w = new LEWriter();
            w.WriteByte(0x10);
            w.WriteUInt24((int)clientId); // peer is u24

            // Force u24 body length EXACTLY (3 bytes). Avoid any buggy helper.
            w.WriteByte((byte)(bodyLen & 0xFF));
            w.WriteByte((byte)((bodyLen >> 8) & 0xFF));
            w.WriteByte((byte)((bodyLen >> 16) & 0xFF));

            w.WriteByte(channel);
            if (bodyLen > 0) w.WriteBytes(body);

            var payload = w.ToArray();

            // Sanity: 0x10 frame must be 1+3+3+1 + bodyLen = 8 + bodyLen
            int expected = 8 + (int)bodyLen;
            if (payload.Length != expected)
                Debug.LogError($"[Wire][0x10] BAD SIZE: got={payload.Length} expected={expected}");

            if (!string.IsNullOrEmpty(fullDumpTag))
                DumpUtil.DumpFullFrame(fullDumpTag, payload);

            await conn.Stream.WriteAsync(payload, 0, payload.Length);
            Debug.Log($"[Wire][0x10] Sent peer=0x{clientId:X6} bodyLen(u24)={bodyLen} ch=0x{channel:X2} total={payload.Length}");
            return payload;
        }

        private uint GetClientId24(int connId) => _peerId24.TryGetValue(connId, out var id) ? id : 0u;

        public void Stop()
        {
            lock (_gameLoopLock) { _gameLoopRunning = false; }
            Debug.Log("[Game] Server stopping...");
        }

        // Entity Manager Interval Message
        private async Task SendEntityManagerInterval(RRConnection conn)
        {
            Debug.Log($"[Game] SendEntityManagerInterval: Sending entity manager interval message");

            var w = new LEWriter();
            w.WriteByte(7);  // Entity manager channel
            w.WriteByte(13); // Interval message type
            w.WriteUInt32(0x05040302); // Unknown 4 bytes from GO repo
            w.WriteUInt32(0x66666666); // Unknown 4 bytes from GO repo
            w.WriteUInt32(0x88888888); // Unknown 4 bytes from GO repo
            w.WriteUInt32(0x99999999); // Unknown 4 bytes from GO repo
            w.WriteUInt16(30);        // perUpdate
            w.WriteUInt16(7);         // perPath
            w.WriteByte(6);           // End packet

            await SendCompressedEResponseWithDump(conn, w.ToArray(), "entity_interval_7_13");

            Debug.Log($"[Game] SendEntityManagerInterval: Entity manager interval message sent");
        }

        // Entity Manager Random Seed Message
        private async Task SendEntityManagerRandomSeed(RRConnection conn)
        {
            Debug.Log($"[Game] SendEntityManagerRandomSeed: Sending entity manager random seed message");

            var w = new LEWriter();
            w.WriteByte(7);  // Entity manager channel
            w.WriteByte(12); // Random seed message type
            w.WriteUInt32(0x12345678); // Random seed value
            w.WriteByte(6);           // Unknown byte from GO repo

            await SendCompressedEResponseWithDump(conn, w.ToArray(), "entity_random_seed_7_12");

            Debug.Log($"[Game] SendEntityManagerRandomSeed: Entity manager random seed message sent");
        }

        // Entity Manager Entity Create Init Message
        private async Task SendEntityManagerEntityCreateInit(RRConnection conn)
        {
            Debug.Log($"[Game] SendEntityManagerEntityCreateInit: Sending entity manager entity create init message");

            var w = new LEWriter();
            w.WriteByte(7);  // Entity manager channel
            w.WriteByte(8);  // Entity create init message type
            w.WriteUInt16(0x50); // Entity ID
            w.WriteByte(0xFF);   // GCClassRegistry::readType
            w.WriteCString("Player"); // Class name
            w.WriteCString(conn.LoginName + "_NewHero"); // Player name
            w.WriteUInt32(5);    // Unknown 4 bytes from GO repo
            w.WriteUInt32(5);    // Unknown 4 bytes from GO repo
            w.WriteByte(8);      // Entity create init message type (repeated)
            w.WriteByte(6);      // End packet

            await SendCompressedEResponseWithDump(conn, w.ToArray(), "entity_create_init_7_8");

            Debug.Log($"[Game] SendEntityManagerEntityCreateInit: Entity manager entity create init message sent");
        }

        // Entity Manager Component Create Message
        private async Task SendEntityManagerComponentCreate(RRConnection conn)
        {
            Debug.Log($"[Game] SendEntityManagerComponentCreate: Sending entity manager component create message");

            var w = new LEWriter();
            w.WriteByte(7);  // Entity manager channel
            w.WriteByte(50); // Component create message type
            w.WriteUInt16(0x51); // Entity ID
            w.WriteUInt16(0x0A); // Component ID
            w.WriteByte(0xFF);   // GCClassRegistry::readType
            w.WriteCString("UnitContainer"); // Class name
            w.WriteByte(1);      // Unknown byte from GO repo

            // Container::readInit
            w.WriteBytes(new byte[] { 0x66, 0x66, 0x66, 0x66 }); // Unknown 4 bytes
            w.WriteBytes(new byte[] { 0x77, 0x77, 0x77, 0x77 }); // Unknown 4 bytes

            // GCObject::readChildData<Inventory>
            w.WriteByte(3);      // Number of inventory items

            // Inventory 1
            w.WriteByte(0xFF);   // GCClassRegistry::readType
            w.WriteCString("avatar.base.Inventory"); // Class name
            w.WriteByte(1);      // Inventory type (1=backpack)
            w.WriteByte(1);      // Unknown byte
            w.WriteByte(0);      // Unknown byte

            // Inventory 2
            w.WriteByte(0xFF);   // GCClassRegistry::readType
            w.WriteCString("avatar.base.Bank"); // Class name
            w.WriteByte(2);      // Inventory type (2=bank)
            w.WriteByte(1);      // Unknown byte
            w.WriteByte(0);      // Unknown byte

            // Inventory 3
            w.WriteByte(0xFF);   // GCClassRegistry::readType
            w.WriteCString("avatar.base.Bank2"); // Class name
            w.WriteByte(2);      // Inventory type (2=bank)
            w.WriteByte(1);      // Unknown byte
            w.WriteByte(0);      // Unknown byte

            w.WriteByte(50);     // Component create message type (repeated)
            w.WriteByte(6);      // End packet

            await SendCompressedEResponseWithDump(conn, w.ToArray(), "entity_component_create_7_50");

            Debug.Log($"[Game] SendEntityManagerComponentCreate: Entity manager component create message sent");
        }


        // Entity Manager Connect Message
        private async Task SendEntityManagerConnect(RRConnection conn)
        {
            Debug.Log($"[Game] SendEntityManagerConnect: Sending entity manager connect message");

            var w = new LEWriter();
            w.WriteByte(7);  // Entity manager channel
            w.WriteByte(70); // Connect message type

            // Add additional data - the client expects more than just channel and message type
            w.WriteUInt32(33752069); // CHAR_ID from GO repo
            w.WriteUInt32(1);        // Some additional data

            await SendCompressedEResponseWithDump(conn, w.ToArray(), "entity_connect_7_70");

            Debug.Log($"[Game] SendEntityManagerConnect: Entity manager connect message sent");
        }
    }
}

public class RRConnection
{
    public int ConnId { get; }
    public TcpClient Client { get; }
    public NetworkStream Stream { get; }
    public string LoginName { get; set; } = "";
    public bool IsConnected { get; set; } = true;

    public RRConnection(int connId, TcpClient client, NetworkStream stream)
    {
        ConnId = connId;
        Client = client;
        Stream = stream;
    }
}

    // Added closing brace for the namespace Server.Game opened at the top of
    // this file.  Without this, the namespace scope leaked and caused
    // methods such as HandleCharacterCreate or HandleGroupChannelMessages to
    // fall out of scope in the compiled project.
    
