// --- GameServer.cs (full, with ALL your Debug.Logs kept and new proof logs added) ---
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
using Org.BouncyCastle.Utilities;
using System.Reflection;
using Unity.VisualScripting.Antlr3.Runtime.Tree;
using System.IO; // for dump helper

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
        // ✅ Fixed generic brackets here
        private readonly ConcurrentDictionary<int, List<Server.Game.GCObject>> _playerCharacters = new();

        // Tracks whether we've already sent a character list for this connection (watchdog / nudge uses this)
        private readonly ConcurrentDictionary<int, bool> _charListSent = new();

        // Add these fields for persistent character creation
        private readonly ConcurrentDictionary<string, List<Server.Game.GCObject>> _persistentCharacters = new();
        // private readonly ConcurrentDictionary<string, bool> _characterCreationPending = new();

        private bool _gameLoopRunning = false;
        private readonly object _gameLoopLock = new object();

        // ==== Tunables / toggles (kept at top for quick A/B) ====
        // Some DR builds expect the Avatar to be included only as a CHILD of Player (single write).
        // If you need to test the "duplicate avatar record" (child + standalone) like some Go payloads,
        // flip this to true and re-run. All proof logs remain.
        // Duplicate standalone Avatar record after Player (some Go builds do this).
        // We'll keep it ON for now while we stabilize; if size mismatches appear we can toggle off.
        private const bool DUPLICATE_AVATAR_RECORD = true;

        // ===== Dump helper (writes uncompressed/compressed + CRC to <Base>\dump) =====
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
        }
        // ============================================================================

        public GameServer(string ip, int port)
        {
            bindIp = ip;
            this.port = port;
            net = new NetServer(ip, port, HandleClient);
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

            // (intentionally left commented; keep it that way to avoid desync)
            /*
            try
            {
                byte[] welcome = { 0x10, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01 };
                await s.WriteAsync(welcome, 0, welcome.Length);
                await s.FlushAsync();
                Debug.Log($"[Game] Client {connId} - Sent welcome handshake");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] Failed to send welcome to {connId}: {ex.Message}");
            }
            */

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
                await SendMessage0x10(conn, 0xFF, keepAlive.ToArray());
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

            if (msgType != 0x0A && string.IsNullOrEmpty(conn.LoginName))
            {
                Debug.LogError($"[Game] ReadPacket: Received invalid message type 0x{msgType:X2} before login for client {conn.ConnId}");
                Debug.LogError($"[Game] ReadPacket: Only 0x0A messages allowed before authentication!");
                return;
            }

            switch (msgType)
            {
                case 0x0A:
                    Debug.Log($"[Game] ReadPacket: Handling Compressed A message for client {conn.ConnId}");
                    await HandleCompressedA(conn, reader);
                    break;
                case 0x0E:
                    Debug.Log($"[Game] ReadPacket: Handling Compressed E message for client {conn.ConnId}");
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

                    if (msgType == 0x31)
                    {
                        Debug.Log($"[Game] ReadPacket: 0x31 message details - Length: {data.Length}");
                        if (data.Length > 1)
                        {
                            Debug.Log($"[Game] ReadPacket: 0x31 - Next bytes: {BitConverter.ToString(data, 1, Math.Min(16, data.Length - 1))}");
                        }
                    }
                    break;
            }
        }

        private async Task HandleCompressedA(RRConnection conn, LEReader reader)
        {
            Debug.Log($"[Game] HandleCompressedA: Starting for client {conn.ConnId}");
            Debug.Log($"[Game] HandleCompressedA: Remaining bytes: {reader.Remaining}");

            // [0x0A already consumed]
            // [peer:u24][packetLen:u32][dest:u8][sub:u8][zero:u8][unclen:u32][zlib...]
            const int MIN_HDR = 3 + 4 + 1 + 1 + 1 + 4; // 14
            if (reader.Remaining < MIN_HDR)
            {
                Debug.LogError($"[Game] HandleCompressedA: Insufficient data - need {MIN_HDR} bytes, have {reader.Remaining}");
                return;
            }

            uint peer = reader.ReadUInt24();
            uint packetLen = reader.ReadUInt32();
            byte dest = reader.ReadByte();
            byte msgTypeA = reader.ReadByte();
            byte zero = reader.ReadByte();
            uint unclen = reader.ReadUInt32();

            _peerId24[conn.ConnId] = peer;

            Debug.Log($"[Game] HandleCompressedA: peer=0x{peer:X6} dest=0x{dest:X2} sub=0x{msgTypeA:X2} zero=0x{zero:X2} unclen={unclen} packetLen={packetLen}");

            int compLen = (int)packetLen - 7;
            Debug.Log($"[Game] HandleCompressedA: Calculated compressed length: {compLen}");
            if (compLen < 0)
            {
                Debug.LogError($"[Game] HandleCompressedA: Invalid compressed length {compLen} - packetLen too small");
                if (compLen == 0 || unclen == 0)
                {
                    Debug.Log($"[Game] HandleCompressedA: Treating as uncompressed/empty inner");
                    await ProcessUncompressedMessage(conn, dest, msgTypeA, Array.Empty<byte>());
                }
                return;
            }

            if (reader.Remaining < compLen)
            {
                Debug.LogError($"[Game] HandleCompressedA: Not enough data for compressed content - need {compLen}, have {reader.Remaining}");
                return;
            }

            byte[] comp = reader.ReadBytes(compLen);
            Debug.Log($"[Game] HandleCompressedA: Read {comp.Length} compressed bytes");
            Debug.Log($"[Game] HandleCompressedA: Compressed data: {BitConverter.ToString(comp)}");

            if (zero != 0)
                Debug.LogWarning($"[Game] HandleCompressedA: expected zero==0 but got {zero}");

            byte[] inner;
            try
            {
                if (compLen == 0 || unclen == 0)
                {
                    inner = Array.Empty<byte>();
                }
                else
                {
                    inner = ZlibUtil.Inflate(comp, unclen);
                }
                Debug.Log($"[Game] HandleCompressedA: Decompressed to {inner.Length} bytes (expected {unclen})");
                if (inner.Length > 0)
                    Debug.Log($"[Game] HandleCompressedA: Uncompressed data: {BitConverter.ToString(inner)}");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] HandleCompressedA: Decompression failed: {ex.Message}");
                return;
            }

            await ProcessUncompressedMessage(conn, dest, msgTypeA, inner);
        }

        private async Task ProcessUncompressedMessage(RRConnection conn, byte dest, byte msgTypeA, byte[] uncompressed)
        {
            Debug.Log($"[Game] ProcessUncompressedMessage: Processing A message - dest=0x{dest:X2} sub=0x{msgTypeA:X2}");

            if (msgTypeA != 0x00 && string.IsNullOrEmpty(conn.LoginName))
            {
                Debug.LogError($"[Game] ProcessUncompressedMessage: Received msgTypeA 0x{msgTypeA:X2} before login for client {conn.ConnId}");
                return;
            }

            switch (msgTypeA)
            {
                case 0x00:
                    Debug.Log($"[Game] ProcessUncompressedMessage: Processing initial login (0x00) for client {conn.ConnId}");
                    await HandleInitialLogin(conn, uncompressed);
                    break;
                case 0x02:
                    Debug.Log($"[Game] ProcessUncompressedMessage: Processing secondary message (0x02) for client {conn.ConnId}");
                    Debug.Log($"[Game] ProcessUncompressedMessage: Sending empty 0x02 response");
                    await SendCompressedAResponse(conn, 0x00, 0x02, Array.Empty<byte>());
                    break;
                case 0x03:
                    Debug.Log($"[Game] ProcessUncompressedMessage: Processing message type 0x03 for client {conn.ConnId}");
                    if (uncompressed.Length >= 4)
                    {
                        var reader = new LEReader(uncompressed);
                        uint sessionToken = reader.ReadUInt32();
                        Debug.Log($"[Game] ProcessUncompressedMessage: Found session token 0x{sessionToken:X8}");

                        if (GlobalSessions.TryConsume(sessionToken, out var user) && !string.IsNullOrEmpty(user))
                        {
                            conn.LoginName = user;
                            _users[conn.ConnId] = user;
                            Debug.Log($"[Game] ProcessUncompressedMessage: Auth OK for user '{user}' on client {conn.ConnId}");

                            var ack = new LEWriter();
                            ack.WriteByte(0x03);
                            await SendMessage0x10(conn, 0x0A, ack.ToArray());

                            await Task.Delay(50);
                            await StartCharacterFlow(conn);
                        }
                        else
                        {
                            Debug.LogError($"[Game] ProcessUncompressedMessage: Invalid session token 0x{sessionToken:X8}");
                        }
                    }
                    break;
                case 0x0F:
                    Debug.Log($"[Game] ProcessUncompressedMessage: Processing channel messages (0x0F) for client {conn.ConnId}");
                    await HandleChannelMessage(conn, uncompressed);
                    break;
                default:
                    Debug.LogWarning($"[Game] ProcessUncompressedMessage: Unhandled msgTypeA 0x{msgTypeA:X2} for client {conn.ConnId}");
                    break;
            }
        }

        private async Task HandleInitialLogin(RRConnection conn, byte[] data)
        {
            Debug.Log($"[Game] HandleInitialLogin: *** ENTRY POINT *** Processing login for client {conn.ConnId}");
            Debug.Log($"[Game] HandleInitialLogin: Data length: {data.Length}");
            Debug.Log($"[Game] HandleInitialLogin: Data hex: {BitConverter.ToString(data)}");

            try
            {
                if (data.Length < 5)
                {
                    Debug.LogError($"[Game] HandleInitialLogin: *** ERROR *** Insufficient data - need 5 bytes, have {data.Length}");
                    return;
                }

                var reader = new LEReader(data);
                byte subtype = reader.ReadByte();
                uint oneTimeKey = reader.ReadUInt32();

                Debug.Log($"[Game] HandleInitialLogin: Parsed - subtype=0x{subtype:X2}, oneTimeKey=0x{oneTimeKey:X8}");

                if (!GlobalSessions.TryConsume(oneTimeKey, out var user) || string.IsNullOrEmpty(user))
                {
                    Debug.LogError($"[Game] HandleInitialLogin: *** ERROR *** Invalid OneTimeKey 0x{oneTimeKey:X8} for client {conn.ConnId}");
                    Debug.LogError($"[Game] HandleInitialLogin: *** ERROR *** Could not validate session token");
                    return;
                }

                conn.LoginName = user;
                _users[conn.ConnId] = user;
                Debug.Log($"[Game] HandleInitialLogin: *** SUCCESS *** Auth OK for user '{user}' on client {conn.ConnId}");

                Debug.Log($"[Game] HandleInitialLogin: *** STEP 1 *** Sending 0x10 ack message");
                var ack = new LEWriter();
                ack.WriteByte(0x03);
                await SendMessage0x10(conn, 0x0A, ack.ToArray());
                Debug.Log($"[Game] HandleInitialLogin: *** STEP 1 COMPLETE *** Sent 0x10 ack");

                Debug.Log($"[Game] HandleInitialLogin: *** STEP 2 *** Sending A/0x03 advance message");
                var advance = new LEWriter();
                advance.WriteUInt24(0x00B2B3B4);
                advance.WriteByte(0x00);
                byte[] advanceData = advance.ToArray();
                Debug.Log($"[Game] HandleInitialLogin: *** STEP 2 DATA *** Advance data ({advanceData.Length} bytes): {BitConverter.ToString(advanceData)}");
                await SendCompressedAResponse(conn, 0x00, 0x03, advanceData);
                Debug.Log($"[Game] HandleInitialLogin: *** STEP 2 COMPLETE *** Sent advance message");

                Debug.Log("[Game] HandleInitialLogin: *** STEP 2b *** Nudge with empty A/0x02");
                await SendCompressedAResponse(conn, 0x00, 0x02, Array.Empty<byte>());
                await Task.Delay(75);

                Debug.Log($"[Game] HandleInitialLogin: *** STEP 3 *** Starting character flow for user '{user}'");
                await StartCharacterFlow(conn);
                Debug.Log($"[Game] HandleInitialLogin: *** COMPLETE *** All steps finished for client {conn.ConnId}");

            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] HandleInitialLogin: *** CRITICAL EXCEPTION *** {ex.Message}");
                Debug.LogError($"[Game] HandleInitialLogin: *** STACK TRACE *** {ex.StackTrace}");
            }
        }

        private async Task HandleChannelMessage(RRConnection conn, byte[] data)
        {
            Debug.Log($"[Game] HandleChannelMessage: Processing for client {conn.ConnId}, data length: {data.Length}");
            Debug.Log($"[Game] HandleChannelMessage: Data hex: {BitConverter.ToString(data)}");

            if (data.Length < 2)
            {
                Debug.LogWarning($"[Game] HandleChannelMessage: Insufficient data - need 2 bytes, have {data.Length}");
                return;
            }

            byte channel = data[0];
            byte messageType = data[1];

            int preview = Math.Min(16, data.Length);
            Debug.Log($"[Game] HandleChannelMessage: Header ch={channel} type=0x{messageType:X2} preview[0..{preview - 1}]={BitConverter.ToString(data, 0, preview)}");

            Debug.Log($"[Game] HandleChannelMessage: Channel {channel}, Type 0x{messageType:X2} for client {conn.ConnId}");

            switch (channel)
            {
                case 4:
                    Debug.Log($"[Game] HandleCharacterChannelMessages: Type 0x{messageType:X2} for client {conn.ConnId}");
                    switch (messageType)
                    {
                        case 0:
                            Debug.Log($"[Game] HandleCharacterChannelMessages: Character connected");
                            await SendCharacterConnectedResponse(conn);
                            break;

                        case 3:
                            Debug.Log($"[Game] HandleCharacterChannelMessages: Get character list (ENTER)");
                            await SendCharacterList(conn);
                            break;

                        case 5:
                            Debug.Log($"[Game] HandleCharacterChannelMessages: Character play");
                            await HandleCharacterPlay(conn, data);
                            break;

                        case 2:
                            Debug.Log($"[Game] HandleCharacterChannelMessages: Character create");
                            await HandleCharacterCreate(conn, data);
                            break;

                        default:
                            Debug.LogWarning($"[Game] HandleCharacterChannelMessages: Unhandled character msg 0x{messageType:X2}");
                            break;
                    }
                    break;

                case 9:
                    Debug.Log($"[Game] HandleChannelMessage: Routing to group handler");
                    await HandleGroupChannelMessages(conn, messageType);
                    break;
                case 13:
                    Debug.Log($"[Game] HandleChannelMessage: Routing to zone handler");
                    await HandleZoneChannelMessages(conn, messageType, data);
                    break;
                default:
                    Debug.LogWarning($"[Game] HandleChannelMessage: Unhandled channel {channel} for client {conn.ConnId}");
                    break;
            }
        }

        private async Task StartCharacterFlow(RRConnection conn)
        {
            Debug.Log($"[Game] StartCharacterFlow: *** ENTRY *** Beginning character flow for client {conn.ConnId} ({conn.LoginName})");

            try
            {
                Debug.Log($"[Game] StartCharacterFlow: *** STEP 1 *** Sending character connected response");

                // === Timing nudge: small grace period before the first 4/0 like some Go runs ===
                await Task.Delay(50);
                Debug.Log("[Game] StartCharacterFlow: Delay(50ms) before 4/0 to avoid race");

                await SendCharacterConnectedResponse(conn);
                Debug.Log($"[Game] StartCharacterFlow: *** STEP 1 COMPLETE *** Character connected response sent");

                Debug.Log($"[Game] StartCharacterFlow: waiting for client CharacterGetList (4/3)...");

                // === Purist mini-nudge: re-send 4/0 once after 250ms if still no list sent ===
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(250);
                        if (!_charListSent.TryGetValue(conn.ConnId, out var sent) || !sent)
                        {
                            Debug.Log("[Game] Character channel nudge: re-sending 4/0 (CharacterConnected) once");
                            await SendCharacterConnectedResponse(conn);
                        }
                        else
                        {
                            Debug.Log("[Game] Character channel nudge: already saw list path, skipping re-send");
                        }
                    }
                    catch (Exception ex)
                    {
                        Debug.LogWarning($"[Game] Character channel nudge: non-fatal error: {ex.Message}");
                    }
                });

                // === Secondary A/0x02 ticks for clients that expect a "heartbeat" before asking for 4/3 ===
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(500);
                        Debug.Log("[Game][A02] Secondary tick at 500ms");
                        await SendCompressedAResponse(conn, 0x00, 0x02, Array.Empty<byte>());
                        await Task.Delay(500);
                        if (!_charListSent.TryGetValue(conn.ConnId, out var sent2) || !sent2)
                        {
                            Debug.Log("[Game][A02] Secondary tick at 1000ms");
                            await SendCompressedAResponse(conn, 0x00, 0x02, Array.Empty<byte>());
                        }
                        else
                        {
                            Debug.Log("[Game][A02] Secondary tick skipped (list already sent)");
                        }
                    }
                    catch (Exception ex)
                    {
                        Debug.LogWarning($"[Game] Secondary A/0x02 tick failed: {ex.Message}");
                    }
                });

                // === Watchdog: proactively send 4/3 if client never asks ===
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(300);
                        if (!_charListSent.TryGetValue(conn.ConnId, out var sent3) || !sent3)
                        {
                            Debug.Log("[Game] Character list watchdog: client did not request 4/3; proactively sending list now");
                            await SendCharacterList(conn);
                        }
                        else
                        {
                            Debug.Log("[Game] Character list watchdog: list already sent; skipping proactive send");
                        }
                    }
                    catch (Exception ex)
                    {
                        Debug.LogWarning($"[Game] Character list watchdog: non-fatal error: {ex.Message}");
                    }
                });

            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] StartCharacterFlow: *** CRITICAL EXCEPTION *** {ex.Message}");
                Debug.LogError($"[Game] StartCharacterFlow: *** STACK TRACE *** {ex.StackTrace}");
            }
        }

        private async Task SendCharacterConnectedResponse(RRConnection conn)
        {
            // Some client builds only ask for 4/3 after seeing A/0x02 once.
            Debug.Log($"[Game] SendCharacterConnectedResponse: pre-nudge A/0x02");
            await SendCompressedAResponse(conn, 0x00, 0x02, Array.Empty<byte>());
            await Task.Delay(75);

            Debug.Log($"[Game] SendCharacterConnectedResponse: *** ENTRY *** For client {conn.ConnId} - creating 2 characters like Go server");

            try
            {
                if (!_persistentCharacters.ContainsKey(conn.LoginName))
                {
                    Debug.Log($"[Game] SendCharacterConnectedResponse: *** CREATING CHARACTERS *** No existing characters for {conn.LoginName}");
                    var characters = new List<Server.Game.GCObject>();
                    for (int i = 0; i < 2; i++)
                    {
                        Debug.Log($"[Game] SendCharacterConnectedResponse: *** CREATING CHARACTER {i + 1} *** Calling Server.Game.Objects.NewPlayer");

                        try
                        {
                            var character = Server.Game.Objects.NewPlayer($"{conn.LoginName}");
                            character.ID = (uint)(Server.Game.Objects.NewID());
                            characters.Add(character);
                            Debug.Log($"[Game] SendCharacterConnectedResponse: *** CHARACTER {i + 1} CREATED *** ID: {character.ID}, Type: {character.GCType}");
                        }
                        catch (Exception charEx)
                        {
                            Debug.LogError($"[Game] SendCharacterConnectedResponse: *** ERROR CREATING CHARACTER {i + 1} *** {charEx.Message}");
                            Debug.LogError($"[Game] SendCharacterConnectedResponse: *** CHARACTER CREATION STACK TRACE *** {charEx.StackTrace}");
                        }
                    }
                    _persistentCharacters[conn.LoginName] = characters;
                    Debug.Log($"[Game] SendCharacterConnectedResponse: *** SUCCESS *** Created {characters.Count} characters for {conn.LoginName}");
                }
                else
                {
                    Debug.Log($"[Game] SendCharacterConnectedResponse: *** USING EXISTING *** Found existing characters for {conn.LoginName}");
                }

                Debug.Log($"[Game] SendCharacterConnectedResponse: *** SENDING MESSAGE *** Creating response message");
                var w = new LEWriter();
                w.WriteByte(4);  // Character channel
                w.WriteByte(0);  // Character connected

                var inner = w.ToArray();
                Debug.Log($"[SEND][inner] CH=4,TYPE=0 : {BitConverter.ToString(inner)} (len={inner.Length})");
                if (!(inner.Length >= 2 && inner[0] == 0x04 && inner[1] == 0x00))
                    Debug.LogWarning($"[Game] SendCharacterConnectedResponse: Unexpected 4/0 header: {BitConverter.ToString(inner.Take(2).ToArray())}");

                Debug.Log($"[SEND][A][prep] 4/0 using peer=0x{GetClientId24(conn.ConnId):X6} dest=0x01 sub=0x0F innerLen={inner.Length}");
                await SendCompressedAResponseWithDump(conn, 0x01, 0x0F, inner, "char_connected");
                Debug.Log($"[Game] SendCharacterConnectedResponse: *** SUCCESS *** Sent character connected message");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] SendCharacterConnectedResponse: *** CRITICAL EXCEPTION *** {ex.Message}");
                Debug.LogError($"[Game] SendCharacterConnectedResponse: *** STACK TRACE *** {ex.StackTrace}");
            }
        }

        // === Helper: ensure exactly one Avatar child during serialization and remove it afterwards ===
        private static bool EnsureSingleAvatarChild(Server.Game.GCObject player, out Server.Game.GCObject? addedAvatar)
        {
            addedAvatar = null;
            try
            {
                if (player.Children != null && player.Children.Count > 0)
                {
                    // Assume first child is Avatar; do not add a second one.
                    return false;
                }

                var avatar = Server.Game.Objects.LoadAvatar();
                player.AddChild(avatar);
                addedAvatar = avatar;
                Debug.Log("[Game] EnsureSingleAvatarChild: Temp Avatar added for serialization.");
                return true;
            }
            catch (Exception ex)
            {
                Debug.LogWarning($"[Game] EnsureSingleAvatarChild: failed: {ex.Message}");
                return false;
            }
        }

        // === Player serializer (uses EnsureSingleAvatarChild; removes temp avatar after write) ===
        private void WriteGoSendPlayer(LEWriter body, Server.Game.GCObject character)
        {
            Server.Game.GCObject? avatarAdded = null;
            bool added = false;

            try
            {
                added = EnsureSingleAvatarChild(character, out avatarAdded);

                long start = body.ToArray().Length;
                character.WriteFullGCObject(body);
                long end = body.ToArray().Length;
                Debug.Log($"[Game] WriteGoSendPlayer: Player full write bytes={end - start}");

                if (DUPLICATE_AVATAR_RECORD)
                {
                    Debug.Log("[Game] WriteGoSendPlayer: DUPLICATE_AVATAR_RECORD=TRUE -> writing avatar twice (child + standalone) to mirror some Go payloads");

                    var avatarToWrite =
                        avatarAdded
                        ?? (character.Children != null && character.Children.Count > 0 ? character.Children[0] : null)
                        ?? Server.Game.Objects.LoadAvatar();

                    long a0 = body.ToArray().Length;
                    avatarToWrite.WriteFullGCObject(body);
                    long a1 = body.ToArray().Length;
                    Debug.Log($"[Game] WriteGoSendPlayer: Standalone Avatar write bytes={a1 - a0}");

                    // Cosmetic tail exactly like Go
                    body.WriteByte(0x01);
                    body.WriteByte(0x01);
                    var normalBytes = Encoding.UTF8.GetBytes("Normal");
                    body.WriteBytes(normalBytes);
                    body.WriteByte(0x00);
                    body.WriteByte(0x01);
                    body.WriteByte(0x01);
                    body.WriteUInt32(0x01);
                }
                else
                {
                    Debug.Log("[Game] WriteGoSendPlayer: DUPLICATE_AVATAR_RECORD=FALSE -> sending only Player (with Avatar child)");
                }
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] WriteGoSendPlayer: EXCEPTION {ex.Message}");
            }
            finally
            {
                // Remove temp avatar to avoid child accumulation across repeated serializations
                if (added && avatarAdded != null && character.Children != null)
                {
                    character.Children.Remove(avatarAdded);
                    Debug.Log("[Game] WriteGoSendPlayer: Removed temporary Avatar child post-serialize");
                }
            }
        }

        private async Task SendCharacterList(RRConnection conn)
        {
            Debug.Log($"[Game] SendCharacterList: *** ENTRY *** Matching Go server exactly (minus crashy 4/1)");

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
                body.WriteByte(4);   // messages.CharacterChannel
                body.WriteByte(3);   // CharacterGetList 
                body.WriteByte((byte)count); // count

                Debug.Log($"[Game] SendCharacterList: *** WRITING CHARACTERS *** Processing {count} characters");

                for (int i = 0; i < count; i++)
                {
                    var character = characters[i];
                    Debug.Log($"[Game] SendCharacterList: *** CHARACTER {i + 1} *** ID: {character.ID}, Writing character data");

                    try
                    {
                        body.WriteUInt32(character.ID);
                        Debug.Log($"[Game] SendCharacterList: *** CHARACTER {i + 1} *** Wrote ID=0x{character.ID:X8}, calling WriteGoSendPlayer");

                        WriteGoSendPlayer(body, character);

                        Debug.Log($"[Game] SendCharacterList: *** CHARACTER {i + 1} *** WriteGoSendPlayer complete; current bodyLen={body.ToArray().Length}");
                    }
                    catch (Exception charEx)
                    {
                        Debug.LogError($"[Game] SendCharacterList: *** ERROR CHARACTER {i + 1} *** {charEx.Message}");
                        Debug.LogError($"[Game] SendCharacterList: *** CHARACTER {i + 1} STACK TRACE *** {charEx.StackTrace}");
                    }
                }

                var inner = body.ToArray();
                Debug.Log($"[Game] SendCharacterList: *** SENDING MESSAGE *** Total body length: {inner.Length} bytes");
                Debug.Log($"[SEND][inner] CH=4,TYPE=3 : {BitConverter.ToString(inner)} (len={inner.Length})");

                // PROOF CHECKS
                if (inner.Length < 1000)
                {
                    Debug.LogError($"[Game][FATAL] SendCharacterList built only {inner.Length} bytes. First 16: {BitConverter.ToString(inner, 0, Math.Min(inner.Length, 16))}");
                }
                else if (!(inner.Length >= 2 && inner[0] == 0x04 && inner[1] == 0x03))
                {
                    Debug.LogError($"[Game][FATAL] SendCharacterList header wrong: {BitConverter.ToString(inner, 0, Math.Min(inner.Length, 8))}");
                }
                else
                {
                    Debug.Log($"[Game] SendCharacterList: Header OK (04-03), bytes={inner.Length}");
                }

                if (!(inner.Length >= 3 && inner[0] == 0x04 && inner[1] == 0x03))
                    Debug.LogWarning($"[Game] SendCharacterList: Header unexpected; got {BitConverter.ToString(inner.Take(3).ToArray())}");
                else
                    Debug.Log($"[Game] SendCharacterList: Header OK -> 04-03 count={inner[2]}");

                int head = Math.Min(16, inner.Length);
                Debug.Log($"[Game] SendCharacterList: First {head} bytes: {BitConverter.ToString(inner, 0, head)}");

                Debug.Log($"[SEND][A][prep] 4/3 using peer=0x{GetClientId24(conn.ConnId):X6} dest=0x01 sub=0x0F innerLen={inner.Length}");
                await SendCompressedAResponseWithDump(conn, 0x01, 0x0F, inner, "charlist");

                Debug.Log($"[Game] SendCharacterList: *** SUCCESS *** Sent Go format with {count} characters");
                _charListSent[conn.ConnId] = true;

                // IMPORTANT: DO NOT send 4/1 (Disconnected) — that caused the client crash in processGotCharacter.
                Debug.Log("[Game] SendCharacterList: UI nudge 4/1 suppressed by protocol guard (prevents crash).");

            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] SendCharacterList: *** CRITICAL EXCEPTION *** {ex.Message}");
                Debug.LogError($"[Game] SendCharacterList: *** STACK TRACE *** {ex.StackTrace}");
            }
        }

        private async Task SendToCharacterCreation(RRConnection conn)
        {
            Debug.Log($"[Game] SendToCharacterCreation: Sending client {conn.ConnId} to character creation screen");

            var createMessage = new LEWriter();
            createMessage.WriteByte(4);
            createMessage.WriteByte(4); // 0x04 = Enter Character Creation Screen

            LegacyWriters.WriteCompressedA(conn.Stream, (int)GetClientId24(conn.ConnId), 0x01, 0x0F, createMessage.ToArray(), 1);

            Debug.Log($"[Game] SendToCharacterCreation: Sent 4/4 enter creation");
        }

        private async Task HandleCharacterPlay(RRConnection conn, byte[] data)
        {
            Debug.Log($"[Game] HandleCharacterPlay: For client {conn.ConnId} (Go slot-based)");

            var r = new LEReader(data);
            if (r.Remaining < 3)
            {
                Debug.LogWarning("[Game] HandleCharacterPlay: too short");
                await SendFallback();
                return;
            }

            byte ch = r.ReadByte();
            byte mt = r.ReadByte();
            if (ch != 0x04 || mt != 0x05)
            {
                Debug.LogWarning($"[Game] HandleCharacterPlay: unexpected header ch=0x{ch:X2} mt=0x{mt:X2}");
                await SendFallback();
                return;
            }

            if (r.Remaining < 1)
            {
                Debug.LogWarning("[Game] HandleCharacterPlay: missing slot byte");
                await SendFallback();
                return;
            }

            byte slot = r.ReadByte();
            Debug.Log($"[Game] HandleCharacterPlay: slot={slot}");

            if (!_persistentCharacters.TryGetValue(conn.LoginName, out var chars) || slot >= chars.Count)
            {
                Debug.LogWarning($"[Game] HandleCharacterPlay: invalid slot {slot} (have {(chars?.Count ?? 0)} chars)");
                await SendFallback();
                return;
            }

            Debug.Log($"[Game] HandleCharacterPlay: selecting {slot} -> id={chars[slot].ID}");

            var w = new LEWriter();
            w.WriteByte(4);
            w.WriteByte(5);

            LegacyWriters.WriteCompressedA(conn.Stream, (int)GetClientId24(conn.ConnId), 0x01, 0x0F, w.ToArray(), 1);
            Debug.Log("[Game] HandleCharacterPlay: sent minimal 04,05 ack (Go-style)");

            await Task.Delay(100);
            await SendGroupConnectedResponse(conn);
            return;

            async Task SendFallback()
            {
                var fallback = new LEWriter();
                fallback.WriteByte(4);
                fallback.WriteByte(5);
                fallback.WriteByte(1);

                await SendCompressedAResponse(conn, 0x01, 0x0F, fallback.ToArray());
                Debug.Log("[Game] HandleCharacterPlay: Sent fallback response");
            }
        }

        private async Task InitiateWorldEntry(RRConnection conn)
        {
            Debug.Log($"[Game] InitiateWorldEntry: Starting world entry for client {conn.ConnId}");
            await SendGoToZone(conn, "town");
            Debug.Log($"[Game] InitiateWorldEntry: Sent zone change, waiting for client zone join request");
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

                if (!_persistentCharacters.TryGetValue(conn.LoginName, out var existing))
                {
                    existing = new List<Server.Game.GCObject>();
                    _persistentCharacters[conn.LoginName] = existing;
                    Debug.Log($"[Game] HandleCharacterCreate: No existing list for {conn.LoginName}; created new list");
                }
                existing.Add(newCharacter);
                Debug.Log($"[Game] HandleCharacterCreate: Persisted new character (ID: {newCharId}) for {conn.LoginName}. Total now: {existing.Count}");
            }
            catch (Exception persistEx)
            {
                Debug.LogError($"[Game] HandleCharacterCreate: *** ERROR persisting character *** {persistEx.Message}");
                Debug.LogError($"[Game] HandleCharacterCreate: *** STACK TRACE *** {persistEx.StackTrace}");
            }

            var response = new LEWriter();
            response.WriteByte(4);
            response.WriteByte(2);
            response.WriteByte(1);
            response.WriteUInt32(newCharId);

            LegacyWriters.WriteCompressedA(conn.Stream, (int)GetClientId24(conn.ConnId), 0x01, 0x0F, response.ToArray(), 1);

            Debug.Log($"[Game] HandleCharacterCreate: Sent character creation success for {characterName} (ID: {newCharId})");

            await Task.Delay(100);
            await SendUpdatedCharacterList(conn, newCharId, characterName);
        }

        private async Task SendUpdatedCharacterList(RRConnection conn, uint charId, string charName)
        {
            Debug.Log($"[Game] SendUpdatedCharacterList: Sending list with newly created character");

            try
            {
                if (!_persistentCharacters.TryGetValue(conn.LoginName, out var chars))
                {
                    Debug.LogWarning($"[Game] SendUpdatedCharacterList: No persistent list found after create; falling back to single entry build");
                    var w = new LEWriter();
                    w.WriteByte(4);
                    w.WriteByte(3);
                    w.WriteByte(1);
                    var newCharacter = Server.Game.Objects.NewPlayer(charName);
                    newCharacter.ID = charId;
                    WriteGoSendPlayer(w, newCharacter);
                    var innerSingle = w.ToArray();
                    Debug.Log($"[SEND][inner] CH=4,TYPE=3 (updated single) : {BitConverter.ToString(innerSingle)} (len={innerSingle.Length})");
                    Debug.Log($"[SEND][A][prep] 4/3(using SINGLE) peer=0x{GetClientId24(conn.ConnId):X6} dest=0x01 sub=0x0F innerLen={innerSingle.Length}");
                    await SendCompressedAResponseWithDump(conn, 0x01, 0x0F, innerSingle, "charlist_single");
                    Debug.Log($"[Game] SendUpdatedCharacterList: Sent updated character list (SINGLE fallback) with new character (ID {charId})");
                    return;
                }
                else
                {
                    Debug.Log($"[Game] SendUpdatedCharacterList: Found persistent list (count={chars.Count}); delegating to SendCharacterList()");
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
            Debug.Log($"[Game] SendGroupConnectedResponse: For client {conn.ConnId}");
            var w = new LEWriter();
            w.WriteByte(9);
            w.WriteByte(0);

            LegacyWriters.WriteCompressedA(conn.Stream, (int)GetClientId24(conn.ConnId), 0x01, 0x0F, w.ToArray(), 1);

            Debug.Log("[Game] Sent group connected");

            await Task.Delay(50);
            Debug.Log($"[Game] SendGroupConnectedResponse: Sending go-to-zone");
            await SendGoToZone(conn, "town");
        }

        private async Task HandleGroupChannelMessages(RRConnection conn, byte messageType)
        {
            Debug.Log($"[Game] HandleGroupChannelMessages: Type 0x{messageType:X2} for client {conn.ConnId}");

            switch (messageType)
            {
                case 0:
                    Debug.Log($"[Game] HandleGroupChannelMessages: Group connected");
                    await SendGoToZone(conn, "town");
                    break;
                default:
                    Debug.LogWarning($"[Game] HandleGroupChannelMessages: Unhandled group msg 0x{messageType:X2}");
                    break;
            }
        }

        private async Task SendGoToZone(RRConnection conn, string zoneName)
        {
            Debug.Log($"[Game] SendGoToZone: Sending '{zoneName}' to client {conn.ConnId}");

            var w = new LEWriter();
            w.WriteByte(9);
            w.WriteByte(48);

            var zoneBytes = Encoding.UTF8.GetBytes(zoneName);
            w.WriteBytes(zoneBytes);
            w.WriteByte(0);

            byte[] goToZoneData = w.ToArray();
            Debug.Log($"[Game] SendGoToZone: Go-to-zone data ({goToZoneData.Length} bytes): {BitConverter.ToString(goToZoneData)}");

            LegacyWriters.WriteCompressedA(conn.Stream, (int)GetClientId24(conn.ConnId), 0x01, 0x0F, goToZoneData, 1);

            Debug.Log($"[Game] SendGoToZone: Sent go-to-zone '{zoneName}' to client {conn.ConnId}");
        }

        private async Task HandleZoneChannelMessages(RRConnection conn, byte messageType, byte[] data)
        {
            Debug.Log($"[Game] HandleZoneChannelMessages: Type 0x{messageType:X2} for client {conn.ConnId}");

            switch (messageType)
            {
                case 6:
                    Debug.Log($"[Game] HandleZoneJoin: Zone join request");
                    await HandleZoneJoin(conn);
                    break;
                case 8:
                    Debug.Log($"[Game] HandleZoneReady: Zone ready");
                    await HandleZoneReady(conn);
                    break;
                case 0:
                    Debug.Log($"[Game] HandleZoneConnected: Zone connected");
                    await HandleZoneConnected(conn);
                    break;
                case 1:
                    Debug.Log($"[Game] HandleZoneReadyResponse: Zone ready response");
                    await HandleZoneReadyResponse(conn);
                    break;
                case 5:
                    Debug.Log($"[Game] HandleZoneInstanceCount: Zone instance count");
                    await HandleZoneInstanceCount(conn);
                    break;
                default:
                    Debug.LogWarning($"[Game] HandleZoneChannelMessages: Unhandled zone msg 0x{messageType:X2}");
                    break;
            }
        }

        private async Task HandleZoneJoin(RRConnection conn)
        {
            Debug.Log($"[Game] HandleZoneJoin: Zone join request from client {conn.ConnId} ({conn.LoginName})");

            var w = new LEWriter();
            w.WriteByte(13);
            w.WriteByte(1);
            w.WriteUInt32(1);

            w.WriteUInt16(0x12);
            for (int i = 0; i < 0x12; i++)
            {
                w.WriteUInt32(0xFFFFFFFF);
            }

            LegacyWriters.WriteCompressedA(conn.Stream, (int)GetClientId24(conn.ConnId), 0x01, 0x0F, w.ToArray(), 1);

            Debug.Log($"[Game] HandleZoneJoin: Sent zone join response");
            Debug.Log($"[Game] HandleZoneJoin: Waiting to see if client expects more data...");
        }

        private async Task HandleZoneConnected(RRConnection conn)
        {
            var w = new LEWriter();
            w.WriteByte(13);
            w.WriteByte(0);

            LegacyWriters.WriteCompressedA(conn.Stream, (int)GetClientId24(conn.ConnId), 0x01, 0x0F, w.ToArray(), 1);

            Debug.Log("[Game] Sent zone connected response");
        }

        private async Task HandleZoneReady(RRConnection conn)
        {
            var w = new LEWriter();
            w.WriteByte(13);
            w.WriteByte(8);

            LegacyWriters.WriteCompressedA(conn.Stream, (int)GetClientId24(conn.ConnId), 0x01, 0x0F, w.ToArray(), 1);

            Debug.Log("[Game] Sent zone ready response");
        }

        private async Task HandleZoneReadyResponse(RRConnection conn)
        {
            var w = new LEWriter();
            w.WriteByte(13);
            w.WriteByte(1);

            LegacyWriters.WriteCompressedA(conn.Stream, (int)GetClientId24(conn.ConnId), 0x01, 0x0F, w.ToArray(), 1);

            Debug.Log("[Game] Sent zone ready confirmation");
        }

        private async Task HandleZoneInstanceCount(RRConnection conn)
        {
            var w = new LEWriter();
            w.WriteByte(13);
            w.WriteByte(5);
            w.WriteUInt32(1);

            LegacyWriters.WriteCompressedA(conn.Stream, (int)GetClientId24(conn.ConnId), 0x01, 0x0F, w.ToArray(), 1);

            Debug.Log("[Game] Sent zone instance count");
        }

        private async Task HandleType31(RRConnection conn, LEReader reader)
        {
            Debug.Log($"[Game] HandleType31: Processing for client {conn.ConnId}, remaining bytes: {reader.Remaining}");

            if (reader.Remaining < 4)
            {
                Debug.LogWarning($"[Game] HandleType31: Insufficient data - need at least 4 bytes, have {reader.Remaining}");
                return;
            }

            byte unknown1 = reader.ReadByte();
            byte messageType = reader.ReadByte();

            Debug.Log($"[Game] HandleType31: unknown1=0x{unknown1:X2}, messageType=0x{messageType:X2}");

            if (messageType == 0x31 && reader.Remaining >= 2)
            {
                byte subType = reader.ReadByte();
                byte flags = reader.ReadByte();

                Debug.Log($"[Game] HandleType31: Nested 0x31 - subType=0x{subType:X2}, flags=0x{flags:X2}");

                if (reader.Remaining >= 4)
                {
                    uint dataLength = reader.ReadUInt32();
                    Debug.Log($"[Game] HandleType31: dataLength={dataLength}");

                    if (reader.Remaining >= dataLength)
                    {
                        byte[] payload = reader.ReadBytes((int)dataLength);
                        Debug.Log($"[Game] HandleType31: Payload ({payload.Length} bytes): {BitConverter.ToString(payload)}");

                        if (payload.Length >= 2 && payload[0] == 0x78 && payload[1] == 0x9C)
                        {
                            Debug.Log($"[Game] HandleType31: Found zlib compressed data");

                            uint[] trySizes = { 64, 128, 256, 512, 1024, 2048 };

                            foreach (uint trySize in trySizes)
                            {
                                try
                                {
                                    byte[] decompressed = ZlibUtil.Inflate(payload, trySize);
                                    Debug.Log($"[Game] HandleType31: Successfully decompressed with size {trySize} ({decompressed.Length} bytes): {BitConverter.ToString(decompressed)}");

                                    await ProcessType31Data(conn, decompressed, subType);
                                    break;
                                }
                                catch (Exception ex)
                                {
                                    Debug.Log($"[Game] HandleType31: Decompression failed with size {trySize}: {ex.Message}");
                                }
                            }
                        }
                        else
                        {
                            Debug.Log($"[Game] HandleType31: Processing uncompressed payload");
                            await ProcessType31Data(conn, payload, subType);
                        }
                    }
                }
            }

            Debug.Log($"[Game] HandleType31: Sending acknowledgment");
            await SendType31Ack(conn);
        }

        private async Task ProcessType31Data(RRConnection conn, byte[] data, byte subType)
        {
            Debug.Log($"[Game] ProcessType31Data: Processing {data.Length} bytes with subType 0x{subType:X2} for client {conn.ConnId}");
            Debug.Log($"[Game] ProcessType31Data: Data: {BitConverter.ToString(data)}");

            if (data.Length >= 4)
            {
                var dataReader = new LEReader(data);
                try
                {
                    uint channelOrType = dataReader.ReadUInt32();
                    Debug.Log($"[Game] ProcessType31Data: Channel/Type: {channelOrType}");

                    if (channelOrType == 4)
                    {
                        Debug.Log($"[Game] ProcessType31Data: This appears to be a channel 4 (character) message");

                        if (dataReader.Remaining > 0)
                        {
                            byte[] remaining = dataReader.ReadBytes(dataReader.Remaining);
                            Debug.Log($"[Game] ProcessType31Data: Additional data: {BitConverter.ToString(remaining)}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Debug.Log($"[Game] ProcessType31Data: Error parsing data: {ex.Message}");
                }
            }
        }

        private async Task SendType31Ack(RRConnection conn)
        {
            Debug.Log($"[Game] SendType31Ack: Sending to client {conn.ConnId}");

            try
            {
                var response = new LEWriter();
                response.WriteByte(4);
                response.WriteByte(1);
                response.WriteUInt32(0);

                LegacyWriters.WriteCompressedA(conn.Stream, (int)GetClientId24(conn.ConnId), 0x01, 0x0F, response.ToArray(), 1);

                Debug.Log($"[Game] SendType31Ack: Sent channel 4 response via compressed A");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] SendType31Ack: Failed to send compressed A response: {ex.Message}");

                try
                {
                    var w = new LEWriter();
                    w.WriteByte(0x31);
                    w.WriteByte(0x00);
                    w.WriteUInt32(4);

                    byte[] ackData = w.ToArray();
                    await conn.Stream.WriteAsync(ackData, 0, ackData.Length);
                    Debug.Log($"[Game] SendType31Ack: Sent fallback response ({ackData.Length} bytes)");
                }
                catch (Exception fallbackEx)
                {
                    Debug.LogError($"[Game] SendType31Ack: Fallback also failed: {fallbackEx.Message}");
                }
            }
        }

        private async Task HandleCompressedE(RRConnection conn, LEReader reader)
        {
            Debug.Log($"[Game] HandleCompressedE: For client {conn.ConnId}");
        }

        private async Task HandleType06(RRConnection conn, LEReader reader)
        {
            Debug.Log($"[Game] HandleType06: For client {conn.ConnId}");
        }

        private async Task SendCompressedAResponse(RRConnection conn, byte dest, byte subType, byte[] innerData)
        {
            Debug.Log($"[Game] SendCompressedAResponse: *** ENTRY *** Sending to client {conn.ConnId} - dest=0x{dest:X2}, subType=0x{subType:X2}, dataLen={innerData.Length}");
            Debug.Log($"[Game] SendCompressedAResponse: *** INNER DATA *** {BitConverter.ToString(innerData)}");

            try
            {
                byte[] compressed = ZlibUtil.Deflate(innerData);
                Debug.Log($"[Game] SendCompressedAResponse: *** COMPRESSION *** Compressed from {innerData.Length} to {compressed.Length} bytes");
                Debug.Log($"[Game] SendCompressedAResponse: *** COMPRESSED DATA *** {BitConverter.ToString(compressed)}");

                uint clientId = GetClientId24(conn.ConnId);
                Debug.Log($"[Game] SendCompressedAResponse: *** CLIENT ID *** Using client ID 0x{clientId:X6} for connection {conn.ConnId}");

                var w = new LEWriter();
                w.WriteByte(0x0A);
                w.WriteUInt24((int)clientId);
                w.WriteUInt32((uint)(7 + compressed.Length));
                w.WriteByte(dest);
                w.WriteByte(subType);
                w.WriteByte(0x00);
                w.WriteUInt32((uint)innerData.Length);
                w.WriteBytes(compressed);

                byte[] payload = w.ToArray();
                Debug.Log($"[Game] SendCompressedAResponse: *** PAYLOAD *** Built payload ({payload.Length} bytes): {BitConverter.ToString(payload)}");
                Debug.Log($"[SEND][A][wire] peer=0x{clientId:X6} lenField={7 + compressed.Length} dest=0x{dest:X2} sub=0x{subType:X2} unclen={innerData.Length}");

                await conn.Stream.WriteAsync(payload, 0, payload.Length);
                Debug.Log($"[Game] SendCompressedAResponse: *** SUCCESS *** Sent {payload.Length} bytes to client {conn.ConnId}");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] SendCompressedAResponse: *** CRITICAL EXCEPTION *** {ex.Message}");
                Debug.LogError($"[Game] SendCompressedAResponse: *** STACK TRACE *** {ex.StackTrace}");
            }
        }

        private async Task SendCompressedAResponseWithDump(RRConnection conn, byte dest, byte subType, byte[] innerData, string tag)
        {
            try
            {
                DumpUtil.DumpBlob(tag, "unity.uncompressed.bin", innerData);
                DumpUtil.DumpCrc(tag, "uncompressed", innerData);

                var compressed = ZlibUtil.Deflate(innerData);
                DumpUtil.DumpBlob(tag, "unity.compressed.bin", compressed);
                DumpUtil.DumpCrc(tag, "compressed", compressed);
            }
            catch (Exception ex)
            {
                Debug.LogWarning($"[DUMP] Failed to write dumps for tag '{tag}': {ex.Message}");
            }

            await SendCompressedAResponse(conn, dest, subType, innerData);
        }

        private async Task<byte[]> SendMessage0x10(RRConnection conn, byte channel, byte[] body)
        {
            Debug.Log($"[Game] SendMessage0x10: Sending to client {conn.ConnId} - channel=0x{channel:X2}, bodyLen={body?.Length ?? 0}");
            if (body != null)
                Debug.Log($"[Game] SendMessage0x10: Body data: {BitConverter.ToString(body)}");

            uint clientId = GetClientId24(conn.ConnId);
            uint bodyLen = (uint)(body?.Length ?? 0);

            Debug.Log($"[Game] SendMessage0x10: Using client ID 0x{clientId:X6}, bodyLen={bodyLen}");

            var w = new LEWriter();
            w.WriteByte(0x10);
            w.WriteUInt24((int)clientId);
            w.WriteUInt24((int)bodyLen);
            w.WriteByte(channel);
            if (bodyLen > 0)
                w.WriteBytes(body);

            byte[] payload = w.ToArray();
            Debug.Log($"[Game] SendMessage0x10: Built payload ({payload.Length} bytes): {BitConverter.ToString(payload)}");

            await conn.Stream.WriteAsync(payload, 0, payload.Length);
            Debug.Log($"[Game] SendMessage0x10: Sent {payload.Length} bytes to client {conn.ConnId}");
            return payload;
        }

        private uint GetClientId24(int connId) => _peerId24.TryGetValue(connId, out var id) ? id : 0u;

        public void Stop()
        {
            lock (_gameLoopLock)
            {
                _gameLoopRunning = false;
            }
            Debug.Log("[Game] Server stopping...");
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
}
