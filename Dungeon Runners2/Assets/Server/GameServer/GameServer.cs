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
        private readonly ConcurrentDictionary<int, List<GCObject>> _playerCharacters = new();

        // Add these fields for persistent character creation
        private readonly ConcurrentDictionary<string, List<GCObject>> _persistentCharacters = new();
        //  private readonly ConcurrentDictionary<string, bool> _characterCreationPending = new();

        private bool _gameLoopRunning = false;
        private readonly object _gameLoopLock = new object();

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

                    // Create a new array with just the received data
                    byte[] receivedData = new byte[bytesRead];
                    Buffer.BlockCopy(buffer, 0, receivedData, 0, bytesRead);

                    // Process all complete packets in this data
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
                // Just pass the raw data to ReadPacket like before, but handle errors gracefully
                await ReadPacket(conn, data);
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] ProcessReceivedData: Error processing data for client {conn.ConnId}: {ex.Message}");

                // If we're authenticated, try to send a keep-alive to prevent timeout
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

            // Send a simple empty message to keep the connection alive
            var keepAlive = new LEWriter();
            keepAlive.WriteByte(0); // Empty payload

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

                    // Also add a case for 0x31 to see what it contains
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

            if (reader.Remaining < 14)
            {
                Debug.LogError($"[Game] HandleCompressedA: Insufficient data - need 14 bytes, have {reader.Remaining}");
                return;
            }

            uint clientId = reader.ReadUInt24();
            Debug.Log($"[Game] CRITICAL DEBUG: Client sent ID: 0x{clientId:X6}");
            uint packetLen = reader.ReadUInt32();
            byte dest = reader.ReadByte();
            byte msgTypeA = reader.ReadByte();
            byte zero = reader.ReadByte();
            uint unclen = reader.ReadUInt32();

            Debug.Log($"[Game] HandleCompressedA: clientId=0x{clientId:X6}, packetLen={packetLen}, dest=0x{dest:X2}, msgTypeA=0x{msgTypeA:X2}, zero=0x{zero:X2}, unclen={unclen}");

            _peerId24[conn.ConnId] = clientId;
            Debug.Log($"[Game] HandleCompressedA: Stored client ID 0x{clientId:X6} for connection {conn.ConnId}");

            int compLen = (int)packetLen - 7;
            Debug.Log($"[Game] HandleCompressedA: Calculated compressed length: {compLen}");

            if (compLen < 0 || reader.Remaining < compLen)
            {
                Debug.LogError($"[Game] HandleCompressedA: Invalid compressed length {compLen}, remaining data: {reader.Remaining}");
                return;
            }

            byte[] compressed = reader.ReadBytes(compLen);
            Debug.Log($"[Game] HandleCompressedA: Read {compressed.Length} compressed bytes");
            Debug.Log($"[Game] HandleCompressedA: Compressed data: {BitConverter.ToString(compressed)}");

            byte[] uncompressed;
            try
            {
                uncompressed = ZlibUtil.Inflate(compressed, unclen);
                Debug.Log($"[Game] HandleCompressedA: Decompressed to {uncompressed.Length} bytes (expected {unclen})");
                Debug.Log($"[Game] HandleCompressedA: Uncompressed data: {BitConverter.ToString(uncompressed)}");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] HandleCompressedA: Decompression failed: {ex.Message}");
                return;
            }

            Debug.Log($"[Game] HandleCompressedA: Processing A message - dest=0x{dest:X2} sub=0x{msgTypeA:X2}");

            if (msgTypeA != 0x00 && string.IsNullOrEmpty(conn.LoginName))
            {
                Debug.LogError($"[Game] HandleCompressedA: Received msgTypeA 0x{msgTypeA:X2} before login for client {conn.ConnId}");
                return;
            }

            switch (msgTypeA)
            {
                case 0x00:
                    Debug.Log($"[Game] HandleCompressedA: Processing initial login (0x00) for client {conn.ConnId}");
                    await HandleInitialLogin(conn, uncompressed);
                    break;
                case 0x02:
                    Debug.Log($"[Game] HandleCompressedA: Processing secondary message (0x02) for client {conn.ConnId}");
                    Debug.Log($"[Game] HandleCompressedA: Sending empty 0x02 response");
                    await SendCompressedAResponse(conn, 0x00, 0x02, Array.Empty<byte>());
                    break;
                case 0x0F:
                    Debug.Log($"[Game] HandleCompressedA: Processing channel messages (0x0F) for client {conn.ConnId}");
                    await HandleChannelMessage(conn, uncompressed);
                    break;
                default:
                    Debug.LogWarning($"[Game] HandleCompressedA: Unhandled msgTypeA 0x{msgTypeA:X2} for client {conn.ConnId}");
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
                byte[] ackMessage = await SendMessage0x10(conn, 0x0A, ack.ToArray());
                Debug.Log($"[Game] HandleInitialLogin: *** STEP 1 COMPLETE *** Sent 0x10 ack ({ackMessage.Length} bytes): {BitConverter.ToString(ackMessage)}");

                Debug.Log($"[Game] HandleInitialLogin: *** STEP 2 *** Sending A/0x03 advance message");
                var advance = new LEWriter();
                advance.WriteUInt24(0x00B2B3B4);
                advance.WriteByte(0x00);
                byte[] advanceData = advance.ToArray();
                Debug.Log($"[Game] HandleInitialLogin: *** STEP 2 DATA *** Advance data ({advanceData.Length} bytes): {BitConverter.ToString(advanceData)}");
                await SendCompressedAResponse(conn, 0x00, 0x03, advanceData);
                Debug.Log($"[Game] HandleInitialLogin: *** STEP 2 COMPLETE *** Sent advance message");

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

        /*  private async Task HandleCharacterCreationReconnect(RRConnection conn)
          {
              Debug.Log($"[Game] HandleCharacterCreationReconnect: Checking for pending creation for {conn.LoginName}");

              if (_characterCreationPending.TryGetValue(conn.LoginName, out var pending) && pending)
              {
                  Debug.Log($"[Game] HandleCharacterCreationReconnect: Creating character for {conn.LoginName}");

                  // Create the character (simulating what the Go server does)
                  var newCharacter = Objects.NewPlayer($"{conn.LoginName}_Hero");
                  newCharacter.ID = (uint)(conn.ConnId * 100);

                  var characterList = new List<GCObject> { newCharacter };
                  _persistentCharacters[conn.LoginName] = characterList;
                  _characterCreationPending[conn.LoginName] = false;

                  Debug.Log($"[Game] HandleCharacterCreationReconnect: Created character {newCharacter.Name} (ID: {newCharacter.ID})");

                  // Send character connected first
                  await SendCharacterConnectedResponse(conn);
                  await Task.Delay(50);

                  // Now send the character list with the new character
                  await SendExistingCharacterList(conn, characterList);

                  // Continue with group flow
                  await Task.Delay(50);
                  await SendGroupConnectedResponse(conn);
              }
          }*/

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

            Debug.Log($"[Game] HandleChannelMessage: Channel {channel}, Type 0x{messageType:X2} for client {conn.ConnId}");

            switch (channel)
            {
                case 4:
                    Debug.Log($"[Game] HandleChannelMessage: Routing to character handler");
                    await HandleCharacterChannelMessages(conn, messageType, data);
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
                await SendCharacterConnectedResponse(conn);
                Debug.Log($"[Game] StartCharacterFlow: *** STEP 1 COMPLETE *** Character connected response sent");

                Debug.Log($"[Game] StartCharacterFlow: *** STEP 2 *** Sending character list immediately");
                await SendCharacterList(conn);
                Debug.Log($"[Game] StartCharacterFlow: *** STEP 2 COMPLETE *** Character list sent");

                await Task.Delay(25);
                Debug.Log($"[Game] StartCharacterFlow: *** STEP 3 *** Sending group connected response");
                await SendGroupConnectedResponse(conn);
                Debug.Log($"[Game] StartCharacterFlow: *** STEP 3 COMPLETE *** Group connected response sent");

                Debug.Log($"[Game] StartCharacterFlow: *** COMPLETE *** Character flow finished for client {conn.ConnId}");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] StartCharacterFlow: *** CRITICAL EXCEPTION *** {ex.Message}");
                Debug.LogError($"[Game] StartCharacterFlow: *** STACK TRACE *** {ex.StackTrace}");
            }
        }

        private async Task HandleCharacterChannelMessages(RRConnection conn, byte messageType, byte[] data)
        {
            Debug.Log($"[Game] HandleCharacterChannelMessages: Type 0x{messageType:X2} for client {conn.ConnId}");

            switch (messageType)
            {
                case 0:
                    Debug.Log($"[Game] HandleCharacterChannelMessages: Character connected");
                    await SendCharacterConnectedResponse(conn);
                    break;
                case 3:
                    Debug.Log($"[Game] HandleCharacterChannelMessages: Get character list");
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
        }

        private async Task SendCharacterConnectedResponse(RRConnection conn)
        {
            Debug.Log($"[Game] SendCharacterConnectedResponse: *** ENTRY *** For client {conn.ConnId} - creating 2 characters like Go server");

            try
            {
                if (!_persistentCharacters.ContainsKey(conn.LoginName))
                {
                    Debug.Log($"[Game] SendCharacterConnectedResponse: *** CREATING CHARACTERS *** No existing characters for {conn.LoginName}");
                    var characters = new List<GCObject>();
                    for (int i = 0; i < 2; i++)
                    {
                        Debug.Log($"[Game] SendCharacterConnectedResponse: *** CREATING CHARACTER {i + 1} *** Calling Objects.NewPlayer");

                        try
                        {
                            var character = Objects.NewPlayer($"{conn.LoginName}");
                            character.ID = (uint)(Objects.NewID());
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
                await SendCompressedAResponse(conn, 0x01, 0x0F, w.ToArray());
                Debug.Log($"[Game] SendCharacterConnectedResponse: *** SUCCESS *** Sent character connected message");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] SendCharacterConnectedResponse: *** CRITICAL EXCEPTION *** {ex.Message}");
                Debug.LogError($"[Game] SendCharacterConnectedResponse: *** STACK TRACE *** {ex.StackTrace}");
            }
        }

        private void WriteGoSendPlayer(LEWriter body, GCObject character)
        {
            Debug.Log($"[Game] WriteGoSendPlayer: *** ENTRY *** Writing Go sendPlayer format for character ID {character.ID}");

            try
            {
                Debug.Log($"[Game] WriteGoSendPlayer: *** STEP 1 *** Calling Objects.LoadAvatar()");
                var avatar = Objects.LoadAvatar();
                Debug.Log($"[Game] WriteGoSendPlayer: *** STEP 1 SUCCESS *** Avatar created - ID: {avatar.ID}, Type: {avatar.GCType}, Children: {avatar.Children.Count}");

                Debug.Log($"[Game] WriteGoSendPlayer: *** STEP 2 *** Writing main character object (without avatar as child)");
                var beforeCharacter = body.ToArray().Length;
                character.WriteFullGCObject(body);
                var afterCharacter = body.ToArray().Length;
                Debug.Log($"[Game] WriteGoSendPlayer: *** STEP 2 SUCCESS *** Wrote character, bytes added: {afterCharacter - beforeCharacter}");

                Debug.Log($"[Game] WriteGoSendPlayer: *** STEP 3 *** Writing avatar separately like Go server does");
                var beforeAvatar = body.ToArray().Length;
                avatar.WriteFullGCObject(body);
                var afterAvatar = body.ToArray().Length;
                Debug.Log($"[Game] WriteGoSendPlayer: *** STEP 3 SUCCESS *** Wrote avatar, bytes added: {afterAvatar - beforeAvatar}");

                Debug.Log($"[Game] WriteGoSendPlayer: *** STEP 4 *** Writing additional data that Go server adds");
                body.WriteByte(0x01);
                body.WriteByte(0x01);

                var normalBytes = Encoding.UTF8.GetBytes("Normal");
                body.WriteBytes(normalBytes);
                body.WriteByte(0x00);

                body.WriteByte(0x01);
                body.WriteByte(0x01);
                body.WriteUInt32(0x01);

                var finalSize = body.ToArray().Length;
                Debug.Log($"[Game] WriteGoSendPlayer: *** SUCCESS *** Completed Go format, total size: {finalSize} bytes");
                Debug.Log($"[Game] WriteGoSendPlayer: *** SUCCESS *** Character children: {character.Children.Count}, Avatar children: {avatar.Children.Count}");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] WriteGoSendPlayer: *** CRITICAL EXCEPTION *** {ex.Message}");
                Debug.LogError($"[Game] WriteGoSendPlayer: *** STACK TRACE *** {ex.StackTrace}");
            }
        }

        private async Task SendCharacterList(RRConnection conn)
        {
            Debug.Log($"[Game] SendCharacterList: *** ENTRY *** Matching Go server exactly");

            try
            {
                if (!_persistentCharacters.TryGetValue(conn.LoginName, out var characters))
                {
                    Debug.LogError($"[Game] SendCharacterList: *** ERROR *** No characters found for {conn.LoginName}");
                    return;
                }

                Debug.Log($"[Game] SendCharacterList: *** FOUND CHARACTERS *** Count: {characters.Count} for {conn.LoginName}");

                var body = new LEWriter();
                body.WriteByte(4);  // messages.CharacterChannel
                body.WriteByte(3);  // CharacterGetList 
                body.WriteByte((byte)characters.Count); // count

                Debug.Log($"[Game] SendCharacterList: *** WRITING CHARACTERS *** Processing {characters.Count} characters");

                for (int i = 0; i < characters.Count; i++)
                {
                    var character = characters[i];
                    Debug.Log($"[Game] SendCharacterList: *** CHARACTER {i + 1} *** ID: {character.ID}, Writing character data");

                    try
                    {
                        body.WriteUInt32(character.ID); // character.EntityProperties.ID
                        Debug.Log($"[Game] SendCharacterList: *** CHARACTER {i + 1} *** Wrote ID, calling WriteGoSendPlayer");
                        WriteGoSendPlayer(body, character); // sendPlayer(character, conn.Client, body)
                        Debug.Log($"[Game] SendCharacterList: *** CHARACTER {i + 1} *** WriteGoSendPlayer complete");
                    }
                    catch (Exception charEx)
                    {
                        Debug.LogError($"[Game] SendCharacterList: *** ERROR CHARACTER {i + 1} *** {charEx.Message}");
                        Debug.LogError($"[Game] SendCharacterList: *** CHARACTER {i + 1} STACK TRACE *** {charEx.StackTrace}");
                    }
                }

                Debug.Log($"[Game] SendCharacterList: *** SENDING MESSAGE *** Total body length: {body.ToArray().Length} bytes");
                await SendCompressedAResponse(conn, 0x01, 0x0F, body.ToArray());
                Debug.Log($"[Game] SendCharacterList: *** SUCCESS *** Sent Go format with {characters.Count} characters");
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

            // Send character creation initiation message
            var createMessage = new LEWriter();
            createMessage.WriteByte(4);  // Character channel
            createMessage.WriteByte(2);  // Character create message
            createMessage.WriteByte(0);  // Initiate creation mode

            await SendCompressedAResponse(conn, 0x01, 0x0F, createMessage.ToArray());
            Debug.Log($"[Game] SendToCharacterCreation: Sent character creation initiation");
        }
        private async Task HandleCharacterPlay(RRConnection conn, byte[] data)
        {
            Debug.Log($"[Game] HandleCharacterPlay: For client {conn.ConnId}");
            Debug.Log($"[Game] HandleCharacterPlay: Data length: {data.Length}");
            Debug.Log($"[Game] HandleCharacterPlay: Data: {BitConverter.ToString(data)}");

            if (data.Length >= 6) // Should have character selection data
            {
                var reader = new LEReader(data);
                reader.ReadByte(); // Skip channel (4)
                reader.ReadByte(); // Skip message type (5)

                if (reader.Remaining >= 4)
                {
                    uint selectedCharId = reader.ReadUInt32();
                    Debug.Log($"[Game] HandleCharacterPlay: Selected character ID: {selectedCharId}");

                    // Send character play success response with character data
                    var response = new LEWriter();
                    response.WriteByte(4);  // Channel 4
                    response.WriteByte(5);  // Character play response
                    response.WriteByte(1);  // Success
                    response.WriteUInt32(selectedCharId); // Echo character ID

                    // Add the character object data
                    //  WritePlayerWithGCObject(response, $"{conn.LoginName}_Hero");

                    await SendCompressedAResponse(conn, 0x01, 0x0F, response.ToArray());
                    Debug.Log($"[Game] HandleCharacterPlay: Sent character play success with character data");

                    // After successful character play, initiate world entry
                    // After successful character play, send to character creation
                    await Task.Delay(100);
                    await SendToCharacterCreation(conn);
                    return;
                }
            }

            // Fallback - send simple success
            var fallback = new LEWriter();
            fallback.WriteByte(4);
            fallback.WriteByte(5);
            fallback.WriteByte(1); // Success
            await SendCompressedAResponse(conn, 0x01, 0x0F, fallback.ToArray());
            Debug.Log("[Game] HandleCharacterPlay: Sent fallback response");
        }

        private async Task InitiateWorldEntry(RRConnection conn)
        {
            Debug.Log($"[Game] InitiateWorldEntry: Starting world entry for client {conn.ConnId}");

            // First send the go-to-zone message
            await SendGoToZone(conn, "town");

            // DON'T send zone ready immediately - wait for client to request zone join
            // The client should send a zone join request after receiving go-to-zone
            Debug.Log($"[Game] InitiateWorldEntry: Sent zone change, waiting for client zone join request");
        }

        private async Task HandleCharacterCreate(RRConnection conn, byte[] data)
        {
            Debug.Log($"[Game] HandleCharacterCreate: Character creation request from client {conn.ConnId}");
            Debug.Log($"[Game] HandleCharacterCreate: Data ({data.Length} bytes): {BitConverter.ToString(data)}");

            // Parse character creation data if needed
            string characterName = $"{conn.LoginName}_NewHero";
            uint newCharId = (uint)(conn.ConnId * 100 + 1);

            var response = new LEWriter();
            response.WriteByte(4);  // Channel 4
            response.WriteByte(2);  // Character create response
            response.WriteByte(1);  // Success
            response.WriteUInt32(newCharId);

            // Write the new character object
            //  WritePlayerWithGCObject(response, characterName);

            await SendCompressedAResponse(conn, 0x01, 0x0F, response.ToArray());
            Debug.Log($"[Game] HandleCharacterCreate: Sent character creation success for {characterName} (ID: {newCharId})");

            // After creation, send updated character list
            await Task.Delay(100);
            await SendUpdatedCharacterList(conn, newCharId, characterName);
        }

        private async Task SendUpdatedCharacterList(RRConnection conn, uint charId, string charName)
        {
            Debug.Log($"[Game] SendUpdatedCharacterList: Sending list with newly created character");

            var w = new LEWriter();
            w.WriteByte(4);   // Channel 4  
            w.WriteByte(3);   // Character list message
            w.WriteByte(1);   // 1 character now

            w.WriteUInt32(charId);
            //WritePlayerWithGCObject(w, charName);

            await SendCompressedAResponse(conn, 0x01, 0x0F, w.ToArray());
            Debug.Log($"[Game] SendUpdatedCharacterList: Sent updated character list with new character");
        }

        private async Task SendGroupConnectedResponse(RRConnection conn)
        {
            Debug.Log($"[Game] SendGroupConnectedResponse: For client {conn.ConnId}");
            var w = new LEWriter();
            w.WriteByte(9);
            w.WriteByte(0);
            await SendCompressedAResponse(conn, 0x01, 0x0F, w.ToArray());
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

            await SendCompressedAResponse(conn, 0x01, 0x0F, goToZoneData);
            Debug.Log($"[Game] SendGoToZone: Sent go-to-zone '{zoneName}' to client {conn.ConnId}");
        }

        private async Task HandleZoneChannelMessages(RRConnection conn, byte messageType, byte[] data)
        {
            Debug.Log($"[Game] HandleZoneChannelMessages: Type 0x{messageType:X2} for client {conn.ConnId}");

            switch (messageType)
            {
                case 6:
                    Debug.Log($"[Game] HandleZoneChannelMessages: Zone join request");
                    await HandleZoneJoin(conn);
                    break;
                case 8:
                    Debug.Log($"[Game] HandleZoneChannelMessages: Zone ready");
                    await HandleZoneReady(conn);
                    break;
                case 0:
                    Debug.Log($"[Game] HandleZoneChannelMessages: Zone connected");
                    await HandleZoneConnected(conn);
                    break;
                case 1:
                    Debug.Log($"[Game] HandleZoneChannelMessages: Zone ready response");
                    await HandleZoneReadyResponse(conn);
                    break;
                case 5:
                    Debug.Log($"[Game] HandleZoneChannelMessages: Zone instance count");
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
            w.WriteByte(13);  // Zone channel
            w.WriteByte(1);   // Zone ready message
            w.WriteUInt32(1); // Zone ID

            // Send minimap data
            w.WriteUInt16(0x12);
            for (int i = 0; i < 0x12; i++)
            {
                w.WriteUInt32(0xFFFFFFFF);
            }

            await SendCompressedAResponse(conn, 0x01, 0x0F, w.ToArray());
            Debug.Log($"[Game] HandleZoneJoin: Sent zone join response");

            // Maybe the client expects additional zone state data?
            // Let's see if it stays connected longer without sending anything else
            Debug.Log($"[Game] HandleZoneJoin: Waiting to see if client expects more data...");
        }

        private async Task HandleZoneConnected(RRConnection conn)
        {
            var w = new LEWriter();
            w.WriteByte(13);
            w.WriteByte(0);
            await SendCompressedAResponse(conn, 0x01, 0x0F, w.ToArray());
            Debug.Log("[Game] Sent zone connected response");
        }

        private async Task HandleZoneReady(RRConnection conn)
        {
            var w = new LEWriter();
            w.WriteByte(13);
            w.WriteByte(8);
            await SendCompressedAResponse(conn, 0x01, 0x0F, w.ToArray());
            Debug.Log("[Game] Sent zone ready response");
        }

        private async Task HandleZoneReadyResponse(RRConnection conn)
        {
            var w = new LEWriter();
            w.WriteByte(13);
            w.WriteByte(1);
            await SendCompressedAResponse(conn, 0x01, 0x0F, w.ToArray());
            Debug.Log("[Game] Sent zone ready confirmation");
        }

        private async Task HandleZoneInstanceCount(RRConnection conn)
        {
            var w = new LEWriter();
            w.WriteByte(13);
            w.WriteByte(5);
            w.WriteUInt32(1);
            await SendCompressedAResponse(conn, 0x01, 0x0F, w.ToArray());
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

            // Try to parse as a potential message format
            byte unknown1 = reader.ReadByte();
            byte messageType = reader.ReadByte();

            Debug.Log($"[Game] HandleType31: unknown1=0x{unknown1:X2}, messageType=0x{messageType:X2}");

            if (messageType == 0x31 && reader.Remaining >= 2)
            {
                // This might be a nested 0x31 message
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

                        // Check if payload starts with zlib header (0x78 0x9C)
                        if (payload.Length >= 2 && payload[0] == 0x78 && payload[1] == 0x9C)
                        {
                            Debug.Log($"[Game] HandleType31: Found zlib compressed data");

                            // Try different uncompressed sizes
                            uint[] trySizes = { 64, 128, 256, 512, 1024, 2048 };

                            foreach (uint trySize in trySizes)
                            {
                                try
                                {
                                    byte[] decompressed = ZlibUtil.Inflate(payload, trySize);
                                    Debug.Log($"[Game] HandleType31: Successfully decompressed with size {trySize} ({decompressed.Length} bytes): {BitConverter.ToString(decompressed)}");

                                    // Process the decompressed data
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

            // Send acknowledgment
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
                        // This might be a character update, movement, or status message

                        if (dataReader.Remaining > 0)
                        {
                            // There might be additional data
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
                // Since the client is sending channel 4 data, respond with a proper channel 4 message
                // This should be sent as a compressed A response like other channel messages
                var response = new LEWriter();
                response.WriteByte(4);  // Channel 4 (character channel)
                response.WriteByte(1);  // Response type (could be status update)
                response.WriteUInt32(0); // No additional data for now

                await SendCompressedAResponse(conn, 0x01, 0x0F, response.ToArray());
                Debug.Log($"[Game] SendType31Ack: Sent channel 4 response via compressed A");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] SendType31Ack: Failed to send compressed A response: {ex.Message}");

                // Fallback to direct response
                try
                {
                    var w = new LEWriter();
                    w.WriteByte(0x31);
                    w.WriteByte(0x00);
                    w.WriteUInt32(4); // Echo back the channel number

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

                await conn.Stream.WriteAsync(payload, 0, payload.Length);
                Debug.Log($"[Game] SendCompressedAResponse: *** SUCCESS *** Sent {payload.Length} bytes to client {conn.ConnId}");
            }
            catch (Exception ex)
            {
                Debug.LogError($"[Game] SendCompressedAResponse: *** CRITICAL EXCEPTION *** {ex.Message}");
                Debug.LogError($"[Game] SendCompressedAResponse: *** STACK TRACE *** {ex.StackTrace}");
            }
        }

        private async Task<byte[]> SendMessage0x10(RRConnection conn, byte channel, byte[] body)
        {
            Debug.Log($"[Game] SendMessage0x10: Sending to client {conn.ConnId} - channel=0x{channel:X2}, bodyLen={body?.Length ?? 0}");
            if (body != null)
                Debug.Log($"[Game] SendMessage0x10: Body data: {BitConverter.ToString(body)}");

            uint clientId = GetClientId24(conn.ConnId);
            uint bodyLen = (uint)(body?.Length ?? 0);
            Debug.Log($"[Game] SendMessage0x10: Using client ID 0x{clientId:X6}, total bodyLen={bodyLen}");

            var w = new LEWriter();
            w.WriteByte(0x10);
            w.WriteUInt24((int)clientId);
            w.WriteUInt24((int)bodyLen);
            w.WriteByte(channel);
            if (body != null && body.Length > 0) w.WriteBytes(body);

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
