namespace Server.Game
{
    // Simple channel constants to replace the missing ones
    public static class ChannelConstants
    {
        public const byte CharacterChannel = 4;
        public const byte GroupChannel = 9;
        public const byte ZoneChannel = 13;
    }

    public enum CharacterMessage : byte
    {
        CharacterConnected = 0,
        CharacterDisconnected = 1,
        CharacterCreate = 2,
        CharacterGetList = 3,
        CharacterDelete = 4,
        CharacterPlay = 5
    }

    public enum ZoneMessage : byte
    {
        ZoneMessageConnected = 0,
        ZoneMessageReady = 1,
        ZoneMessageDisconnected = 2,
        ZoneMessageInstanceCount = 5,
        ZoneMessageJoin = 6,
        ZoneMessageReadyResponse = 8
    }

    public enum GroupMessage : byte
    {
        GroupConnected = 0,
        GroupDisconnected = 1,
        GroupJoin = 2,
        GroupLeave = 3,
        GroupGoToZone = 48
    }
}