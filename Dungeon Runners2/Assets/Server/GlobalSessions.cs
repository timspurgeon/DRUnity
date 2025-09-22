using System.Collections.Concurrent;

namespace Server.Common
{
    // Simple global token -> username map.
    public static class GlobalSessions
    {
        private static readonly ConcurrentDictionary<uint, string> _map = new();

        public static void Set(uint token, string username)
            => _map[token] = username ?? "guest";

        // Non-consuming lookup (useful for diagnostics)
        public static bool TryGet(uint token, out string username)
            => _map.TryGetValue(token, out username);

        // One-time use
        public static bool TryConsume(uint token, out string username)
            => _map.TryRemove(token, out username);

        public static string Get(uint token)
        {
            _map.TryGetValue(token, out var user);
            return user;
        }

        public static bool Remove(uint token) => _map.TryRemove(token, out _);
    }
}
