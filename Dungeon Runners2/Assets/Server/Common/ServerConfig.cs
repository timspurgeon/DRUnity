using System;
using System.IO;
using UnityEngine; // JsonUtility

namespace Server.Common
{
    [Serializable]
    public class ServerConfig
    {
        // Use public fields (JsonUtility requires fields, not properties)
        public string BindIP = "0.0.0.0";
        public int AuthPort = 2110;
        public int GamePort = 2603;
        public string SqlitePath = "server.sqlite";
        public bool UseSqlite = false;

        public static ServerConfig Load(string path)
        {
            try
            {
                if (!File.Exists(path)) return new ServerConfig();
                var json = File.ReadAllText(path);
                var cfg = JsonUtility.FromJson<ServerConfig>(json);
                return cfg ?? new ServerConfig();
            }
            catch
            {
                // On any parse error, fall back to defaults
                return new ServerConfig();
            }
        }
    }
}

