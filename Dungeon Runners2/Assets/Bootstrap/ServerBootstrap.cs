using UnityEngine;
using System.Threading.Tasks;
using Server.Auth;
using Server.Game;
using Server.Common;
using Server.Store;

public class ServerBootstrap : MonoBehaviour
{
    async void Start()
    {
        Application.targetFrameRate = 60;
        var cfg = ServerConfig.Load("serverconfig.json");
        IStore store = new MemoryStore(); // TODO: swap to SqliteStore later

        // AUTH
        var auth = new AuthServer(cfg.BindIP, cfg.AuthPort, store);
        Debug.Log($"[Boot] Starting Auth on {cfg.BindIP}:{cfg.AuthPort}");

        // GAME
        var game = new GameServer(cfg.BindIP, cfg.GamePort); // GamePort must exist in serverconfig.json
        Debug.Log($"[Boot] Starting Game on {cfg.BindIP}:{cfg.GamePort}");

        await Task.WhenAll(
            auth.RunAsync(),
            game.RunAsync()
        );
    }
}
