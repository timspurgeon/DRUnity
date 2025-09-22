
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Server.Net {
  public class NetServer {
    readonly IPEndPoint ep;
    public delegate Task ClientHandler(TcpClient c);
    readonly ClientHandler handler;
    public NetServer(string ip, int port, ClientHandler h) {
      ep = new IPEndPoint(IPAddress.Parse(ip), port); handler = h;
    }
    public async Task RunAsync() {
      var listener = new TcpListener(ep); listener.Start();
      while (true) {
        var c = await listener.AcceptTcpClientAsync();
        _ = Task.Run(() => handler(c));
      }
    }
  }
}
